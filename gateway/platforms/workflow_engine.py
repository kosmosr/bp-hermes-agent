"""WorkflowEngine — Deterministic DAG execution for task templates.

Bypasses AI Manager reasoning for task ordering — desktop.py directly
orchestrates child agents via delegate_task according to the workflow
definition's dependency graph.

Execution model:
- Topological sort of steps by depends_on
- Independent steps (no mutual dependencies) run in parallel (up to 3)
- Dependent steps wait for all dependencies to complete
- Results from completed steps are injected into dependent steps via context
- Each step emits delegation.started/progress/completed envelopes

Concurrency model (D31):
- Each step creates its own parent agent via _create_agent_for_turn.
  This isolates callback + interrupt state across concurrent steps.
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)


class WorkflowEngine:

    def __init__(self, adapter, session_id: str, turn_id: str, loop, team_id: str = None):
        self._adapter = adapter
        self._session_id = session_id
        self._turn_id = turn_id
        self._loop = loop
        self._team_id = team_id
        self._results: Dict[str, str] = {}       # step_id → output summary
        self._step_status: Dict[str, str] = {}   # step_id → pending|running|completed|error|skipped
        self._failed: set = set()                 # step_ids that errored or skipped
        self._step_agents: Dict[str, Any] = {}   # step_id → agent (for per-step interrupt)
        self._interrupted = False
        self._usage_totals = {"input_tokens": 0, "output_tokens": 0}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def execute(self, workflow: dict, overall_context: str) -> Dict[str, str]:
        """Execute a workflow DAG. Returns {step_id: output_summary}.

        Raises ValueError on invalid workflow (cycles, unknown deps).
        Raises RuntimeError on deadlock or interrupt.
        """
        steps = {s["id"]: s for s in workflow["steps"]}

        # Validate: cycle detection
        if self._has_cycle(steps):
            raise ValueError("Circular dependency in workflow definition")

        # Validate: all depends_on and inject_context reference valid step IDs
        all_ids = set(steps.keys())
        for s in steps.values():
            for dep in s.get("depends_on", []):
                if dep not in all_ids:
                    raise ValueError(f"Step '{s['id']}' depends on unknown step '{dep}'")
            for ctx in s.get("inject_context", []):
                if ctx not in s.get("depends_on", []):
                    raise ValueError(
                        f"Step '{s['id']}': inject_context '{ctx}' must also be in depends_on"
                    )

        # Initialize all steps as pending
        for sid in steps:
            self._step_status[sid] = "pending"

        completed = set()   # successfully completed steps
        processed = set()   # completed + failed + skipped

        while len(processed) < len(steps):
            if self._interrupted:
                break

            # BFS skip propagation: transitively skip dependents of failed steps
            skip_queue = [
                sid for sid, s in steps.items()
                if sid not in processed
                and any(d in self._failed for d in s.get("depends_on", []))
            ]
            while skip_queue:
                sid = skip_queue.pop(0)
                if sid in processed:
                    continue
                self._step_status[sid] = "skipped"
                self._failed.add(sid)
                processed.add(sid)
                await self._adapter._broadcast_to_session(self._session_id, {
                    "kind": "delegation.completed",
                    "turn_id": self._turn_id,
                    "delegation_id": f"{self._turn_id}:{sid}",
                    "source": "workflow",
                    "error": True,
                    "output_preview": "Skipped: dependency failed",
                })
                # Enqueue transitive dependents
                for other_sid, other_s in steps.items():
                    if other_sid not in processed and sid in other_s.get("depends_on", []):
                        skip_queue.append(other_sid)

            # Find ready steps: all dependencies successfully completed
            ready = [
                s for sid, s in steps.items()
                if sid not in processed
                and all(d in completed for d in s.get("depends_on", []))
            ]

            if not ready and len(processed) < len(steps):
                remaining = [sid for sid in steps if sid not in processed]
                raise RuntimeError(f"Deadlock: steps {remaining} have unresolvable dependencies")

            if not ready:
                break

            # Execute ready steps in parallel (up to 3 concurrent)
            batch = ready[:3]
            results = await asyncio.gather(*[
                self._execute_step(step, overall_context)
                for step in batch
            ], return_exceptions=True)

            for step, result in zip(batch, results):
                sid = step["id"]
                if isinstance(result, Exception):
                    self._step_status[sid] = "error"
                    self._results[sid] = f"Error: {result}"
                    self._failed.add(sid)
                    processed.add(sid)
                    await self._adapter._broadcast_to_session(self._session_id, {
                        "kind": "delegation.completed",
                        "turn_id": self._turn_id,
                        "delegation_id": f"{self._turn_id}:{sid}",
                        "source": "workflow",
                        "error": True,
                        "output_preview": str(result)[:200],
                    })
                else:
                    completed.add(sid)
                    processed.add(sid)

        return dict(self._results)

    def cancel(self):
        """Signal the engine to stop after current batch completes.

        Interrupts all running step agents individually (D31).
        """
        self._interrupted = True
        for step_id, agent in self._step_agents.items():
            if self._step_status.get(step_id) == "running":
                try:
                    agent.interrupt("workflow cancelled by user")
                except Exception as e:
                    logger.debug("Failed to interrupt step %s: %s", step_id, e)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _execute_step(self, step: dict, overall_context: str) -> str:
        """Execute a single workflow step via delegate_task.

        Returns the output summary string on success.
        Raises on error or interrupt.
        """
        step_id = step["id"]
        delegation_id = f"{self._turn_id}:{step_id}"

        if self._interrupted:
            raise RuntimeError("Workflow cancelled by user")

        self._step_status[step_id] = "running"

        # Create per-step agent (D31) — isolates callback + interrupt state
        step_agent = self._adapter._create_agent_for_turn(
            session_id=self._session_id,
            model_override=step.get("model"),
        )
        self._step_agents[step_id] = step_agent

        # Build context: overall task + dependency outputs
        context_parts = [f"Overall task: {overall_context}"]
        for dep_id in step.get("inject_context", []):
            if dep_id in self._results and not self._results[dep_id].startswith("Error:"):
                context_parts.append(f"\n=== Output from '{dep_id}' ===\n{self._results[dep_id]}")
        context = "\n".join(context_parts)

        # Broadcast delegation.started
        await self._adapter._broadcast_to_session(self._session_id, {
            "kind": "delegation.started",
            "turn_id": self._turn_id,
            "delegation_id": delegation_id,
            "source": "workflow",
            "goal": step["goal"],
            "role_id": step.get("role_id"),
            "role_name": step.get("role_name", ""),
        })

        # Execute via delegate_task
        from tools.delegate_tool import delegate_task
        start_time = time.monotonic()
        result_json = await self._loop.run_in_executor(None, lambda:
            delegate_task(
                goal=step["goal"],
                context=context,
                toolsets=step.get("toolsets"),
                model=step.get("model"),
                max_iterations=step.get("max_iterations", 50),
                parent_agent=step_agent,
            )
        )
        duration = time.monotonic() - start_time

        result = json.loads(result_json)
        entry = result["results"][0]
        summary = entry.get("summary", "")
        self._results[step_id] = summary
        self._step_status[step_id] = "completed"

        # Aggregate token usage
        tokens = entry.get("tokens", {})
        self._usage_totals["input_tokens"] += tokens.get("input", 0)
        self._usage_totals["output_tokens"] += tokens.get("output", 0)

        # Broadcast delegation.completed
        is_error = entry.get("status") != "completed"
        await self._adapter._broadcast_to_session(self._session_id, {
            "kind": "delegation.completed",
            "turn_id": self._turn_id,
            "delegation_id": delegation_id,
            "source": "workflow",
            "error": is_error,
            "output_preview": summary[:200],
            "duration": round(duration, 3),
        })

        # Write to delegation_log (D33)
        self._adapter._write_delegation_log(
            session_id=self._session_id,
            turn_id=self._turn_id,
            call_id=delegation_id,
            duration=duration,
            error=is_error,
            output_preview=summary[:200],
            team_id=self._team_id,
            role_id=step.get("role_id"),
            role_name=step.get("role_name", ""),
            goal=step["goal"],
            source="workflow",
        )

        return summary

    @staticmethod
    def _has_cycle(steps: Dict[str, dict]) -> bool:
        """Detect cycles via DFS. Returns True if cycle found."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {sid: WHITE for sid in steps}

        def dfs(u):
            color[u] = GRAY
            for v in steps[u].get("depends_on", []):
                if v not in color:
                    raise ValueError(f"Step '{u}' depends on unknown step '{v}'")
                if color[v] == GRAY:
                    return True
                if color[v] == WHITE and dfs(v):
                    return True
            color[u] = BLACK
            return False

        return any(dfs(u) for u in steps if color[u] == WHITE)
