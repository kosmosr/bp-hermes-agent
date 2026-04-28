"""Per-turn session_id propagation for desktop platform → MCP tools.

Set by ``gateway.platforms.desktop._run_agent_async`` on the agent
executor thread before invoking ``run_conversation``; read by
``tools.mcp_tool._make_tool_handler`` when injecting ``_meta.session_id``
into ``hermes_fs.*`` MCP calls (Phase 6 reverse MCP fs server).

Why ``threading.local()`` instead of ``contextvars.ContextVar`` or env var:

- ``ContextVar`` does not reliably propagate through
  ``loop.run_in_executor`` (see ``desktop.py`` L1991-1992 — the repo has
  hit this for ``HERMES_SESSION_KEY`` propagation already).
- ``os.environ`` would race across concurrent turns since the desktop
  platform allows ``Semaphore(max_connections)`` (default 8) turns
  in flight; one turn would clobber another's session_id mid-flight.
- ``threading.local()`` works because each turn runs ``run_conversation``
  on its own dedicated executor thread; values are isolated by thread
  identity. Cleanup in a ``try/finally`` is mandatory because the
  ``ThreadPoolExecutor`` may reuse worker threads across turns and a
  stale value would leak into the next turn that grabs the same worker.

The MCP tool handler reads this value on the same agent thread (sync
segment), BEFORE ``_call()`` schedules onto the background ``_mcp_loop``
via ``run_coroutine_threadsafe`` — that boundary crosses both threads
and event loops and loses thread-local / ContextVar context. The value
must be captured into the ``arguments`` closure before the boundary.
"""

import threading

_local = threading.local()


def set_session_id(session_id: str) -> None:
    """Set the session_id for the current thread."""
    _local.session_id = session_id


def get_session_id() -> str:
    """Return the session_id for the current thread, or empty string if unset."""
    return getattr(_local, "session_id", "")


def clear_session_id() -> None:
    """Remove the session_id for the current thread (idempotent)."""
    try:
        del _local.session_id
    except AttributeError:
        pass
