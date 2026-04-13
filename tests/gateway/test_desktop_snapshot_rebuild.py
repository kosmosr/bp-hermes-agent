"""Tests for DesktopAdapter._db_messages_to_snapshot_events().

Validates that DB messages (role/content/tool_calls/tool_call_id format)
are correctly converted to the full client event vocabulary:
user.message, turn.started, message.delta, reasoning.delta,
tool.started, tool.completed, turn.complete.

NOTE: We extract the static method source via AST to avoid importing the
full desktop module, which pulls in heavy dependencies that may not be
available in all test environments.
"""
from __future__ import annotations

import ast
import os
import textwrap
import types

import pytest

# ---------------------------------------------------------------------------
# Extract the static method from desktop.py without importing the module.
# This avoids the heavy import chain (gateway.config -> hermes_cli -> ...).
# ---------------------------------------------------------------------------

_DESKTOP_PY = os.path.join(
    os.path.dirname(__file__), "..", "..", "gateway", "platforms", "desktop.py"
)


def _extract_static_method(filepath: str, class_name: str, method_name: str):
    """Parse *filepath*, find *class_name*.*method_name*, compile & return as a callable."""
    with open(filepath, "r") as f:
        tree = ast.parse(f.read(), filename=filepath)

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == method_name:
                    # Dedent the function so it becomes a module-level def
                    source_lines = open(filepath).readlines()
                    func_lines = source_lines[item.lineno - 1 : item.end_lineno]
                    func_source = textwrap.dedent("".join(func_lines))
                    # Remove @staticmethod decorator if present
                    func_source = func_source.replace("@staticmethod\n", "")
                    code = compile(func_source, filepath, "exec")
                    ns: dict = {}
                    exec(code, ns)  # noqa: S102
                    return ns[method_name]

    raise RuntimeError(f"Could not find {class_name}.{method_name} in {filepath}")


_convert = _extract_static_method(_DESKTOP_PY, "DesktopAdapter", "_db_messages_to_snapshot_events")


# ===========================================================================
# Tests
# ===========================================================================


class TestTextOnlyConversation:
    """user + assistant(content only) -> user.message, turn cycle."""

    def test_text_only_conversation(self):
        db_msgs = [
            {"role": "user", "content": "Hello", "timestamp": "t1"},
            {"role": "assistant", "content": "Hi there!", "timestamp": "t2"},
            {"role": "user", "content": "How are you?", "timestamp": "t3"},
            {"role": "assistant", "content": "I'm fine.", "timestamp": "t4"},
        ]
        events = _convert(db_msgs)

        assert events[0] == {"kind": "user.message", "content": "Hello", "ts": "t1"}
        assert events[1] == {"kind": "turn.started", "turn_id": "hist-1", "ts": "t2"}
        assert events[2] == {"kind": "message.delta", "turn_id": "hist-1", "text": "Hi there!", "ts": "t2"}
        assert events[3] == {"kind": "turn.complete", "turn_id": "hist-1", "ts": "t2"}
        assert events[4] == {"kind": "user.message", "content": "How are you?", "ts": "t3"}
        assert events[5] == {"kind": "turn.started", "turn_id": "hist-2", "ts": "t4"}
        assert events[6] == {"kind": "message.delta", "turn_id": "hist-2", "text": "I'm fine.", "ts": "t4"}
        assert events[7] == {"kind": "turn.complete", "turn_id": "hist-2", "ts": "t4"}
        assert len(events) == 8


class TestSingleToolCall:
    """user -> assistant(tool_calls=[A]) -> tool(result) -> assistant(content)
    Should produce proper tool.started/tool.completed in one turn, final text in second turn.
    """

    def test_single_tool_call(self):
        db_msgs = [
            {"role": "user", "content": "Run ls", "timestamp": "t1"},
            {
                "role": "assistant",
                "content": "",
                "timestamp": "t2",
                "tool_calls": [
                    {
                        "id": "call-1",
                        "function": {"name": "shell", "arguments": '{"cmd": "ls"}'},
                    }
                ],
            },
            {
                "role": "tool",
                "content": "file1.txt\nfile2.txt",
                "timestamp": "t3",
                "tool_call_id": "call-1",
                "tool_name": "shell",
            },
            {"role": "assistant", "content": "Here are the files.", "timestamp": "t4"},
        ]
        events = _convert(db_msgs)

        # user.message
        assert events[0]["kind"] == "user.message"

        # Turn 1: assistant with tool call
        assert events[1] == {"kind": "turn.started", "turn_id": "hist-1", "ts": "t2"}
        assert events[2] == {
            "kind": "tool.started",
            "turn_id": "hist-1",
            "call_id": "call-1",
            "tool": "shell",
            "preview": '{"cmd": "ls"}',
            "ts": "t2",
        }
        assert events[3] == {
            "kind": "tool.completed",
            "turn_id": "hist-1",
            "call_id": "call-1",
            "tool": "shell",
            "duration": 0,
            "error": False,
            "output_preview": "file1.txt\nfile2.txt",
            "ts": "t3",
        }

        # Turn 1 closed, Turn 2: final assistant text
        assert events[4] == {"kind": "turn.complete", "turn_id": "hist-1", "ts": "t4"}
        assert events[5] == {"kind": "turn.started", "turn_id": "hist-2", "ts": "t4"}
        assert events[6] == {"kind": "message.delta", "turn_id": "hist-2", "text": "Here are the files.", "ts": "t4"}
        assert events[7] == {"kind": "turn.complete", "turn_id": "hist-2", "ts": "t4"}
        assert len(events) == 8


class TestMultipleToolCallsOneMessage:
    """assistant with tool_calls=[A,B] -> tool(A) -> tool(B)
    Two tool.started, two tool.completed in the same turn.
    """

    def test_multiple_tool_calls_one_message(self):
        db_msgs = [
            {"role": "user", "content": "Check both", "timestamp": "t1"},
            {
                "role": "assistant",
                "content": "",
                "timestamp": "t2",
                "tool_calls": [
                    {"id": "call-a", "function": {"name": "read_file", "arguments": '{"path": "a.txt"}'}},
                    {"id": "call-b", "function": {"name": "read_file", "arguments": '{"path": "b.txt"}'}},
                ],
            },
            {"role": "tool", "content": "contents of a", "timestamp": "t3", "tool_call_id": "call-a", "tool_name": "read_file"},
            {"role": "tool", "content": "contents of b", "timestamp": "t4", "tool_call_id": "call-b", "tool_name": "read_file"},
        ]
        events = _convert(db_msgs)

        assert events[0]["kind"] == "user.message"
        assert events[1]["kind"] == "turn.started"
        assert events[1]["turn_id"] == "hist-1"

        # Two tool.started from the same assistant message
        started_events = [e for e in events if e["kind"] == "tool.started"]
        assert len(started_events) == 2
        assert started_events[0]["call_id"] == "call-a"
        assert started_events[1]["call_id"] == "call-b"

        # Two tool.completed
        completed_events = [e for e in events if e["kind"] == "tool.completed"]
        assert len(completed_events) == 2
        assert completed_events[0]["call_id"] == "call-a"
        assert completed_events[1]["call_id"] == "call-b"

        # Dangling turn closed at end
        assert events[-1]["kind"] == "turn.complete"
        assert events[-1]["turn_id"] == "hist-1"


class TestMultiStepAgenticLoop:
    """assistant(tc) -> tool -> assistant(tc) -> tool -> assistant(content)
    Three turns total.
    """

    def test_multi_step_agentic_loop(self):
        db_msgs = [
            {"role": "user", "content": "Deploy", "timestamp": "t1"},
            # Step 1: tool call
            {
                "role": "assistant", "content": "", "timestamp": "t2",
                "tool_calls": [{"id": "c1", "function": {"name": "shell", "arguments": "build"}}],
            },
            {"role": "tool", "content": "ok", "timestamp": "t3", "tool_call_id": "c1", "tool_name": "shell"},
            # Step 2: another tool call
            {
                "role": "assistant", "content": "", "timestamp": "t4",
                "tool_calls": [{"id": "c2", "function": {"name": "shell", "arguments": "deploy"}}],
            },
            {"role": "tool", "content": "deployed", "timestamp": "t5", "tool_call_id": "c2", "tool_name": "shell"},
            # Step 3: final text
            {"role": "assistant", "content": "All done!", "timestamp": "t6"},
        ]
        events = _convert(db_msgs)

        turn_ids = [e["turn_id"] for e in events if e.get("kind") == "turn.started"]
        assert turn_ids == ["hist-1", "hist-2", "hist-3"]

        complete_ids = [e["turn_id"] for e in events if e.get("kind") == "turn.complete"]
        assert complete_ids == ["hist-1", "hist-2", "hist-3"]

        # Final turn has message.delta
        final_deltas = [e for e in events if e["kind"] == "message.delta"]
        assert len(final_deltas) == 1
        assert final_deltas[0]["text"] == "All done!"
        assert final_deltas[0]["turn_id"] == "hist-3"


class TestAssistantWithContentAndToolCalls:
    """content + tool_calls in same assistant message.
    message.delta + tool.started should coexist.
    """

    def test_assistant_with_content_and_tool_calls(self):
        db_msgs = [
            {"role": "user", "content": "Explain and run", "timestamp": "t1"},
            {
                "role": "assistant",
                "content": "Let me check that for you.",
                "timestamp": "t2",
                "tool_calls": [
                    {"id": "call-x", "function": {"name": "shell", "arguments": "ls"}},
                ],
            },
            {"role": "tool", "content": "result", "timestamp": "t3", "tool_call_id": "call-x", "tool_name": "shell"},
        ]
        events = _convert(db_msgs)

        assert events[0]["kind"] == "user.message"
        assert events[1]["kind"] == "turn.started"

        # Both message.delta and tool.started in the same turn
        kinds_in_turn = [e["kind"] for e in events if e.get("turn_id") == "hist-1"]
        assert "message.delta" in kinds_in_turn
        assert "tool.started" in kinds_in_turn
        assert "tool.completed" in kinds_in_turn

        # message.delta comes before tool.started
        delta_idx = next(i for i, e in enumerate(events) if e["kind"] == "message.delta")
        started_idx = next(i for i, e in enumerate(events) if e["kind"] == "tool.started")
        assert delta_idx < started_idx


class TestReasoningField:
    """assistant with reasoning="thinking..." -> reasoning.delta event."""

    def test_reasoning_field(self):
        db_msgs = [
            {"role": "user", "content": "Think about this", "timestamp": "t1"},
            {
                "role": "assistant",
                "content": "Here is the answer.",
                "timestamp": "t2",
                "reasoning": "thinking step by step...",
            },
        ]
        events = _convert(db_msgs)

        reasoning_events = [e for e in events if e["kind"] == "reasoning.delta"]
        assert len(reasoning_events) == 1
        assert reasoning_events[0]["text"] == "thinking step by step..."
        assert reasoning_events[0]["turn_id"] == "hist-1"

        # Reasoning comes before message.delta
        r_idx = next(i for i, e in enumerate(events) if e["kind"] == "reasoning.delta")
        m_idx = next(i for i, e in enumerate(events) if e["kind"] == "message.delta")
        assert r_idx < m_idx


class TestUserWithToolCallId:
    """role=user, tool_call_id set -> treated as tool.completed."""

    def test_user_with_tool_call_id(self):
        db_msgs = [
            {"role": "user", "content": "Go", "timestamp": "t1"},
            {
                "role": "assistant", "content": "", "timestamp": "t2",
                "tool_calls": [{"id": "c1", "function": {"name": "shell", "arguments": "echo hi"}}],
            },
            # Some APIs store tool results as role=user with tool_call_id
            {
                "role": "user",
                "content": "hi",
                "timestamp": "t3",
                "tool_call_id": "c1",
                "tool_name": "shell",
            },
        ]
        events = _convert(db_msgs)

        completed = [e for e in events if e["kind"] == "tool.completed"]
        assert len(completed) == 1
        assert completed[0]["call_id"] == "c1"
        assert completed[0]["tool"] == "shell"
        assert completed[0]["output_preview"] == "hi"

        # The role=user with tool_call_id should NOT generate a user.message
        user_msgs = [e for e in events if e["kind"] == "user.message"]
        assert len(user_msgs) == 1  # Only the genuine one


class TestEmptyMessages:
    """empty list -> empty list."""

    def test_empty_messages(self):
        assert _convert([]) == []


class TestDanglingTurnAtEnd:
    """Last message is tool result -> turn still closed."""

    def test_dangling_turn_at_end(self):
        db_msgs = [
            {"role": "user", "content": "Do it", "timestamp": "t1"},
            {
                "role": "assistant", "content": "", "timestamp": "t2",
                "tool_calls": [{"id": "c1", "function": {"name": "shell", "arguments": "run"}}],
            },
            {"role": "tool", "content": "done", "timestamp": "t3", "tool_call_id": "c1", "tool_name": "shell"},
        ]
        events = _convert(db_msgs)

        # Turn should be closed even though last msg is a tool result
        assert events[-1]["kind"] == "turn.complete"
        assert events[-1]["turn_id"] == "hist-1"

        # Verify the complete event sequence
        kinds = [e["kind"] for e in events]
        assert "turn.started" in kinds
        assert "tool.started" in kinds
        assert "tool.completed" in kinds
        assert kinds.count("turn.complete") == 1
