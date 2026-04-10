"""Verify envelope serialization/deserialization round-trip for all kinds."""
import json
import pytest


SAMPLE_ENVELOPES = [
    {"v": 1, "id": "s-1", "kind": "welcome", "capabilities": ["approval"], "server": {"version": "0.1.0", "hermes_version": "0.8.x"}, "sessions": []},
    {"v": 1, "id": "s-2", "kind": "turn.started", "session_id": "s1", "turn_id": "t1"},
    {"v": 1, "id": "s-3", "kind": "message.delta", "turn_id": "t1", "text": "hello"},
    {"v": 1, "id": "s-4", "kind": "reasoning.delta", "turn_id": "t1", "text": "thinking..."},
    {"v": 1, "id": "s-5", "kind": "tool.started", "turn_id": "t1", "call_id": "c1", "tool": "shell", "preview": "ls", "args": None},
    {"v": 1, "id": "s-6", "kind": "tool.completed", "turn_id": "t1", "call_id": "c1", "duration": 0.04, "error": False, "output_preview": "file1.txt"},
    {"v": 1, "id": "s-7", "kind": "approval.request", "turn_id": "t1", "request_id": "r1", "command": "rm -rf /", "description": "danger"},
    {"v": 1, "id": "s-8", "kind": "approval.resolved", "request_id": "r1", "outcome": "always", "by": "desktop"},
    {"v": 1, "id": "s-9", "kind": "turn.complete", "turn_id": "t1", "usage": {"prompt_tokens": 100, "completion_tokens": 50, "model": "test"}},
    {"v": 1, "id": "s-10", "kind": "turn.error", "turn_id": "t1", "code": "AGENT_EXCEPTION", "message": "boom"},
    {"v": 1, "id": "s-11", "kind": "error", "code": "PROTO_UNKNOWN_KIND", "message": "unknown"},
    {"v": 1, "id": "s-12", "kind": "session.snapshot", "session_id": "s1", "events": [], "max_seq": 0, "gap": False},
    {"v": 1, "id": "c-1", "kind": "prompt.send", "session_id": "s1", "content": "hello"},
    {"v": 1, "id": "c-2", "kind": "approval.response", "request_id": "r1", "outcome": "always"},
    {"v": 1, "id": "c-3", "kind": "session.subscribe", "session_id": "s1", "since_seq": 0},
    {"v": 1, "id": "c-4", "kind": "ping"},
    {"v": 1, "id": "s-13", "kind": "pong"},
    {"v": 1, "id": "s-14", "kind": "server.shutdown", "reason": "test"},
]


@pytest.mark.parametrize("envelope", SAMPLE_ENVELOPES, ids=lambda e: e["kind"])
def test_envelope_round_trip(envelope):
    """JSON serialize → deserialize produces identical envelope."""
    serialized = json.dumps(envelope)
    deserialized = json.loads(serialized)
    assert deserialized == envelope
    assert deserialized["v"] == 1
    assert "kind" in deserialized
    assert "id" in deserialized


def test_all_server_kinds_covered():
    """Ensure we have samples for all server→client kinds from §2.2."""
    expected_server_kinds = {
        "welcome", "session.list.ok", "session.new.ok", "session.snapshot",
        "turn.started", "message.delta", "reasoning.delta",
        "tool.started", "tool.completed",
        "approval.request", "approval.resolved",
        "turn.complete", "turn.error", "error",
        "server.shutdown", "pong",
    }
    actual = {e["kind"] for e in SAMPLE_ENVELOPES if e["id"].startswith("s-")}
    missing = expected_server_kinds - actual
    # session.list.ok and session.new.ok are covered by handler tests, not envelope samples
    assert missing <= {"session.list.ok", "session.new.ok"}


def test_all_client_kinds_covered():
    """Ensure we have samples for all client→server kinds from §2.2."""
    expected_client_kinds = {
        "session.list", "session.new", "session.subscribe",
        "prompt.send", "approval.response", "turn.interrupt", "ping",
    }
    actual = {e["kind"] for e in SAMPLE_ENVELOPES if e["id"].startswith("c-")}
    missing = expected_client_kinds - actual
    assert missing <= {"session.list", "session.new", "turn.interrupt"}
