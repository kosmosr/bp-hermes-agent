"""Integration tests for session.subscribe + session.snapshot."""
import asyncio
import pytest
import aiohttp

from tests.gateway.test_desktop_handshake import desktop_adapter, ws_url, token


@pytest.mark.asyncio
async def test_subscribe_returns_snapshot(desktop_adapter, ws_url, token):
    """session.subscribe → session.snapshot with events + max_seq."""
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            ws_url, headers={"Authorization": f"Bearer {token}"}
        ) as ws:
            welcome = await asyncio.wait_for(ws.receive_json(), timeout=3)
            assert welcome["kind"] == "welcome"

            await ws.send_json({"v": 1, "id": "c-1", "kind": "session.subscribe",
                                "session_id": "test-session"})
            snap = await asyncio.wait_for(ws.receive_json(), timeout=3)
            assert snap["kind"] == "session.snapshot"
            assert snap["session_id"] == "test-session"
            assert isinstance(snap["events"], list)
            assert "max_seq" in snap
            assert snap["gap"] is False


@pytest.mark.asyncio
async def test_subscribe_missing_session_id(desktop_adapter, ws_url, token):
    """session.subscribe without session_id → error PROTO_MISSING_FIELD."""
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            ws_url, headers={"Authorization": f"Bearer {token}"}
        ) as ws:
            await asyncio.wait_for(ws.receive_json(), timeout=3)  # welcome
            await ws.send_json({"v": 1, "id": "c-1", "kind": "session.subscribe"})
            resp = await asyncio.wait_for(ws.receive_json(), timeout=3)
            assert resp["kind"] == "error"
            assert resp["code"] == "PROTO_MISSING_FIELD"


@pytest.mark.asyncio
async def test_subscribe_replay_since_seq(desktop_adapter, ws_url, token):
    """Reconnect with since_seq replays missed envelopes from ring buffer."""
    sid = "replay-session"

    # Pre-populate the ring buffer directly
    buf = desktop_adapter._session_buffers[sid]
    for i in range(5):
        buf.append({"kind": "message.delta", "turn_id": "t1", "text": f"msg{i}"})

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            ws_url, headers={"Authorization": f"Bearer {token}"}
        ) as ws:
            await asyncio.wait_for(ws.receive_json(), timeout=3)  # welcome
            # Subscribe with since_seq=2 → should get events 3,4,5
            await ws.send_json({"v": 1, "id": "c-1", "kind": "session.subscribe",
                                "session_id": sid, "since_seq": 2})
            snap = await asyncio.wait_for(ws.receive_json(), timeout=3)
            assert snap["kind"] == "session.snapshot"
            assert len(snap["events"]) == 3
            assert snap["gap"] is False
