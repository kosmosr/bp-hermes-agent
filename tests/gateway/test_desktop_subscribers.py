"""Broadcast reaches all subscribers; one disconnect doesn't affect others."""
import asyncio
import pytest
import aiohttp
from tests.gateway.test_desktop_handshake import desktop_adapter, ws_url, token


@pytest.mark.asyncio
async def test_broadcast_reaches_all_subscribers(desktop_adapter, ws_url, token):
    """Two clients subscribe to same session; both receive broadcast."""
    sid = "shared-session"
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(ws_url, headers={"Authorization": f"Bearer {token}"}) as ws1, \
                   s.ws_connect(ws_url, headers={"Authorization": f"Bearer {token}"}) as ws2:
            # Drain welcome
            await asyncio.wait_for(ws1.receive_json(), timeout=3)
            await asyncio.wait_for(ws2.receive_json(), timeout=3)

            # Both subscribe
            await ws1.send_json({"v": 1, "id": "c-1", "kind": "session.subscribe", "session_id": sid})
            await ws2.send_json({"v": 1, "id": "c-2", "kind": "session.subscribe", "session_id": sid})
            await asyncio.wait_for(ws1.receive_json(), timeout=3)  # snapshot
            await asyncio.wait_for(ws2.receive_json(), timeout=3)  # snapshot

            # Broadcast directly
            await desktop_adapter._broadcast_to_session(sid, {
                "kind": "message.delta", "turn_id": "t1", "text": "test broadcast",
            })

            msg1 = await asyncio.wait_for(ws1.receive_json(), timeout=3)
            msg2 = await asyncio.wait_for(ws2.receive_json(), timeout=3)
            assert msg1["kind"] == "message.delta"
            assert msg2["kind"] == "message.delta"
            assert msg1["text"] == "test broadcast"
