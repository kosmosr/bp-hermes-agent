"""outcome must be one of hermes' 4 values: once/session/always/deny."""
import asyncio
import pytest
import aiohttp
from tests.gateway.test_desktop_handshake import desktop_adapter, ws_url, token


@pytest.mark.asyncio
async def test_valid_outcomes_accepted(desktop_adapter, ws_url, token):
    """All four hermes outcomes are accepted without error."""
    for outcome in ("once", "session", "always", "deny"):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                ws_url, headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await asyncio.wait_for(ws.receive_json(), timeout=3)  # welcome
                await ws.send_json({"v": 1, "id": "c-1",
                                    "kind": "approval.response",
                                    "request_id": "nonexistent",
                                    "outcome": outcome})
                # No error should come back (nonexistent request_id is silently ignored)
                try:
                    msg = await asyncio.wait_for(ws.receive_json(), timeout=0.5)
                    # If we get a response, it should NOT be an error about outcome
                    if msg.get("kind") == "error":
                        assert msg["code"] != "PROTO_INVALID_OUTCOME"
                except asyncio.TimeoutError:
                    pass  # expected — no error


@pytest.mark.asyncio
async def test_invalid_outcome_rejected(desktop_adapter, ws_url, token):
    """Old-style outcomes like allow_once or reject_always are rejected."""
    for bad_outcome in ("allow_once", "allow_always", "reject_once", "reject_always", "ALLOW", ""):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                ws_url, headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await asyncio.wait_for(ws.receive_json(), timeout=3)
                await ws.send_json({"v": 1, "id": "c-1",
                                    "kind": "approval.response",
                                    "request_id": "x",
                                    "outcome": bad_outcome})
                msg = await asyncio.wait_for(ws.receive_json(), timeout=3)
                assert msg["kind"] == "error"
                assert msg["code"] == "PROTO_INVALID_OUTCOME"
