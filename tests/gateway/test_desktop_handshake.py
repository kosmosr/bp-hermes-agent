"""Integration tests for desktop adapter WebSocket handshake."""
import asyncio
import pytest
import pytest_asyncio
import aiohttp
from aiohttp import web
import tempfile
import os
import shutil

from gateway.config import PlatformConfig, Platform


@pytest_asyncio.fixture
async def desktop_adapter():
    """Create a DesktopAdapter with in-memory config for testing."""
    from gateway.platforms.desktop import DesktopAdapter

    token_dir = tempfile.mkdtemp()
    token_file = os.path.join(token_dir, "desktop_token")

    config = PlatformConfig(
        enabled=True,
        extra={
            "host": "127.0.0.1",
            "port": 0,  # OS picks free port
            "token_file": token_file,
            "max_connections": 4,
        },
    )
    adapter = DesktopAdapter(config)
    await adapter.connect()
    # adapter._port is the actual bound port after connect()
    yield adapter
    await adapter.disconnect()
    shutil.rmtree(token_dir, ignore_errors=True)


@pytest.fixture
def ws_url(desktop_adapter):
    return f"ws://127.0.0.1:{desktop_adapter._port}/ws"


@pytest.fixture
def token(desktop_adapter):
    return desktop_adapter._token


@pytest.mark.asyncio
async def test_valid_token_gets_welcome(desktop_adapter, ws_url, token):
    """Valid Bearer token → ws upgrade succeeds → receive welcome envelope."""
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            ws_url, headers={"Authorization": f"Bearer {token}"}
        ) as ws:
            msg = await asyncio.wait_for(ws.receive_json(), timeout=3)
            assert msg["kind"] == "welcome"
            assert msg["v"] == 1
            assert "capabilities" in msg
            assert "server" in msg


@pytest.mark.asyncio
async def test_invalid_token_gets_401(desktop_adapter, ws_url):
    """Invalid Bearer token → HTTP 401."""
    async with aiohttp.ClientSession() as session:
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc_info:
            async with session.ws_connect(
                ws_url, headers={"Authorization": "Bearer wrong-token"}
            ) as ws:
                pass
        assert exc_info.value.status == 401


@pytest.mark.asyncio
async def test_missing_auth_header_gets_401(desktop_adapter, ws_url):
    """No Authorization header → HTTP 401."""
    async with aiohttp.ClientSession() as session:
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc_info:
            async with session.ws_connect(ws_url) as ws:
                pass
        assert exc_info.value.status == 401


@pytest.mark.asyncio
async def test_welcome_contains_sessions(desktop_adapter, ws_url, token):
    """welcome envelope should contain a sessions array."""
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            ws_url, headers={"Authorization": f"Bearer {token}"}
        ) as ws:
            msg = await asyncio.wait_for(ws.receive_json(), timeout=3)
            assert isinstance(msg.get("sessions"), list)
