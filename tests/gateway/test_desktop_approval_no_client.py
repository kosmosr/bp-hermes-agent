"""When no Electron clients are subscribed, send_exec_approval returns success=False."""
import pytest
from gateway.platforms.desktop import DesktopAdapter
from tests.gateway.test_desktop_handshake import desktop_adapter


@pytest.mark.asyncio
async def test_no_subscribers_rejects_approval(desktop_adapter):
    """send_exec_approval with no subscribers → immediate SendResult(success=False)."""
    result = await desktop_adapter.send_exec_approval(
        chat_id="orphan-session",
        command="rm -rf /",
        session_key="desktop:orphan-session",
        description="dangerous command",
    )
    assert result.success is False
    assert "no desktop clients" in (result.error or "").lower()
