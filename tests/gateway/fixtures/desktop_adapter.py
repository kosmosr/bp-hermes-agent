"""Pre-configured DesktopAdapter fixture for integration tests."""
import tempfile
import os
import shutil
import pytest
from gateway.config import PlatformConfig
from gateway.platforms.desktop import DesktopAdapter


@pytest.fixture
async def desktop_adapter_with_agent(monkeypatch):
    """DesktopAdapter with mocked _create_agent_for_turn returning FakeAIAgent."""
    from tests.gateway.fixtures.fake_ai_agent import FakeAIAgent

    token_dir = tempfile.mkdtemp()
    token_file = os.path.join(token_dir, "desktop_token")

    config = PlatformConfig(
        enabled=True,
        extra={
            "host": "127.0.0.1",
            "port": 0,
            "token_file": token_file,
            "max_connections": 4,
        },
    )
    adapter = DesktopAdapter(config)

    # Patch agent creation to return fake
    fake_agent = FakeAIAgent()
    monkeypatch.setattr(adapter, "_create_agent_for_turn",
                        lambda **kw: fake_agent)

    await adapter.connect()
    yield adapter, fake_agent
    await adapter.disconnect()
    shutil.rmtree(token_dir, ignore_errors=True)
