"""Tests for DesktopAdapter._build_models_payload has_credentials field.

Regression coverage for the welcome-envelope `source="fallback"` placeholder
that the client used to misclassify as "external credential ready".  The
server now explicitly stamps `has_credentials=True/False` so the client can
tell real auth from a UI-only placeholder.
"""
import os
import tempfile

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.desktop import DesktopAdapter


def _make_adapter() -> DesktopAdapter:
    """DesktopAdapter without connect() — sufficient for invoking the
    pure _build_models_payload coroutine.  Skips the websocket bind that
    `desktop_adapter_with_agent` does because we don't need it here."""
    fd, token_file = tempfile.mkstemp(suffix=".token")
    os.close(fd)
    config = PlatformConfig(
        enabled=True,
        extra={
            "host": "127.0.0.1",
            "port": 0,
            "token_file": token_file,
            "max_connections": 4,
        },
    )
    return DesktopAdapter(config)


@pytest.mark.asyncio
async def test_authed_providers_get_has_credentials_true(monkeypatch):
    adapter = _make_adapter()
    monkeypatch.setattr(
        "hermes_cli.model_switch.list_authenticated_providers",
        lambda **kwargs: [
            {
                "slug": "anthropic",
                "name": "Anthropic",
                "is_current": True,
                "is_user_defined": False,
                "models": ["claude-opus-4.6"],
                "total_models": 1,
                "source": "built-in",
            }
        ],
    )
    monkeypatch.setattr(
        "gateway.run._load_gateway_config",
        lambda: {"model": {"provider": "anthropic"}},
    )
    monkeypatch.setattr(
        "gateway.run._resolve_gateway_model",
        lambda cfg=None: "claude-opus-4.6",
    )

    payload = await adapter._build_models_payload()

    assert payload["providers"][0]["slug"] == "anthropic"
    assert payload["providers"][0]["has_credentials"] is True


@pytest.mark.asyncio
async def test_fallback_placeholder_gets_has_credentials_false(monkeypatch):
    adapter = _make_adapter()
    monkeypatch.setattr(
        "hermes_cli.model_switch.list_authenticated_providers",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "gateway.run._load_gateway_config",
        lambda: {"model": {"provider": "openrouter"}},
    )
    monkeypatch.setattr(
        "gateway.run._resolve_gateway_model",
        lambda cfg=None: "anthropic/claude-opus-4.6",
    )

    payload = await adapter._build_models_payload()

    fallback = payload["providers"][0]
    assert fallback["slug"] == "openrouter"
    assert fallback["source"] == "fallback"
    assert fallback["has_credentials"] is False


@pytest.mark.asyncio
async def test_local_mode_endpoint_has_credentials_reflects_api_key(monkeypatch):
    adapter = _make_adapter()
    monkeypatch.setattr(
        "hermes_cli.model_switch.list_authenticated_providers",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "gateway.run._load_gateway_config",
        lambda: {
            "model": {
                "provider": "custom",
                "base_url": "http://localhost:1234/v1",
                "api_key": "sk-test-real-key",
            }
        },
    )
    monkeypatch.setattr(
        "gateway.run._resolve_gateway_model",
        lambda cfg=None: "test-model",
    )

    async def _fake_fetch(base_url, api_key):
        return ["test-model"]

    monkeypatch.setattr(adapter, "_fetch_endpoint_models", _fake_fetch)

    payload = await adapter._build_models_payload()

    endpoint = payload["providers"][0]
    assert endpoint["slug"] == "custom"
    assert endpoint["source"] == "endpoint"
    assert endpoint["has_credentials"] is True


@pytest.mark.asyncio
async def test_local_mode_endpoint_without_api_key_has_credentials_false(monkeypatch):
    adapter = _make_adapter()
    monkeypatch.setattr(
        "hermes_cli.model_switch.list_authenticated_providers",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "gateway.run._load_gateway_config",
        lambda: {
            "model": {
                "provider": "lmstudio",
                "base_url": "http://localhost:1234/v1",
                "api_key": "",
            }
        },
    )
    monkeypatch.setattr(
        "gateway.run._resolve_gateway_model",
        lambda cfg=None: "local-model",
    )
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)

    async def _fake_fetch(base_url, api_key):
        return []

    monkeypatch.setattr(adapter, "_fetch_endpoint_models", _fake_fetch)

    payload = await adapter._build_models_payload()

    endpoint = payload["providers"][0]
    assert endpoint["slug"] == "lmstudio"
    assert endpoint["source"] == "endpoint"
    assert endpoint["has_credentials"] is False
