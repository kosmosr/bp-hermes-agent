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


@pytest.mark.asyncio
async def test_custom_slug_provider_pulls_api_key_from_custom_providers(monkeypatch):
    """custom:<slug> form (written by hermes-desktop EndpointCard) should
    match _LOCAL_PROVIDERS after root-splitting, then reach into
    custom_providers[] for the matching api_key/base_url. Previously the
    bare-string compare missed this case and welcomed clients with a
    has_credentials=false fallback placeholder."""
    adapter = _make_adapter()
    monkeypatch.setattr(
        "hermes_cli.model_switch.list_authenticated_providers",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "gateway.run._load_gateway_config",
        lambda: {
            "model": {"provider": "custom:ikun", "default": "claude-sonnet-4-6"},
            "custom_providers": [
                {
                    "slug": "ikun",
                    "name": "ikun",
                    "base_url": "https://api.ikuncode.cc",
                    "api_key": "sk-ikun-real",
                    "api_mode": "anthropic_messages",
                },
            ],
        },
    )
    monkeypatch.setattr(
        "gateway.run._resolve_gateway_model",
        lambda cfg=None: "claude-sonnet-4-6",
    )

    fetched: dict = {}

    async def _fake_fetch(base_url, api_key):
        fetched["base_url"] = base_url
        fetched["api_key"] = api_key
        return ["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5"]

    monkeypatch.setattr(adapter, "_fetch_endpoint_models", _fake_fetch)

    payload = await adapter._build_models_payload()

    endpoint = payload["providers"][0]
    assert endpoint["slug"] == "custom:ikun"
    assert endpoint["name"] == "ikun"
    assert endpoint["source"] == "endpoint"
    assert endpoint["has_credentials"] is True
    assert endpoint["models"] == ["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5"]
    # Confirms _fetch_endpoint_models received the values from custom_providers[]
    assert fetched["base_url"] == "https://api.ikuncode.cc"
    assert fetched["api_key"] == "sk-ikun-real"


@pytest.mark.asyncio
async def test_custom_slug_provider_keyless_local_llm_still_credentialed(monkeypatch):
    """Local LLM endpoints registered via custom:<slug> with empty api_key
    (typical for Ollama / LM Studio fronted by a custom slug) should still
    surface as has_credentials=true because the *registration* is the
    credential — the user has explicitly told the client this endpoint
    exists and is reachable without a key."""
    adapter = _make_adapter()
    monkeypatch.setattr(
        "hermes_cli.model_switch.list_authenticated_providers",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "gateway.run._load_gateway_config",
        lambda: {
            "model": {"provider": "custom:local-llama"},
            "custom_providers": [
                {
                    "slug": "local-llama",
                    "name": "Local Llama",
                    "base_url": "http://localhost:11434/v1",
                    "api_key": "",
                    "api_mode": "chat_completions",
                },
            ],
        },
    )
    monkeypatch.setattr(
        "gateway.run._resolve_gateway_model",
        lambda cfg=None: "llama3:latest",
    )
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)

    async def _fake_fetch(base_url, api_key):
        return []

    monkeypatch.setattr(adapter, "_fetch_endpoint_models", _fake_fetch)

    payload = await adapter._build_models_payload()

    endpoint = payload["providers"][0]
    assert endpoint["slug"] == "custom:local-llama"
    assert endpoint["name"] == "Local Llama"
    assert endpoint["has_credentials"] is True
    assert endpoint["models"] == ["llama3:latest"]


@pytest.mark.asyncio
async def test_custom_slug_provider_missing_from_custom_providers_list(monkeypatch):
    """If config.yaml's model.provider points at a custom:<slug> that has
    *no* matching custom_providers[] entry (stale config / hand-edit),
    still take the local-provider branch but mark has_credentials=true so
    the user can at least see and switch away. The reverse — emitting a
    fallback placeholder that the client filters out — leaves the user
    with no way to recover from settings UI."""
    adapter = _make_adapter()
    monkeypatch.setattr(
        "hermes_cli.model_switch.list_authenticated_providers",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "gateway.run._load_gateway_config",
        lambda: {
            "model": {"provider": "custom:ghost"},
            "custom_providers": [],
        },
    )
    monkeypatch.setattr(
        "gateway.run._resolve_gateway_model",
        lambda cfg=None: "stale-model",
    )
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)

    async def _fake_fetch(base_url, api_key):
        return []

    monkeypatch.setattr(adapter, "_fetch_endpoint_models", _fake_fetch)

    payload = await adapter._build_models_payload()

    endpoint = payload["providers"][0]
    assert endpoint["slug"] == "custom:ghost"
    assert endpoint["source"] == "endpoint"
    # custom_slug truthy keeps the user-defined entry visible
    assert endpoint["has_credentials"] is True


@pytest.mark.asyncio
async def test_custom_slug_provider_legacy_entry_without_slug_field(monkeypatch):
    """Older custom_providers entries written before the slug field was
    persisted only carry name/base_url/api_key. _build_models_payload must
    fall back to _slugify(name) when matching, otherwise the api_key /
    base_url stay empty, _fetch_endpoint_models gets no auth, and the
    welcome envelope downgrades to [current_model]."""
    adapter = _make_adapter()
    monkeypatch.setattr(
        "hermes_cli.model_switch.list_authenticated_providers",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "gateway.run._load_gateway_config",
        lambda: {
            "model": {"provider": "custom:ikun", "default": "claude-sonnet-4-6"},
            # Legacy entry — no slug field, only name. _slugify("ikun") == "ikun".
            "custom_providers": [
                {
                    "name": "ikun",
                    "base_url": "https://api.ikuncode.cc",
                    "api_key": "sk-legacy",
                    "api_mode": "anthropic_messages",
                },
            ],
        },
    )
    monkeypatch.setattr(
        "gateway.run._resolve_gateway_model",
        lambda cfg=None: "claude-sonnet-4-6",
    )

    fetched: dict = {}

    async def _fake_fetch(base_url, api_key):
        fetched["base_url"] = base_url
        fetched["api_key"] = api_key
        return ["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5"]

    monkeypatch.setattr(adapter, "_fetch_endpoint_models", _fake_fetch)

    payload = await adapter._build_models_payload()

    endpoint = payload["providers"][0]
    assert endpoint["slug"] == "custom:ikun"
    assert endpoint["name"] == "ikun"
    assert endpoint["has_credentials"] is True
    assert endpoint["models"] == ["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5"]
    assert fetched["base_url"] == "https://api.ikuncode.cc"
    assert fetched["api_key"] == "sk-legacy"
