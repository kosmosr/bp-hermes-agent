"""Unit tests for _redact_token."""
from gateway.platforms.desktop import DesktopAdapter


def test_redact_normal_token():
    assert DesktopAdapter._redact_token("Bearer deadbeefcafe1234abcd") == "Bearer sk-***abcd"


def test_redact_short_token():
    assert DesktopAdapter._redact_token("Bearer abc") == "Bearer sk-***"


def test_redact_empty():
    assert DesktopAdapter._redact_token("") == ""


def test_redact_non_bearer():
    assert DesktopAdapter._redact_token("Basic xyz") == "Basic xyz"


def test_redact_none_safe():
    assert DesktopAdapter._redact_token(None) is None
