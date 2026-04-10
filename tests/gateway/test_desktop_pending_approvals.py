"""_pending_approvals dict: first responder wins, second is ignored."""
import pytest
from gateway.platforms.desktop import DesktopAdapter


def test_first_approval_wins():
    adapter = DesktopAdapter.__new__(DesktopAdapter)
    adapter._pending_approvals = {"appr-001": "desktop:sess-a"}

    # First pop succeeds
    key1 = adapter._pending_approvals.pop("appr-001", None)
    assert key1 == "desktop:sess-a"

    # Second pop returns None (already handled)
    key2 = adapter._pending_approvals.pop("appr-001", None)
    assert key2 is None
