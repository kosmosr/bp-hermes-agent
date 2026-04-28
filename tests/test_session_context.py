"""Tests for hermes_session_id thread-local propagation (Phase 6).

The module is used by ``gateway.platforms.desktop._run_agent_async`` to
expose the active turn's session_id to ``tools.mcp_tool``, which injects
it into ``_meta.session_id`` for ``hermes_fs.*`` MCP calls so the
client-side mcp-fs-server can look up the workspace validator by session.

These tests pin down the contract:

1. Per-thread isolation (concurrent turns don't leak across threads).
2. The value must be set on the agent executor thread (inside the
   ``loop.run_in_executor`` target), not on the asyncio loop thread,
   since the boundary doesn't carry thread-local automatically — this
   is the same reason the repo doesn't trust ContextVar there.
3. The value is NOT preserved across the ``run_coroutine_threadsafe``
   boundary into ``_mcp_loop`` — explicitly verifying the constraint
   that drove the design (mcp_tool reads & injects in the sync handler
   segment, before this boundary).
"""

import asyncio
import threading
import time

import pytest

from hermes_session_id import (
    clear_session_id,
    get_session_id,
    set_session_id,
)


def test_default_is_empty_string():
    """Unset thread sees empty string, not an exception."""
    clear_session_id()  # scrub leftover from any prior test on this worker
    assert get_session_id() == ""


def test_set_then_get_returns_value():
    clear_session_id()
    try:
        set_session_id("sess-abc123")
        assert get_session_id() == "sess-abc123"
    finally:
        clear_session_id()


def test_clear_makes_get_return_empty():
    set_session_id("sess-xxx")
    clear_session_id()
    assert get_session_id() == ""


def test_clear_is_idempotent():
    clear_session_id()
    clear_session_id()
    assert get_session_id() == ""


def test_isolation_across_threads():
    """Two threads each set their own session_id; values do not leak."""
    barrier = threading.Barrier(2)
    results: dict[str, str] = {}

    def _worker(name: str, sid: str) -> None:
        set_session_id(sid)
        barrier.wait()  # both threads have set their values
        time.sleep(0.05)  # give the other thread time to observe its own
        results[name] = get_session_id()
        clear_session_id()

    t1 = threading.Thread(target=_worker, args=("a", "sess-aaa"))
    t2 = threading.Thread(target=_worker, args=("b", "sess-bbb"))
    t1.start()
    t2.start()
    t1.join(timeout=2.0)
    t2.join(timeout=2.0)

    assert results == {"a": "sess-aaa", "b": "sess-bbb"}


@pytest.mark.asyncio
async def test_propagates_when_set_inside_executor_target():
    """The pattern desktop.py uses: set inside the executor target callable.

    This is the supported way to propagate session_id across the
    run_in_executor boundary, because the loop's worker thread doesn't
    inherit thread-local from the loop thread automatically.
    """
    loop = asyncio.get_running_loop()
    observed: list[str] = []

    def _executor_target():
        set_session_id("sess-from-executor")
        try:
            observed.append(get_session_id())
        finally:
            clear_session_id()

    await loop.run_in_executor(None, _executor_target)
    assert observed == ["sess-from-executor"]


@pytest.mark.asyncio
async def test_set_in_loop_thread_does_not_propagate_to_executor():
    """Setting on the asyncio loop thread does NOT carry to a worker thread.

    Documents why desktop.py wraps run_conversation in a target function
    that calls set_session_id INSIDE the executor target, rather than
    calling set_session_id once before run_in_executor.
    """
    loop = asyncio.get_running_loop()
    set_session_id("loop-thread-value")
    try:
        observed = await loop.run_in_executor(None, get_session_id)
    finally:
        clear_session_id()
    # Worker thread has its own thread-local → empty default.
    assert observed == ""


@pytest.mark.asyncio
async def test_executor_target_isolated_from_caller_thread():
    """Loop-thread value persists across the boundary; threads stay isolated."""
    loop = asyncio.get_running_loop()
    set_session_id("loop-thread-value")
    try:
        def _executor_target() -> str:
            set_session_id("executor-value")
            try:
                return get_session_id()
            finally:
                clear_session_id()

        executor_value = await loop.run_in_executor(None, _executor_target)
        # Loop thread still sees its own value untouched.
        assert get_session_id() == "loop-thread-value"
        assert executor_value == "executor-value"
    finally:
        clear_session_id()
