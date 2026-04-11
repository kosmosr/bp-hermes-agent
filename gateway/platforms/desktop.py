"""
Desktop platform adapter — WebSocket endpoint for local Electron client.

Config (gateway config YAML: platforms.desktop.extra):
  host: 127.0.0.1           # loopback only by default
  port: 8643
  token_file: ~/.hermes/desktop_token
  max_connections: 8
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import secrets
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Set

from aiohttp import web

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult

logger = logging.getLogger(__name__)

VERSION = "0.1.0"


def check_desktop_requirements() -> bool:
    """Check if aiohttp is available."""
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------

@dataclass
class _Connection:
    """One WebSocket connection from one Electron window."""
    id: str
    ws: web.WebSocketResponse
    client_info: dict = field(default_factory=dict)
    subscribed_session_id: Optional[str] = None
    last_ping: float = field(default_factory=time.time)
    _seq: int = 0

    async def send(self, kind: str, **payload) -> None:
        self._seq += 1
        await self.ws.send_json({
            "v": 1,
            "id": f"s-{self.id[:8]}-{self._seq}",
            "kind": kind,
            "ts": time.time(),
            **payload,
        })


@dataclass
class _ActiveTurn:
    """A turn currently in flight inside an agent task."""
    turn_id: str
    session_id: str
    initiator_conn_id: str
    agent: Any = None
    task: Optional[asyncio.Task] = None
    session_key: str = ""


class _EnvelopeRingBuffer:
    """Fixed-capacity ring buffer for session envelope history.

    Supports reconnect replay via since_seq. Each envelope is assigned
    a monotonically increasing seq number.
    """

    def __init__(self, capacity: int = 500):
        self._capacity = capacity
        self._buf: deque[tuple[int, dict]] = deque(maxlen=capacity)
        self._seq = 0

    @property
    def max_seq(self) -> int:
        return self._seq

    def append(self, envelope: dict) -> int:
        self._seq += 1
        self._buf.append((self._seq, envelope))
        return self._seq

    def since(self, seq: int) -> tuple[list[dict], bool]:
        """Return (envelopes_after_seq, gap).

        gap=True if the buffer has already discarded entries before seq.
        """
        if not self._buf:
            return [], False
        oldest_seq = self._buf[0][0]
        gap = seq < oldest_seq
        result = [env for s, env in self._buf if s > seq]
        return result, gap


# ---------------------------------------------------------------------------
# DesktopAdapter
# ---------------------------------------------------------------------------

class DesktopAdapter(BasePlatformAdapter):
    """Desktop platform adapter — local WebSocket server for Electron client."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.DESKTOP)
        self._host: str = config.extra.get("host", "127.0.0.1")
        self._port: int = int(config.extra.get("port", 8643))
        self._token_file = Path(
            config.extra.get("token_file", "~/.hermes/desktop_token")
        ).expanduser()
        self._max_conn: int = int(config.extra.get("max_connections", 8))

        self._token: Optional[str] = None
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._session_db = None
        # Semaphore to limit concurrent agent turns (replaces private executor introspection)
        self._turn_semaphore = asyncio.Semaphore(self._max_conn)

        # Connection registry
        self._connections: Dict[str, _Connection] = {}
        # session_id → set of conn_ids subscribing to it
        self._session_subscribers: Dict[str, Set[str]] = defaultdict(set)
        # session_id → active turn (one turn per session at a time)
        self._active_turns: Dict[str, _ActiveTurn] = {}
        # session_id → ring buffer for reconnect replay
        self._session_buffers: Dict[str, _EnvelopeRingBuffer] = defaultdict(
            _EnvelopeRingBuffer
        )
        # request_id → session_key for pending approvals
        self._pending_approvals: Dict[str, str] = {}
        # session_id → {title, created_at} — lightweight in-memory registry
        self._known_sessions: Dict[str, dict] = {}

    # ------------------------------------------------------------------
    # BasePlatformAdapter abstract methods
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        self._token = self._load_or_create_token()
        self._app = web.Application()
        self._app.router.add_get("/ws", self._handle_ws)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        # Capture actual port (important when port=0 for tests)
        for sock in site._server.sockets:  # type: ignore[union-attr]
            self._port = sock.getsockname()[1]
            break
        self._mark_connected()
        logger.info("[desktop] ws listening on ws://%s:%d/ws", self._host, self._port)
        return True

    async def disconnect(self):
        for conn in list(self._connections.values()):
            try:
                await conn.send("server.shutdown", reason="disconnect")
                await conn.ws.close(code=1001)
            except Exception:
                pass
        for active in list(self._active_turns.values()):
            if active.agent:
                try:
                    active.agent.interrupt("desktop adapter shutdown")
                except Exception:
                    pass
        if self._runner:
            await self._runner.cleanup()
        self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message to a session (broadcast to subscribers)."""
        await self._broadcast_to_session(chat_id, {
            "kind": "message.delta",
            "turn_id": None,
            "text": content,
        })
        return SendResult(success=True, message_id=f"msg-{uuid.uuid4().hex[:10]}")

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        subs = self._session_subscribers.get(chat_id, set())
        return {
            "name": f"desktop:{chat_id}",
            "type": "desktop",
            "subscribers": len(subs),
        }

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Emit typing indicator to subscribers."""
        await self._broadcast_to_session(chat_id, {
            "kind": "typing.start", "session_id": chat_id,
        })

    async def send_image(
        self, chat_id: str, image_url: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send image URL as a message.delta with markdown image syntax."""
        md = f"![image]({image_url})"
        if caption:
            md += f"\n{caption}"
        await self._broadcast_to_session(chat_id, {
            "kind": "message.delta", "turn_id": None, "text": md,
        })
        return SendResult(success=True)

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _load_or_create_token(self) -> str:
        if self._token_file.exists():
            return self._token_file.read_text().strip()
        self._token_file.parent.mkdir(parents=True, exist_ok=True)
        token = secrets.token_hex(32)
        # Atomic create with restricted permissions (avoids world-readable window)
        fd = os.open(str(self._token_file), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(fd, token.encode())
        finally:
            os.close(fd)
        return token

    @staticmethod
    def _redact_token(raw: str | None) -> str | None:
        """Redact Bearer tokens for logging. Keeps last 4 chars."""
        if raw is None:
            return None
        if not raw or not raw.startswith("Bearer "):
            return raw
        token = raw[7:]
        if len(token) < 8:
            return "Bearer sk-***"
        return f"Bearer sk-***{token[-4:]}"

    # ------------------------------------------------------------------
    # Session DB (lazy init, mirrors api_server.py pattern)
    # ------------------------------------------------------------------

    def _ensure_session_db(self):
        """Lazily initialise and return the shared SessionDB instance."""
        if self._session_db is None:
            try:
                from hermes_state import SessionDB
                self._session_db = SessionDB()
            except Exception as e:
                logger.debug("SessionDB unavailable for desktop: %s", e)
        return self._session_db

    # ------------------------------------------------------------------
    # WebSocket handler
    # ------------------------------------------------------------------

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        # --- Auth ---
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or not hmac.compare_digest(
            auth[7:], self._token or ""
        ):
            raise web.HTTPUnauthorized(text="Invalid or missing Bearer token")

        if len(self._connections) >= self._max_conn:
            raise web.HTTPServiceUnavailable(text="Too many connections")

        ws = web.WebSocketResponse(max_msg_size=1_048_576)  # 1 MB
        await ws.prepare(request)

        # Protocol version check — client should send v=1
        client_version = request.query.get("v", "1")
        if client_version != "1":
            await ws.close(code=4403, message=b"Protocol version mismatch")
            return ws

        conn = _Connection(
            id=uuid.uuid4().hex,
            ws=ws,
            client_info={
                "name": request.query.get("client", "unknown"),
                "version": request.query.get("version", ""),
                "platform": request.query.get("platform", ""),
            },
        )
        self._connections[conn.id] = conn
        logger.info(
            "[desktop] new connection %s from %s (client=%s)",
            conn.id[:8], request.remote, conn.client_info.get("name"),
        )

        try:
            # Send welcome
            await conn.send(
                "welcome",
                capabilities=["approval", "reasoning", "tool_events", "interrupt", "markdown"],
                server={"version": VERSION, "hermes_version": "0.8.x"},
                sessions=[
                    {"session_id": sid, "title": info["title"], "created_at": info.get("created_at")}
                    for sid, info in self._known_sessions.items()
                ],
            )

            # Message loop
            async for raw_msg in ws:
                if raw_msg.type == web.WSMsgType.TEXT:
                    if len(raw_msg.data) > 1_048_576:
                        await conn.send("error", code="PROTO_FRAME_TOO_LARGE",
                                        message="Envelope exceeds 1 MB limit")
                        await ws.close(code=4413, message=b"frame too large")
                        break
                    try:
                        data = json.loads(raw_msg.data)
                    except json.JSONDecodeError:
                        logger.warning("[desktop] non-JSON from %s", conn.id[:8])
                        continue
                    await self._dispatch(conn, data)
                elif raw_msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                    break
        finally:
            self._connections.pop(conn.id, None)
            for sid, subs in list(self._session_subscribers.items()):
                subs.discard(conn.id)
            # Interrupt agent if no subscribers left
            for sid, active in list(self._active_turns.items()):
                if active.initiator_conn_id == conn.id:
                    if not self._session_subscribers.get(sid):
                        if active.agent:
                            try:
                                active.agent.interrupt("desktop client disconnected")
                            except Exception:
                                pass
            logger.info("[desktop] connection %s closed", conn.id[:8])

        return ws

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    async def _dispatch(self, conn: _Connection, msg: dict) -> None:
        kind = msg.get("kind", "")
        handler_map = {
            "ping": self._handle_ping,
            "session.list": self._handle_session_list,
            "session.new": self._handle_session_new,
            "session.subscribe": self._handle_session_subscribe,
            "prompt.send": self._handle_prompt_send,
            "approval.response": self._handle_approval_response,
            "turn.interrupt": self._handle_turn_interrupt,
        }
        handler = handler_map.get(kind)
        if handler is None:
            logger.warning("[desktop] unknown envelope kind=%s from %s", kind, conn.id[:8])
            await conn.send("error", code="PROTO_UNKNOWN_KIND", ref_id=msg.get("id"),
                            message=f"Unknown kind: {kind}")
            return
        try:
            await handler(conn, msg)
        except Exception as e:
            logger.exception("[desktop] handler %s raised", kind)
            await conn.send("error", code="INTERNAL", ref_id=msg.get("id"),
                            message=str(e)[:500])

    # ------------------------------------------------------------------
    # Handlers — session / ping
    # ------------------------------------------------------------------

    async def _handle_ping(self, conn: _Connection, msg: dict) -> None:
        conn.last_ping = time.time()
        await conn.send("pong")

    async def _handle_session_list(self, conn: _Connection, msg: dict) -> None:
        await conn.send("session.list.ok", sessions=[
            {"session_id": sid, "title": info["title"], "created_at": info.get("created_at")}
            for sid, info in self._known_sessions.items()
        ])

    async def _handle_session_new(self, conn: _Connection, msg: dict) -> None:
        session_id = f"sess-{uuid.uuid4().hex[:12]}"
        title = msg.get("title", "New Chat")
        created_at = time.time()
        self._known_sessions[session_id] = {
            "title": title, "created_at": created_at,
        }
        await conn.send("session.new.ok", session={
            "session_id": session_id,
            "title": title,
            "created_at": created_at,
        })

    async def _handle_session_subscribe(self, conn: _Connection, msg: dict) -> None:
        session_id = msg.get("session_id", "")
        if not session_id:
            await conn.send("error", code="PROTO_MISSING_FIELD", ref_id=msg.get("id"),
                            message="session_id is required")
            return
        # Unsubscribe from previous session
        if conn.subscribed_session_id:
            self._session_subscribers[conn.subscribed_session_id].discard(conn.id)
        conn.subscribed_session_id = session_id
        self._session_subscribers[session_id].add(conn.id)

        # Replay from ring buffer
        since_seq = msg.get("since_seq", 0)
        buf = self._session_buffers[session_id]
        events, gap = buf.since(since_seq)
        await conn.send("session.snapshot",
                        session_id=session_id,
                        events=events,
                        max_seq=buf.max_seq,
                        gap=gap)
        logger.info("[desktop] subscribe session=%s since_seq=%s → snapshot(%d events, gap=%s)",
                    session_id, since_seq, len(events), gap)

    # ------------------------------------------------------------------
    # Handlers — prompt.send + agent runner
    # ------------------------------------------------------------------

    async def _handle_prompt_send(self, conn: _Connection, msg: dict) -> None:
        session_id = msg.get("session_id", "")
        content = msg.get("content", "")

        if not session_id:
            await conn.send("error", code="PROTO_MISSING_FIELD", ref_id=msg.get("id"),
                            message="session_id is required")
            return
        if not content:
            await conn.send("error", code="PROTO_MISSING_FIELD", ref_id=msg.get("id"),
                            message="content is required")
            return
        if session_id not in self._session_subscribers or not self._session_subscribers[session_id]:
            await conn.send("error", code="SESSION_NOT_FOUND", ref_id=msg.get("id"),
                            message=f"Session {session_id} not found or not subscribed")
            return
        if session_id in self._active_turns:
            await conn.send("error", code="TURN_IN_PROGRESS", ref_id=msg.get("id"),
                            message=f"Session {session_id} has an active turn")
            return
        # Check capacity via semaphore (avoids CPython private internals)
        if self._turn_semaphore.locked():
            await conn.send("error", code="AGENT_BUSY", ref_id=msg.get("id"),
                            message="Agent thread pool is full, retry later")
            return

        turn_id = f"turn-{uuid.uuid4().hex[:12]}"
        session_key = f"desktop:{session_id}"
        active = _ActiveTurn(
            turn_id=turn_id, session_id=session_id,
            initiator_conn_id=conn.id, session_key=session_key,
        )
        self._active_turns[session_id] = active

        loop = asyncio.get_running_loop()

        def _on_delta(delta):
            if delta is None:
                return
            asyncio.run_coroutine_threadsafe(
                self._broadcast_to_session(session_id, {
                    "kind": "message.delta", "turn_id": turn_id, "text": delta,
                }), loop
            )

        def _on_tool_progress(event_type, tool_name=None, preview=None, **kw):
            if event_type == "tool.started":
                env = {"kind": "tool.started", "turn_id": turn_id,
                       "call_id": kw.get("call_id", ""), "tool": tool_name,
                       "preview": preview, "args": kw.get("args")}
            elif event_type == "tool.completed":
                env = {"kind": "tool.completed", "turn_id": turn_id,
                       "call_id": kw.get("call_id", ""), "tool": tool_name,
                       "duration": round(kw.get("duration", 0), 3),
                       "error": kw.get("is_error", False),
                       "output_preview": kw.get("output_preview")}
            elif event_type == "reasoning.available":
                env = {"kind": "reasoning.delta", "turn_id": turn_id, "text": preview or ""}
            else:
                return
            asyncio.run_coroutine_threadsafe(
                self._broadcast_to_session(session_id, env), loop
            )

        agent = self._create_agent_for_turn(
            session_id=session_id,
            stream_delta_callback=_on_delta,
            tool_progress_callback=_on_tool_progress,
        )
        active.agent = agent

        await self._broadcast_to_session(session_id, {
            "kind": "turn.started", "session_id": session_id, "turn_id": turn_id,
        })

        active.task = asyncio.create_task(
            self._run_agent_async(active, user_message=content)
        )
        logger.info("[desktop] prompt.send session=%s turn=%s len=%d",
                     session_id, turn_id, len(content))

    def _create_agent_for_turn(self, session_id, stream_delta_callback=None,
                                tool_progress_callback=None,
                                ephemeral_system_prompt=None):
        """Create an AIAgent instance — mirrors api_server.py:404 pattern."""
        from run_agent import AIAgent
        from gateway.run import (
            _resolve_runtime_agent_kwargs, _resolve_gateway_model,
            _load_gateway_config, GatewayRunner,
        )
        from hermes_cli.tools_config import _get_platform_tools

        runtime_kwargs = _resolve_runtime_agent_kwargs()
        model = _resolve_gateway_model()
        user_config = _load_gateway_config()
        enabled_toolsets = sorted(_get_platform_tools(user_config, "desktop"))
        max_iterations = int(os.getenv("HERMES_MAX_ITERATIONS", "90"))
        fallback_model = GatewayRunner._load_fallback_model()

        return AIAgent(
            model=model, **runtime_kwargs,
            max_iterations=max_iterations,
            quiet_mode=True, verbose_logging=False,
            ephemeral_system_prompt=ephemeral_system_prompt or None,
            enabled_toolsets=enabled_toolsets,
            session_id=session_id,
            platform="desktop",
            stream_delta_callback=stream_delta_callback,
            tool_progress_callback=tool_progress_callback,
            session_db=self._ensure_session_db(),
            fallback_model=fallback_model,
        )

    async def _run_agent_async(self, active: _ActiveTurn, user_message: str) -> None:
        """Run agent.run_conversation in executor with approval contextvar binding."""
        from tools.approval import (
            register_gateway_notify,
            reset_current_session_key,
            set_current_session_key,
            unregister_gateway_notify,
        )

        session_id = active.session_id
        turn_id = active.turn_id
        session_key = active.session_key
        loop = asyncio.get_running_loop()

        approval_token = set_current_session_key(session_key)

        def _notify(approval_data: dict) -> None:
            logger.warning("[desktop] _notify fallback called for session=%s", session_key)

        try:
            register_gateway_notify(session_key, _notify)
        except Exception as exc:
            logger.exception("[desktop] register_gateway_notify failed for %s", session_key)
            reset_current_session_key(approval_token)
            self._active_turns.pop(session_id, None)
            await self._broadcast_to_session(session_id, {
                "kind": "turn.error", "turn_id": turn_id,
                "code": "APPROVAL_BIND_FAILED", "message": str(exc)[:500],
            })
            return

        async with self._turn_semaphore:
            try:
                result = await loop.run_in_executor(
                    None,
                    lambda: active.agent.run_conversation(
                        user_message=user_message,
                        conversation_history=None,
                        task_id=session_id,
                    ),
                )
                # Send final response if not already streamed via deltas
                final_text = (result or {}).get("final_response") or ""
                if final_text:
                    await self._broadcast_to_session(session_id, {
                        "kind": "message.delta", "turn_id": turn_id,
                        "text": final_text,
                    })
                usage = {
                    "prompt_tokens": getattr(active.agent, "session_prompt_tokens", 0) or 0,
                    "completion_tokens": getattr(active.agent, "session_completion_tokens", 0) or 0,
                    "model": getattr(active.agent, "model", "unknown"),
                }
                await self._broadcast_to_session(session_id, {
                    "kind": "turn.complete", "turn_id": turn_id, "usage": usage,
                })
            except Exception as exc:
                logger.exception("[desktop] turn %s failed", turn_id)
                await self._broadcast_to_session(session_id, {
                    "kind": "turn.error", "turn_id": turn_id,
                    "code": "AGENT_EXCEPTION", "message": str(exc)[:500],
                })
            finally:
                unregister_gateway_notify(session_key)
                reset_current_session_key(approval_token)
                self._active_turns.pop(session_id, None)

    # ------------------------------------------------------------------
    # Handlers — approval
    # ------------------------------------------------------------------

    async def send_exec_approval(
        self, chat_id: str, command: str, session_key: str,
        description: str = "dangerous command",
        metadata: Optional[dict] = None,
    ) -> SendResult:
        """Called by hermes run.py:6841 when a dangerous command needs approval.

        Must return within 15s (hermes hard timeout at run.py:6849).
        """
        if not self._session_subscribers.get(chat_id):
            logger.warning("[desktop] send_exec_approval: no subscribers for session=%s", chat_id)
            return SendResult(success=False, error="No desktop clients connected")

        request_id = f"appr-{uuid.uuid4().hex[:10]}"
        self._pending_approvals[request_id] = session_key

        turn = self._active_turns.get(chat_id)
        await self._broadcast_to_session(chat_id, {
            "kind": "approval.request",
            "turn_id": turn.turn_id if turn else None,
            "request_id": request_id,
            "command": command,
            "description": description,
        })
        logger.info("[desktop] approval.request session=%s req=%s cmd=%r",
                     chat_id, request_id, command[:80])
        return SendResult(success=True, message_id=request_id)

    async def _handle_approval_response(self, conn: _Connection, msg: dict) -> None:
        request_id = msg.get("request_id", "")
        outcome = msg.get("outcome", "")

        if outcome not in ("once", "session", "always", "deny"):
            await conn.send("error", code="PROTO_INVALID_OUTCOME", ref_id=msg.get("id"),
                            message=f"outcome must be once/session/always/deny, got {outcome!r}")
            return

        session_key = self._pending_approvals.pop(request_id, None)
        if session_key is None:
            return  # already handled by another window

        from tools.approval import resolve_gateway_approval
        resolve_gateway_approval(session_key, outcome)

        sid = conn.subscribed_session_id
        if sid:
            await self._broadcast_to_session(sid, {
                "kind": "approval.resolved",
                "request_id": request_id,
                "outcome": outcome,
                "by": conn.client_info.get("name", "desktop"),
            })
        logger.info("[desktop] approval.resolved session=%s req=%s outcome=%s",
                     sid, request_id, outcome)

    # ------------------------------------------------------------------
    # Handlers — interrupt
    # ------------------------------------------------------------------

    async def _handle_turn_interrupt(self, conn: _Connection, msg: dict) -> None:
        session_id = msg.get("session_id", "")
        active = self._active_turns.get(session_id)
        if active and active.agent:
            try:
                active.agent.interrupt("user interrupt")
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Broadcast
    # ------------------------------------------------------------------

    async def _broadcast_to_session(self, session_id: str, envelope: dict) -> None:
        """Fan-out one envelope to every subscriber of a session."""
        buf = self._session_buffers[session_id]
        # Stamp envelope with standard fields before buffering for replay
        stamped = {"v": 1, "ts": time.time(), **envelope}
        buf.append(stamped)

        for conn_id in list(self._session_subscribers.get(session_id, ())):
            conn = self._connections.get(conn_id)
            if conn is None:
                self._session_subscribers[session_id].discard(conn_id)
                continue
            try:
                to_send = dict(envelope)  # shallow copy
                kind = to_send.pop("kind")
                await conn.send(kind, **to_send)
            except ConnectionResetError:
                self._connections.pop(conn_id, None)
                self._session_subscribers[session_id].discard(conn_id)
            except Exception as e:
                logger.warning("[desktop] broadcast to %s failed: %s", conn_id[:8], e)
