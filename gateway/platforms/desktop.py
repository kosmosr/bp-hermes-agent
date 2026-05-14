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
import base64
import fcntl
import hashlib
import hmac
import json
import logging
import mimetypes
import re
import os
import secrets
import shutil
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Set

from aiohttp import web

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from hermes_cli.config import get_config_path, get_env_path, get_hermes_home
from hermes_session_id import clear_session_id, set_session_id

logger = logging.getLogger(__name__)

VERSION = "0.1.0"

# Phase 6 Slice 5: injected into every turn's ephemeral system prompt so the
# LLM translates hermes_fs error codes into user-friendly Chinese replies.
_HERMES_FS_ERROR_GUIDE = """

当 hermes_fs 文件工具返回错误时,请按以下规则向用户解释:
- out_of_workspace → 路径超出工作目录范围,只能操作工作目录内的文件
- catastrophic_workspace_root → 不允许对整个工作目录执行高风险操作,请用户手动确认或缩小范围
- unc_not_supported → 暂不支持 Windows 网络共享路径(\\\\server\\share),请改用本机目录
- path_too_long_app → 路径超过客户端限制,请缩短路径
- path_too_long_os → 路径超过操作系统限制,Windows 可提示启用 LongPathsEnabled 或换短路径
- invalid_path → 路径格式不正确,请检查是否为空或包含非法字符
- invalid_filename → 文件名不合法,可能是 Windows 保留名或包含禁用字符
- invalid_pattern → 搜索 glob 或正则表达式不合法,请修正搜索模式
- not_found → 文件或目录不存在,检查路径是否正确
- not_a_directory → 预期是目录但给了文件路径
- is_a_directory → 预期是文件但给了目录路径
- not_empty → 目录不为空,需要用户确认是否 recursive=true 删除
- new_exists → 目标路径已存在,不能覆盖,请询问用户改名或覆盖策略
- parent_not_found → 父目录不存在,需要先创建或用 create_parents=true
- permission_denied → 没有权限访问该文件,可能需要调整权限
- disk_full → 磁盘空间不足,写入失败
- file_busy → 文件正在被其他程序占用,可建议关闭占用程序后重试
- file_too_large → 文件太大,超出大小限制
- not_text_file → 不是文本文件,试试用 read_media_file
- search_timeout → 搜索超时,可能只有部分结果,可建议缩小范围
- approval_denied → 用户拒绝了该操作
- approval_timeout → 等待用户确认超时(30秒),可以建议用户重新发起请求
- no_workspace → 还没有选择工作目录,请先选择一个文件夹
- cross_device_dir → 源和目标不在同一磁盘分区,目录跨盘移动暂不支持
- service_unavailable → 客户端文件服务暂时不可用,请稍后重试
不要原样输出错误码,用自然语言告诉用户发生了什么、怎么解决。
"""


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
    debouncer: Optional[_OutputDebouncer] = None
    workflow_engine: Any = None  # WorkflowEngine instance for workflow mode
    mentions: Optional[dict] = None  # role_id → mention dict for AI-driven delegation


# ANSI escape sequence pattern — comprehensive 7-bit C1 catch-all.
# Matches SGR (\x1b[...m), cursor movement, OSC, and all CSI sequences.
_ANSI_RE = re.compile(r'\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


class _OutputDebouncer:
    """Aggregate tool output into 200ms batches per call_id.

    Thread-safe: push() is called from the agent thread (synchronous),
    schedules work onto the asyncio event loop. All timer/flush logic
    runs on the event loop thread.
    """

    INTERVAL = 0.2        # 200ms debounce window
    MAX_FLUSH_SIZE = 65536  # 64KB per flush

    def __init__(self, broadcast_fn, session_id: str, turn_id: str, loop):
        self._broadcast = broadcast_fn
        self._session_id = session_id
        self._turn_id = turn_id
        self._loop = loop
        self._pending: Dict[str, Dict[str, str]] = {}  # call_id → {stream → text}
        self._timers: Dict[str, asyncio.TimerHandle] = {}

    def push(self, call_id: str, stream: str, text: str) -> None:
        """Thread-safe: schedule aggregation onto the event loop."""
        self._loop.call_soon_threadsafe(self._push_on_loop, call_id, stream, text)

    def _push_on_loop(self, call_id: str, stream: str, text: str) -> None:
        """Runs on event loop thread. Aggregates text and manages timers."""
        if call_id not in self._pending:
            self._pending[call_id] = {}
        if call_id not in self._timers:
            self._timers[call_id] = self._loop.call_later(
                self.INTERVAL, lambda k=call_id: asyncio.ensure_future(self._flush(k))
            )
        buf = self._pending[call_id]
        buf[stream] = buf.get(stream, "") + text

    async def _flush(self, call_id: str) -> None:
        """Flush aggregated text for call_id as envelope(s).

        After flushing, checks if new data arrived during the async
        broadcast and re-arms the timer if so (fixes re-entry race).
        """
        self._timers.pop(call_id, None)
        entries = self._pending.pop(call_id, {})
        for stream, text in entries.items():
            if not text:
                continue
            if len(text) > self.MAX_FLUSH_SIZE:
                text = text[:self.MAX_FLUSH_SIZE] + "\n...[truncated]"
            text = _ANSI_RE.sub('', text)
            try:
                await self._broadcast(
                    self._session_id,
                    {"kind": "tool.output.delta", "turn_id": self._turn_id,
                     "call_id": call_id, "stream": stream, "text": text},
                    skip_buffer=True,
                )
            except Exception as exc:
                logger.warning("[desktop] debouncer flush error: %s", exc)
        # Re-arm: if new data arrived while we were awaiting broadcast,
        # _push_on_loop would have added to _pending but NOT created a
        # timer (because we popped _timers before the await). Schedule
        # another flush to prevent orphaned data.
        if call_id in self._pending and call_id not in self._timers:
            self._timers[call_id] = self._loop.call_later(
                self.INTERVAL, lambda k=call_id: asyncio.ensure_future(self._flush(k))
            )

    async def flush_all(self, call_id: str) -> None:
        """Force-flush before tool.completed. Cancel pending timer."""
        handle = self._timers.pop(call_id, None)
        if handle:
            handle.cancel()
        await self._flush(call_id)

    def cancel_all(self) -> None:
        """Cancel all pending timers and discard buffers.

        Call on turn.complete / turn.error / turn.interrupt to prevent
        stale flushes after the turn has ended.
        """
        for handle in self._timers.values():
            handle.cancel()
        self._timers.clear()
        self._pending.clear()


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
        gap = seq > 0 and seq < oldest_seq
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
        # request_id → threading.Event for pending clarify questions
        self._pending_clarifies: Dict[str, threading.Event] = {}
        # request_id → answer string for resolved clarify questions
        self._clarify_results: Dict[str, str] = {}
        # session_id → {title, created_at} — lightweight in-memory registry
        self._known_sessions: Dict[str, dict] = {}
        # session_id -> list of message dicts for agent conversation_history
        self._session_histories: Dict[str, list] = {}
        # session_id -> model/provider override from model.switch
        self._session_model_overrides: Dict[str, dict] = {}
        # Persistent MemoryStore for between-turn memory.read/update access
        self._memory_store = None  # lazy init in _ensure_memory_store

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _fetch_endpoint_models(self, base_url: str, api_key: str) -> list:
        """Query an OpenAI-compatible /models endpoint for available model IDs.

        Returns a list of model ID strings, or [] on any error.
        Timeout: 5 seconds to avoid blocking welcome.
        """
        if not base_url:
            return []
        url = base_url.rstrip("/")
        # Handle both /v1 and non-/v1 base URLs
        if url.endswith("/v1"):
            url = url + "/models"
        else:
            url = url + "/v1/models"
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        try:
            import aiohttp as _aiohttp
            async with _aiohttp.ClientSession() as session:
                async with session.get(
                    url, headers=headers,
                    timeout=_aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        logger.debug("[desktop] /models query returned %d", resp.status)
                        return []
                    data = await resp.json()
                    models = [
                        m["id"] for m in data.get("data", [])
                        if isinstance(m, dict) and "id" in m
                    ]
                    logger.info("[desktop] fetched %d models from endpoint", len(models))
                    return models
        except Exception as exc:
            logger.debug("[desktop] /models query failed: %s", exc)
            return []

    # ------------------------------------------------------------------
    # BasePlatformAdapter abstract methods
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        # Enable the approval system — without this env var,
        # tools/approval.py auto-approves all commands (see check_all_command_guards).
        os.environ["HERMES_EXEC_ASK"] = "1"
        logger.info("[desktop] HERMES_EXEC_ASK set to '1' (approval system enabled)")

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
        self._load_sessions_from_db()
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
        """Send a text message to a session (broadcast to subscribers).

        When called during an active turn (by hermes core after agent completes),
        the content was already streamed via deltas — skip to avoid duplication.
        Only broadcast when no active turn exists (true system notifications).
        """
        if chat_id in self._active_turns:
            return SendResult(success=True, message_id=f"msg-{uuid.uuid4().hex[:10]}")
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
                # Create desktop-specific auxiliary table for working_dir persistence.
                # The upstream sessions table has no working_dir column, so we store
                # it separately to avoid modifying hermes_state.py.
                # Use _execute_write for proper BEGIN IMMEDIATE transaction.
                def _create_meta_table(conn):
                    conn.execute(
                        """CREATE TABLE IF NOT EXISTS desktop_session_meta (
                            session_id TEXT PRIMARY KEY,
                            working_dir TEXT NOT NULL
                        )"""
                    )
                    conn.execute(
                        """CREATE TABLE IF NOT EXISTS delegation_log (
                            session_id TEXT NOT NULL,
                            turn_id TEXT NOT NULL,
                            call_id TEXT NOT NULL,
                            team_id TEXT,
                            role_id TEXT,
                            role_name TEXT,
                            goal TEXT,
                            source TEXT DEFAULT 'ai',
                            duration REAL DEFAULT 0,
                            error INTEGER DEFAULT 0,
                            output_preview TEXT,
                            created_at REAL NOT NULL,
                            PRIMARY KEY (session_id, turn_id, call_id)
                        )"""
                    )
                    conn.execute(
                        """CREATE INDEX IF NOT EXISTS idx_delegation_log_team
                           ON delegation_log (team_id, created_at)"""
                    )
                    conn.execute(
                        """CREATE INDEX IF NOT EXISTS idx_delegation_log_session
                           ON delegation_log (session_id, created_at)"""
                    )
                self._session_db._execute_write(_create_meta_table)
            except Exception as e:
                logger.debug("SessionDB unavailable for desktop: %s", e)
        return self._session_db

    def _build_mentions_prompt(self, mentions):
        """Build system prompt describing available team roles for AI-driven delegation."""
        lines = [
            "# Available Team Roles",
            "Delegate tasks to these roles using the delegate_task tool.",
            "CRITICAL: When calling delegate_task, you MUST include 'role_id:<id>' "
            "on the FIRST LINE of the `context` parameter so the system can track "
            "which role is executing.",
            "",
        ]
        for m in mentions:
            role_name = m.get("role_name", "Agent")
            role_id = m.get("role_id", "")
            ts = m.get("toolsets") or []
            sk = m.get("skills") or []
            mi = m.get("max_iterations", 30)
            lines.append(f"## {role_name}")
            lines.append(f"- role_id: {role_id}")
            if m.get("role_prompt"):
                lines.append(f"- System prompt: {m['role_prompt']}")
            if ts:
                lines.append(f"- toolsets: {json.dumps(ts)}")
            if sk:
                lines.append(f"- skills: {json.dumps(sk)}")
            lines.append(f"- max_iterations: {mi}")
            lines.append("")
            lines.append(f"Example delegate_task call for {role_name}:")
            lines.append(f'  delegate_task(goal="<task>", '
                         f'context="role_id:{role_id}\\n<additional context>", '
                         f'toolsets={json.dumps(ts)}, '
                         f'max_iterations={mi})')
            lines.append("")
        return "\n".join(lines)

    def _write_delegation_log(self, session_id, turn_id, call_id,
                               duration=0, error=False, output_preview=None,
                               team_id=None, role_id=None, role_name=None,
                               goal=None, source="ai"):
        """Write a delegation completion record to the delegation_log table."""
        db = self._ensure_session_db()
        if not db:
            return
        try:
            import time as _time
            def _insert(conn):
                conn.execute(
                    """INSERT OR REPLACE INTO delegation_log
                       (session_id, turn_id, call_id, team_id, role_id,
                        role_name, goal, source, duration, error,
                        output_preview, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (session_id, turn_id, call_id, team_id, role_id,
                     role_name, goal, source, duration,
                     1 if error else 0,
                     (output_preview or "")[:500],
                     _time.time()),
                )
            db._execute_write(_insert)
        except Exception as e:
            logger.debug("Failed to write delegation_log: %s", e)

    def _load_sessions_from_db(self):
        """Seed _known_sessions from SessionDB (SQLite) for history persistence."""
        db = self._ensure_session_db()
        if not db:
            return
        try:
            rows = db.list_sessions_rich(source="desktop", limit=50)
            # Load working_dir from desktop_session_meta auxiliary table
            meta_map: Dict[str, str] = {}
            try:
                with db._lock:
                    cursor = db._conn.execute(
                        "SELECT session_id, working_dir FROM desktop_session_meta"
                    )
                    for mrow in cursor.fetchall():
                        meta_map[mrow[0]] = mrow[1]
                logger.info("[desktop] loaded %d working_dir entries from desktop_session_meta", len(meta_map))
            except Exception as e:
                logger.warning("[desktop] Failed to load desktop_session_meta: %s", e)

            for row in rows:
                sid = row["id"]
                if sid not in self._known_sessions:
                    # Sessions missing from desktop_session_meta keep working_dir=""
                    # so the frontend workspace filter (!s.workingDir) shows them
                    # in all workspaces rather than hiding them.
                    self._known_sessions[sid] = {
                        "title": row.get("title") or "Untitled",
                        "created_at": row.get("started_at", 0),
                        "working_dir": meta_map.get(sid, ""),
                    }
        except Exception as e:
            logger.warning("[desktop] Failed to load sessions from DB: %s", e)

    async def _check_and_push_title(self, session_id: str):
        """Poll session_db for title change and push session.update if changed.

        Also persists the in-memory simple title to session_db as a fallback
        when the LLM title generator fails (no title in DB after delay).
        """
        if session_id not in self._known_sessions:
            return
        db = self._ensure_session_db()
        if not db:
            return
        try:
            db_title = db.get_session_title(session_id)
            mem_title = self._known_sessions[session_id].get("title")
            if db_title and db_title != mem_title:
                # LLM generated a better title → push it to client
                self._known_sessions[session_id]["title"] = db_title
                await self._broadcast_to_session(session_id, {
                    "kind": "session.update",
                    "session_id": session_id,
                    "title": db_title,
                })
            elif not db_title and mem_title and mem_title not in ("New Chat", "Untitled"):
                # LLM failed — persist simple title as permanent fallback
                try:
                    db.set_session_title(session_id, mem_title)
                except Exception:
                    pass
        except Exception as e:
            logger.debug("Title check failed for %s: %s", session_id, e)

    @staticmethod
    def _db_messages_to_snapshot_events(db_msgs: list) -> list:
        """Convert DB messages (get_messages format) to snapshot events for client replay.

        DB messages have: role, content, timestamp, tool_call_id, tool_calls, etc.
        Client expects the full event vocabulary: user.message, turn.started,
        message.delta, reasoning.delta, tool.started, tool.completed, turn.complete.
        """
        events: list[dict] = []
        turn_counter = 0
        current_turn_id: str | None = None

        for msg in db_msgs:
            role = msg.get("role", "")
            content = msg.get("content", "") or ""
            ts = msg.get("timestamp")
            tool_calls = msg.get("tool_calls")
            tool_call_id = msg.get("tool_call_id")
            reasoning = msg.get("reasoning", "") or ""

            if role == "user" and not tool_call_id:
                # Genuine user message — close any open turn first
                if current_turn_id:
                    events.append({"kind": "turn.complete", "turn_id": current_turn_id, "ts": ts})
                    current_turn_id = None
                events.append({"kind": "user.message", "content": content, "ts": ts})

            elif role == "assistant":
                # Close previous open turn before starting a new one
                if current_turn_id:
                    events.append({"kind": "turn.complete", "turn_id": current_turn_id, "ts": ts})
                turn_counter += 1
                current_turn_id = f"hist-{turn_counter}"
                events.append({"kind": "turn.started", "turn_id": current_turn_id, "ts": ts})

                if reasoning:
                    events.append({"kind": "reasoning.delta", "turn_id": current_turn_id, "text": reasoning, "ts": ts})
                if content:
                    events.append({"kind": "message.delta", "turn_id": current_turn_id, "text": content, "ts": ts})

                if tool_calls and isinstance(tool_calls, list):
                    for i, tc in enumerate(tool_calls):
                        call_id = tc.get("id", f"call-hist-{turn_counter}-{i}")
                        func = tc.get("function", {})
                        tool_name = func.get("name", "unknown")
                        args_str = func.get("arguments", "")
                        tool_event = {
                            "kind": "tool.started",
                            "turn_id": current_turn_id,
                            "call_id": call_id,
                            "tool": tool_name,
                            "ts": ts,
                        }
                        # Parse args and build human-readable preview
                        try:
                            import json as _json
                            args_dict = _json.loads(args_str) if args_str else None
                            tool_event["args"] = args_dict
                            from agent.display import build_tool_preview
                            tool_event["preview"] = build_tool_preview(tool_name, args_dict or {}) or (args_str[:80] if args_str else None)
                        except Exception:
                            tool_event["preview"] = (args_str[:80] if args_str else None)
                        # Attach tool metadata for desktop frontend
                        try:
                            from tools.registry import registry
                            tool_event["tool_emoji"] = registry.get_emoji(tool_name, "")
                            tool_event["toolset"] = registry.get_toolset_for_tool(tool_name) or ""
                            from acp_adapter.tools import get_tool_kind
                            tool_event["tool_kind"] = get_tool_kind(tool_name)
                        except Exception:
                            pass
                        events.append(tool_event)
                    # Keep turn open — tool results will follow
                else:
                    # No tool calls — close turn immediately
                    events.append({"kind": "turn.complete", "turn_id": current_turn_id, "ts": ts})
                    current_turn_id = None

            elif role == "tool" or (role == "user" and tool_call_id):
                # Tool result — emit tool.completed within the open turn
                if current_turn_id:
                    events.append({
                        "kind": "tool.completed",
                        "turn_id": current_turn_id,
                        "call_id": tool_call_id or "call-hist-unknown",
                        "tool": msg.get("tool_name", "unknown"),
                        "duration": 0,
                        "error": False,
                        "output_preview": (content[:200] if content else None),
                        "ts": ts,
                    })

        # Close any dangling turn at the end
        if current_turn_id:
            events.append({"kind": "turn.complete", "turn_id": current_turn_id, "ts": ts})

        return events

    # ------------------------------------------------------------------
    # Media helpers (4A / 4B)
    # ------------------------------------------------------------------

    def _cache_file(self, src_path: str) -> str:
        """Copy a local file into ~/.hermes/cache/media/{sha256_first16}.{ext}.

        Returns the cached file path. Skips copy if already cached.
        """
        cache_dir = Path("~/.hermes/cache/media").expanduser()
        cache_dir.mkdir(parents=True, exist_ok=True)

        with open(src_path, "rb") as f:
            h = hashlib.sha256()
            while chunk := f.read(8192):
                h.update(chunk)
            file_hash = h.hexdigest()[:16]

        ext = Path(src_path).suffix
        cached = cache_dir / f"{file_hash}{ext}"
        if not cached.exists():
            shutil.copy2(src_path, cached)
        return str(cached)

    async def _post_process_media(
        self, session_id: str, turn_id: str, final_response: str,
    ) -> None:
        """Extract images, MEDIA: tags, and local files from agent response, broadcast structured envelopes."""
        # 1. Extract MEDIA: tagged paths (agent uses this for all file outputs)
        media_items, cleaned_text = BasePlatformAdapter.extract_media(final_response)
        for file_path, _is_voice in media_items:
            expanded = os.path.expanduser(file_path)
            if os.path.isfile(expanded):
                ext_lower = os.path.splitext(expanded)[1].lower()
                is_image = ext_lower in ('.png', '.jpg', '.jpeg', '.gif', '.webp')
                try:
                    cached_path = self._cache_file(expanded)
                    stat_info = os.stat(cached_path)
                    mime, _ = mimetypes.guess_type(expanded)
                    if is_image:
                        await self._broadcast_to_session(session_id, {
                            "kind": "content.image",
                            "turn_id": turn_id,
                            "url": f"hermes-media://{cached_path}",
                            "alt": os.path.basename(expanded),
                        })
                    else:
                        await self._broadcast_to_session(session_id, {
                            "kind": "content.file",
                            "turn_id": turn_id,
                            "name": os.path.basename(expanded),
                            "path": cached_path,
                            "size": stat_info.st_size,
                            "mime": mime,
                        })
                except Exception as e:
                    logger.warning("[desktop] _post_process_media: failed to cache MEDIA path %s: %s", file_path, e)
            else:
                logger.warning("[desktop] _post_process_media: MEDIA file not found: %s", file_path)

        # 2. Extract markdown/HTML image URLs from remaining text
        images, cleaned_text = BasePlatformAdapter.extract_images(cleaned_text)
        for url, alt in images:
            await self._broadcast_to_session(session_id, {
                "kind": "content.image",
                "turn_id": turn_id,
                "url": url,
                "alt": alt,
            })

        # 3. Extract bare local file paths
        local_files, _final_text = BasePlatformAdapter.extract_local_files(cleaned_text)
        for file_path in local_files:
            try:
                if not os.path.isfile(file_path):
                    logger.warning("[desktop] _post_process_media: file not found: %s", file_path)
                    continue
                cached_path = self._cache_file(file_path)
                stat = os.stat(cached_path)
                mime, _ = mimetypes.guess_type(file_path)
                await self._broadcast_to_session(session_id, {
                    "kind": "content.file",
                    "turn_id": turn_id,
                    "name": os.path.basename(file_path),
                    "path": cached_path,
                    "size": stat.st_size,
                    "mime": mime,
                })
            except Exception as e:
                logger.warning("[desktop] _post_process_media: failed to cache %s: %s", file_path, e)

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

        ws = web.WebSocketResponse(max_msg_size=16_777_216)  # 16 MB — accommodate base64 file attachments
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
            # Build dynamic data for welcome envelope
            welcome_commands = []
            welcome_toolsets = []
            welcome_skills = []
            try:
                from hermes_cli.commands import COMMAND_REGISTRY
                welcome_commands = [
                    {"name": c.name, "description": c.description,
                     "category": c.category, "args_hint": c.args_hint or None}
                    for c in COMMAND_REGISTRY
                    if not c.cli_only or c.gateway_config_gate
                ]
            except Exception:
                logger.debug("[desktop] could not load command registry for welcome")

            try:
                from hermes_cli.tools_config import CONFIGURABLE_TOOLSETS, _get_platform_tools
                from gateway.run import _load_gateway_config
                user_config = _load_gateway_config()
                enabled_set = set(_get_platform_tools(user_config, "desktop"))
                welcome_toolsets = [
                    {"id": ts_key, "label": ts_label, "enabled": ts_key in enabled_set}
                    for ts_key, ts_label, _desc in CONFIGURABLE_TOOLSETS
                ]
            except Exception:
                logger.debug("[desktop] could not load toolsets for welcome")

            try:
                from agent.skill_commands import get_skill_commands
                welcome_skills = [
                    {"slug": slug.lstrip("/"), "name": info["name"],
                     "description": info.get("description", "")}
                    for slug, info in get_skill_commands().items()
                ]
            except Exception:
                logger.debug("[desktop] could not load skills for welcome")

            welcome_models = {}
            try:
                welcome_models = await self._build_models_payload()
            except Exception:
                logger.debug("[desktop] could not load model catalog for welcome")

            # Send welcome
            try:
                from hermes_cli import __version__ as _hermes_version
            except ImportError:
                _hermes_version = "unknown"
            await conn.send(
                "welcome",
                capabilities=["approval", "reasoning", "tool_events", "interrupt", "markdown"],
                server={"version": VERSION, "hermes_version": _hermes_version},
                sessions=[
                    {"session_id": sid, "title": info["title"],
                     "created_at": info.get("created_at"),
                     "working_dir": info.get("working_dir", "")}
                    for sid, info in self._known_sessions.items()
                ],
                commands=welcome_commands,
                toolsets=welcome_toolsets,
                skills=welcome_skills,
                models=welcome_models,
                working_dir=os.environ.get("TERMINAL_CWD", ""),
            )

            # Message loop
            async for raw_msg in ws:
                if raw_msg.type == web.WSMsgType.TEXT:
                    if len(raw_msg.data) > 16_777_216:
                        await conn.send("error", code="PROTO_FRAME_TOO_LARGE",
                                        message="Envelope exceeds 16 MB limit")
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
            "session.delete": self._handle_session_delete,
            "session.rename": self._handle_session_rename,
            "session.subscribe": self._handle_session_subscribe,
            "prompt.send": self._handle_prompt_send,
            "approval.response": self._handle_approval_response,
            "clarify.response": self._handle_clarify_response,
            "turn.interrupt": self._handle_turn_interrupt,
            "model.switch": self._handle_model_switch,
            "memory.read": self._handle_memory_read,
            "memory.update": self._handle_memory_update,
            "session.search": self._handle_session_search,
            "team.stats": self._handle_team_stats,
            "config.get": self._handle_config_get,
            "config.set-default-model": self._handle_config_set_default_model,
            "config.set-credential": self._handle_config_set_credential,
            "config.set-endpoint": self._handle_config_set_endpoint,
            "config.delete-endpoint": self._handle_config_delete_endpoint,
            "config.update": self._handle_config_update,
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
            {"session_id": sid, "title": info["title"],
             "created_at": info.get("created_at"),
             "working_dir": info.get("working_dir", "")}
            for sid, info in self._known_sessions.items()
        ])

    async def _handle_session_new(self, conn: _Connection, msg: dict) -> None:
        # Validate working_dir
        working_dir = msg.get("working_dir", "")
        if not working_dir:
            await conn.send("session.new.error", code="INVALID_WORKING_DIR",
                            message="working_dir is required", ref_id=msg.get("id"))
            return
        if not os.path.isabs(working_dir):
            await conn.send("session.new.error", code="INVALID_WORKING_DIR",
                            message="working_dir must be an absolute path",
                            ref_id=msg.get("id"))
            return
        if not os.path.isdir(working_dir):
            await conn.send("session.new.error", code="DIR_NOT_FOUND",
                            message=f"Directory not found: {working_dir}",
                            ref_id=msg.get("id"))
            return

        session_id = f"sess-{uuid.uuid4().hex[:12]}"
        title = msg.get("title", "New Chat")
        created_at = time.time()
        self._known_sessions[session_id] = {
            "title": title, "created_at": created_at,
            "working_dir": working_dir,
        }
        self._session_histories.pop(session_id, None)

        # Persist to DB so empty sessions survive restarts
        db = self._ensure_session_db()
        if db:
            try:
                db.create_session(session_id=session_id, source="desktop")
            except Exception as e:
                logger.warning("[desktop] session.new create_session failed: %s", e)
            # Persist working_dir — must not be blocked by title errors
            try:
                def _persist_wd(conn):
                    conn.execute(
                        "INSERT OR REPLACE INTO desktop_session_meta (session_id, working_dir) VALUES (?, ?)",
                        (session_id, working_dir),
                    )
                db._execute_write(_persist_wd)
            except Exception as e:
                logger.warning("[desktop] session.new meta persist failed: %s", e)
            # Title is best-effort — set_session_title raises ValueError on
            # duplicate titles, which must not block the working_dir persist above.
            if title:
                try:
                    db.set_session_title(session_id, title)
                except Exception as e:
                    logger.debug("[desktop] session.new set_title failed (non-fatal): %s", e)

        await conn.send("session.new.ok", session={
            "session_id": session_id,
            "title": title,
            "created_at": created_at,
            "working_dir": working_dir,
        })

    async def _handle_session_delete(self, conn: _Connection, msg: dict) -> None:
        session_id = msg.get("session_id", "")
        if session_id not in self._known_sessions:
            await conn.send("error", code="SESSION_NOT_FOUND", ref_id=msg.get("id"),
                            message=f"Session {session_id} not found")
            return
        if session_id in self._active_turns:
            await conn.send("error", code="TURN_IN_PROGRESS", ref_id=msg.get("id"),
                            message=f"Cannot delete session {session_id} while a turn is running")
            return

        # Clean up in-memory state
        self._known_sessions.pop(session_id, None)
        self._session_buffers.pop(session_id, None)
        self._session_histories.pop(session_id, None)
        self._session_subscribers.pop(session_id, None)

        # Delete from DB
        db = self._ensure_session_db()
        if db:
            try:
                db.delete_session(session_id)
                # Clean up desktop-specific auxiliary table
                def _del_meta(conn):
                    conn.execute(
                        "DELETE FROM desktop_session_meta WHERE session_id = ?",
                        (session_id,),
                    )
                db._execute_write(_del_meta)
            except Exception as e:
                logger.warning("[desktop] session.delete DB failed: %s", e)

        # Broadcast to all connections (multi-window support)
        for c in list(self._connections.values()):
            try:
                await c.send("session.deleted", session_id=session_id)
            except Exception:
                pass

    async def _handle_session_rename(self, conn: _Connection, msg: dict) -> None:
        session_id = msg.get("session_id", "")
        title = msg.get("title", "").strip()
        if session_id not in self._known_sessions:
            await conn.send("error", code="SESSION_NOT_FOUND", ref_id=msg.get("id"),
                            message=f"Session {session_id} not found")
            return
        if not title or len(title) > 200:
            await conn.send("error", code="INVALID_TITLE", ref_id=msg.get("id"),
                            message="Title must be 1-200 characters")
            return

        # Update in-memory
        self._known_sessions[session_id]["title"] = title

        # Persist to DB
        db = self._ensure_session_db()
        if db:
            try:
                db.set_session_title(session_id, title)
            except ValueError as e:
                await conn.send("error", code="TITLE_CONFLICT", ref_id=msg.get("id"),
                                message=str(e))
                return
            except Exception as e:
                logger.warning("[desktop] session.rename DB failed: %s", e)

        # Broadcast session.update to all connections
        for c in list(self._connections.values()):
            try:
                await c.send("session.update", session_id=session_id, title=title)
            except Exception:
                pass

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

        # If ring buffer is empty (e.g. after gateway restart), rebuild
        # snapshot events from DB messages so the client can display history.
        if not events and since_seq == 0:
            db = self._ensure_session_db()
            if db:
                try:
                    db_msgs = db.get_messages(session_id)
                    if db_msgs:
                        events = self._db_messages_to_snapshot_events(db_msgs)
                        gap = False
                        logger.info("[desktop] rebuilt %d snapshot events from DB for session=%s",
                                    len(events), session_id)
                except Exception as e:
                    logger.warning("[desktop] failed to rebuild snapshot from DB: %s", e)

        working_dir = self._known_sessions.get(session_id, {}).get("working_dir", "")
        await conn.send("session.snapshot",
                        session_id=session_id,
                        events=events,
                        max_seq=buf.max_seq,
                        gap=gap,
                        working_dir=working_dir)
        logger.info("[desktop] subscribe session=%s since_seq=%s → snapshot(%d events, gap=%s)",
                    session_id, since_seq, len(events), gap)

        # Lazily restore conversation history for historical sessions
        if session_id not in self._session_histories:
            db = self._ensure_session_db()
            if db:
                try:
                    msgs = db.get_messages_as_conversation(session_id)
                    if msgs:
                        self._session_histories[session_id] = msgs
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Handler — model.switch
    # ------------------------------------------------------------------

    async def _handle_model_switch(self, conn: _Connection, msg: dict) -> None:
        session_id = msg.get("session_id", "")
        model_id = msg.get("model_id", "")
        provider_slug = msg.get("provider_slug", "")

        if not session_id or not model_id:
            await conn.send("model.switch.error", ref_id=msg.get("id"),
                            session_id=session_id,
                            code="PROTO_MISSING_FIELD",
                            message="session_id and model_id are required")
            return

        try:
            from hermes_cli.model_switch import switch_model as _switch_model
            from gateway.run import _load_gateway_config, _resolve_gateway_model

            cfg = _load_gateway_config()
            model_cfg = cfg.get("model", {})
            cfg_provider = (model_cfg.get("provider", "openrouter")
                            if isinstance(model_cfg, dict) else "openrouter")
            cur_provider = cfg_provider
            cur_model = _resolve_gateway_model(cfg)
            cfg_base_url = (model_cfg.get("base_url", "")
                            if isinstance(model_cfg, dict) else "")

            # Apply existing session override as baseline
            override = self._session_model_overrides.get(session_id, {})
            if override:
                cur_model = override.get("model", cur_model)
                cur_provider = override.get("provider", cur_provider) or cur_provider

            # Detect custom/local provider — these use a single endpoint
            # that serves multiple models, so switching should only change
            # the model name, NOT the provider/credentials.
            # "custom:<slug>" form (e.g. "custom:ikuncode" written by
            # hermes-desktop EndpointCard) is normalised back to "custom"
            # so it matches the local-provider set.
            _LOCAL_PROVIDERS = {"custom", "lmstudio", "ollama", "vllm", "llamacpp"}
            cfg_provider_root = cfg_provider.split(":", 1)[0] if cfg_provider else ""
            is_local = cfg_provider_root in _LOCAL_PROVIDERS

            if is_local:
                # For custom providers: just update the model name, keep
                # the current provider, base_url, and api_key intact.
                self._session_model_overrides[session_id] = {
                    "model": model_id,
                    "provider": "",   # empty = inherit from config
                    "api_key": "",
                    "base_url": "",
                    "api_mode": "",
                }
                await conn.send(
                    "model.switch.ok",
                    ref_id=msg.get("id"),
                    session_id=session_id,
                    model=model_id,
                    provider=cfg_provider,
                    provider_label=cfg_provider,
                )
                logger.info("[desktop] model.switch (local) session=%s → %s (%s)",
                            session_id, model_id, cfg_provider)
                return

            result = _switch_model(
                raw_input=model_id,
                current_provider=cur_provider,
                current_model=cur_model,
                explicit_provider=provider_slug or "",
            )

            if result.success:
                self._session_model_overrides[session_id] = {
                    "model": result.new_model,
                    "provider": result.target_provider,
                    "api_key": result.api_key or "",
                    "base_url": result.base_url or "",
                    "api_mode": getattr(result, "api_mode", "") or "",
                }
                await conn.send(
                    "model.switch.ok",
                    ref_id=msg.get("id"),
                    session_id=session_id,
                    model=result.new_model,
                    provider=result.target_provider,
                    provider_label=getattr(result, "provider_label", "") or result.target_provider,
                )
                logger.info("[desktop] model.switch session=%s → %s (%s)",
                            session_id, result.new_model, result.target_provider)
            else:
                await conn.send(
                    "model.switch.error",
                    ref_id=msg.get("id"),
                    session_id=session_id,
                    code="MODEL_SWITCH_FAILED",
                    message=result.error_message or "Unknown error",
                )
                logger.warning("[desktop] model.switch failed session=%s model=%s: %s",
                               session_id, model_id, result.error_message)
        except Exception as exc:
            logger.exception("[desktop] model.switch error")
            await conn.send(
                "model.switch.error",
                ref_id=msg.get("id"),
                session_id=session_id,
                code="MODEL_SWITCH_INTERNAL",
                message=str(exc),
            )

    # ------------------------------------------------------------------
    # Handlers — memory + session search
    # ------------------------------------------------------------------

    def _ensure_memory_store(self):
        """Lazily initialise and return the shared MemoryStore instance."""
        if self._memory_store is None:
            try:
                from tools.memory_tool import MemoryStore
                self._memory_store = MemoryStore()
                self._memory_store.load_from_disk()
            except Exception as e:
                logger.warning("[desktop] MemoryStore unavailable: %s", e)
        return self._memory_store

    async def _handle_memory_read(self, conn: _Connection, msg: dict) -> None:
        store = self._ensure_memory_store()
        if store is None:
            await conn.send("error", ref_id=msg.get("id"),
                            code="MEMORY_UNAVAILABLE",
                            message="MemoryStore not available")
            return
        store.load_from_disk()  # reload to pick up changes from turns
        await self._send_memory_state(conn, msg)

    async def _handle_memory_update(self, conn: _Connection, msg: dict) -> None:
        store = self._ensure_memory_store()
        if store is None:
            await conn.send("error", ref_id=msg.get("id"),
                            code="MEMORY_UNAVAILABLE",
                            message="MemoryStore not available")
            return
        action = msg.get("action", "")
        target = msg.get("target", "memory")
        if action not in ("add", "replace", "remove"):
            await conn.send("error", ref_id=msg.get("id"),
                            code="PROTO_INVALID_FIELD",
                            message=f"Invalid action: {action}")
            return
        if target not in ("memory", "user"):
            await conn.send("error", ref_id=msg.get("id"),
                            code="PROTO_INVALID_FIELD",
                            message=f"Invalid target: {target}")
            return
        try:
            if action == "add":
                result = store.add(target, msg.get("content", ""))
            elif action == "replace":
                result = store.replace(target, msg.get("old_text", ""), msg.get("new_text", ""))
            else:
                result = store.remove(target, msg.get("old_text", ""))
            if not result.get("success"):
                await conn.send("error", ref_id=msg.get("id"),
                                code="MEMORY_UPDATE_FAILED",
                                message=result.get("error", "Unknown error"))
                return
        except Exception as e:
            await conn.send("error", ref_id=msg.get("id"),
                            code="MEMORY_UPDATE_FAILED",
                            message=str(e)[:500])
            return
        await self._send_memory_state(conn, msg)

    async def _send_memory_state(self, conn: _Connection, msg: dict) -> None:
        """Send current memory state to the requesting connection."""
        store = self._memory_store
        mem = store._entries_for("memory")
        usr = store._entries_for("user")
        await conn.send(
            "memory.state",
            ref_id=msg.get("id"),
            memory_entries=mem,
            user_entries=usr,
            memory_char_limit=store.memory_char_limit,
            user_char_limit=store.user_char_limit,
            memory_chars_used=store._char_count("memory"),
            user_chars_used=store._char_count("user"),
        )

    async def _handle_session_search(self, conn: _Connection, msg: dict) -> None:
        query = msg.get("query", "")
        limit = min(msg.get("limit", 5), 5)
        db = self._ensure_session_db()
        if db is None:
            await conn.send("error", ref_id=msg.get("id"),
                            code="DB_UNAVAILABLE",
                            message="Session database not available")
            return
        try:
            from tools.session_search_tool import session_search
            result_json = await asyncio.get_event_loop().run_in_executor(
                None, lambda: session_search(query=query, limit=limit, db=db)
            )
            parsed = json.loads(result_json)
            results = parsed.get("results", [])
            await conn.send(
                "session.search.ok",
                ref_id=msg.get("id"),
                results=[{
                    "session_id": r.get("session_id", ""),
                    "title": r.get("title", ""),
                    "date": r.get("when", r.get("date", "")),
                    "summary": r.get("summary", ""),
                    "match_count": r.get("match_count", 0),
                } for r in results],
            )
        except Exception as e:
            logger.exception("[desktop] session.search error")
            await conn.send("error", ref_id=msg.get("id"),
                            code="SEARCH_ERROR",
                            message=str(e)[:500])

    async def _handle_team_stats(self, conn: _Connection, msg: dict) -> None:
        """Query delegation_log for team statistics within a date range."""
        team_id = msg.get("team_id")  # optional — None means all teams
        start = msg.get("start")      # epoch float, optional
        end = msg.get("end")          # epoch float, optional
        db = self._ensure_session_db()
        if db is None:
            await conn.send("error", ref_id=msg.get("id"),
                            code="DB_UNAVAILABLE",
                            message="Session database not available")
            return
        try:
            def _query():
                clauses = []
                params = []
                if team_id:
                    clauses.append("team_id = ?")
                    params.append(team_id)
                if start:
                    clauses.append("created_at >= ?")
                    params.append(float(start))
                if end:
                    clauses.append("created_at <= ?")
                    params.append(float(end))
                where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
                with db._lock:
                    cursor = db._conn.execute(
                        f"""SELECT session_id, turn_id, call_id, team_id,
                                   role_id, role_name, goal, source,
                                   duration, error, output_preview, created_at
                            FROM delegation_log{where}
                            ORDER BY created_at DESC LIMIT 200""",
                        params,
                    )
                    cols = [d[0] for d in cursor.description]
                    return [dict(zip(cols, row)) for row in cursor.fetchall()]

            results = await asyncio.get_event_loop().run_in_executor(
                None, _query
            )
            # Compute summary stats
            total = len(results)
            errors = sum(1 for r in results if r.get("error"))
            total_duration = sum(r.get("duration", 0) for r in results)
            await conn.send(
                "team.stats.ok",
                ref_id=msg.get("id"),
                stats={
                    "total": total,
                    "errors": errors,
                    "total_duration": round(total_duration, 2),
                    "avg_duration": round(total_duration / total, 2) if total else 0,
                },
                entries=results[:100],  # cap response size
            )
        except Exception as e:
            logger.exception("[desktop] team.stats error")
            await conn.send("error", ref_id=msg.get("id"),
                            code="TEAM_STATS_ERROR",
                            message=str(e)[:500])

    # ------------------------------------------------------------------
    # Handlers — prompt.send + agent runner
    # ------------------------------------------------------------------

    async def _handle_prompt_send(self, conn: _Connection, msg: dict) -> None:
        session_id = msg.get("session_id", "")
        content = msg.get("content", "")
        model_override = msg.get("model")

        if not session_id:
            await conn.send("error", code="PROTO_MISSING_FIELD", ref_id=msg.get("id"),
                            message="session_id is required")
            return
        if not content and not msg.get("attachments"):
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

        # --- File upload (4B) ---
        ALLOWED_EXTENSIONS = {
            ".png", ".jpg", ".jpeg", ".gif", ".webp",
            ".pdf", ".md", ".txt", ".csv", ".json",
            ".yaml", ".yml", ".py", ".js", ".ts",
            ".html", ".css",
        }
        MAX_UPLOAD_SIZE = 10 * 1024 * 1024  # 10 MB

        attachments = msg.get("attachments", [])
        file_refs: list[str] = []
        upload_dir = Path("~/.hermes/cache/uploads").expanduser()

        for att in attachments:
            name = att.get("name", "")
            data_b64 = att.get("data", "")
            if not name or not data_b64:
                continue

            ext = os.path.splitext(name)[1].lower()
            if ext not in ALLOWED_EXTENSIONS:
                await conn.send("error", code="FILE_TYPE_NOT_ALLOWED",
                                ref_id=msg.get("id"),
                                message=f"File type {ext} is not allowed")
                return

            try:
                raw = base64.b64decode(data_b64)
            except Exception:
                await conn.send("error", code="FILE_DECODE_ERROR",
                                ref_id=msg.get("id"),
                                message=f"Failed to decode base64 for {name}")
                return

            if len(raw) > MAX_UPLOAD_SIZE:
                await conn.send("error", code="FILE_TOO_LARGE",
                                ref_id=msg.get("id"),
                                message=f"File {name} exceeds 10 MB limit")
                return

            upload_dir.mkdir(parents=True, exist_ok=True)
            saved_name = f"{uuid.uuid4().hex}_{name}"
            saved_path = str(upload_dir / saved_name)
            with open(saved_path, "wb") as f:
                f.write(raw)

            await conn.send("file.upload.ok", name=name, path=saved_path)
            file_refs.append(f"[附件: {name} → {saved_path}]")
            logger.info("[desktop] file.upload.ok name=%s path=%s size=%d",
                        name, saved_path, len(raw))

        # Prepend file references to user content
        if file_refs:
            prefix = "\n".join(file_refs) + "\n\n"
            content = prefix + content

        turn_id = f"turn-{uuid.uuid4().hex[:12]}"
        session_key = f"desktop:{session_id}"

        # D36: explicit rejection if both workflow and mentions are present
        workflow = msg.get("workflow")
        mentions = msg.get("mentions")
        team_id = msg.get("team_id")
        if workflow and mentions:
            await conn.send("error", code="PROTO_CONFLICT", ref_id=msg.get("id"),
                            message="Cannot combine workflow and mentions in same prompt.send")
            return

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

        # --- Tool output streaming ---
        debouncer = _OutputDebouncer(self._broadcast_to_session, session_id, turn_id, loop)
        active.debouncer = debouncer

        def _on_tool_output(call_id, stream, text):
            """Receive intermediate tool output (stdout/stderr/progress)."""
            if not text:
                return
            debouncer.push(call_id, stream, text)

        # --- Iteration progress ---
        def _on_step(iteration, prev_tools):
            """Receive iteration progress from agent loop."""
            from agent.display import _detect_tool_failure
            tool_summaries = []
            for t in (prev_tools or []):
                name = t.get("name", "?")
                result = t.get("result")
                is_err, _ = _detect_tool_failure(name, result)
                tool_summaries.append({
                    "name": name,
                    "result": "error" if is_err else
                              "ok" if result is not None else None,
                })
            asyncio.run_coroutine_threadsafe(
                self._broadcast_to_session(session_id, {
                    "kind": "turn.progress",
                    "turn_id": turn_id,
                    "iteration": iteration,
                    "max_iterations": 90,  # updated after agent creation
                    "status": "calling model",
                    "prev_tools": tool_summaries,
                }), loop
            )

        # Auto-generate call_id for tool events since upstream AIAgent
        # doesn't provide one. Tracks current tool_name → call_id mapping
        # so tool.completed can reference the same id as tool.started.
        _tool_call_seq = 0
        _active_tool_calls: dict[str, str] = {}  # tool_name → call_id
        _delegation_meta: dict[str, dict] = {}   # call_id → {role_id, role_name, goal}

        def _on_tool_progress(event_type, tool_name=None, preview=None, args=None, **kw):
            nonlocal _tool_call_seq

            # ── Delegation interception ──
            # Detect delegate_task tool calls and emit delegation-specific
            # envelopes so the desktop client can render DelegationCard UI.
            if event_type == "delegation.progress":
                # Relayed from child agent's step_callback via
                # _build_child_progress_callback._step_cb.
                # call_id matches the active delegate_task tool call so the
                # renderer (turns.ts applyEvent) can locate the correct
                # DelegationState. Batch mode: all concurrent children share
                # the parent's call_id (one card per delegate_task call).
                env = {
                    "kind": "delegation.progress",
                    "turn_id": turn_id,
                    "call_id": _active_tool_calls.get("delegate_task"),
                    "iteration": kw.get("iteration", 0),
                    "prev_tools": kw.get("prev_tools", []),
                    "task_index": kw.get("task_index", 0),
                }
                asyncio.run_coroutine_threadsafe(
                    self._broadcast_to_session(session_id, env, skip_buffer=True), loop
                )
                return

            if tool_name == "delegate_task" and event_type == "tool.started":
                _tool_call_seq += 1
                call_id = kw.get("call_id") or f"call-{turn_id[-8:]}-{_tool_call_seq}"
                _active_tool_calls[tool_name] = call_id
                # Extract role_id from context parameter (AI instructed to include it)
                role_id = None
                role_name = None
                if args and isinstance(args, dict):
                    context_str = args.get("context") or ""
                    if isinstance(context_str, str):
                        for line in context_str.split("\n")[:3]:
                            stripped = line.strip()
                            if stripped.startswith("role_id:"):
                                role_id = stripped[8:].strip()
                                break
                    # Fallback: direct field (for workflow mode or future use)
                    if not role_id:
                        role_id = args.get("_role_id")
                # Lookup role_name from stored mentions
                mention = None
                if role_id and active.mentions:
                    mention = active.mentions.get(role_id)
                    if mention:
                        role_name = mention.get("role_name")
                # Phase 8: prefer mention.model (authoritative per-role config
                # from the client) over the AI-provided tool kwarg.  Fallback
                # to args.model lets AI-authored one-off overrides still flow.
                mention_model = None
                if isinstance(mention, dict):
                    _mm = mention.get("model")
                    if isinstance(_mm, str) and _mm.strip():
                        mention_model = _mm.strip()
                env = {
                    "kind": "delegation.started",
                    "turn_id": turn_id,
                    "call_id": call_id,
                    "source": "ai",
                    "goal": (args or {}).get("goal", ""),
                    "role_id": role_id,
                    "role_name": role_name,
                    "model": mention_model or (args or {}).get("model"),
                }
                asyncio.run_coroutine_threadsafe(
                    self._broadcast_to_session(session_id, env), loop
                )
                # Cache metadata for completion handler (args is None at tool.completed)
                _delegation_meta[call_id] = {
                    "role_id": role_id, "role_name": role_name,
                    "goal": (args or {}).get("goal", ""),
                }
                # Also emit standard tool.started for ToolCard tracking
                env_tool = {"kind": "tool.started", "turn_id": turn_id,
                            "call_id": call_id, "tool": tool_name,
                            "preview": preview, "args": args}
                asyncio.run_coroutine_threadsafe(
                    self._broadcast_to_session(session_id, env_tool), loop
                )
                return

            if tool_name == "delegate_task" and event_type == "tool.completed":
                call_id = kw.get("call_id") or _active_tool_calls.pop(tool_name, f"call-{turn_id[-8:]}-0")
                duration = round(kw.get("duration", 0), 3)
                is_error = kw.get("is_error", False)
                output_preview = kw.get("output_preview")
                # Lookup metadata cached at delegation.started (args is None at completion)
                meta = _delegation_meta.pop(call_id, {})
                _del_role_id = meta.get("role_id")
                _del_role_name = meta.get("role_name")
                _del_goal = meta.get("goal", "")
                # Write to delegation log with team context
                self._write_delegation_log(
                    session_id=session_id, turn_id=turn_id,
                    call_id=call_id, duration=duration,
                    error=is_error, output_preview=output_preview,
                    team_id=team_id,
                    role_id=_del_role_id,
                    role_name=_del_role_name,
                    goal=_del_goal,
                    source="ai",
                )

                async def _flush_then_complete_delegation(_cid=call_id, _dur=duration, _err=is_error, _op=output_preview,
                                                          _rid=_del_role_id, _rn=_del_role_name):
                    await debouncer.flush_all(_cid)
                    await self._broadcast_to_session(session_id, {
                        "kind": "delegation.completed", "turn_id": turn_id,
                        "call_id": _cid, "source": "ai",
                        "duration": _dur, "error": _err,
                        "output_preview": _op,
                        "role_id": _rid, "role_name": _rn,
                    })
                    # Also emit standard tool.completed
                    await self._broadcast_to_session(session_id, {
                        "kind": "tool.completed", "turn_id": turn_id,
                        "call_id": _cid, "tool": "delegate_task",
                        "duration": _dur, "error": _err,
                        "output_preview": _op,
                    })
                asyncio.run_coroutine_threadsafe(_flush_then_complete_delegation(), loop)
                return

            # ── Standard tool handling ──
            if event_type == "tool.started":
                _tool_call_seq += 1
                call_id = kw.get("call_id") or f"call-{turn_id[-8:]}-{_tool_call_seq}"
                _active_tool_calls[tool_name or ""] = call_id
                env = {"kind": "tool.started", "turn_id": turn_id,
                       "call_id": call_id, "tool": tool_name,
                       "preview": preview, "args": args}
                # Attach tool metadata for desktop frontend
                try:
                    from tools.registry import registry
                    env["tool_emoji"] = registry.get_emoji(tool_name or "", "")
                    env["toolset"] = registry.get_toolset_for_tool(tool_name or "") or ""
                    from acp_adapter.tools import get_tool_kind
                    env["tool_kind"] = get_tool_kind(tool_name or "")
                except Exception:
                    pass  # Don't break tool execution if metadata lookup fails
            elif event_type == "tool.completed":
                call_id = kw.get("call_id") or _active_tool_calls.pop(tool_name or "", f"call-{turn_id[-8:]}-0")
                # Combined coroutine: flush remaining output, THEN broadcast completed.
                # Using a single coroutine guarantees ordering — the flush awaits
                # before the completed envelope is sent to subscribers.
                async def _flush_then_complete(_cid=call_id, _tn=tool_name, _kw=kw):
                    await debouncer.flush_all(_cid)
                    await self._broadcast_to_session(session_id, {
                        "kind": "tool.completed", "turn_id": turn_id,
                        "call_id": _cid, "tool": _tn,
                        "duration": round(_kw.get("duration", 0), 3),
                        "error": _kw.get("is_error", False),
                        "output_preview": _kw.get("output_preview"),
                    })
                asyncio.run_coroutine_threadsafe(_flush_then_complete(), loop)
                return  # Skip default broadcast — handled above
            elif event_type == "reasoning.available":
                # SKIP — upstream sends assistant_message.content (the response
                # text) as "reasoning.available", which is wrong for our use case.
                # Real reasoning arrives via reasoning_callback below.
                return
            else:
                return
            asyncio.run_coroutine_threadsafe(
                self._broadcast_to_session(session_id, env), loop
            )

        def _on_reasoning(text):
            """Receive streaming reasoning/thinking tokens from the model."""
            if not text:
                return
            asyncio.run_coroutine_threadsafe(
                self._broadcast_to_session(session_id, {
                    "kind": "reasoning.delta", "turn_id": turn_id, "text": text,
                }), loop
            )

        # Set TERMINAL_CWD BEFORE agent creation — AIAgent.__init__
        # snapshots this value at construction time (run_agent.py:1178).
        session_working_dir = self._known_sessions.get(
            session_id, {}
        ).get("working_dir", "")
        if session_working_dir:
            os.environ["TERMINAL_CWD"] = session_working_dir

        # Store user message in ring buffer so snapshot replay includes it
        await self._broadcast_to_session(session_id, {
            "kind": "user.message", "session_id": session_id,
            "turn_id": turn_id, "content": content,
        })

        await self._broadcast_to_session(session_id, {
            "kind": "turn.started", "session_id": session_id, "turn_id": turn_id,
        })

        # === WORKFLOW MODE: deterministic DAG execution ===
        if workflow:
            from gateway.platforms.workflow_engine import WorkflowEngine
            engine = WorkflowEngine(self, session_id, turn_id, loop, team_id=team_id)
            active.workflow_engine = engine

            async def _run_workflow():
                try:
                    results = await engine.execute(workflow, content)
                    # Synthesize summary from step outputs
                    summary_parts = [
                        f"**{sid}**: {text[:100]}..."
                        for sid, text in results.items()
                        if not text.startswith("Error:")
                    ]
                    summary = "Workflow completed.\n\n" + "\n".join(summary_parts)
                    await self._broadcast_to_session(session_id, {
                        "kind": "message.delta", "turn_id": turn_id, "text": summary,
                    })
                    await self._broadcast_to_session(session_id, {
                        "kind": "turn.complete", "turn_id": turn_id,
                        "session_id": session_id,
                        "usage": engine._usage_totals,
                    })
                except Exception as e:
                    logger.exception("[desktop] workflow error session=%s", session_id)
                    await self._broadcast_to_session(session_id, {
                        "kind": "turn.error", "turn_id": turn_id,
                        "session_id": session_id,
                        "code": "WORKFLOW_ERROR", "message": str(e),
                    })
                finally:
                    self._active_turns.pop(session_id, None)

            active.task = asyncio.create_task(_run_workflow())
            logger.info("[desktop] workflow.send session=%s turn=%s steps=%d",
                        session_id, turn_id, len(workflow.get("steps", [])))
            return

        # === AI-DRIVEN MODE ===
        ephemeral_prompt = None
        if mentions:
            ephemeral_prompt = self._build_mentions_prompt(mentions)
            active.mentions = {
                m.get("role_id", ""): m for m in mentions if m.get("role_id")
            }

        # Phase 6 Slice 5: hermes_fs error code guide for LLM.
        # Appended to every turn so the LLM translates fs error codes
        # into user-friendly Chinese replies instead of echoing raw codes.
        ephemeral_prompt = (ephemeral_prompt or "") + _HERMES_FS_ERROR_GUIDE
        ephemeral_prompt = ephemeral_prompt.strip() or None

        agent = self._create_agent_for_turn(
            session_id=session_id,
            stream_delta_callback=_on_delta,
            tool_progress_callback=_on_tool_progress,
            reasoning_callback=_on_reasoning,
            clarify_callback=self._make_clarify_callback(session_id, turn_id, loop),
            step_callback=_on_step,
            model_override=model_override,
            ephemeral_system_prompt=ephemeral_prompt,
        )
        # Assign tool output callback as an attribute rather than __init__ kwarg,
        # so AIAgent.__init__ stays identical to upstream. The parallel/sequential
        # tool execution paths in run_agent.py read this via getattr() and set
        # thread-local state within each worker before invoking the tool.
        agent.tool_output_callback = _on_tool_output
        active.agent = agent

        active.task = asyncio.create_task(
            self._run_agent_async(active, user_message=content)
        )
        logger.info("[desktop] prompt.send session=%s turn=%s len=%d",
                     session_id, turn_id, len(content))

    def _create_agent_for_turn(self, session_id, stream_delta_callback=None,
                                tool_progress_callback=None,
                                reasoning_callback=None,
                                clarify_callback=None,
                                ephemeral_system_prompt=None,
                                step_callback=None,
                                model_override=None):
        """Create an AIAgent instance — mirrors api_server.py:404 pattern.

        Model resolution precedence:
          1. model_override (per-message from prompt.send) — resolved via switch_model()
          2. _session_model_overrides (from model.switch envelope)
          3. _resolve_gateway_model() (config.yaml default)
        """
        from run_agent import AIAgent
        from gateway.run import (
            _resolve_runtime_agent_kwargs, _resolve_gateway_model,
            _load_gateway_config, GatewayRunner,
        )
        from hermes_cli.tools_config import _get_platform_tools

        runtime_kwargs = _resolve_runtime_agent_kwargs()
        user_config = _load_gateway_config()
        # Mirror GatewayRunner.run_agent paths (gateway/run.py:5739/5907/8527):
        # without reasoning_config, anthropic_adapter._build_api_kwargs skips the
        # `thinking` kwarg and the model never streams reasoning tokens, leaving
        # the desktop UI with empty 思考过程 blocks. Reads agent.reasoning_effort
        # from config.yaml each turn so UI changes via config.update apply live.
        reasoning_config = GatewayRunner._load_reasoning_config()
        # Desktop default-on policy: 沸点 产品定位"开箱体验 AI 思考",空 effort →
        # parse_reasoning_effort()=None → 默认禁用 thinking,是 CLI 时代的行为继承。
        # 桌面端覆盖此默认:None → medium(只覆盖未设值,用户显式 "none" 仍走
        # {"enabled": False} 关闭分支)。这样老 PVC 上 seed 早于 reasoning_effort
        # 字段引入的用户也能立刻体验思考过程,无需手动改 config.yaml。
        if reasoning_config is None:
            reasoning_config = {"enabled": True, "effort": "medium"}
        service_tier = GatewayRunner._load_service_tier()
        session_override = self._session_model_overrides.get(session_id, {})

        if model_override:
            # Per-message override — for custom/local providers, just use the
            # model name as-is (the endpoint serves multiple models).
            # For cloud providers, resolve through the alias system.
            model_cfg = user_config.get("model", {})
            cfg_provider = (model_cfg.get("provider") if isinstance(model_cfg, dict) else "openrouter") or "openrouter"
            _LOCAL_PROVIDERS = {"custom", "lmstudio", "ollama", "vllm", "llamacpp"}
            if cfg_provider in _LOCAL_PROVIDERS:
                model = model_override
            else:
                try:
                    from hermes_cli.model_switch import switch_model as _switch_model
                    cur_prov = (session_override.get("provider") or cfg_provider)
                    cur_mod = session_override.get("model") or _resolve_gateway_model(user_config)
                    result = _switch_model(
                        raw_input=model_override,
                        current_provider=cur_prov,
                        current_model=cur_mod,
                    )
                    if result.success:
                        model = result.new_model
                        if result.target_provider:
                            runtime_kwargs["provider"] = result.target_provider
                        if result.api_key:
                            runtime_kwargs["api_key"] = result.api_key
                        if result.base_url:
                            runtime_kwargs["base_url"] = result.base_url
                        if result.api_mode:
                            runtime_kwargs["api_mode"] = result.api_mode
                    else:
                        logger.warning("[desktop] model override '%s' resolution failed: %s",
                                       model_override, result.error_message)
                        model = _resolve_gateway_model(user_config)
                except Exception as exc:
                    logger.warning("[desktop] model override resolution error: %s", exc)
                    model = _resolve_gateway_model(user_config)
        elif session_override:
            model = session_override["model"]
            if session_override.get("provider"):
                runtime_kwargs["provider"] = session_override["provider"]
            if session_override.get("api_key"):
                runtime_kwargs["api_key"] = session_override["api_key"]
            if session_override.get("base_url"):
                runtime_kwargs["base_url"] = session_override["base_url"]
            if session_override.get("api_mode"):
                runtime_kwargs["api_mode"] = session_override["api_mode"]
        else:
            model = _resolve_gateway_model(user_config)

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
            reasoning_callback=reasoning_callback,
            clarify_callback=clarify_callback,
            step_callback=step_callback,
            reasoning_config=reasoning_config,
            service_tier=service_tier,
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

        # Set both contextvar AND env var for session key propagation.
        # Contextvar doesn't reliably propagate through run_in_executor,
        # so the env var serves as fallback (same pattern as upstream run.py:6633).
        approval_token = set_current_session_key(session_key)
        os.environ["HERMES_SESSION_KEY"] = session_key

        def _notify(approval_data: dict) -> None:
            """Forward approval request to connected desktop clients.

            Called from the agent thread (sync); must schedule the async
            send_exec_approval on the event loop and block until it completes
            — mirroring upstream run.py:_approval_notify_sync.
            """
            cmd = approval_data.get("command", "")
            desc = approval_data.get("description", "dangerous command")
            try:
                asyncio.run_coroutine_threadsafe(
                    self.send_exec_approval(
                        chat_id=session_id,
                        command=cmd,
                        session_key=session_key,
                        description=desc,
                    ),
                    loop,
                ).result(timeout=15)
            except Exception as exc:
                logger.error("[desktop] send_exec_approval failed: %s", exc)

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
                history = self._session_histories.get(session_id)
                # Phase 8: expose active.mentions to the agent so delegate_task
                # can look up per-role {model, provider_slug} from PromptSendMsg
                # and override the spawned child's runtime credentials.
                active.agent._delegation_mentions = active.mentions or {}

                # Phase 6: propagate session_id into the executor thread so
                # tools.mcp_tool._make_tool_handler can inject it into
                # _meta.session_id for hermes_fs.* MCP calls. set/clear must
                # run INSIDE the executor target — ContextVar does not
                # reliably cross run_in_executor (L1991-1992) and env vars
                # would race across the up-to-max_conn concurrent turns
                # the semaphore allows. clear in finally because the
                # ThreadPoolExecutor reuses worker threads across turns.
                def _run_with_session_id():
                    set_session_id(session_id)
                    try:
                        return active.agent.run_conversation(
                            user_message=user_message,
                            conversation_history=history,
                            task_id=session_id,
                        )
                    finally:
                        clear_session_id()

                result = await loop.run_in_executor(None, _run_with_session_id)
                # Capture full message history for next turn
                if result and isinstance(result, dict) and "messages" in result:
                    self._session_histories[session_id] = result["messages"]
                # final_response is skipped — content was already streamed
                # via _on_delta callback during agent execution.

                # Auto-generate session title after first exchange (non-blocking).
                # desktop.py bypasses GatewayRunner which normally calls this,
                # so we invoke it directly here.
                all_msgs = result.get("messages", []) if result and isinstance(result, dict) else []
                final_response = result.get("final_response", "") if result and isinstance(result, dict) else ""
                if not final_response:
                    # Fallback: extract last assistant message from history
                    for m in reversed(all_msgs):
                        if m.get("role") == "assistant" and m.get("content"):
                            final_response = m["content"]
                            break
                if final_response:
                    db = self._ensure_session_db()
                    if db:
                        try:
                            from agent.title_generator import maybe_auto_title
                            maybe_auto_title(db, session_id, user_message, final_response, all_msgs)
                        except Exception:
                            pass

                # Immediate title from user message — zero latency, no LLM dependency.
                # Only when title is still the default "New Chat" (first exchange).
                # Writes only to _known_sessions, NOT session_db, so maybe_auto_title
                # (which checks session_db) can still generate a better LLM title.
                session_info = self._known_sessions.get(session_id, {})
                if session_info.get("title") in ("New Chat", "Untitled"):
                    simple = user_message.strip()
                    if len(simple) > 40:
                        cut = simple[:40].rfind(" ")
                        simple = (simple[:cut] if cut > 10 else simple[:40]) + "…"
                    if simple:
                        session_info["title"] = simple
                        await self._broadcast_to_session(session_id, {
                            "kind": "session.update",
                            "session_id": session_id,
                            "title": simple,
                        })

                # Extract media (images, local files) from agent response
                if final_response:
                    await self._post_process_media(session_id, turn_id, final_response)

                usage = {
                    "prompt_tokens": getattr(active.agent, "session_prompt_tokens", 0) or 0,
                    "completion_tokens": getattr(active.agent, "session_completion_tokens", 0) or 0,
                    "model": getattr(active.agent, "model", "unknown"),
                }
                # Cancel debouncer before broadcasting turn.complete to prevent
                # stale output deltas from arriving after the turn has ended.
                if active.debouncer:
                    active.debouncer.cancel_all()
                await self._broadcast_to_session(session_id, {
                    "kind": "turn.complete", "turn_id": turn_id, "usage": usage,
                })
                # Schedule delayed title check — gives title_generator thread
                # time to complete (generate_title has 30s timeout, so wait 35s).
                asyncio.get_running_loop().call_later(
                    35.0,
                    lambda sid=session_id: asyncio.ensure_future(
                        self._check_and_push_title(sid)
                    ),
                )
            except Exception as exc:
                logger.exception("[desktop] turn %s failed", turn_id)
                if active.debouncer:
                    active.debouncer.cancel_all()
                await self._broadcast_to_session(session_id, {
                    "kind": "turn.error", "turn_id": turn_id,
                    "code": "AGENT_EXCEPTION", "message": str(exc)[:500],
                })
            finally:
                # Phase 8: clear per-turn delegation mentions to prevent leakage
                # into a subsequent turn on the same agent instance.
                try:
                    active.agent._delegation_mentions = None
                except Exception:
                    pass
                unregister_gateway_notify(session_key)
                reset_current_session_key(approval_token)
                os.environ.pop("HERMES_SESSION_KEY", None)
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
    # Handlers — clarify
    # ------------------------------------------------------------------

    def _make_clarify_callback(self, session_id: str, turn_id: str, loop: asyncio.AbstractEventLoop):
        """Create a clarify callback for an agent turn.

        Returns a sync callback (question, choices) -> str that blocks the
        agent thread until the desktop client responds or 120s timeout.
        """
        def _callback(question, choices):
            request_id = f"clar-{uuid.uuid4().hex[:10]}"
            event = threading.Event()
            self._pending_clarifies[request_id] = event

            # Broadcast clarify.request to subscribed clients
            try:
                asyncio.run_coroutine_threadsafe(
                    self._broadcast_to_session(session_id, {
                        "kind": "clarify.request",
                        "turn_id": turn_id,
                        "request_id": request_id,
                        "question": question,
                        "choices": choices,
                    }),
                    loop,
                ).result(timeout=5)
            except Exception as exc:
                logger.error("[desktop] clarify.request broadcast failed: %s", exc)
                self._pending_clarifies.pop(request_id, None)
                return "(发送失败，请自行判断)"

            logger.info("[desktop] clarify.request session=%s req=%s question=%r",
                         session_id, request_id, question[:80])

            # Block agent thread until client responds (120s timeout)
            resolved = event.wait(timeout=120)
            self._pending_clarifies.pop(request_id, None)
            answer = self._clarify_results.pop(request_id, None)

            if not resolved or answer is None:
                # Broadcast timeout resolved
                asyncio.run_coroutine_threadsafe(
                    self._broadcast_to_session(session_id, {
                        "kind": "clarify.resolved",
                        "request_id": request_id,
                        "answer": "",
                        "timed_out": True,
                    }),
                    loop,
                )
                logger.info("[desktop] clarify timeout session=%s req=%s", session_id, request_id)
                return "(用户未回答，请自行判断)"

            return answer

        return _callback

    async def _handle_clarify_response(self, conn: _Connection, msg: dict) -> None:
        """Handle clarify.response from client — unblock the waiting agent thread."""
        request_id = msg.get("request_id", "")
        answer = msg.get("answer", "")

        event = self._pending_clarifies.get(request_id)
        if not event:
            return  # already resolved or timed out

        self._clarify_results[request_id] = answer
        event.set()

        # Broadcast clarify.resolved to all subscribers
        sid = conn.subscribed_session_id
        if sid:
            await self._broadcast_to_session(sid, {
                "kind": "clarify.resolved",
                "request_id": request_id,
                "answer": answer,
            })
        logger.info("[desktop] clarify.resolved session=%s req=%s answer=%r",
                     sid, request_id, answer[:80] if answer else "")

    # ------------------------------------------------------------------
    # Handlers — interrupt
    # ------------------------------------------------------------------

    async def _handle_turn_interrupt(self, conn: _Connection, msg: dict) -> None:
        session_id = msg.get("session_id", "")
        active = self._active_turns.get(session_id)
        if active:
            if active.debouncer:
                active.debouncer.cancel_all()
            # Workflow mode: cancel the engine (interrupts all step agents)
            if active.workflow_engine:
                try:
                    active.workflow_engine.cancel()
                except Exception:
                    pass
            if active.agent:
                try:
                    active.agent.interrupt("user interrupt")
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Broadcast
    # ------------------------------------------------------------------

    async def _broadcast_to_session(
        self, session_id: str, envelope: dict, *, skip_buffer: bool = False
    ) -> None:
        """Fan-out one envelope to every subscriber of a session.

        Args:
            skip_buffer: If True, skip ring buffer write. Used for
                ephemeral envelopes like tool.output.delta.
        """
        stamped = {"v": 1, "ts": time.time(), **envelope}
        if not skip_buffer:
            buf = self._session_buffers[session_id]
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

    # ------------------------------------------------------------------
    # Handlers — config management
    # ------------------------------------------------------------------

    _BUILTIN_PROVIDERS = [
        {"slug": "anthropic", "name": "Anthropic", "env_var": "ANTHROPIC_API_KEY", "group": "international"},
        {"slug": "openai", "name": "OpenAI", "env_var": "OPENAI_API_KEY", "group": "international"},
        {"slug": "gemini", "name": "Google / Gemini", "env_var": "GOOGLE_API_KEY", "group": "international"},
        {"slug": "openrouter", "name": "OpenRouter", "env_var": "OPENROUTER_API_KEY", "group": "international"},
        {"slug": "deepseek", "name": "DeepSeek", "env_var": "DEEPSEEK_API_KEY", "group": "international"},
        {"slug": "huggingface", "name": "Hugging Face", "env_var": "HF_TOKEN", "group": "international"},
        {"slug": "zai", "name": "z.ai / GLM", "env_var": "GLM_API_KEY", "group": "china"},
        {"slug": "kimi-coding", "name": "Kimi (国际)", "env_var": "KIMI_API_KEY", "group": "china"},
        {"slug": "kimi-coding-cn", "name": "Kimi (国内)", "env_var": "KIMI_CN_API_KEY", "group": "china"},
        {"slug": "minimax", "name": "MiniMax (国际)", "env_var": "MINIMAX_API_KEY", "group": "china"},
        {"slug": "minimax-cn", "name": "MiniMax (国内)", "env_var": "MINIMAX_CN_API_KEY", "group": "china"},
        {"slug": "alibaba", "name": "阿里 / DashScope", "env_var": "DASHSCOPE_API_KEY", "group": "china"},
        {"slug": "xiaomi", "name": "小米 / MiMo", "env_var": "XIAOMI_API_KEY", "group": "china"},
        {"slug": "arcee", "name": "Arcee", "env_var": "ARCEEAI_API_KEY", "group": "other"},
        {"slug": "kilocode", "name": "KiloCode", "env_var": "KILOCODE_API_KEY", "group": "other"},
        {"slug": "opencode-zen", "name": "OpenCode Zen", "env_var": "OPENCODE_ZEN_API_KEY", "group": "other"},
        {"slug": "opencode-go", "name": "OpenCode Go", "env_var": "OPENCODE_GO_API_KEY", "group": "other"},
        {"slug": "ai-gateway", "name": "AI Gateway", "env_var": "AI_GATEWAY_API_KEY", "group": "other"},
        {"slug": "nous", "name": "Nous", "env_var": "", "group": "other"},
    ]

    @staticmethod
    def _mask_key(key: str) -> str:
        """Mask an API key, preserving prefix and last 4 chars."""
        if not key or len(key) <= 8:
            return "***"
        try:
            first = key.index("-")
            second = key.index("-", first + 1)
            prefix_end = second + 1
        except ValueError:
            prefix_end = 4
        return key[:prefix_end] + "***" + key[-4:]

    async def _build_models_payload(self) -> dict:
        """Build models info dict (same logic as welcome envelope)."""
        try:
            from hermes_cli.model_switch import list_authenticated_providers
            from gateway.run import _load_gateway_config, _resolve_gateway_model

            # Reload .env so freshly-written credentials are visible
            _env_path = get_env_path()
            if _env_path.exists():
                from dotenv import load_dotenv
                try:
                    load_dotenv(str(_env_path), override=True, encoding="utf-8")
                except UnicodeDecodeError:
                    load_dotenv(str(_env_path), override=True, encoding="latin-1")
                except Exception:
                    pass

            cfg = _load_gateway_config()
            model_cfg = cfg.get("model", {})
            current_provider = model_cfg.get("provider", "openrouter") if isinstance(model_cfg, dict) else "openrouter"
            current_model = _resolve_gateway_model(cfg)

            _LOCAL_PROVIDERS = {"custom", "lmstudio", "ollama", "vllm", "llamacpp"}
            # Root-split so "custom:<slug>" form (written by hermes-desktop
            # EndpointCard) also matches the local-provider set. Mirrors the
            # same handling in _handle_model_switch — both code paths must
            # agree, otherwise welcome/refresh sends a fallback placeholder
            # with has_credentials=false while runtime resolves through the
            # custom-endpoint api_key, and the client filters the provider
            # out of the model dropdown.
            current_provider_root = current_provider.split(":", 1)[0] if current_provider else ""
            if current_provider_root in _LOCAL_PROVIDERS:
                base_url = (model_cfg.get("base_url", "") if isinstance(model_cfg, dict) else "")
                api_key = (model_cfg.get("api_key", "") if isinstance(model_cfg, dict) else "")
                # For "custom:<slug>" form, prefer the matching entry in
                # custom_providers[] (where setEndpoint writes api_key). The
                # top-level model.{base_url,api_key} can be stale or empty.
                custom_slug = ""
                if current_provider.startswith("custom:") and ":" in current_provider:
                    custom_slug = current_provider.split(":", 1)[1]
                    for cp in (cfg.get("custom_providers") or []):
                        if not isinstance(cp, dict):
                            continue
                        # Match either an explicit slug field (newer entries)
                        # or _slugify(name) (older entries written before the
                        # slug field was persisted).
                        cp_slug = cp.get("slug") or self._slugify(cp.get("name", ""))
                        if cp_slug == custom_slug:
                            if not base_url:
                                base_url = cp.get("base_url", "") or ""
                            if not api_key:
                                api_key = cp.get("api_key", "") or ""
                            break
                if not api_key:
                    api_key = (os.environ.get("OPENAI_API_KEY")
                               or os.environ.get("ANTHROPIC_API_KEY")
                               or os.environ.get("API_KEY") or "")
                proxy_models = await self._fetch_endpoint_models(base_url, api_key)
                # Pick a display name: custom_providers[].name for custom:<slug>,
                # title-cased provider for the bare aliases (ollama, lmstudio…).
                provider_display_name = current_provider.title()
                if custom_slug:
                    for cp in (cfg.get("custom_providers") or []):
                        if not isinstance(cp, dict):
                            continue
                        cp_slug = cp.get("slug") or self._slugify(cp.get("name", ""))
                        if cp_slug == custom_slug:
                            provider_display_name = cp.get("name") or custom_slug or provider_display_name
                            break
                providers = [{
                    "slug": current_provider,
                    "name": provider_display_name,
                    "is_current": True,
                    "is_user_defined": True,
                    "models": proxy_models if proxy_models else [current_model],
                    "total_models": len(proxy_models) if proxy_models else 1,
                    "source": "endpoint",
                    # Treat the entry as credentialed when *either* we got
                    # a model list back from /v1/models (network proof the
                    # endpoint is reachable & authenticated) *or* the user
                    # explicitly registered this custom_providers[] slug
                    # (local LLMs commonly have no api_key but are still
                    # valid). Bare "custom" without a slug falls back to
                    # bool(api_key) like before.
                    "has_credentials": bool(api_key) or bool(custom_slug) or bool(proxy_models),
                }]
                # 也带上其他已认证 provider 的 model 候选,方便前端在 local/custom 模式下
                # 切回别的 provider 时拿到推荐模型(不带的话 Settings > Model 里点「使用」
                # 会错把当前 local 的 model id 带到新 provider)。
                try:
                    others = list_authenticated_providers(
                        current_provider=current_provider,
                        user_providers=cfg.get("providers"),
                        max_models=20,
                    )
                    seen = {current_provider}
                    for p in others:
                        if p["slug"] in seen:
                            continue
                        seen.add(p["slug"])
                        providers.append(p)
                except Exception:
                    logger.debug("[desktop] could not enumerate other authenticated providers in local mode")
            else:
                providers = list_authenticated_providers(
                    current_provider=current_provider,
                    user_providers=cfg.get("providers"),
                    max_models=20,
                )

                # Ensure the current provider always appears in the list even
                # if list_authenticated_providers missed it (e.g. models.dev
                # data unavailable or env var not detected).
                if not any(p["slug"] == current_provider for p in providers):
                    from hermes_cli.models import _PROVIDER_MODELS
                    from hermes_cli.providers import get_label
                    logger.warning(
                        "[desktop] no creds detected for current provider %s; emitting placeholder",
                        current_provider,
                    )
                    curated = list(_PROVIDER_MODELS.get(current_provider, []))
                    providers.insert(0, {
                        "slug": current_provider,
                        "name": get_label(current_provider),
                        "is_current": True,
                        "is_user_defined": False,
                        "models": curated[:20] if curated else [current_model],
                        "total_models": len(curated) if curated else 1,
                        "source": "fallback",
                        "has_credentials": False,
                    })

            # Entries from list_authenticated_providers all passed the
            # has_creds check (env var / auth_store / credential_pool) — mark
            # them True. The fallback placeholder above explicitly sets False
            # so setdefault won't clobber it.
            for _p in providers:
                _p.setdefault("has_credentials", True)

            return {
                "providers": providers,
                "current_model": current_model,
                "current_provider": current_provider,
            }
        except Exception:
            logger.debug("[desktop] could not build models payload")
            return {}

    @staticmethod
    def _slugify(name: str) -> str:
        """Derive a URL-safe slug from a display name."""
        return name.lower().replace(" ", "-")

    async def _handle_config_get(self, conn: _Connection, msg: dict) -> None:
        """Return full config snapshot: default model, provider credentials, custom endpoints."""
        from dotenv import dotenv_values

        hermes_dir = get_hermes_home()
        config_path = get_config_path()
        env_path = get_env_path()

        try:
            config: dict = {}
            if config_path.exists():
                import yaml
                with open(config_path) as f:
                    fcntl.flock(f, fcntl.LOCK_SH)
                    try:
                        config = yaml.safe_load(f) or {}
                    finally:
                        fcntl.flock(f, fcntl.LOCK_UN)

            model_cfg = config.get("model", {})
            if not isinstance(model_cfg, dict):
                model_cfg = {}
            default_model = model_cfg.get("default", "")
            default_provider = model_cfg.get("provider", "")
            model_base_url = model_cfg.get("base_url") or None
            model_api_mode = model_cfg.get("api_mode") or None
            _raw_ctx = model_cfg.get("context_length")
            model_context_length = int(_raw_ctx) if _raw_ctx is not None else None

            env_vars = dotenv_values(str(env_path)) if env_path.exists() else {}

            providers = []
            for bp in self._BUILTIN_PROVIDERS:
                key_val = env_vars.get(bp["env_var"], "") if bp["env_var"] else ""
                providers.append({
                    "slug": bp["slug"],
                    "name": bp["name"],
                    "credential_status": "configured" if key_val else "missing",
                    "credential_hint": self._mask_key(key_val) if key_val else None,
                    "is_builtin": True,
                    "group": bp["group"],
                })

            custom_endpoints = []
            for cp in config.get("custom_providers", []):
                cp_name = cp.get("name", "")
                custom_endpoints.append({
                    "slug": self._slugify(cp_name) if cp_name else "",
                    "name": cp_name,
                    "base_url": cp.get("base_url", ""),
                    "default_model": cp.get("default_model", ""),
                    "api_key_configured": bool(cp.get("api_key")),
                    "api_mode": cp.get("api_mode", "chat_completions"),
                    "context_length": cp.get("context_length"),
                })

            # --- Agent settings ---
            agent_cfg = config.get("agent", {})
            if not isinstance(agent_cfg, dict):
                agent_cfg = {}

            # --- Memory settings ---
            memory_cfg = config.get("memory", {})
            if not isinstance(memory_cfg, dict):
                memory_cfg = {}

            # --- Compression settings ---
            compression_cfg = config.get("compression", {})
            if not isinstance(compression_cfg, dict):
                compression_cfg = {}

            # --- Approvals ---
            approvals_cfg = config.get("approvals", {})
            if not isinstance(approvals_cfg, dict):
                approvals_cfg = {}

            # --- Display ---
            display_cfg = config.get("display", {})
            if not isinstance(display_cfg, dict):
                display_cfg = {}

            # --- Security ---
            security_cfg = config.get("security", {})
            if not isinstance(security_cfg, dict):
                security_cfg = {}

            _tool_progress = display_cfg.get("tool_progress")
            if _tool_progress is None:
                _tool_progress = "all"

            await conn.send("config.state",
                             ref_id=msg.get("id"),
                             default_model=default_model,
                             default_provider=default_provider,
                             providers=providers,
                             custom_endpoints=custom_endpoints,
                             model_base_url=model_base_url,
                             model_api_mode=model_api_mode,
                             model_context_length=model_context_length,
                             # Agent
                             agent_max_turns=agent_cfg.get("max_turns", 90),
                             agent_reasoning_effort=agent_cfg.get("reasoning_effort", ""),
                             agent_tool_use_enforcement=str(agent_cfg.get("tool_use_enforcement", "auto")),
                             # Memory
                             memory_enabled=memory_cfg.get("memory_enabled", True),
                             memory_user_profile_enabled=memory_cfg.get("user_profile_enabled", True),
                             memory_char_limit=memory_cfg.get("memory_char_limit", 2200),
                             memory_user_char_limit=memory_cfg.get("user_char_limit", 1375),
                             # Compression
                             compression_enabled=compression_cfg.get("enabled", True),
                             compression_threshold=compression_cfg.get("threshold", 0.50),
                             compression_target_ratio=compression_cfg.get("target_ratio", 0.20),
                             compression_protect_last_n=compression_cfg.get("protect_last_n", 20),
                             # Safety
                             approvals_mode=approvals_cfg.get("mode", "manual"),
                             file_read_max_chars=config.get("file_read_max_chars", 100000),
                             security_redact_secrets=security_cfg.get("redact_secrets", True),
                             # Display
                             display_show_reasoning=display_cfg.get("show_reasoning", False),
                             display_show_cost=display_cfg.get("show_cost", False),
                             display_tool_progress=_tool_progress,
                             display_compact=display_cfg.get("compact", False),
                             display_streaming=display_cfg.get("streaming", False),
                             )
        except Exception as exc:
            logger.exception("[desktop] config.get error")
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="get",
                            code="CONFIG_READ_FAILED",
                            message=str(exc)[:500])

    async def _handle_config_set_default_model(self, conn: _Connection, msg: dict) -> None:
        """Update default model in config.yaml."""
        model_id = msg.get("model_id", "")
        provider_slug = msg.get("provider_slug", "")
        if not model_id or not provider_slug:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="set-default-model",
                            code="INVALID_MODEL",
                            message="model_id and provider_slug are required")
            return

        config_path = get_config_path()
        config_path.parent.mkdir(parents=True, exist_ok=True)

        import yaml

        try:
            with open(config_path, "a+") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                try:
                    f.seek(0)
                    config = yaml.safe_load(f) or {}
                    if "model" not in config:
                        config["model"] = {}

                    # Auto-backup: if switching away from "custom" with a base_url,
                    # save the current config to custom_providers so it can be restored
                    old_model_cfg = config.get("model", {})
                    old_provider = old_model_cfg.get("provider", "")
                    old_base_url = old_model_cfg.get("base_url", "")
                    if old_provider == "custom" and old_base_url and provider_slug != "custom":
                        existing = config.get("custom_providers", [])
                        has_match = any(cp.get("base_url") == old_base_url for cp in existing)
                        if not has_match:
                            from urllib.parse import urlparse as _urlparse
                            _host = _urlparse(old_base_url).hostname or "custom"
                            auto_entry = {
                                "name": f"Auto-saved ({_host})",
                                "base_url": old_base_url,
                                "default_model": old_model_cfg.get("default", ""),
                                "api_mode": old_model_cfg.get("api_mode", "chat_completions"),
                            }
                            if old_model_cfg.get("context_length"):
                                auto_entry["context_length"] = old_model_cfg["context_length"]
                            if "custom_providers" not in config:
                                config["custom_providers"] = []
                            config["custom_providers"].append(auto_entry)
                            logger.info("[desktop] auto-saved custom provider config: %s", old_base_url)

                    config["model"]["default"] = model_id
                    config["model"]["provider"] = provider_slug
                    # Optional override fields — present means write/clear, absent means leave untouched
                    if "base_url" in msg:
                        val = (msg["base_url"] or "").strip()
                        if val:
                            from urllib.parse import urlparse
                            parsed = urlparse(val)
                            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                                await conn.send("config.error", ref_id=msg.get("id"),
                                                action="set-default-model",
                                                code="INVALID_URL",
                                                message="Base URL must be a valid HTTP(S) address")
                                return
                            config["model"]["base_url"] = val
                        else:
                            config["model"].pop("base_url", None)
                    if "api_mode" in msg:
                        val = (msg["api_mode"] or "").strip()
                        if val and val in ("chat_completions", "anthropic_messages"):
                            config["model"]["api_mode"] = val
                        else:
                            config["model"].pop("api_mode", None)
                    if "context_length" in msg:
                        val = msg["context_length"]
                        if val is not None:
                            try:
                                config["model"]["context_length"] = int(val)
                            except (TypeError, ValueError):
                                pass
                        else:
                            config["model"].pop("context_length", None)
                    f.seek(0)
                    f.truncate()
                    yaml.dump(config, f, default_flow_style=False, sort_keys=False)
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)
        except Exception as exc:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="set-default-model",
                            code="CONFIG_WRITE_FAILED",
                            message=str(exc)[:500])
            return

        await conn.send("config.ok", ref_id=msg.get("id"),
                         action="set-default-model")
        logger.info("[desktop] config.set-default-model → %s (%s)", model_id, provider_slug)

        # Refresh model catalog for the new provider
        models_payload = await self._build_models_payload()
        if models_payload:
            await conn.send("models.refresh", **models_payload)

    async def _handle_config_set_credential(self, conn: _Connection, msg: dict) -> None:
        """Write API key to ~/.hermes/.env."""
        from dotenv import set_key

        provider_slug = msg.get("provider_slug", "")
        api_key = msg.get("api_key", "")

        bp = next((p for p in self._BUILTIN_PROVIDERS if p["slug"] == provider_slug), None)
        if not bp or not bp["env_var"]:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="set-credential",
                            code="INVALID_PROVIDER",
                            message=f"Unknown provider: {provider_slug}")
            return

        if not api_key or len(api_key) < 4:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="set-credential",
                            code="INVALID_CREDENTIAL",
                            message="API Key 格式无效")
            return

        env_path = get_env_path()
        env_path.parent.mkdir(parents=True, exist_ok=True)
        if not env_path.exists():
            env_path.touch()

        try:
            set_key(str(env_path), bp["env_var"], api_key)
        except Exception as exc:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="set-credential",
                            code="CONFIG_WRITE_FAILED",
                            message=str(exc)[:500])
            return

        # Sync to os.environ so list_authenticated_providers sees it immediately
        os.environ[bp["env_var"]] = api_key

        await conn.send("config.ok", ref_id=msg.get("id"),
                         action="set-credential",
                         provider_slug=provider_slug,
                         credential_hint=self._mask_key(api_key))
        logger.info("[desktop] config.set-credential → %s", provider_slug)

        # Refresh model catalog so the new provider's models appear in the dropdown
        models_payload = await self._build_models_payload()
        if models_payload:
            await conn.send("models.refresh", **models_payload)

    async def _handle_config_set_endpoint(self, conn: _Connection, msg: dict) -> None:
        """Upsert a custom endpoint in config.yaml custom_providers."""
        from urllib.parse import urlparse

        slug = msg.get("slug", "")
        name = msg.get("name", "")
        base_url = msg.get("base_url", "")

        if not name or not base_url:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="set-endpoint",
                            code="INVALID_URL",
                            message="name and base_url are required")
            return

        parsed = urlparse(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="set-endpoint",
                            code="INVALID_URL",
                            message="请输入有效的 HTTP(S) URL")
            return

        config_path = get_config_path()
        config_path.parent.mkdir(parents=True, exist_ok=True)

        import yaml

        try:
            with open(config_path, "a+") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                try:
                    f.seek(0)
                    config = yaml.safe_load(f) or {}
                    if "custom_providers" not in config:
                        config["custom_providers"] = []

                    entry = {
                        "slug": slug or self._slugify(name),
                        "name": name,
                        "base_url": base_url,
                        "default_model": msg.get("default_model", ""),
                        "api_mode": msg.get("api_mode", "chat_completions"),
                    }
                    if msg.get("api_key"):
                        entry["api_key"] = msg["api_key"]
                    if msg.get("context_length"):
                        entry["context_length"] = msg["context_length"]

                    found = False
                    target_slug = slug or self._slugify(name)
                    for i, cp in enumerate(config["custom_providers"]):
                        cp_slug = self._slugify(cp.get("name", ""))
                        if cp_slug == target_slug:
                            config["custom_providers"][i] = entry
                            found = True
                            break

                    if not found:
                        config["custom_providers"].append(entry)

                    f.seek(0)
                    f.truncate()
                    yaml.dump(config, f, default_flow_style=False, sort_keys=False)
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)
        except Exception as exc:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="set-endpoint",
                            code="CONFIG_WRITE_FAILED",
                            message=str(exc)[:500])
            return

        await conn.send("config.ok", ref_id=msg.get("id"),
                         action="set-endpoint")
        logger.info("[desktop] config.set-endpoint → %s (%s)", name, base_url)

        # Re-emit models snapshot so a freshly-saved endpoint immediately
        # reflects in the ModelSelector dropdown (without it the client has
        # to wait for the next reconnect for welcome to refresh).
        models_payload = await self._build_models_payload()
        if models_payload:
            await conn.send("models.refresh", **models_payload)

    async def _handle_config_delete_endpoint(self, conn: _Connection, msg: dict) -> None:
        """Remove a custom endpoint from config.yaml."""
        slug = msg.get("slug", "")
        if not slug:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="delete-endpoint",
                            code="INVALID_PROVIDER",
                            message="slug is required")
            return

        config_path = get_config_path()
        if not config_path.exists():
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="delete-endpoint",
                            code="INVALID_PROVIDER",
                            message=f"Endpoint not found: {slug}")
            return

        import yaml

        not_found = False
        try:
            with open(config_path, "r+") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                try:
                    config = yaml.safe_load(f) or {}
                    providers = config.get("custom_providers", [])
                    original_len = len(providers)
                    config["custom_providers"] = [
                        cp for cp in providers
                        if self._slugify(cp.get("name", "")) != slug
                    ]

                    if len(config["custom_providers"]) == original_len:
                        not_found = True
                    else:
                        f.seek(0)
                        f.truncate()
                        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)
        except Exception as exc:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="delete-endpoint",
                            code="CONFIG_WRITE_FAILED",
                            message=str(exc)[:500])
            return

        if not_found:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="delete-endpoint",
                            code="INVALID_PROVIDER",
                            message=f"Endpoint not found: {slug}")
            return

        await conn.send("config.ok", ref_id=msg.get("id"),
                         action="delete-endpoint")
        logger.info("[desktop] config.delete-endpoint → %s", slug)

    # Allowlist of keys the desktop UI is permitted to write via config.update.
    _UPDATABLE_KEYS: frozenset = frozenset({
        "agent.max_turns", "agent.reasoning_effort", "agent.tool_use_enforcement",
        "memory.memory_enabled", "memory.user_profile_enabled",
        "memory.memory_char_limit", "memory.user_char_limit",
        "compression.enabled", "compression.threshold",
        "compression.target_ratio", "compression.protect_last_n",
        "approvals.mode",
        "file_read_max_chars",
        "security.redact_secrets",
        "display.show_reasoning", "display.show_cost",
        "display.tool_progress", "display.compact",
        "display.streaming",
    })

    async def _handle_config_update(self, conn: _Connection, msg: dict) -> None:
        """Allowlisted config.yaml updater — applies dot-path key:value pairs."""
        updates = msg.get("updates")
        if not isinstance(updates, dict) or not updates:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="update",
                            code="INVALID_UPDATES",
                            message="updates must be a non-empty dict")
            return

        for dot_key, value in updates.items():
            if dot_key not in self._UPDATABLE_KEYS:
                await conn.send("config.error", ref_id=msg.get("id"),
                                action="update",
                                code="FORBIDDEN_KEY",
                                message=f"Key not allowed: {dot_key}")
                return
            if not isinstance(value, (str, int, float, bool, type(None))):
                await conn.send("config.error", ref_id=msg.get("id"),
                                action="update",
                                code="INVALID_VALUE",
                                message=f"Invalid value type for {dot_key}")
                return
            parts = dot_key.split(".")
            if not all(parts):
                await conn.send("config.error", ref_id=msg.get("id"),
                                action="update",
                                code="INVALID_KEY",
                                message=f"Malformed key: {dot_key}")
                return

        config_path = get_config_path()
        config_path.parent.mkdir(parents=True, exist_ok=True)

        import yaml

        try:
            with open(config_path, "a+") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                try:
                    f.seek(0)
                    config = yaml.safe_load(f) or {}

                    for dot_key, value in updates.items():
                        parts = dot_key.split(".")
                        target = config
                        for part in parts[:-1]:
                            if part not in target or not isinstance(target[part], dict):
                                target[part] = {}
                            target = target[part]
                        target[parts[-1]] = value

                    f.seek(0)
                    f.truncate()
                    yaml.dump(config, f, default_flow_style=False, sort_keys=False)
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)
        except Exception as exc:
            await conn.send("config.error", ref_id=msg.get("id"),
                            action="update",
                            code="CONFIG_WRITE_FAILED",
                            message=str(exc)[:500])
            return

        await conn.send("config.ok", ref_id=msg.get("id"), action="update")
        logger.info("[desktop] config.update → %s", list(updates.keys()))
