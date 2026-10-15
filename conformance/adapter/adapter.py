"""ws-mixer conformance runner adapter for ws-mixer-py (docs/CONFORMANCE.md section 1).

JSON Lines commands on stdin, JSON Lines events on stdout (flushed per line), free-form logs on
stderr. A thin shell over the public MixerClient API plus the test-only `Timing` hook; client
role only, so `listen`/`open_stream` answer the "unsupported" shape the runner turns into a SKIP.
One process is one client; the runner may SIGKILL it at any point, so nothing here relies on
cleanup running.

Run through `run` next to this file, which starts `python -I` (isolated: PYTHON* variables
ignored, no user site-packages, no script directory on sys.path); the SDK's `src/` is put on
sys.path below.

Timing: the runner already scales `set_options.hello_timeout_ms` (with its floor applied) and
the welcome's ping values; nothing else this adapter owns needs scaling, so `time_scale` and
`floor_ms` are stored but unused (a client `drain` maps to the SDK's graceful close, whose
grace period is fixed).
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
import time
from collections import deque
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from ws_mixer import (  # noqa: E402
    SDK_VERSION,
    DisconnectReason,
    DrainInfo,
    MixerClient,
    MixerStream,
    ReconnectOptions,
    Reset,
    Timing,
    Welcome,
    code_name,
)

STREAM_QUEUE_CAP = 64
_START_NS = time.monotonic_ns()

Emit = Callable[[dict[str, Any]], None]


def t_ms() -> int:
    """Milliseconds since this process started, monotonic."""
    return (time.monotonic_ns() - _START_NS) // 1_000_000


def _stdout_emit(event: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(event, separators=(",", ":")) + "\n")
    sys.stdout.flush()


class _StreamQueue:
    """write/close_write for one stream, run one at a time in command order."""

    def __init__(self) -> None:
        self.items: deque[tuple[int, Callable[[], Awaitable[None]]]] = deque()
        self.busy = False
        self.aborted = False


class Adapter:
    def __init__(self, emit: Emit = _stdout_emit) -> None:
        self.emit = emit
        self.window = 262_144
        self.max_streams = 64
        self.hello_timeout_ms: float | None = None
        self.time_scale = 1.0
        self.floor_ms = 300.0
        self.allow_subfloor = False
        self.client: MixerClient | None = None
        self.streams: dict[int, MixerStream] = {}
        self.half_closed: dict[int, set[str]] = {}
        self.queues: dict[int, _StreamQueue] = {}
        self.locally_reset: set[int] = set()
        self.tasks: set[asyncio.Task[Any]] = set()
        self.connected_once = False
        self.last_welcome: Welcome | None = None

    # --- plumbing --------------------------------------------------------------------------

    def ack(self, seq: Any) -> None:
        self.emit({"event": "ack", "seq": seq})

    def cmd_err(self, seq: Any, message: str) -> None:
        self.emit({"event": "error", "seq": seq, "message": message})

    def spawn(self, coro: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(coro)
        self.tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        self.tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self.emit({"event": "error", "message": f"adapter task failed: {task.exception()!r}"})

    # --- per-stream FIFO (write, close_write); reset bypasses it ---------------------------

    def enqueue(self, sid: int, seq: Any, op: Callable[[], Awaitable[None]]) -> None:
        q = self.queues.setdefault(sid, _StreamQueue())
        if len(q.items) >= STREAM_QUEUE_CAP:
            self.cmd_err(
                seq, f"queue_full: stream {sid}'s write/close_write queue (cap {STREAM_QUEUE_CAP})"
            )
            return
        q.items.append((seq, op))
        self._pump(sid, q)

    def _pump(self, sid: int, q: _StreamQueue) -> None:
        while q.items and not q.busy:
            seq, op = q.items.popleft()
            if q.aborted:
                self.cmd_err(seq, "reset: queued command aborted by RESET")
                continue
            q.busy = True

            async def run(op: Callable[[], Awaitable[None]] = op) -> None:
                try:
                    await op()
                finally:
                    q.busy = False
                    self._pump(sid, q)

            self.spawn(run())

    def reset_stream(
        self, sid: int, stream: MixerStream, code: int, message: str, seq: Any
    ) -> None:
        q = self.queues.get(sid)
        if q is not None:
            q.aborted = True
            while q.items:
                pending_seq, _ = q.items.popleft()
                self.cmd_err(pending_seq, "reset: queued command aborted by RESET")
        self.locally_reset.add(sid)
        self.teardown_stream(sid)
        try:
            stream.reset(code, message)
        except Exception as e:
            self.cmd_err(seq, f"reset: {e}")
            return
        self.ack(seq)

    def teardown_stream(self, sid: int) -> None:
        self.streams.pop(sid, None)
        self.half_closed.pop(sid, None)
        self.queues.pop(sid, None)

    def note_half_closed(self, sid: int, direction: str) -> None:
        self.emit({"event": "stream_closed", "id": sid, "direction": direction, "t_ms": t_ms()})
        halves = self.half_closed.setdefault(sid, set())
        halves.add(direction)
        if halves == {"read", "write"}:
            self.emit({"event": "stream_closed", "id": sid, "direction": "both", "t_ms": t_ms()})
            self.teardown_stream(sid)

    # --- SDK callbacks ---------------------------------------------------------------------

    def on_stream(self, stream: MixerStream) -> None:
        sid = stream.id
        self.streams[sid] = stream
        self.locally_reset.discard(sid)
        self.emit({"event": "stream_opened", "id": sid})
        self.spawn(self._read_loop(stream))
        self.spawn(self._watch_reset(stream))

    async def _read_loop(self, stream: MixerStream) -> None:
        while True:
            try:
                chunk = await stream.read()
            except Exception:
                return
            if not chunk:
                if self.streams.get(stream.id) is stream:
                    self.note_half_closed(stream.id, "read")
                return
            data = base64.b64encode(chunk).decode("ascii")
            self.emit({"event": "data", "id": stream.id, "data_b64": data, "t_ms": t_ms()})

    async def _watch_reset(self, stream: MixerStream) -> None:
        reason = await stream.wait_closed()
        if not isinstance(reason, Reset) or stream.id in self.locally_reset:
            return
        self.emit(
            {
                "event": "stream_reset",
                "id": stream.id,
                "code": reason.code,
                "name": reason.name,
                "message": reason.message,
                "t_ms": t_ms(),
            }
        )
        if self.streams.get(stream.id) is stream:
            self.teardown_stream(stream.id)

    def on_connect(self, welcome: Welcome) -> None:
        self.last_welcome = welcome
        if self.connected_once:
            self.emit({"event": "reconnected", "session": welcome["session"], "welcome": welcome})
        self.connected_once = True

    def on_app(self, body: dict[str, Any]) -> None:
        self.emit({"event": "app", "body": body})

    def on_drain(self, d: DrainInfo) -> None:
        event: dict[str, Any] = {
            "event": "drain",
            "reason": d.reason,
            "last_stream_id": d.last_stream_id,
        }
        if d.deadline_ms is not None:
            event["deadline_ms"] = d.deadline_ms
        if d.message is not None:
            event["message"] = d.message
        self.emit(event)

    def on_disconnect(self, r: DisconnectReason, superseded: bool = False) -> None:
        # A superseded report is about a connection other than the serving one (a drained
        # predecessor, or a failed replacement dial): the serving connection's streams live on.
        if not superseded:
            for sid in list({*self.streams, *self.queues}):
                self.teardown_stream(sid)
        event: dict[str, Any] = {
            "event": "disconnected",
            "phase": r.phase,
            "message": r.message,
            "fatal": r.fatal,
        }
        if r.ws_code is not None:
            event["ws_code"] = r.ws_code
        if r.close_reason:
            event["close_reason"] = r.close_reason
        if r.error_code is not None:
            event["error_code"] = r.error_code
            event["error_name"] = r.error_name or code_name(r.error_code)
        self.emit(event)

    # --- commands --------------------------------------------------------------------------

    async def handle(self, cmd: dict[str, Any]) -> None:
        seq = cmd.get("seq")
        name = cmd.get("cmd")
        try:
            await self._dispatch(name, seq, cmd)
        except Exception as e:
            self.cmd_err(seq, f"{name}: {type(e).__name__}: {e}")

    async def _dispatch(self, name: Any, seq: Any, cmd: dict[str, Any]) -> None:
        if name == "set_options":
            self.window = int(cmd.get("window", self.window))
            self.max_streams = int(cmd.get("max_streams", self.max_streams))
            if cmd.get("hello_timeout_ms") is not None:
                self.hello_timeout_ms = float(cmd["hello_timeout_ms"])
            if cmd.get("time_scale") is not None:
                self.time_scale = float(cmd["time_scale"])
            if cmd.get("floor_ms") is not None:
                self.floor_ms = float(cmd["floor_ms"])
            self.allow_subfloor = bool(cmd.get("allow_subfloor_timing", self.allow_subfloor))
            self.ack(seq)
        elif name in ("listen", "open_stream"):
            self.emit(
                {
                    "event": "error",
                    "seq": seq,
                    "ok": False,
                    "unsupported": True,
                    "error": "unsupported",
                    "message": f"unsupported command {name}: ws-mixer-py has no server role",
                }
            )
        elif name == "connect":
            self.spawn(self._connect(seq, cmd))
        elif name == "write":
            sid = int(cmd["id"])
            stream = self.streams.get(sid)
            if stream is None:
                self.cmd_err(seq, f"write: no such stream {sid}")
                return
            data = base64.b64decode(cmd.get("data_b64", ""))

            async def write(stream: MixerStream = stream) -> None:
                try:
                    await stream.write(data)
                except Exception as e:
                    self.cmd_err(seq, f"write: {e}")
                    return
                self.ack(seq)

            self.enqueue(sid, seq, write)
        elif name == "close_write":
            sid = int(cmd["id"])
            stream = self.streams.get(sid)
            if stream is None:
                self.cmd_err(seq, f"close_write: no such stream {sid}")
                return

            async def close_write(stream: MixerStream = stream) -> None:
                await stream.close_write()
                self.ack(seq)
                self.note_half_closed(sid, "write")

            self.enqueue(sid, seq, close_write)
        elif name == "reset":
            sid = int(cmd["id"])
            stream = self.streams.get(sid)
            if stream is None:
                self.cmd_err(seq, f"reset: no such stream {sid}")
                return
            self.reset_stream(sid, stream, int(cmd["code"]), cmd.get("message") or "", seq)
        elif name == "send_app":
            client = self.client
            if client is None:
                self.cmd_err(seq, "send_app before connection established")
                return

            async def send_app(client: MixerClient = client) -> None:
                try:
                    await client.send_app(cmd.get("body") or {})
                except Exception as e:
                    self.cmd_err(seq, f"send_app: {e}")
                    return
                self.ack(seq)

            self.spawn(send_app())
        elif name == "drain":
            # Clients may only send drain{client_requested}, which is the first step of the
            # SDK's graceful close; there is no "drain and stay connected" API.
            if self.client is None:
                self.cmd_err(seq, "drain before connection established")
                return
            self.ack(seq)
            self.spawn(self.client.close())
        elif name == "close":
            if self.client is None:
                self.cmd_err(seq, "close before connection established")
                return
            code = cmd.get("code") or 0
            if code not in (0, 14):
                self.cmd_err(seq, "close: only code 0 (graceful) or 14 (application close)")
                return
            message = (cmd.get("message") or "") if code == 14 else None
            self.spawn(self.client.close(message))
            self.ack(seq)
        elif name == "shutdown":
            self.ack(seq)
            sys.stdout.flush()
            os._exit(0)
        else:
            self.cmd_err(seq, f"unsupported command {name}")

    def reconnect_options(self, spec: Any) -> ReconnectOptions:
        if not isinstance(spec, dict) or not spec.get("enabled"):
            return ReconnectOptions(max_attempts=0)
        kw: dict[str, Any] = {"max_attempts": spec.get("maxAttempts")}
        if isinstance(spec.get("baseMs"), int | float) and spec["baseMs"] > 0:
            kw["base_ms"] = float(spec["baseMs"])
        if isinstance(spec.get("capMs"), int | float) and spec["capMs"] > 0:
            kw["cap_ms"] = float(spec["capMs"])
        return ReconnectOptions(**kw)

    async def _connect(self, seq: Any, cmd: dict[str, Any]) -> None:
        if self.client is not None:
            self.cmd_err(seq, "connect: already connected (one connection per process)")
            return
        try:
            client = MixerClient(
                cmd["url"],
                token=cmd["token"],
                window=self.window,
                max_streams=self.max_streams,
                reconnect=self.reconnect_options(cmd.get("reconnect")),
                on_stream=self.on_stream,
                on_app=self.on_app,
                on_drain=self.on_drain,
                on_connect=self.on_connect,
                _timing=Timing(
                    hello_timeout_ms=self.hello_timeout_ms, allow_subfloor=self.allow_subfloor
                ),
            )
            client._on_disconnect_from = self.on_disconnect
            self.client = client
            await client.start()
        except Exception as e:
            self.cmd_err(seq, f"connect: {e}")
            return
        welcome: Any = self.last_welcome or {}
        self.ack(seq)
        self.emit(
            {
                "event": "connected",
                "seq": seq,
                "session": welcome.get("session", ""),
                "welcome": welcome,
            }
        )


async def main() -> None:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=64 * 1024 * 1024)
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    adapter = Adapter()
    adapter.emit(
        {"event": "ready", "sdk": "ws-mixer-py", "sdk_version": SDK_VERSION, "roles": ["client"]}
    )
    while True:
        line = await reader.readline()
        if not line:
            break
        text = line.strip()
        if not text:
            continue
        try:
            cmd = json.loads(text)
        except ValueError as e:
            adapter.emit({"event": "error", "message": f"bad command json: {e}"})
            continue
        await adapter.handle(cmd)
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    asyncio.run(main())
