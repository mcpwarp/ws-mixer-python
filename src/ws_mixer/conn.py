"""MixerConn: one ws-mixer.v1 connection over an open WebSocket (WIRE.md sections 2.5-2.10).

Owns the handshake, the reader task (decode and dispatch; never awaits application code), one
writer task (the control queue first, then one DATA chunk per ready stream, round-robin), the
ordered delivery loop for stream/app/drain handlers, keepalive, drain and retire, and
teardown. No reconnect: that is MixerClient's job, built on the `close` event's `CloseInfo`.

Every timer goes through the injected `Clock` and all randomness through the injected
`RandomFn`, so the whole connection runs on a `FakeClock` in tests.
"""

from __future__ import annotations

import asyncio
import inspect
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from ._emitter import Emitter
from ._jscompat import truncate_utf8
from .clock import Clock, RandomFn, TimerHandle
from .control import (
    DEFAULT_MAX_STREAMS,
    DEFAULT_WINDOW,
    AgentInfo,
    ControlMessage,
    Welcome,
    encode_control,
    known_drain_reason,
    parse_control,
)
from .errors import (
    ConnError,
    ErrorCode,
    StreamError,
    WsMixerError,
    close_code,
    code_name,
    wire_close_code,
)
from .frame import (
    MAX_RESET_MESSAGE,
    MAX_STREAM_ZERO_PAYLOAD,
    FrameType,
    decode_frame,
    encode_data,
    encode_reset,
    frame_type_name,
    reset_code,
    reset_message,
    window_increment,
)
from .socket import SUBPROTOCOL, WebSocketClosed, WebSocketConnection
from .stream import MixerStream

DEFAULT_HELLO_TIMEOUT_MS = 10_000
PING_INTERVAL_FLOOR_MS = 5000
STREAM0_RATE_PER_SEC = 50
STREAM0_BURST = 100
DELIVERY_QUEUE_MIN = 128
DELIVERY_QUEUE_MARGIN = 64
WATCHDOG_TICK_MS = 1000
CLOSE_BOUND_MS = 2000
"""How long teardown waits for the socket to flush the last frames and close before aborting."""
MAX_CLOSE_REASON = 123
MAX_ERROR_MESSAGE = 1024

_KNOWN_FRAME_TYPES = frozenset(int(t) for t in FrameType)
_WINDOW_RANGE = (16_384, 0x7FFF_FFFF)
_MAX_STREAMS_RANGE = (1, 100_000)

Phase = Literal["dial", "handshake", "connected"]
OnStream = Callable[[MixerStream], Awaitable[None] | None]


@dataclass(frozen=True)
class Timing:
    """Test and conformance hooks; never set in production.

    `allow_subfloor` accepts a welcome below the keepalive floors (ping_interval >= 5000 ms,
    ping_timeout >= 2x ping_interval); `min_ping_interval` replaces the 5000 ms floor applied to
    the welcome; `hello_timeout_ms` overrides the constructor's hello timeout.
    """

    hello_timeout_ms: float | None = None
    allow_subfloor: bool = False
    min_ping_interval: float | None = None


@dataclass(frozen=True)
class Stats:
    unknown_frame_types: int = 0
    stale_frames: int = 0
    duplicate_pongs: int = 0
    refused_opens: int = 0
    drain_violations: int = 0
    unknown_drain_reasons: int = 0
    handler_errors: int = 0
    protocol_violations: int = 0
    bytes_in: int = 0
    """Whole WebSocket messages received, ws-mixer header included."""
    bytes_out: int = 0


@dataclass(frozen=True)
class DrainInfo:
    """A received `drain`, with an unknown reason already normalized to "maintenance".

    `last_stream_id` is the boundary the connection enforces: the smallest value seen across
    every drain received so far, not necessarily this message's own.
    """

    reason: str
    last_stream_id: int
    deadline_ms: int | None = None
    retry_after_ms: int | None = None
    message: str | None = None


@dataclass(frozen=True)
class PongInfo:
    id: int
    rtt_ms: float


@dataclass(frozen=True)
class HandlerErrorInfo:
    event: str
    error: Exception


@dataclass(frozen=True)
class CloseInfo:
    """How the connection ended: the raw facts MixerClient turns into a DisconnectReason.

    - `local`: this side raised the error (a protocol violation it detected, the hello timeout,
      keepalive, a missing subprotocol echo, or its own close()/close_with_message()/retire()).
    - A peer `error` message: `local` False, `received_close` False, `error_code` set.
    - A bare close frame: `received_close` True, `error_code` None (the client derives it from
      a 4001-4999 `ws_code`), `close_reason` the peer's reason as received.
    - Transport death with no close frame: both False, `ws_code` 1006 (or what the socket said).
    - A missing or mismatched subprotocol echo: `phase` "dial", `ws_code` None, `error_code`
      UNSUPPORTED, `local` True (the wire still carried a 4010 close).

    `ws_code` is semantic (4000 + code, 1000 for NO_ERROR) whenever this side closed on an
    error; the wire carried `wire_close_code(ws_code)`.

    Take error codes from `error_code` only, never from `error.code`: for a close that carries
    no ws-mixer code (1000, 1001, 1006, ...) `error.code` is INTERNAL_ERROR, a placeholder for
    the streams and pending sends that need some ConnError, while `error_code` stays None.
    """

    ws_code: int | None
    message: str
    phase: Phase
    local: bool
    received_close: bool
    error: ConnError
    error_code: int | None = None
    error_name: str | None = None
    close_reason: str | None = None
    hello_timeout: bool = False
    """True only when this side gave up waiting for welcome (the local 4001)."""


def hello_payload(
    *,
    token: str,
    agent: AgentInfo,
    window: int = DEFAULT_WINDOW,
    max_streams: int = DEFAULT_MAX_STREAMS,
    capabilities: Sequence[str] | None = None,
    meta: Mapping[str, Any] | None = None,
) -> bytes:
    """The encoded stream-0 `hello`. Raises ValueError for out-of-range values or a message over
    the 16 KiB stream-0 limit (the server would answer 4009), so a client can check before
    dialing."""
    if not _WINDOW_RANGE[0] <= window <= _WINDOW_RANGE[1]:
        raise ValueError("window must be in 16384..2147483647")
    if not _MAX_STREAMS_RANGE[0] <= max_streams <= _MAX_STREAMS_RANGE[1]:
        raise ValueError("max_streams must be in 1..100000")
    hello: dict[str, Any] = {
        "t": "hello",
        "v": 1,
        "token": token,
        "agent": dict(agent),
        "window": window,
        "max_streams": max_streams,
    }
    if capabilities is not None:
        hello["capabilities"] = list(capabilities)
    if meta is not None:
        hello["meta"] = dict(meta)
    payload = encode_control(hello)
    if len(payload) > MAX_STREAM_ZERO_PAYLOAD:
        raise ValueError(
            f"hello of {len(payload)} bytes exceeds the {MAX_STREAM_ZERO_PAYLOAD} byte "
            "stream-0 limit (meta too large?)"
        )
    return payload


def as_conn_error(err: BaseException) -> ConnError:
    """What streams and pending sends see when the connection ends: always a ConnError."""
    if isinstance(err, ConnError):
        return err
    if isinstance(err, WsMixerError):
        out = ConnError(
            err.code,
            err.message,
            fatal=err.fatal,
            stream_id=err.stream_id,
            ws_code=err.ws_code,
            close_reason=err.close_reason,
        )
    else:
        out = ConnError(ErrorCode.INTERNAL_ERROR, f"{type(err).__name__}: {err}")
    out.__cause__ = err
    return out


def _settle(fut: asyncio.Future[None] | None, exc: BaseException | None = None) -> None:
    if fut is None or fut.done():
        return
    if exc is None:
        fut.set_result(None)
    else:
        fut.set_exception(exc)


def _quiet(fut: asyncio.Future[Any]) -> None:
    """Marks a settled future's exception as retrieved, so asyncio never logs it."""
    if not fut.cancelled():
        fut.exception()


def _retired(stream_id: int) -> StreamError:
    return StreamError(ErrorCode.STREAM_CLOSED, stream_id, "DATA not sent: stream already closed")


@dataclass(eq=False)
class _ControlItem:
    frame: bytes | None
    """None is a barrier: done once everything queued before it was written."""
    fut: asyncio.Future[None] | None
    priority: bool = False


@dataclass(eq=False)
class _OutboxItem:
    chunk: bytes
    fut: asyncio.Future[None]


@dataclass
class _Counters:
    unknown_frame_types: int = 0
    stale_frames: int = 0
    duplicate_pongs: int = 0
    refused_opens: int = 0
    drain_violations: int = 0
    unknown_drain_reasons: int = 0
    handler_errors: int = 0
    protocol_violations: int = 0
    bytes_in: int = 0
    bytes_out: int = 0


class _StreamHost:
    """The StreamHost streams talk to, so those hooks stay off MixerConn's public surface."""

    __slots__ = ("_conn",)

    def __init__(self, conn: MixerConn) -> None:
        self._conn = conn

    def send_data(self, stream_id: int, chunk: bytes) -> asyncio.Future[None]:
        return self._conn._send_data(stream_id, chunk)

    def send_control_frame(self, frame: bytes) -> None:
        self._conn._enqueue_control(frame)

    def retire_stream(self, stream_id: int) -> None:
        self._conn._retire_stream(stream_id)


class MixerConn(Emitter):
    """One ws-mixer.v1 connection on an already-open WebSocket; the client side only.

    Events (payload): welcome (Welcome), stream (MixerStream), app (dict), drain (DrainInfo),
    drain_received (DrainInfo, emitted synchronously on receipt, ahead of the ordered `drain`),
    pong (PongInfo), error (ConnError), fatal (ConnError), close (CloseInfo), handler_error
    (HandlerErrorInfo). stream/app/drain listeners, and `on_stream`, run from one delivery
    loop in wire order and are awaited when they return an awaitable; one that raises is
    counted and reported as handler_error, and delivery goes on. Everything else is emitted
    synchronously. error/fatal are emitted only when someone listens; close always fires once.

    Construct inside a running loop.
    """

    def __init__(
        self,
        socket: WebSocketConnection,
        *,
        token: str,
        agent: AgentInfo,
        clock: Clock,
        random: RandomFn,
        meta: Mapping[str, Any] | None = None,
        window: int = DEFAULT_WINDOW,
        max_streams: int = DEFAULT_MAX_STREAMS,
        capabilities: Sequence[str] | None = None,
        hello_timeout_ms: float = DEFAULT_HELLO_TIMEOUT_MS,
        on_stream: OnStream | None = None,
        _timing: Timing | None = None,
    ) -> None:
        super().__init__()
        self._hello_payload = hello_payload(
            token=token,
            agent=agent,
            window=window,
            max_streams=max_streams,
            capabilities=capabilities,
            meta=meta,
        )
        loop = asyncio.get_running_loop()
        self._socket = socket
        self._clock = clock
        self._random = random
        self._timing = _timing or Timing()
        self._hello_timeout_ms = (
            self._timing.hello_timeout_ms
            if self._timing.hello_timeout_ms is not None
            else hello_timeout_ms
        )
        self._max_streams_opt = max_streams
        self.on_stream: OnStream | None = on_stream

        self.session = ""
        self.peer_window = DEFAULT_WINDOW
        self.our_window = window
        self.max_streams = max_streams
        self.ping_interval_ms = 30_000
        self.ping_timeout_ms = 90_000
        self.welcome: Welcome | None = None
        self.welcome_meta: dict[str, Any] | None = None

        self._closed = False
        self._handshake_done = False
        self._welcome_fut: asyncio.Future[Welcome] | None = None
        self._draining = False
        self._last_stream_id: int | None = None
        self._local_drain = False
        self._retiring = False
        self._idle_closing = False
        self._streams: dict[int, MixerStream] = {}
        self._highest_opened = 0
        self._counters = _Counters()
        self._host = _StreamHost(self)

        self._control: deque[_ControlItem] = deque()
        self._rotation: deque[int] = deque()
        self._in_rotation: set[int] = set()
        self._outbox: dict[int, deque[_OutboxItem]] = {}
        self._wake = asyncio.Event()
        self._idle_waiters: list[Callable[[], None]] = []

        self._delivery: deque[tuple[str, Any]] = deque()
        self._delivering = False

        self._bucket_tokens = float(STREAM0_BURST)
        self._bucket_last = clock.now_ms()

        self._next_ping_id = 0
        self._lowest_unacked = 0
        self._last_pong_at = 0.0
        self._outstanding_pings: dict[int, float] = {}
        self._hello_timer: TimerHandle | None = None
        self._ping_timer: TimerHandle | None = None
        self._watchdog_timer: TimerHandle | None = None
        self._retire_timer: TimerHandle | None = None

        self._reader_task: asyncio.Task[None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._delivery_task: asyncio.Task[None] | None = None
        self._closer_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._terminal: ConnError | None = None
        self._close_info: CloseInfo | None = None
        self._done: asyncio.Future[CloseInfo] = loop.create_future()

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open" if self._handshake_done else "handshaking"
        return f"<MixerConn session={self.session!r} {state} streams={len(self._streams)}>"

    # --- public surface --------------------------------------------------------------------

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def close_info(self) -> CloseInfo | None:
        return self._close_info

    def stats(self) -> Stats:
        return Stats(**vars(self._counters))

    def is_draining(self) -> bool:
        return self._draining

    def live_stream_count(self) -> int:
        return len(self._streams)

    async def handshake(self) -> Welcome:
        """Sends hello and returns the validated welcome.

        Raises the ConnError the connection was torn down with: UNSUPPORTED for a missing
        subprotocol echo (close 4010) or welcome.v != 1, PROTOCOL_ERROR for a malformed welcome
        or no welcome within the hello timeout (close 4001), the peer's code for a peer
        `error`, or a derived code (with `ws_code`) for a close before welcome; it raises once
        the socket is closed. Cancelling the caller tears the connection down with NO_ERROR.
        """
        if self._welcome_fut is not None:
            raise RuntimeError("handshake() was already called")
        loop = asyncio.get_running_loop()
        self._welcome_fut = loop.create_future()
        if self._closed:
            assert self._terminal is not None
            raise self._terminal.with_traceback(None)
        if self._socket.subprotocol != SUBPROTOCOL:
            # The peer never agreed to speak ws-mixer.v1, so no error frame: just close 4010.
            # Reported as a dial failure with no ws_code (CLIENT-SDK.md, four sources rule 3).
            err = ConnError(
                ErrorCode.UNSUPPORTED,
                "the server did not select the ws-mixer.v1 subprotocol",
                fatal=True,
            )
            self._teardown(
                err,
                send_error=False,
                local=True,
                reason="subprotocol not echoed",
                subprotocol_rejected=True,
            )
        else:
            self._enqueue_control(encode_data(0, self._hello_payload))
            self._hello_timer = self._clock.call_later(
                self._hello_timeout_ms, self._on_hello_timeout
            )
            self._reader_task = loop.create_task(self._run_reader())
            self._writer_task = loop.create_task(self._run_writer())
        try:
            return await self._welcome_fut
        except asyncio.CancelledError:
            if not self._closed:
                self._teardown(
                    ConnError(ErrorCode.NO_ERROR, "handshake cancelled"),
                    send_error=True,
                    local=True,
                )
            raise
        except ConnError:
            await self.wait_closed()
            raise

    async def send_control(self, msg: ControlMessage | Mapping[str, Any]) -> None:
        """Queues a stream-0 message ahead of all DATA; returns once it was written.

        Raises ValueError for a payload over 16 KiB, and the connection's error once closed.
        """
        payload = encode_control(msg)
        if len(payload) > MAX_STREAM_ZERO_PAYLOAD:
            raise ValueError(
                f"control message of {len(payload)} bytes exceeds the "
                f"{MAX_STREAM_ZERO_PAYLOAD} byte stream-0 limit"
            )
        if self._closed:
            raise self._closed_error().with_traceback(None)
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._enqueue_control(encode_data(0, payload), fut)
        await fut

    async def send_app(self, body: Mapping[str, Any]) -> None:
        """Sends `app{body}`; returns once it was written. Only legal after welcome."""
        if not self._handshake_done and not self._closed:
            raise WsMixerError(ErrorCode.PROTOCOL_ERROR, "send_app() before welcome")
        await self.send_control({"t": "app", "body": dict(body)})

    def fail(self, err: WsMixerError) -> None:
        """Tears the connection down on a local error: error{code, message}, WS close
        4000+code (1000 for NO_ERROR), every stream and pending send failed. Idempotent."""
        self._teardown(err, send_error=True, local=True)

    async def close(self, grace_ms: float = 5000) -> None:
        """Graceful close: drain{client_requested}, wait until no stream is open or `grace_ms`
        passed, then error{NO_ERROR} and close 1000. An OPEN arriving meanwhile is refused with
        RESET(REFUSED_STREAM). Returns once the socket is closed.

        Every caller awaits the same shutdown; a later call's `grace_ms` is ignored. Cancelling a
        caller does not abandon the shutdown: it skips the rest of the grace period (error
        {NO_ERROR} and close 1000 right away) and re-raises CancelledError.

        Handlers for events received before the close may still run after this returns.
        """
        if self._close_task is None:
            self._close_task = asyncio.get_running_loop().create_task(
                self._graceful_close(grace_ms)
            )
        await self._await_shutdown(self._close_task)

    async def close_with_message(self, message: str) -> None:
        """Application close: error{APPLICATION_CLOSE, message} and close 4014, no drain.
        Overrides a graceful close() still waiting out its grace period."""
        if not self._closed:
            self._teardown(
                ConnError(ErrorCode.APPLICATION_CLOSE, message), send_error=True, local=True
            )
        await self._await_shutdown(self._done)

    async def _graceful_close(self, grace_ms: float) -> None:
        try:
            if not self._closed and self._handshake_done:
                self._local_drain = True
                drain = {
                    "t": "drain",
                    "reason": "client_requested",
                    # WIRE.md 2.9 Client side of a drain: the highest OPEN id received so far.
                    "last_stream_id": self._highest_opened,
                    "deadline_ms": max(0, int(grace_ms)),
                }
                self._enqueue_control(encode_data(0, encode_control(drain)))
                deadline = self._clock.now_ms() + grace_ms
                if self._streams:
                    await self._wait_until(deadline, idle=True)
                if not self._closed:
                    await self._wait_until(deadline, barrier=True)
        finally:
            # Also on cancellation: never leave a drained connection half-closed.
            if not self._closed:
                self._teardown(
                    ConnError(ErrorCode.NO_ERROR, "client closing"), send_error=True, local=True
                )
        await self.wait_closed()

    async def _await_shutdown(self, shutdown: asyncio.Future[Any]) -> None:
        try:
            await asyncio.shield(shutdown)
            await self.wait_closed()
        except asyncio.CancelledError:
            if not self._closed:
                self._teardown(
                    ConnError(ErrorCode.NO_ERROR, "close cancelled"), send_error=True, local=True
                )
            task = asyncio.current_task()
            if shutdown.cancelled() and task is not None and task.cancelling() == 0:
                # The shared shutdown was cancelled, not this caller (its teardown ran anyway).
                await self.wait_closed()
                return
            raise

    def retire(self, deadline_ms: float) -> None:
        """Hand-over after a received drain: in-flight streams finish here, then error{NO_ERROR}
        and close 1000 once no stream is open. At `deadline_ms` the survivors are reset with
        CANCEL first. The caller (MixerClient) times this from the replacement's welcome."""
        if self._closed or self._retiring:
            return
        self._retiring = True
        if not self._streams:
            self._close_idle()
            return
        self._retire_timer = self._clock.call_later(deadline_ms, self._on_retire_deadline)
        self._idle_waiters.append(self._close_idle)

    def cancel_delivery(self) -> None:
        """Drops every queued stream/app/drain event and cancels the handler in flight (it sees
        CancelledError). For a host's hard dispose; by default the backlog is still delivered
        after the connection ends. Events received later are delivered as usual."""
        self._delivery.clear()
        task, self._delivery_task = self._delivery_task, None
        self._delivering = False
        if task is not None and not task.done():
            task.cancel()

    async def wait_closed(self) -> CloseInfo:
        """Waits until the connection is torn down and its socket closed."""
        return await asyncio.shield(self._done)

    # --- handshake and welcome -------------------------------------------------------------

    def _on_hello_timeout(self) -> None:
        self._hello_timer = None
        if self._closed or self._handshake_done:
            return
        self._teardown(
            ConnError(
                ErrorCode.PROTOCOL_ERROR,
                f"no welcome within {self._hello_timeout_ms:g}ms of hello",
            ),
            send_error=True,
            local=True,
            hello_timeout=True,
        )

    def _apply_welcome(self, welcome: Welcome) -> None:
        interval = welcome["ping_interval"]
        timeout = welcome["ping_timeout"]
        floor = self._timing.min_ping_interval
        if floor is None:
            floor = 1 if self._timing.allow_subfloor else PING_INTERVAL_FLOOR_MS
        if interval < floor:
            raise ConnError(
                ErrorCode.PROTOCOL_ERROR,
                f"welcome.ping_interval {interval} is below the {floor:g}ms floor",
            )
        if not self._timing.allow_subfloor and timeout < 2 * interval:
            raise ConnError(
                ErrorCode.PROTOCOL_ERROR,
                f"welcome.ping_timeout ({timeout}) must be at least 2x ping_interval ({interval})",
            )
        self.session = welcome["session"]
        self.peer_window = welcome["window"]
        self.max_streams = min(self._max_streams_opt, welcome["max_streams"])
        self.ping_interval_ms = interval
        self.ping_timeout_ms = timeout
        self.welcome = welcome
        self.welcome_meta = welcome.get("meta")
        self._handshake_done = True
        self._last_pong_at = self._clock.now_ms()
        if self._hello_timer is not None:
            self._hello_timer.cancel()
            self._hello_timer = None
        self.emit("welcome", welcome)
        if self._welcome_fut is not None and not self._welcome_fut.done():
            self._welcome_fut.set_result(welcome)
        self._start_keepalive()

    # --- reader and dispatch (never awaits application code) -------------------------------

    async def _run_reader(self) -> None:
        while True:
            try:
                data = await self._socket.recv()
            except WebSocketClosed as e:
                self._on_socket_closed(e)
                return
            except Exception as e:
                self._on_socket_closed(WebSocketClosed(1006), f"receive failed: {e!r}")
                return
            if self._closed:
                continue
            self._on_message(data)

    def _on_message(self, data: bytes | str) -> None:
        try:
            if isinstance(data, str):
                raise ConnError(
                    ErrorCode.PROTOCOL_ERROR, "text frame received: ws-mixer.v1 is binary only"
                )
            self._counters.bytes_in += len(data)
            self._dispatch_frame(data)
        except Exception as e:
            self._handle_dispatch_error(e)

    def _handle_dispatch_error(self, e: Exception) -> None:
        if isinstance(e, ConnError):
            self._counters.protocol_violations += 1
            # Never answer an `error` with another one, malformed or not (WIRE.md 2.7).
            self._teardown(e, send_error=e.control_type != "error", local=True)
        elif isinstance(e, StreamError):
            assert e.stream_id is not None
            stream = self._streams.get(e.stream_id)
            if stream is not None:
                stream.abort(e.code, e.message)
            else:
                self._enqueue_control(encode_reset(e.stream_id, e.code, e.message))
        else:
            self._teardown(
                ConnError(ErrorCode.INTERNAL_ERROR, f"internal error: {type(e).__name__}: {e}"),
                send_error=True,
                local=True,
            )

    def _dispatch_frame(self, data: bytes) -> None:
        try:
            frame = decode_frame(data)
        except StreamError as e:
            # WINDOW increment 0: a never-opened id is a connection error, a retired one is a
            # stale frame like any other (WIRE.md 2.5 receive table), a live one is reset.
            sid = e.stream_id or 0
            if not self._handshake_done or sid > self._highest_opened:
                raise ConnError(ErrorCode.PROTOCOL_ERROR, e.message) from e
            if sid not in self._streams:
                self._counters.stale_frames += 1
                return
            raise

        if frame.type not in _KNOWN_FRAME_TYPES:
            self._counters.unknown_frame_types += 1
            return
        if frame.stream_id == 0:
            self._dispatch_control(frame.payload)
            return
        if not self._handshake_done:
            raise ConnError(
                ErrorCode.PROTOCOL_ERROR,
                f"{frame_type_name(frame.type)} frame received before welcome completed "
                "the handshake",
            )

        sid = frame.stream_id
        if frame.type == FrameType.OPEN:
            self._handle_open(sid)
            return
        if sid > self._highest_opened:
            raise ConnError(
                ErrorCode.PROTOCOL_ERROR,
                f"{frame_type_name(frame.type)} for stream {sid}, which was never opened",
            )
        stream = self._streams.get(sid)
        if stream is None:
            # Benign race: the stream was open and is gone now.
            self._counters.stale_frames += 1
            return
        if frame.type == FrameType.DATA:
            stream.handle_data(frame.payload)
        elif frame.type == FrameType.WINDOW:
            stream.handle_window(window_increment(frame))
        elif frame.type == FrameType.CLOSE:
            stream.handle_close()
        elif frame.type == FrameType.RESET:
            stream.handle_reset(
                reset_code(frame), truncate_utf8(reset_message(frame), MAX_RESET_MESSAGE)
            )

    def _handle_open(self, sid: int) -> None:
        if sid <= self._highest_opened:
            raise ConnError(
                ErrorCode.PROTOCOL_ERROR, f"duplicate or out-of-order OPEN for stream {sid}"
            )
        self._highest_opened = sid
        if self._draining and self._last_stream_id is not None and sid > self._last_stream_id:
            self._counters.drain_violations += 1
            raise ConnError(
                ErrorCode.PROTOCOL_ERROR,
                f"OPEN for stream {sid} received after drain "
                f"(last_stream_id={self._last_stream_id})",
            )
        if self._local_drain:
            # Crossed our own drain on the wire: refuse the stream, keep the connection.
            self._counters.refused_opens += 1
            self._enqueue_control(
                encode_reset(sid, ErrorCode.REFUSED_STREAM, "client is closing the connection")
            )
            return
        if len(self._streams) >= self.max_streams:
            self._counters.refused_opens += 1
            self._enqueue_control(
                encode_reset(
                    sid,
                    ErrorCode.STREAM_LIMIT,
                    f"max_streams={self.max_streams} exceeded by stream {sid}",
                )
            )
            return
        stream = MixerStream(sid, self._host, self.our_window, self.peer_window)
        self._streams[sid] = stream
        self._enqueue_delivery("stream", stream)

    def _bucket_allow(self) -> bool:
        now = self._clock.now_ms()
        refill = (now - self._bucket_last) / 1000 * STREAM0_RATE_PER_SEC
        self._bucket_tokens = min(float(STREAM0_BURST), self._bucket_tokens + refill)
        self._bucket_last = now
        if self._bucket_tokens < 1:
            return False
        self._bucket_tokens -= 1
        return True

    def _dispatch_control(self, payload: bytes) -> None:
        # Before parsing, so a flood of junk cannot burn CPU on top of the bucket.
        if not self._bucket_allow():
            raise ConnError(
                ErrorCode.ENHANCE_YOUR_CALM,
                f"stream-0 message rate exceeded {STREAM0_RATE_PER_SEC}/s (burst {STREAM0_BURST})",
            )
        msg = parse_control(payload, allow_subfloor=self._timing.allow_subfloor)
        if msg["t"] == "welcome":
            if self._handshake_done:
                raise ConnError(
                    ErrorCode.PROTOCOL_ERROR, "duplicate welcome after the handshake completed"
                )
            self._apply_welcome(msg)
        elif msg["t"] == "hello":
            raise ConnError(ErrorCode.PROTOCOL_ERROR, "unexpected hello: only the client sends it")
        elif msg["t"] == "ping":
            if not self._handshake_done:
                raise ConnError(
                    ErrorCode.PROTOCOL_ERROR,
                    "ping received before welcome completed the handshake",
                )
            pong: dict[str, Any] = {"t": "pong", "id": msg["id"]}
            if "ts" in msg:
                pong["ts"] = msg["ts"]
            self._enqueue_control(encode_data(0, encode_control(pong)), priority=True)
        elif msg["t"] == "pong":
            self._handle_pong(msg["id"])
        elif msg["t"] == "drain":
            if not self._handshake_done:
                raise ConnError(
                    ErrorCode.PROTOCOL_ERROR,
                    "drain received before welcome completed the handshake",
                )
            reason = msg["reason"]
            if not known_drain_reason(reason):
                self._counters.unknown_drain_reasons += 1
                reason = "maintenance"
            self._draining = True
            last = msg["last_stream_id"]
            if self._last_stream_id is not None:
                last = min(self._last_stream_id, last)
            self._last_stream_id = last
            info = DrainInfo(
                reason=reason,
                last_stream_id=last,
                deadline_ms=msg.get("deadline_ms"),
                retry_after_ms=msg.get("retry_after_ms"),
                message=msg.get("message"),
            )
            # Synchronously, before delivery: a close right behind the drain must not overtake it.
            self.emit("drain_received", info)
            self._enqueue_delivery("drain", info)
        elif msg["t"] == "error":
            # Legal even before welcome (an auth rejection); never answered with an error.
            err = ConnError(msg["code"], msg["message"], stream_id=msg.get("stream_id"))
            self._teardown(
                err, send_error=False, local=False, reason=f"peer error {code_name(msg['code'])}"
            )
        elif msg["t"] == "app":
            if not self._handshake_done:
                raise ConnError(
                    ErrorCode.PROTOCOL_ERROR,
                    "app received before hello/welcome completed the handshake",
                )
            self._enqueue_delivery("app", msg["body"])

    # --- ordered delivery ------------------------------------------------------------------

    def _enqueue_delivery(self, kind: str, payload: Any) -> None:
        if self._closed:
            return
        capacity = max(DELIVERY_QUEUE_MIN, self.max_streams + DELIVERY_QUEUE_MARGIN)
        if len(self._delivery) >= capacity:
            raise ConnError(
                ErrorCode.ENHANCE_YOUR_CALM,
                f"application delivery queue full (>{capacity} pending stream/app/drain events)",
            )
        self._delivery.append((kind, payload))
        if not self._delivering:
            self._delivering = True
            self._delivery_task = asyncio.get_running_loop().create_task(self._run_delivery())

    async def _run_delivery(self) -> None:
        # Deliberately not gated on _closed: events received before the end are still owed
        # to their handlers (CLIENT-SDK.md, Handler delivery). Nothing is queued once closed,
        # so this always ends.
        try:
            while self._delivery:
                kind, payload = self._delivery.popleft()
                if kind == "stream" and self.on_stream is not None:
                    await self._call_handler(kind, self.on_stream, payload)
                for entry in self._snapshot(kind):
                    if entry.once:
                        self._drop(kind, entry)
                    await self._call_handler(kind, entry.fn, payload)
        finally:
            self._delivering = False

    async def _call_handler(self, event: str, fn: Callable[[Any], object], arg: Any) -> None:
        try:
            result = fn(arg)
            if inspect.isawaitable(result):
                await result
        except Exception as e:
            self._listener_failed(event, e)

    def _listener_failed(self, event: str, exc: Exception) -> None:
        if event == "handler_error":
            return
        self._counters.handler_errors += 1
        if self.listener_count("handler_error") > 0:
            self.emit("handler_error", HandlerErrorInfo(event, exc))

    # --- writer: control queue first, then DATA round-robin --------------------------------

    def _enqueue_control(
        self,
        frame: bytes | None,
        fut: asyncio.Future[None] | None = None,
        *,
        priority: bool = False,
    ) -> None:
        if self._closed:
            _settle(fut, self._closed_error())
            return
        item = _ControlItem(frame, fut, priority)
        if priority:
            # Ahead of everything else queued, behind earlier priority items (pongs stay in
            # the order their pings arrived).
            at = 0
            while at < len(self._control) and self._control[at].priority:
                at += 1
            self._control.insert(at, item)
        else:
            self._control.append(item)
        self._wake.set()

    def _send_data(self, stream_id: int, chunk: bytes) -> asyncio.Future[None]:
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if self._closed:
            fut.set_exception(self._closed_error())
        elif stream_id not in self._streams:
            fut.set_exception(_retired(stream_id))
        else:
            self._outbox.setdefault(stream_id, deque()).append(_OutboxItem(chunk, fut))
            self._mark_ready(stream_id)
        return fut

    def _retire_stream(self, stream_id: int) -> None:
        stream = self._streams.pop(stream_id, None)
        queued = self._outbox.pop(stream_id, None)
        if queued:
            err = (stream.send_done()[1] if stream is not None else None) or _retired(stream_id)
            for item in queued:
                _settle(item.fut, err)
        if not self._streams:
            waiters, self._idle_waiters = self._idle_waiters, []
            for w in waiters:
                w()

    def _mark_ready(self, stream_id: int) -> None:
        if stream_id not in self._in_rotation:
            self._in_rotation.add(stream_id)
            self._rotation.append(stream_id)
        self._wake.set()

    def _next_chunk(self) -> tuple[int, _OutboxItem] | None:
        while self._rotation:
            sid = self._rotation.popleft()
            self._in_rotation.discard(sid)
            queue = self._outbox.get(sid)
            if not queue:
                continue
            item = queue.popleft()
            stream = self._streams.get(sid)
            if stream is None:
                _settle(item.fut, _retired(sid))
                continue
            done, error = stream.send_done()
            if done:
                _settle(item.fut, error)
                continue
            if queue:
                self._mark_ready(sid)
            return sid, item
        return None

    async def _run_writer(self) -> None:
        while True:
            if self._control:
                ctl = self._control.popleft()
                if ctl.frame is not None and not await self._write(ctl.frame, ctl.fut):
                    return
                _settle(ctl.fut)
                continue
            if self._closed:
                return
            nxt = self._next_chunk()
            if nxt is not None:
                sid, data = nxt
                if not await self._write(encode_data(sid, data.chunk), data.fut):
                    return
                _settle(data.fut)
                continue
            self._wake.clear()
            await self._wake.wait()

    async def _write(self, frame: bytes, fut: asyncio.Future[None] | None) -> bool:
        try:
            await self._socket.send(frame)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            closed = e if isinstance(e, WebSocketClosed) else WebSocketClosed(1006)
            self._on_socket_closed(closed, f"send failed: {e!r}")
            _settle(fut, self._closed_error())
            return False
        self._counters.bytes_out += len(frame)
        return True

    # --- keepalive -------------------------------------------------------------------------

    def _start_keepalive(self) -> None:
        if self._closed:
            return
        jitter = self._random() * self.ping_interval_ms
        self._ping_timer = self._clock.call_later(jitter, self._on_ping_timer)
        self._arm_watchdog()

    def _arm_watchdog(self) -> None:
        period = min(WATCHDOG_TICK_MS, self.ping_timeout_ms)
        self._watchdog_timer = self._clock.call_later(period, self._on_watchdog)

    def _on_ping_timer(self) -> None:
        self._ping_timer = None
        if self._closed:
            return
        ping_id = self._next_ping_id
        self._next_ping_id += 1
        self._outstanding_pings[ping_id] = self._clock.now_ms()
        # No `ts`: it is optional, and its unit is epoch ms, which only a wall clock gives.
        self._enqueue_control(encode_data(0, encode_control({"t": "ping", "id": ping_id})))
        self._ping_timer = self._clock.call_later(self.ping_interval_ms, self._on_ping_timer)

    def _on_watchdog(self) -> None:
        self._watchdog_timer = None
        if self._closed:
            return
        now = self._clock.now_ms()
        for ping_id, sent_at in list(self._outstanding_pings.items()):
            if now - sent_at > self.ping_timeout_ms:
                del self._outstanding_pings[ping_id]
        elapsed = now - self._last_pong_at
        if elapsed > self.ping_timeout_ms:
            self.fail(
                ConnError(ErrorCode.KEEPALIVE_TIMEOUT, f"no pong received for {elapsed:.0f}ms")
            )
            return
        self._arm_watchdog()

    def _handle_pong(self, ping_id: int) -> None:
        if ping_id >= self._next_ping_id:
            raise ConnError(ErrorCode.PROTOCOL_ERROR, f"pong for id {ping_id} that was never sent")
        sent_at = (
            self._outstanding_pings.pop(ping_id, None) if ping_id >= self._lowest_unacked else None
        )
        if sent_at is None:
            self._counters.duplicate_pongs += 1
            return
        if ping_id == self._lowest_unacked:
            self._lowest_unacked += 1
            while (
                self._lowest_unacked < self._next_ping_id
                and self._lowest_unacked not in self._outstanding_pings
            ):
                self._lowest_unacked += 1
        now = self._clock.now_ms()
        self._last_pong_at = now
        self.emit("pong", PongInfo(ping_id, now - sent_at))

    # --- drain, retire and graceful close helpers ------------------------------------------

    def _close_idle(self) -> None:
        if self._closed or self._idle_closing:
            return
        self._idle_closing = True
        barrier: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def flushed(f: asyncio.Future[None]) -> None:
            _quiet(f)
            if not self._closed:
                self.fail(ConnError(ErrorCode.NO_ERROR, "superseded by a new connection"))

        barrier.add_done_callback(flushed)
        self._enqueue_control(None, barrier)

    def _on_retire_deadline(self) -> None:
        self._retire_timer = None
        if self._closed:
            return
        # Each abort sends RESET(CANCEL) and retires the stream; the last one runs _close_idle.
        for stream in list(self._streams.values()):
            stream.abort(ErrorCode.CANCEL, "connection drained")

    async def _wait_until(
        self, deadline: float, *, idle: bool = False, barrier: bool = False
    ) -> None:
        """Waits for the stream table to empty (`idle`) or the control queue to flush up to
        here (`barrier`), whichever is asked, bounded by `deadline` and the connection's end."""
        loop = asyncio.get_running_loop()
        done: asyncio.Future[None] = loop.create_future()

        def finish(*_: object) -> None:
            _settle(done)

        def flushed(f: asyncio.Future[None]) -> None:
            _quiet(f)
            finish()

        timer = self._clock.call_later(deadline - self._clock.now_ms(), finish)
        unsubscribe = self.once("close", finish)
        if idle:
            self._idle_waiters.append(finish)
        if barrier:
            marker: asyncio.Future[None] = loop.create_future()
            marker.add_done_callback(flushed)
            self._enqueue_control(None, marker)
        try:
            await done
        finally:
            timer.cancel()
            unsubscribe()
            if finish in self._idle_waiters:
                self._idle_waiters.remove(finish)

    # --- teardown --------------------------------------------------------------------------

    def _closed_error(self) -> ConnError:
        if self._terminal is not None:
            return self._terminal
        return ConnError(ErrorCode.INTERNAL_ERROR, "connection closed")

    def _phase(self) -> Phase:
        return "connected" if self._handshake_done else "handshake"

    def _teardown(
        self,
        err: WsMixerError,
        *,
        send_error: bool,
        local: bool,
        reason: str | None = None,
        subprotocol_rejected: bool = False,
        hello_timeout: bool = False,
    ) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop_timers()
        conn_err = as_conn_error(err)
        ws_code = close_code(err.code)
        # A rejected subprotocol echo is a dial failure: the 4010 still goes on the wire, but
        # nothing is reported as a close code.
        reported_ws_code = None if subprotocol_rejected else ws_code
        if conn_err.ws_code is None:
            conn_err.ws_code = reported_ws_code
        self._terminal = conn_err
        error_frame = None
        if send_error:
            error_frame = encode_data(
                0,
                encode_control(
                    {"t": "error", "code": err.code, "message": err.message[:MAX_ERROR_MESSAGE]}
                ),
            )
        close_reason = truncate_utf8(err.message if reason is None else reason, MAX_CLOSE_REASON)
        self._start_closer(error_frame, wire_close_code(ws_code), close_reason)
        self._reject_outstanding(conn_err)
        self._fail_handshake(conn_err)
        fatal = err.code in (ErrorCode.UNSUPPORTED, ErrorCode.UNAUTHORIZED)
        quiet = err.code == ErrorCode.NO_ERROR or (
            local and err.code == ErrorCode.APPLICATION_CLOSE
        )
        event = "fatal" if fatal else "error"
        if not quiet and self.listener_count(event) > 0:
            self.emit(event, conn_err)
        self._finish_close(
            CloseInfo(
                ws_code=reported_ws_code,
                message=err.message,
                phase="dial" if subprotocol_rejected else self._phase(),
                hello_timeout=hello_timeout,
                local=local,
                received_close=False,
                error=conn_err,
                error_code=err.code,
                error_name=code_name(err.code),
            )
        )

    def _on_socket_closed(self, closed: WebSocketClosed, message: str | None = None) -> None:
        """The socket ended under us: a peer close frame or transport death. Not reached for
        the echo of our own close, which teardown already accounted for."""
        if self._closed:
            return
        self._closed = True
        self._stop_timers()
        reason = closed.reason if closed.received else ""
        text = reason or message or f"socket closed with code {closed.code}"
        code = closed.code - 4000 if 4001 <= closed.code <= 4999 else ErrorCode.INTERNAL_ERROR
        conn_err = ConnError(code, text, ws_code=closed.code, close_reason=reason or None)
        self._terminal = conn_err
        self._start_closer(None, None, "")
        self._reject_outstanding(conn_err)
        self._fail_handshake(conn_err)
        self._finish_close(
            CloseInfo(
                ws_code=closed.code,
                message=text,
                phase=self._phase(),
                local=False,
                received_close=closed.received,
                error=conn_err,
                close_reason=reason or None,
            )
        )

    def _fail_handshake(self, err: ConnError) -> None:
        fut = self._welcome_fut
        if fut is not None and not fut.done():
            fut.set_exception(err)
            _quiet(fut)

    def _finish_close(self, info: CloseInfo) -> None:
        self._close_info = info
        self.emit("close", info)

    def _start_closer(self, error_frame: bytes | None, code: int | None, reason: str) -> None:
        self._closer_task = asyncio.get_running_loop().create_task(
            self._close_socket(error_frame, code, reason)
        )

    async def _close_socket(self, error_frame: bytes | None, code: int | None, reason: str) -> None:
        """Lets an in-flight write finish, sends `error_frame`, closes the socket (bounded by
        CLOSE_BOUND_MS, then abort), and ends the reader."""
        bound = self._clock.call_later(CLOSE_BOUND_MS, self._socket.abort)
        try:
            writer = self._writer_task
            if writer is not None and not writer.done():
                await asyncio.wait([writer])
            if error_frame is not None:
                try:
                    await self._socket.send(error_frame)
                    self._counters.bytes_out += len(error_frame)
                except Exception:
                    pass
            try:
                if code is None:
                    await self._socket.close()
                else:
                    await self._socket.close(code, reason)
            except Exception:
                pass
        finally:
            bound.cancel()
            reader = self._reader_task
            if reader is not None and not reader.done():
                reader.cancel()
                await asyncio.wait([reader])
            assert self._close_info is not None
            if not self._done.done():
                self._done.set_result(self._close_info)

    def _stop_timers(self) -> None:
        for timer in (
            self._hello_timer,
            self._ping_timer,
            self._watchdog_timer,
            self._retire_timer,
        ):
            if timer is not None:
                timer.cancel()
        self._hello_timer = self._ping_timer = self._watchdog_timer = self._retire_timer = None

    def _reject_outstanding(self, err: ConnError) -> None:
        for ctl in self._control:
            _settle(ctl.fut, err)
        self._control.clear()
        for stream in list(self._streams.values()):
            stream.terminate(err)
        # After the streams, so queued DATA fails with its own stream's terminal error.
        for sid, queued in self._outbox.items():
            owner = self._streams.get(sid)
            item_err = (owner.send_done()[1] if owner is not None else None) or err
            for item in queued:
                _settle(item.fut, item_err)
        self._outbox.clear()
        self._rotation.clear()
        self._in_rotation.clear()
        self._streams.clear()
        self._idle_waiters.clear()
        self._wake.set()
