"""MixerClient and connect(): the reconnect loop over MixerConn (WIRE.md 2.9, CLIENT-SDK.md).

MixerConn owns one open socket; this module owns the loop that replaces it: the token provider
and its one refresh-retry, the per-trigger reconnect policy, full-jitter backoff with the
per-connection stability reset, the drain hand-over, and turning each connection's `CloseInfo`
into exactly one `DisconnectReason`.

State machine: idle -> dialing -> connected -> backoff -> dialing ... -> closed. `closed` is
terminal and is entered once: by close()/abort(), a fatal failure, or exhausted attempts.
Every timer goes through the injected `Clock` and all randomness through the injected `RandomFn`.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import weakref
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal

from . import _runtime
from ._emitter import Emitter
from ._rfc6455 import StdlibConnector
from ._version import SDK_VERSION
from .clock import Clock, LoopClock, RandomFn, TimerHandle, default_random
from .conn import (
    DEFAULT_HELLO_TIMEOUT_MS,
    CloseInfo,
    DrainInfo,
    MixerConn,
    OnStream,
    Phase,
    Stats,
    Timing,
    hello_payload,
)
from .control import DEFAULT_MAX_STREAMS, DEFAULT_WINDOW, AgentInfo, Welcome
from .errors import ErrorCode, WsMixerError, code_name, is_token_unavailable
from .socket import (
    MAX_RETRY_AFTER_MS,
    SUBPROTOCOL,
    ProxyRejected,
    UnexpectedSubprotocol,
    UpgradeRejected,
    WebSocketConnection,
    WebSocketConnector,
    WebSocketInit,
    build_headers,
)
from .stream import MixerStream

SDK_NAME = "ws-mixer-py"
DRAIN_JITTER_MS = 2000
RETIRE_DEFAULT_MS = 30_000
RETIRE_SLACK_MS = 1000

ClientState = Literal["idle", "dialing", "connected", "backoff", "closed"]
TokenProvider = str | Callable[[], str | Awaitable[str]]
"""A static token, or a callable (sync or async) invoked on every dial; never cached."""

_FATAL_WS_CODES = frozenset({4000 + ErrorCode.UNSUPPORTED, 4000 + ErrorCode.UNAUTHORIZED})
_PROTOCOL_BUG_WS_CODES = frozenset(
    {
        4000 + ErrorCode.PROTOCOL_ERROR,
        4000 + ErrorCode.FLOW_CONTROL_ERROR,
        4000 + ErrorCode.FRAME_SIZE_ERROR,
    }
)
_GOING_AWAY_WS_CODES = frozenset({4000 + ErrorCode.GOING_AWAY, 1001})
_KEEPALIVE_WS_CODE = 4000 + ErrorCode.KEEPALIVE_TIMEOUT
_CALM_WS_CODE = 4000 + ErrorCode.ENHANCE_YOUR_CALM
_APP_CLOSE_WS_CODE = 4000 + ErrorCode.APPLICATION_CLOSE

_token_refresh: ContextVar[bool] = ContextVar("ws_mixer_token_refresh", default=False)


def token_refresh_requested() -> bool:
    """True only while the token provider is being called for the one refresh-retry after a
    rejected token (HTTP 401 or close 4011 before welcome): skip any cache and fetch fresh."""
    return _token_refresh.get()


def full_jitter_delay(attempt: int, base_ms: float, cap_ms: float, random: RandomFn) -> float:
    """AWS "full jitter": random(0, min(cap, base * 2**attempt)) milliseconds."""
    ceiling = min(cap_ms, base_ms * 2.0 ** min(max(attempt, 0), 1000))
    return random() * ceiling


@dataclass(frozen=True)
class ReconnectOptions:
    base_ms: float = 1000
    cap_ms: float = 60_000
    connect_timeout_ms: float = 10_000
    max_attempts: int | None = None
    """None: unbounded. 0: never reconnect (the first failure is fatal)."""
    stable_after_ms: float = 10_000

    def __post_init__(self) -> None:
        for name in ("base_ms", "cap_ms", "stable_after_ms"):
            _check_ms(name, getattr(self, name), allow_zero=True)
        _check_ms("connect_timeout_ms", self.connect_timeout_ms, allow_zero=False)
        m = self.max_attempts
        if m is not None:
            if isinstance(m, bool) or not isinstance(m, int):
                raise TypeError("reconnect.max_attempts must be an int or None")
            if m < 0:
                raise ValueError("reconnect.max_attempts must be >= 0")


def _check_ms(name: str, value: object, *, allow_zero: bool) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"reconnect.{name} must be a number")
    if not math.isfinite(value) or value < 0 or (value == 0 and not allow_zero):
        bound = ">= 0" if allow_zero else "> 0"
        raise ValueError(f"reconnect.{name} must be a finite number {bound}")


@dataclass(frozen=True)
class ReconnectInfo:
    """Passed to `on_reconnect` before every scheduled redial (zero delays included) and with
    the `reconnecting` event (only when `delay_ms` > 0). `cause` names the trigger: "drain",
    "token-refresh", "going_away", "keepalive_timeout", "enhance_your_calm",
    "application_close", or the failure's message for ordinary backoff."""

    attempt: int
    delay_ms: float
    cause: str


@dataclass(frozen=True)
class DisconnectReason:
    """One per disconnect, recoverable or fatal (CLIENT-SDK.md "Disconnect reason shape").

    `error_code`/`error_name` come only from a peer `error`, a bare 4001-4999 close, a local
    ws-mixer error, or this side's application close; never from an HTTP status, 1006 or
    1000/1001/1009/1011. `protocol_error` marks a 4001/4003/4004 close (an SDK bug somewhere).
    """

    phase: Phase
    fatal: bool
    message: str
    ws_code: int | None = None
    error_code: int | None = None
    error_name: str | None = None
    http_status: int | None = None
    close_reason: str | None = None
    protocol_error: bool = False
    cause: BaseException | None = None


@dataclass(frozen=True)
class _Ctx:
    """Everything about one disconnect except whether it is fatal."""

    phase: Phase
    message: str
    ws_code: int | None = None
    error_code: int | None = None
    http_status: int | None = None
    close_reason: str | None = None
    cause: BaseException | None = None
    hello_timeout: bool = False

    def reason(self, fatal: bool, message: str | None = None) -> DisconnectReason:
        return DisconnectReason(
            phase=self.phase,
            fatal=fatal,
            message=self.message if message is None else message,
            ws_code=self.ws_code,
            error_code=self.error_code,
            error_name=None if self.error_code is None else code_name(self.error_code),
            http_status=self.http_status,
            close_reason=self.close_reason,
            # Our own welcome timeout closes 4001 too, but a slow server is not an SDK bug.
            protocol_error=self.ws_code in _PROTOCOL_BUG_WS_CODES and not self.hello_timeout,
            cause=self.cause,
        )


def _ctx_from_close(info: CloseInfo) -> _Ctx:
    """The four-sources rule (CLIENT-SDK.md) applied to one connection's end."""
    error_code: int | None
    close_reason = None
    if info.local:
        # Our own graceful close, retire or take-over (NO_ERROR, 1000) is none of the four
        # sources; our application close (14) and every local violation are.
        error_code = None if info.error_code == ErrorCode.NO_ERROR else info.error_code
    elif info.error_code is not None:
        error_code = info.error_code
    elif info.received_close:
        ws = info.ws_code
        error_code = ws - 4000 if ws is not None and 4001 <= ws <= 4999 else None
        close_reason = info.close_reason
    else:
        error_code = None
    return _Ctx(
        phase=info.phase,
        message=info.message,
        ws_code=info.ws_code,
        error_code=error_code,
        close_reason=close_reason,
        hello_timeout=info.hello_timeout,
    )


@dataclass(frozen=True)
class _Ok:
    conn: MixerConn


@dataclass(frozen=True)
class _Cancelled:
    pass


@dataclass(frozen=True)
class _Failed:
    ctx: _Ctx
    fatal: bool
    unauthorized: bool = False
    retry_after_ms: float | None = None


_Attempt = _Ok | _Cancelled | _Failed


class _ConnectTimeout(Exception):
    pass


class MixerClient(Emitter):
    """A reconnecting ws-mixer.v1 client.

    Events (payload): welcome (Welcome), stream (MixerStream), app (dict), drain (DrainInfo),
    reconnecting (ReconnectInfo, only when delay_ms > 0), close (DisconnectReason, once per
    disconnect), error (WsMixerError, only when listened to; also the raw exception when an
    `on_connect`/`on_disconnect`/`on_reconnect` callback or a listener raises), fatal
    (WsMixerError), pong (PongInfo), handler_error (HandlerErrorInfo: an on_stream/on_app/
    on_drain handler or listener raised; delivery goes on), closed (DisconnectReason | None:
    once, when the client enters `closed` for any reason; the fatal disconnect's reason after
    a fatal failure, else None, emitted as close()/abort() begin). The `on_*` callbacks fire
    alongside; `on_stream`/`on_app`/`on_drain` run in wire order from each connection's
    delivery loop and are awaited when they return an awaitable.

    In-flight streams are lost on reconnect: there is no resumption (WIRE.md section 2.9).
    """

    def __init__(
        self,
        url: str,
        *,
        token: TokenProvider,
        meta: Mapping[str, Any] | None = None,
        agent: Mapping[str, str] | None = None,
        window: int = DEFAULT_WINDOW,
        max_streams: int = DEFAULT_MAX_STREAMS,
        capabilities: Sequence[str] | None = None,
        headers: Mapping[str, str] | None = None,
        reconnect: ReconnectOptions | None = None,
        web_socket: WebSocketConnector | None = None,
        on_stream: OnStream | None = None,
        on_app: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
        on_drain: Callable[[DrainInfo], Awaitable[None] | None] | None = None,
        on_connect: Callable[[Welcome], None] | None = None,
        on_disconnect: Callable[[DisconnectReason], None] | None = None,
        on_reconnect: Callable[[ReconnectInfo], None] | None = None,
        clock: Clock | None = None,
        random: RandomFn | None = None,
        _timing: Timing | None = None,
    ) -> None:
        super().__init__()
        if not isinstance(token, str) and not callable(token):
            raise TypeError("token must be a str or a callable returning one")
        self._url = url
        self._token = token
        self._meta = meta
        given = dict(agent or {})
        self._agent: AgentInfo = {
            "sdk": given.get("sdk", SDK_NAME),
            "sdk_version": given.get("sdk_version", SDK_VERSION),
            "runtime": given.get("runtime") or _runtime.detect_runtime(),
            "os": given.get("os") or _runtime.detect_os(),
        }
        self._user_agent = _runtime.user_agent(
            SDK_NAME, SDK_VERSION, self._agent["runtime"], self._agent["os"]
        )
        self._window = window
        self._max_streams = max_streams
        self._capabilities = None if capabilities is None else list(capabilities)
        self._headers = dict(headers or {})
        self._opts = reconnect or ReconnectOptions()
        self._connector = web_socket
        self._on_stream = on_stream
        self._on_app = on_app
        self._on_drain = on_drain
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect
        self._on_reconnect = on_reconnect
        self._clock: Clock = clock or LoopClock()
        self._random: RandomFn = random or default_random()
        self._timing = _timing
        hello_timeout = _timing.hello_timeout_ms if _timing else None
        self._hello_timeout_ms = (
            hello_timeout if hello_timeout is not None else DEFAULT_HELLO_TIMEOUT_MS
        )
        # Fail on bad options now rather than at the first dial.
        hello_payload(
            token="",
            agent=self._agent,
            window=window,
            max_streams=max_streams,
            capabilities=self._capabilities,
            meta=meta,
        )
        build_headers(None, self._user_agent, self._headers)

        # mcpwarp hooks: like on_app/on_disconnect, plus whether the event concerns a connection
        # other than the serving one (a drain-superseded predecessor, or a replacement dial
        # failing while the drained connection still serves). `_on_app_from` also gets whether
        # the delivering connection has ended (its queue flushing after the close).
        self._on_app_from: Callable[[dict[str, Any], bool, bool], None] | None = None
        self._on_disconnect_from: Callable[[DisconnectReason, bool], None] | None = None

        self._state: ClientState = "idle"
        self._conn: MixerConn | None = None
        self._dialing_conn: MixerConn | None = None
        self._welcome_burst: MixerConn | None = None
        """A connection whose welcome was read in the current loop turn."""
        self._retiring_conn: MixerConn | None = None
        self._superseded: set[MixerConn] = set()
        self._replaced: weakref.WeakSet[MixerConn] = weakref.WeakSet()
        self._drain_info: weakref.WeakKeyDictionary[MixerConn, DrainInfo] = (
            weakref.WeakKeyDictionary()
        )
        self._drain_decided: weakref.WeakSet[MixerConn] = weakref.WeakSet()
        self._sockets: weakref.WeakKeyDictionary[MixerConn, WebSocketConnection] = (
            weakref.WeakKeyDictionary()
        )

        self._attempt = 0
        self._ever_connected = False
        self._closing = False
        self._fatal = False
        self._drain_reconnect_scheduled = False
        self._drained_no_reconnect = False
        self._keepalive_retry_used = False
        self._unauthorized_retry_used = False

        self._backoff_timer: TimerHandle | None = None
        self._backoff_fut: asyncio.Future[None] | None = None
        self._stability: tuple[MixerConn, TimerHandle] | None = None
        self._connect_task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._start_fut: asyncio.Future[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._connect_timer: TimerHandle | None = None
        self._dial_task: asyncio.Task[WebSocketConnection] | None = None
        self._connect_deadline: float | None = None
        self._closed_emitted = False

    def __repr__(self) -> str:
        return f"<MixerClient {self._state} attempt={self._attempt}>"

    # --- public surface --------------------------------------------------------------------

    @property
    def state(self) -> ClientState:
        return self._state

    def current_conn(self) -> MixerConn | None:
        """The live (welcomed) connection new streams arrive on, or None."""
        return self._conn

    def stats(self) -> Stats | None:
        """The active connection's counters (they restart with each connection), or None."""
        return None if self._conn is None else self._conn.stats()

    async def send_app(self, body: Mapping[str, Any]) -> None:
        """Sends `app{body}` on the active connection; returns once written."""
        conn = self._conn
        if conn is None:
            raise WsMixerError(ErrorCode.INTERNAL_ERROR, "send_app() with no active connection")
        await conn.send_app(body)

    async def start(self) -> None:
        """Dials and returns on the first welcome.

        Raises WsMixerError (fatal) on a fatal failure or exhausted attempts, and a non-fatal
        WsMixerError(NO_ERROR) when close() wins the race. Cancelling the caller aborts the client.
        """
        if self._start_fut is not None:
            raise RuntimeError("start() was already called")
        if self._done():
            raise WsMixerError(ErrorCode.NO_ERROR, "client is closed")
        self._start_fut = asyncio.get_running_loop().create_future()
        self._spawn_connect(0, "initial", None)
        try:
            await asyncio.shield(self._start_fut)
        except asyncio.CancelledError:
            if not self._start_fut.done():
                self.abort()
            raise

    async def close(self, message: str | None = None) -> None:
        """Stops reconnecting for good and closes every connection.

        `message` None: graceful (drain{client_requested}, up to 5 s for open streams, then
        error{NO_ERROR} and close 1000). A str: application close (error{APPLICATION_CLOSE,
        message}, close 4014, no drain). The live connection's end is reported once, non-fatal.
        Idempotent; every caller awaits the same shutdown, and cancelling one does not abandon it.
        """
        if message is not None and not isinstance(message, str):
            raise TypeError("close(message) takes a str; there is no close code to choose")
        self._stop_dialing()
        if self._close_task is None:
            self._close_task = asyncio.get_running_loop().create_task(
                self._close_impl(message, immediate=False)
            )
        elif message is not None and self._conn is not None and not self._conn.closed:
            # Escalate a graceful close still in its grace period to an application close.
            self._track(
                asyncio.get_running_loop().create_task(self._conn.close_with_message(message))
            )
        await asyncio.shield(self._close_task)

    async def _close_now(self) -> None:
        """mcpwarp's take-over close: like close(), but error{NO_ERROR} + 1000 at once."""
        self._stop_dialing()
        if self._close_task is None:
            self._close_task = asyncio.get_running_loop().create_task(
                self._close_impl(None, immediate=True)
            )
        await asyncio.shield(self._close_task)

    def abort(self) -> None:
        """Hard stop for a bounded dispose: no more dials, queued handler events dropped (the
        one running is cancelled), every socket dropped without waiting for a close handshake.
        Synchronous; the live connection's end is still reported once, non-fatal."""
        self._enter_closing()
        err = WsMixerError(ErrorCode.NO_ERROR, "client aborted")
        conns = [self._conn, self._dialing_conn, self._retiring_conn, *self._superseded]
        self._superseded.clear()
        self._retiring_conn = None
        self._dialing_conn = None
        for conn in dict.fromkeys(c for c in conns if c is not None):
            conn.cancel_delivery()
            conn.fail(err)
            sock = self._sockets.get(conn)
            if sock is not None:
                sock.abort()
        self._cancel_connect_task()
        for task in list(self._tasks):
            if task is not asyncio.current_task():
                task.cancel()

    def _stop_dialing(self) -> None:
        """Synchronous first half of close()/_close_now(), for mcpwarp's take-over path: marks
        the client closed, cancels the backoff, connect-timeout and stability timers, and
        cancels the dial task, so a scheduled replacement dial (even a zero-delay one) never
        reaches the connector. Connections are left to close()/_close_now(). Idempotent."""
        self._enter_closing()

    # --- closing ---------------------------------------------------------------------------

    def _enter_closing(self) -> None:
        # A dial already in its handshake is left to the conn failure close() applies, so it
        # closes with the caller's code rather than the cancellation's NO_ERROR.
        if self._dialing_conn is None:
            self._cancel_connect_task()
        self._closing = True
        self._state = "closed"
        self._drained_no_reconnect = False
        self._settle_start(
            WsMixerError(ErrorCode.NO_ERROR, "closed before the first connection completed")
        )
        self._clear_backoff()
        self._clear_stability()
        if self._connect_timer is not None:
            self._connect_timer.cancel()
            self._connect_timer = None
        self._emit_closed(None)

    def _emit_closed(self, reason: DisconnectReason | None) -> None:
        if not self._closed_emitted:
            self._closed_emitted = True
            self.emit("closed", reason)

    async def _close_impl(self, message: str | None, *, immediate: bool) -> None:
        self._enter_closing()
        code = ErrorCode.NO_ERROR if message is None else ErrorCode.APPLICATION_CLOSE
        err = WsMixerError(code, "client closing" if message is None else message)
        others = [*self._superseded, self._retiring_conn, self._dialing_conn]
        self._superseded.clear()
        self._retiring_conn = self._dialing_conn = None
        failed = [c for c in dict.fromkeys(others) if c is not None and c is not self._conn]
        for c in failed:
            c.fail(err)
        self._cancel_connect_task()
        waits: list[Awaitable[Any]] = [c.wait_closed() for c in failed]
        waits.append(self._settle_connect_task())
        conn = self._conn
        if conn is not None:
            if message is not None:
                waits.append(conn.close_with_message(message))
            elif immediate:
                conn.fail(err)
                waits.append(conn.wait_closed())
            else:
                waits.append(conn.close())
        await asyncio.gather(*waits)

    async def _settle_connect_task(self) -> None:
        """Waits for the cancelled dial to unwind, so close() resolves only once a dial in
        flight has settled and its late socket (if any) is aborted. A connector that ignores
        cancellation gets the rest of its connect timeout, then is cancelled again and left."""
        task = self._connect_task
        if task is None or task.done() or task is asyncio.current_task():
            return
        deadline = self._connect_deadline
        if deadline is None:
            remaining = self._opts.connect_timeout_ms
        else:
            remaining = max(0.0, deadline - self._clock.now_ms())
        done: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def finish(*_: object) -> None:
            if not done.done():
                done.set_result(None)

        def give_up() -> None:
            if self._dial_task is not None:
                self._dial_task.cancel()
            finish()

        task.add_done_callback(finish)
        timer = self._clock.call_later(remaining, give_up)
        try:
            await done
        finally:
            timer.cancel()
            task.remove_done_callback(finish)

    def _cancel_connect_task(self) -> None:
        task = self._connect_task
        # Once: a second cancel would cut short the unwind close() waits for.
        if (
            task is not None
            and not task.done()
            and task is not asyncio.current_task()
            and not task.cancelling()
        ):
            task.cancel()

    def _fail_superseded(self, err: WsMixerError) -> None:
        conns = list(self._superseded)
        self._superseded.clear()
        for c in conns:
            c.fail(err)

    def _done(self) -> bool:
        return self._closing or self._state == "closed"

    def _settle_start(self, err: WsMixerError | None) -> None:
        fut = self._start_fut
        if fut is None or fut.done():
            return
        if err is None:
            fut.set_result(None)
        elif not self._ever_connected:
            fut.set_exception(err)
            fut.exception()

    # --- timers ----------------------------------------------------------------------------

    def _clear_backoff(self) -> None:
        if self._backoff_timer is not None:
            self._backoff_timer.cancel()
            self._backoff_timer = None
        fut, self._backoff_fut = self._backoff_fut, None
        if fut is not None and not fut.done():
            fut.set_result(None)

    async def _sleep(self, delay_ms: float) -> None:
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def wake() -> None:
            self._backoff_timer = None
            if not fut.done():
                fut.set_result(None)

        self._backoff_fut = fut
        self._backoff_timer = self._clock.call_later(delay_ms, wake)
        try:
            await fut
        finally:
            if self._backoff_fut is fut:
                self._clear_backoff()

    def _arm_stability(self, conn: MixerConn) -> None:
        self._clear_stability()

        def stable() -> None:
            if self._stability is not None and self._stability[0] is conn:
                self._stability = None
            if self._conn is conn:
                self._attempt = 0
                self._keepalive_retry_used = False
                self._unauthorized_retry_used = False

        self._stability = (conn, self._clock.call_later(self._opts.stable_after_ms, stable))

    def _clear_stability(self, owner: MixerConn | None = None) -> None:
        if self._stability is None:
            return
        if owner is not None and self._stability[0] is not owner:
            return
        self._stability[1].cancel()
        self._stability = None

    # --- the dial loop ---------------------------------------------------------------------

    def _track(self, task: asyncio.Task[None]) -> asyncio.Task[None]:
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _spawn_connect(self, delay_ms: float, cause: str, attempt: int | None) -> None:
        loop = asyncio.get_running_loop()
        self._connect_task = self._track(
            loop.create_task(self._connect_once(delay_ms, cause, attempt))
        )

    async def _connect_once(self, delay_ms: float, cause: str, attempt: int | None) -> None:
        if self._done():
            return
        if cause != "initial":
            info = ReconnectInfo(self._attempt if attempt is None else attempt, delay_ms, cause)
            self._call(self._on_reconnect, info)
            if delay_ms > 0:
                self._state = "backoff"
                self.emit("reconnecting", info)
        if delay_ms > 0:
            await self._sleep(delay_ms)
            if self._done():
                return
        self._state = "dialing"

        result = await self._dial_and_handshake(refresh=False)
        if (
            isinstance(result, _Failed)
            and result.unauthorized
            and callable(self._token)
            and not self._unauthorized_retry_used
            and not self._done()
        ):
            self._unauthorized_retry_used = True
            # Every disconnect is reported (CLIENT-SDK.md Disconnect reason shape), this
            # non-fatal one too; the mcpwarp tunnel turns it into T48.
            self._notify(result.ctx.reason(False), superseded=self._conn is not None)
            if self._done():
                return
            self._call(self._on_reconnect, ReconnectInfo(1, 0, "token-refresh"))
            result = await self._dial_and_handshake(refresh=True)

        if isinstance(result, _Ok | _Cancelled):
            return
        if self._done():
            return
        if result.fatal:
            self._go_fatal(result.ctx)
        elif result.retry_after_ms is not None:
            retry_after = result.retry_after_ms
            self._report_and_schedule(result.ctx, result.ctx.message, lambda: retry_after)
        elif result.ctx.phase == "handshake":
            self._schedule_backoff(result.ctx, "handshake failed: " + result.ctx.message)
        else:
            self._schedule_backoff(result.ctx, result.ctx.message)

    async def _resolve_token(self, refresh: bool) -> str:
        token = self._token
        if isinstance(token, str):
            return token
        reset = _token_refresh.set(refresh)
        try:
            value = token()
            if inspect.isawaitable(value):
                value = await value
        finally:
            _token_refresh.reset(reset)
        if not isinstance(value, str):
            raise TypeError(f"token provider returned {type(value).__name__}, not str")
        return value

    async def _dial(self, init: WebSocketInit) -> WebSocketConnection:
        """One connector call, cancelled by the connect timeout (raised as _ConnectTimeout)."""
        if self._connector is None:
            self._connector = StdlibConnector()
        connector = self._connector
        task: asyncio.Task[WebSocketConnection] = asyncio.get_running_loop().create_task(
            _call_connector(connector, self._url, init)
        )
        timed_out = False

        def on_timeout() -> None:
            nonlocal timed_out
            timed_out = True
            task.cancel()

        timer = self._clock.call_later(self._opts.connect_timeout_ms, on_timeout)
        # On self so abort() can cancel it synchronously, and close() can bound its wait.
        self._connect_timer = timer
        self._dial_task = task
        self._connect_deadline = self._clock.now_ms() + self._opts.connect_timeout_ms
        try:
            return await task
        except asyncio.CancelledError:
            me = asyncio.current_task()
            if timed_out and me is not None and not me.cancelling():
                raise _ConnectTimeout() from None
            if task.done() and not task.cancelled() and task.exception() is None:
                task.result().abort()
            raise
        finally:
            timer.cancel()
            if self._connect_timer is timer:
                self._connect_timer = None
            if self._dial_task is task:
                self._dial_task = None
                self._connect_deadline = None

    async def _dial_and_handshake(self, *, refresh: bool) -> _Attempt:
        try:
            token = await self._resolve_token(refresh)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            msg = str(e) or f"token provider failed: {type(e).__name__}"
            return _Failed(_Ctx("dial", msg, cause=e), fatal=not is_token_unavailable(e))
        if self._done():
            return _Cancelled()
        try:
            headers = build_headers(token, self._user_agent, self._headers)
            hello_payload(
                token=token,
                agent=self._agent,
                window=self._window,
                max_streams=self._max_streams,
                capabilities=self._capabilities,
                meta=self._meta,
            )
        except ValueError as e:
            return _Failed(_Ctx("dial", f"invalid token: {e}", cause=e), fatal=True)

        try:
            ws = await self._dial(WebSocketInit(headers=MappingProxyType(headers)))
        except asyncio.CancelledError:
            raise
        except _ConnectTimeout:
            ms = self._opts.connect_timeout_ms
            return _Failed(_Ctx("dial", f"connect timed out after {ms:g}ms"), fatal=False)
        except Exception as e:
            return _dial_failure(e)
        if self._done():
            ws.abort()
            return _Cancelled()

        conn = MixerConn(
            ws,
            token=token,
            agent=self._agent,
            clock=self._clock,
            random=self._random,
            meta=self._meta,
            window=self._window,
            max_streams=self._max_streams,
            capabilities=self._capabilities,
            hello_timeout_ms=self._hello_timeout_ms,
            _timing=self._timing,
        )
        self._sockets[conn] = ws
        self._dialing_conn = conn
        self._wire(conn)
        try:
            await conn.handshake()
        except asyncio.CancelledError:
            raise
        except WsMixerError:
            if self._dialing_conn is conn:
                self._dialing_conn = None
            if self._done():
                return _Cancelled()
            info = conn.close_info
            assert info is not None
            if ws.subprotocol != SUBPROTOCOL:
                ctx = _Ctx(
                    "dial",
                    info.message,
                    error_code=ErrorCode.UNSUPPORTED,
                )
                return _Failed(ctx, fatal=True)
            ctx = _ctx_from_close(info)
            code = ctx.error_code
            fatal = code in (ErrorCode.UNSUPPORTED, ErrorCode.UNAUTHORIZED)
            return _Failed(ctx, fatal=fatal, unauthorized=code == ErrorCode.UNAUTHORIZED)
        return _Ok(conn)

    # --- per-connection wiring -------------------------------------------------------------

    def _wire(self, conn: MixerConn) -> None:
        conn.on("welcome", lambda w: self._on_welcome(conn, w))
        conn.on("stream", lambda s: self._deliver_stream(s))
        conn.on("app", lambda b: self._deliver_app(conn, b))
        conn.on("drain_received", lambda d: self._drain_received(conn, d))
        conn.on("drain", lambda d: self._deliver_drain(conn, d))
        conn.on("pong", lambda p: self.emit("pong", p))
        conn.on("error", self._forward_error)
        conn.on("handler_error", lambda info: self.emit("handler_error", info))
        conn.on("close", lambda info: self._on_conn_close(conn, info))

    def _forward_error(self, err: WsMixerError) -> None:
        if self.listener_count("error") > 0:
            self.emit("error", err)

    def _on_welcome(self, conn: MixerConn, welcome: Welcome) -> None:
        # Promoted synchronously, from inside the welcome's own dispatch, so a drain or app
        # right behind it on the wire already sees this as the active connection.
        self._welcome_burst = conn
        asyncio.get_running_loop().call_soon(self._end_welcome_burst, conn)
        if self._dialing_conn is conn:
            self._dialing_conn = None
        if self._done():
            conn.fail(WsMixerError(ErrorCode.NO_ERROR, "client closing"))
            return
        self._arm_stability(conn)
        self._ever_connected = True
        self._conn = conn
        self._state = "connected"
        old = self._retiring_conn
        if old is not None and old is not conn:
            self._retiring_conn = None
            self._drain_reconnect_scheduled = False
            self._superseded.add(old)
            self._replaced.add(old)
            drain = self._drain_info.get(old)
            deadline = None if drain is None else drain.deadline_ms
            old.retire(RETIRE_DEFAULT_MS if deadline is None else deadline + RETIRE_SLACK_MS)
        self.emit("welcome", welcome)
        self._call(self._on_connect, welcome)
        self._settle_start(None)

    def _end_welcome_burst(self, conn: MixerConn) -> None:
        if self._welcome_burst is conn:
            self._welcome_burst = None

    def _deliver_stream(self, stream: MixerStream) -> Awaitable[None] | None:
        self.emit("stream", stream)
        return None if self._on_stream is None else self._on_stream(stream)

    def _deliver_app(self, conn: MixerConn, body: dict[str, Any]) -> Awaitable[None] | None:
        self.emit("app", body)
        if self._on_app_from is not None:
            self._on_app_from(body, conn in self._replaced, conn.closed)
        return None if self._on_app is None else self._on_app(body)

    def _drain_received(self, conn: MixerConn, info: DrainInfo) -> None:
        """Records the drain the moment it is read off the wire. The reconnect decision waits
        for its ordered delivery (so an on_drain take-over can still stop it), unless the
        connection closes first: then the close policy acts on it (see `_drain_delay`)."""
        self._drain_info[conn] = info

    def _drain_delay(self, info: DrainInfo) -> float:
        # retry_after_ms is a floor; the jitter goes on top (spec v0.6.3). The hint and the
        # total are capped at 2^31-1 ms as in JS; the int compare also bounds a huge literal.
        hint = float(min(info.retry_after_ms or 0, MAX_RETRY_AFTER_MS))
        return min(hint + self._random() * DRAIN_JITTER_MS, float(MAX_RETRY_AFTER_MS))

    def _deliver_drain(self, conn: MixerConn, info: DrainInfo) -> Awaitable[None] | None:
        self._drain_decided.add(conn)
        # A repeat drain, or one flushed after its connection ended, starts nothing.
        if self._conn is conn and not self._done() and not self._drain_reconnect_scheduled:
            if self._opts.max_attempts != 0:
                self._drain_reconnect_scheduled = True
                self._retiring_conn = conn
                self._spawn_connect(self._drain_delay(info), "drain", 0)
            else:
                self._drained_no_reconnect = True
        # Only after the decision above, so a raising listener cannot skip it.
        self.emit("drain", info)
        return None if self._on_drain is None else self._on_drain(info)

    def _on_conn_close(self, conn: MixerConn, info: CloseInfo) -> None:
        was_active = self._conn is conn
        if self._welcome_burst is conn:
            # Closed in the same delivery burst as its welcome: nothing queued behind the
            # welcome is delivered (CLIENT-SDK.md Handler delivery), a drain included, so the
            # disconnect below takes the connected-phase policy, not the drain's.
            self._welcome_burst = None
            conn.cancel_delivery()
            self._drain_info.pop(conn, None)
        if was_active:
            self._conn = None
        if self._dialing_conn is conn:
            self._dialing_conn = None
        if self._retiring_conn is conn:
            self._retiring_conn = None
        self._clear_stability(owner=conn)

        ctx = _ctx_from_close(info)
        if conn in self._superseded:
            self._superseded.discard(conn)
            if not self._done():
                self._notify(ctx.reason(False), superseded=True)
            return
        if not was_active:
            # A dial/handshake failure (the dial loop reports it) or a connection close()/
            # abort()/a fatal path already failed on purpose.
            return

        if info.ws_code in _FATAL_WS_CODES:
            reason = ctx.reason(True)
            self._notify(reason)
            if self._done() or self._fatal:
                return
            err = self._fatal_error(ctx, ctx.message)
            self._terminate_fatal()
            self.emit("fatal", err)
            self._settle_start(err)
            self._emit_closed(reason)
            return
        if self._fatal:
            return
        if self._done():
            self._notify(ctx.reason(False))
            return
        pending_drain = self._drain_info.get(conn)
        if pending_drain is not None and conn not in self._drain_decided:
            # Received but not yet delivered when the connection closed (e.g. drain, then 4012
            # on an idle connection): still a drain, not a going-away; the flushed delivery that
            # follows starts nothing more.
            self._drain_decided.add(conn)
            if self._opts.max_attempts != 0:
                self._notify(ctx.reason(False))
                self._spawn_connect(self._drain_delay(pending_drain), "drain", 0)
                return
            self._drained_no_reconnect = True
        if self._drained_no_reconnect:
            self._drained_no_reconnect = False
            message = "drained; reconnect disabled"
            reason = ctx.reason(True, message)
            self._notify(reason)
            err = WsMixerError(
                ctx.error_code if ctx.error_code is not None else ErrorCode.GOING_AWAY,
                message,
                fatal=True,
            )
            self._terminate_fatal()
            self.emit("fatal", err)
            self._settle_start(err)
            self._emit_closed(reason)
            return
        if self._drain_reconnect_scheduled:
            self._notify(ctx.reason(False))
            self._drain_reconnect_scheduled = False
            return

        ws = info.ws_code
        cap = self._opts.cap_ms
        if ws in _GOING_AWAY_WS_CODES:
            self._report_and_schedule(ctx, "going_away", lambda: self._random() * DRAIN_JITTER_MS)
        elif ws == _KEEPALIVE_WS_CODE:
            if not self._keepalive_retry_used:
                self._keepalive_retry_used = True
                self._report_and_schedule(ctx, "keepalive_timeout", lambda: 0)
            else:
                self._schedule_backoff(ctx, "keepalive_timeout")
        elif ws == _CALM_WS_CODE:
            self._report_and_schedule(ctx, "enhance_your_calm", lambda: self._random() * cap)
        elif ws == _APP_CLOSE_WS_CODE:
            self._report_and_schedule(ctx, "application_close", lambda: self._random() * cap)
        else:
            self._schedule_backoff(ctx, info.message)

    # --- outcomes --------------------------------------------------------------------------

    def _notify(self, reason: DisconnectReason, *, superseded: bool = False) -> None:
        hook = self._on_disconnect_from
        if hook is not None:
            self._call(lambda r: hook(r, superseded), reason)
        self._call(self._on_disconnect, reason)
        self.emit("close", reason)

    def _call(self, fn: Callable[[Any], object] | None, arg: Any) -> None:
        if fn is None:
            return
        try:
            result = fn(arg)
            if inspect.iscoroutine(result):
                result.close()
        except Exception as e:
            self._listener_failed("callback", e)

    def _fatal_error(self, ctx: _Ctx, message: str) -> WsMixerError:
        code = ctx.error_code if ctx.error_code is not None else ErrorCode.INTERNAL_ERROR
        err = WsMixerError(code, message, fatal=True, http_status=ctx.http_status)
        err.__cause__ = ctx.cause
        return err

    def _terminate_fatal(self) -> None:
        """Enters `closed` for good and ends every other connection and pending dial."""
        self._state = "closed"
        self._fatal = True
        self._closing = True
        self._clear_backoff()
        self._clear_stability()
        closing = WsMixerError(ErrorCode.NO_ERROR, "client closing")
        conns = [self._conn, self._dialing_conn, self._retiring_conn]
        self._conn = self._dialing_conn = self._retiring_conn = None
        for c in dict.fromkeys(c for c in conns if c is not None):
            c.fail(closing)
        self._fail_superseded(closing)
        self._cancel_connect_task()

    def _go_fatal(self, ctx: _Ctx) -> None:
        if self._state == "closed":
            return
        err = self._fatal_error(ctx, ctx.message)
        self._terminate_fatal()
        self.emit("fatal", err)
        reason = ctx.reason(True)
        self._notify(reason)
        self._settle_start(err)
        self._emit_closed(reason)

    def _give_up(self, ctx: _Ctx, message: str) -> None:
        if self._state == "closed":
            return
        err = self._fatal_error(ctx, message)
        self._terminate_fatal()
        self.emit("fatal", err)
        reason = ctx.reason(True, message)
        self._notify(reason)
        self._settle_start(err)
        self._emit_closed(reason)

    def _report_and_schedule(self, ctx: _Ctx, cause: str, delay: Callable[[], float]) -> None:
        """One report and one scheduled redial, or (attempts exhausted) one merged fatal report."""
        if self._done() or self._fatal:
            return
        limit = self._opts.max_attempts
        if limit is not None and self._attempt >= limit:
            self._give_up(ctx, f"max reconnect attempts ({limit}) exhausted: {cause}")
            return
        self._notify(ctx.reason(False), superseded=self._conn is not None)
        self._attempt += 1
        self._spawn_connect(delay(), cause, None)

    def _schedule_backoff(self, ctx: _Ctx, cause: str) -> None:
        self._report_and_schedule(
            ctx,
            cause,
            lambda: full_jitter_delay(
                self._attempt, self._opts.base_ms, self._opts.cap_ms, self._random
            ),
        )


async def _call_connector(
    connector: WebSocketConnector, url: str, init: WebSocketInit
) -> WebSocketConnection:
    return await connector(url, init)


def _dial_failure(e: Exception) -> _Failed:
    """Classifies a connector exception (WIRE.md section 2.9's reconnect table)."""
    if isinstance(e, ProxyRejected):
        return _Failed(
            _Ctx("dial", f"{e} (check the proxy settings)", http_status=e.status, cause=e),
            fatal=True,
        )
    if isinstance(e, UpgradeRejected):
        status = e.status
        ctx = _Ctx(
            "dial",
            f"unexpected HTTP response during handshake: {status}",
            http_status=status,
            cause=e,
        )
        if status == 401:
            return _Failed(ctx, fatal=True, unauthorized=True)
        if status in (403, 404):
            return _Failed(ctx, fatal=True)
        retry_after = e.retry_after_ms() if status == 429 else None
        return _Failed(ctx, fatal=False, retry_after_ms=retry_after)
    if isinstance(e, UnexpectedSubprotocol):
        return _Failed(_Ctx("dial", str(e), error_code=ErrorCode.UNSUPPORTED, cause=e), fatal=True)
    # A configuration error (bad URL, invalid proxy setting) cannot fix itself; anything
    # else (OSError, InvalidHandshake, a library's own error) is an ordinary failed dial.
    # OSError first: ssl.SSLCertVerificationError is both an OSError and a ValueError.
    fatal = not isinstance(e, OSError) and isinstance(e, ValueError | TypeError)
    return _Failed(_Ctx("dial", str(e) or type(e).__name__, cause=e), fatal=fatal)


async def connect(url: str, *, token: TokenProvider, **options: Any) -> MixerClient:
    """`MixerClient(url, token=token, **options)`, started: returns on the first welcome."""
    client = MixerClient(url, token=token, **options)
    await client.start()
    return client
