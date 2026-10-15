"""expose(): publish one local MCP server at its public mcpwarp URL (MCPWARP.md section 5).

One call = one ws-mixer client = one registered service. Registration follows the connection
(the Session registers on every welcome); each inbound stream is one HTTP request, served in its
own task so requests never wait on each other.

Which connection an event belongs to is decided here, by the client's own bookkeeping: messages
and disconnects of a superseded connection (a drained one whose replacement has received
`welcome`) never reach the Session, and a drain with reason "replaced" is a take-over.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
import logging
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlsplit

from .. import _runtime
from .._emitter import Emitter
from .._jscompat import js_number, truncate_utf8
from .._version import SDK_VERSION
from ..client import DisconnectReason, MixerClient, ReconnectInfo, ReconnectOptions, TokenProvider
from ..clock import Clock, LoopClock, RandomFn
from ..conn import DrainInfo
from ..errors import ErrorCode, WsMixerError
from ..socket import WebSocketConnector
from ..stream import MixerStream
from .envelope import (
    DEFAULT_URL,
    INTEGRATION_ID_RE,
    SERVICE_NAME_MAX,
    ServiceKind,
    is_valid_service_name,
)
from .errors import CLOSED, DISCONNECTED_FATAL, TAKEN_OVER, UNAUTHORIZED, McpwarpError
from .http import Handler
from .serve import (
    DEFAULT_MAX_REQUEST_BODY_BYTES,
    MCP_PATH,
    TAKEN_OVER_MESSAGE,
    Availability,
    serve_stream,
)
from .session import (
    Conflict,
    Disabled,
    Enabled,
    ErrorOrigin,
    ExposeState,
    Registration,
    RetryInfo,
    Session,
)
from .target import DEFAULT_TARGET_TIMEOUT_MS, target_handler

META_CLIENT = "sdk-python"
META_VERSION_MAX_BYTES = 100
INTEGRATION_HEADER = "X-Mcpwarp-Integration"
MAX_TIMER_MS = 2_147_483_647
_AGENT_FIELD_RE = re.compile(r"^[\x20-\x7e]{1,64}\Z")

RetrySource = Literal["register", "overloaded", "reconnect", "drain"]


@dataclass(frozen=True)
class Agent:
    """Overrides what the SDK detects for `hello.agent`, User-Agent and `hello.meta.os`.

    Each field, when given, is 1-64 printable ASCII characters."""

    runtime: str | None = None
    os: str | None = None


@dataclass(frozen=True)
class Closed:
    """The `close` event: `error` None after close(), the fatal failure otherwise (a
    McpwarpError), or a CancelledError when the task awaiting expose() was cancelled."""

    error: BaseException | None = None


@dataclass(frozen=True)
class ExposeOptions:
    """expose()'s options; exactly one of `handler` and `target`. None means "not given"."""

    token: TokenProvider
    name: str
    integration: str
    url: str = DEFAULT_URL
    kind: str = "http"
    web_socket: WebSocketConnector | None = None
    agent: Agent | None = None
    reconnect: ReconnectOptions | None = None
    """`max_attempts` also bounds the attempts before the first registration."""
    max_request_body_bytes: int = DEFAULT_MAX_REQUEST_BODY_BYTES
    on_retry: Callable[[RetryInfo], None] | None = None
    """Called before every scheduled reconnect and register re-send; synchronous."""
    handler: Handler | None = None
    target: str | None = None
    target_timeout_ms: float | None = None
    clock: Clock | None = None
    random: RandomFn | None = None
    handler_timeout_ms: float | None = None
    """SDK extension (not in MCPWARP.md), `handler` only: an async handler that has not
    returned its response within this many ms is cancelled and answered 504 `local server
    timed out`. None: no deadline."""


_FIELDS = tuple(f.name for f in dataclasses.fields(ExposeOptions))
_REQUIRED = ("token", "name", "integration")


async def expose(options: ExposeOptions | None = None, /, **kwargs: Any) -> ExposeHandle:
    """Exposes a handler (or a local `target` URL) as a public mcpwarp service.

    Pass an ExposeOptions or its fields as keyword arguments (both: TypeError). The options are
    validated before anything is dialed (MCPWARP.md section 5.2): TypeError for a wrong type,
    ValueError for a bad value, the offending value never echoed.

    Returns the handle once the service is registered, or refused as disabled (state
    "disabled", `url` None). Raises McpwarpError (fatal) when the first registration or
    connection fails for good: the register error's code, UNSUPPORTED_VERSION, UNAUTHORIZED
    (the transport error as `__cause__`), DISCONNECTED_FATAL, or TAKEN_OVER. Dial failures and
    transient register errors are retried, reported through `on_retry`, until
    `reconnect.max_attempts` runs out.

    Cancelling the awaiting task before that closes the client in the background and re-raises
    CancelledError; cancelling it later (or after it returned) leaves the handle to `close()`.
    """
    opts = _collect(options, kwargs)
    handler = validate_options(opts)
    return await Exposure(opts, handler).run()


# --- validation (MCPWARP.md section 5.2, in its order) -----------------------------------------


def _collect(options: ExposeOptions | None, kwargs: Mapping[str, Any]) -> ExposeOptions:
    if options is not None:
        if kwargs:
            raise TypeError("expose() takes an ExposeOptions or keyword arguments, not both")
        if not isinstance(options, ExposeOptions):
            raise TypeError("expose() takes an ExposeOptions or keyword arguments")
        return options
    for key in kwargs:
        if key not in _FIELDS:
            raise TypeError(f"expose() got an unexpected keyword argument {key!r}")
    values = dict(kwargs)
    for key in _REQUIRED:
        values.setdefault(key, None)
    # Built unchecked: validate_options() reports a missing field with the spec's message.
    return ExposeOptions(**values)


def validate_options(opts: ExposeOptions) -> Handler:
    """Runs every section 5.2 check; returns the handler (`target` made into one)."""
    handler = validate_handler_target(
        opts.handler,
        opts.target,
        opts.target_timeout_ms,
        clock=opts.clock,
        handler_timeout_ms=opts.handler_timeout_ms,
    )
    name: object = opts.name
    if not is_valid_service_name(name):
        raise (ValueError if isinstance(name, str) else TypeError)(
            "mcpwarp: invalid service `name`: lowercase letters, digits and inner dashes, "
            f"at most {SERVICE_NAME_MAX} characters"
        )
    validate_integration(opts.integration)
    kind: object = opts.kind
    if kind not in ("http", "stdio"):
        raise (ValueError if isinstance(kind, str) else TypeError)(
            'mcpwarp: `kind` must be "http" or "stdio"'
        )
    token: object = opts.token
    if token is None or (not isinstance(token, str) and not callable(token)):
        raise TypeError("mcpwarp: `token` is required")
    if isinstance(token, str) and token == "":
        raise ValueError("mcpwarp: `token` is required")
    validate_agent(opts.agent)
    _validate_reconnect(opts.reconnect)
    body_cap: object = opts.max_request_body_bytes
    if isinstance(body_cap, bool) or not isinstance(body_cap, int):
        raise TypeError("mcpwarp: `max_request_body_bytes` must be an int")
    if body_cap <= 0:
        raise ValueError("mcpwarp: `max_request_body_bytes` must be positive")
    if not isinstance(opts.url, str):
        raise TypeError("mcpwarp: `url` must be a str")
    if opts.on_retry is not None and not callable(opts.on_retry):
        raise TypeError("mcpwarp: `on_retry` must be callable")
    return handler


def validate_handler_target(
    handler: object,
    target: object,
    target_timeout_ms: object,
    *,
    clock: Clock | None = None,
    handler_timeout_ms: object = None,
) -> Handler:
    """Rows 1-4: exactly one of `handler`/`target`, and `target_timeout_ms` only with a valid
    `target`; plus the SDK's own `handler_timeout_ms`, only with `handler`. Also run by
    create_tunnel() up front."""
    has_handler = handler is not None and callable(handler)
    has_target = target is not None
    # A non-callable `handler` is never "a handler", alone or beside a `target`.
    if has_handler == has_target or (handler is not None and not has_handler):
        raise TypeError("mcpwarp: pass exactly one of `handler` or `target`")
    if has_handler:
        if target_timeout_ms is not None:
            raise TypeError("mcpwarp: `target_timeout_ms` applies only to `target`")
        if handler_timeout_ms is not None:
            limit = (
                "mcpwarp: handler_timeout_ms must be a positive number of at most "
                f"{MAX_TIMER_MS} ms"
            )
            if isinstance(handler_timeout_ms, bool) or not isinstance(
                handler_timeout_ms, int | float
            ):
                raise TypeError(f"{limit}, got {type(handler_timeout_ms).__name__}")
            if not (handler_timeout_ms > 0) or handler_timeout_ms > MAX_TIMER_MS:
                raise ValueError(f"{limit}, got {js_number(handler_timeout_ms)}")
        return handler  # type: ignore[return-value]
    if handler_timeout_ms is not None:
        raise TypeError("mcpwarp: `handler_timeout_ms` applies only to `handler`")
    if not isinstance(target, str):
        raise TypeError("mcpwarp: `target` is not a valid URL")
    if not _is_target_url(target):
        raise ValueError("mcpwarp: `target` is not a valid URL")
    timeout: object = DEFAULT_TARGET_TIMEOUT_MS if target_timeout_ms is None else target_timeout_ms
    prefix = f"mcpwarp: target_timeout_ms must be a positive number of at most {MAX_TIMER_MS} ms"
    if isinstance(timeout, bool) or not isinstance(timeout, int | float):
        raise TypeError(f"{prefix}, got {type(timeout).__name__}")
    if not (timeout > 0) or timeout > MAX_TIMER_MS:
        raise ValueError(f"{prefix}, got {js_number(timeout)}")
    return target_handler(target, timeout_ms=timeout, clock=clock)


def _is_target_url(target: str) -> bool:
    try:
        parts = urlsplit(target)
        parts.port  # noqa: B018 (raises ValueError for a bad port)
    except ValueError:
        return False
    return parts.scheme.lower() in ("http", "https") and bool(parts.hostname)


def validate_integration(integration: object) -> None:
    """Rows 6-7. Also run by create_tunnel() up front."""
    if integration is None or integration == "" or not isinstance(integration, str):
        raise (ValueError if isinstance(integration, str) else TypeError)(
            "mcpwarp: `integration` (your integration's registration UUID) is required"
        )
    if not INTEGRATION_ID_RE.match(integration.lower()):
        raise ValueError(
            "mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently "
            "ignores anything else"
        )


def validate_agent(agent: object) -> None:
    """Row 10. Also run by create_tunnel() up front."""
    if agent is None:
        return
    if not isinstance(agent, Agent):
        raise TypeError("mcpwarp: `agent` must be an object")
    for key in ("runtime", "os"):
        value: object = getattr(agent, key)
        if value is None:
            continue
        message = f"mcpwarp: `agent.{key}` must be 1-64 printable ASCII characters"
        if not isinstance(value, str):
            raise TypeError(message)
        if not _AGENT_FIELD_RE.match(value):
            raise ValueError(message)


def _validate_reconnect(reconnect: object) -> None:
    """Row 11: ReconnectOptions checks its fields when built; re-run them (a subclass or a
    bypassed constructor) so a bad value is still a validation error here."""
    if reconnect is None:
        return
    if not isinstance(reconnect, ReconnectOptions):
        raise TypeError("mcpwarp: `reconnect` must be a ReconnectOptions")
    try:
        dataclasses.replace(reconnect)
    except (TypeError, ValueError) as e:
        raise type(e)(f"mcpwarp: {e}") from None


# --- the handle ----------------------------------------------------------------------------


class ExposeHandle(Emitter):
    """One exposed service. Created by expose(); read-only apart from `close()`.

    Events (payload): registered (Registration), disabled (Disabled), enabled (Enabled),
    conflict (Conflict), error (an Exception; non-fatal), disconnect (DisconnectReason, every
    transport disconnect), close (Closed; once, last). `on`/`once` return an unsubscribe
    function. Every listener is called even when one raises; what it raised is emitted as
    "error" (and dropped when an "error" listener raised it).
    """

    path = MCP_PATH
    """The only request path forwarded (exact, case-sensitive); anything else gets 404."""

    def __init__(self, exposure: Exposure) -> None:
        super().__init__()
        self._exposure = exposure
        self._close_task: asyncio.Task[None] | None = None

    def __repr__(self) -> str:
        return f"<ExposeHandle {self._exposure.session.name!r} {self.state.value}>"

    @property
    def url(self) -> str | None:
        """The public URL of the latest successful register. None only when expose() returned
        because the service was refused as disabled and it has not registered since."""
        return self._exposure.session.url

    @property
    def server_id(self) -> str | None:
        """The latest server id from `registered`, `disable`, `enable` or SERVER_DISABLED."""
        return self._exposure.session.server_id

    @property
    def registration(self) -> Registration | None:
        """The payload of the latest `registered` event; None when expose() returned as
        disabled and the service has not registered since. After a take-over, url, server_id
        and registration keep their last values (now the new holder's)."""
        return self._exposure.session.registration

    @property
    def state(self) -> ExposeState:
        return self._exposure.session.state

    @property
    def client(self) -> MixerClient:
        """The underlying ws-mixer client (stats, events)."""
        return self._exposure.client

    async def close(self) -> None:
        """Unregisters (when this connection sent register; at most 2 s), then closes the
        client gracefully (drain, at most 5 s for in-flight requests, close 1000). After a
        take-over it closes at once and unregisters nothing. Idempotent; never raises;
        cancelling a caller leaves the close running."""
        if self._close_task is None:
            loop = asyncio.get_running_loop()
            self._close_task = loop.create_task(self._exposure.close_impl())
        await asyncio.shield(self._close_task)

    async def __aenter__(self) -> ExposeHandle:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def _listener_failed(self, event: str, exc: Exception) -> None:
        # What an "error" listener raises is dropped (section 5.6), for the observer too.
        if event != "error":
            self._exposure.observe("error", exc, None)
        super()._listener_failed(event, exc)


# --- one exposure ----------------------------------------------------------------------------


@dataclass(frozen=True)
class ExposeHooks:
    """For create_tunnel(): what it observes of one exposure from its start.

    `observe(event, payload, detail)` sees every handle event (also after the handle closed,
    and the superseded disconnects, detail "superseded"), "retry" (RetryInfo; detail its
    RetrySource) and "error" (detail its ErrorOrigin, None for handler, listener and on_retry
    failures). `on_client_close` gets each background client close it may want to await.
    """

    observe: Callable[[str, object, str | None], None] | None = None
    on_client_close: Callable[[asyncio.Future[Any]], None] | None = None


class Exposure:
    """One expose() run: its client, session and handle. Internal (not in `__all__`); used
    directly by create_tunnel() for its hooks."""

    def __init__(self, opts: ExposeOptions, handler: Handler, hooks: ExposeHooks | None = None):
        loop = asyncio.get_running_loop()
        self._loop = loop
        self._opts = opts
        self._handler = handler
        self._hooks = hooks or ExposeHooks()
        self._started: asyncio.Future[ExposeHandle] = loop.create_future()
        self.settled = False
        self.finished = False
        self._client_closed_by_us = False
        self._tasks: set[asyncio.Future[Any]] = set()
        self._clock: Clock = opts.clock or LoopClock()

        kind: ServiceKind = "stdio" if opts.kind == "stdio" else "http"
        self.session = Session(
            opts.name,
            kind,
            send_app=self._send_app,
            clock=self._clock,
            emit=self._on_session_event,
            fatal=self._on_session_fatal,
        )
        integration = opts.integration.lower()
        given = opts.agent or Agent()
        agent = {k: v for k, v in (("runtime", given.runtime), ("os", given.os)) if v is not None}
        meta = {
            "client": META_CLIENT,
            "version": truncate_utf8(SDK_VERSION, META_VERSION_MAX_BYTES),
            "os": given.os or _runtime.detect_os(),
            "integration": integration,
        }
        self.client = MixerClient(
            opts.url,
            token=opts.token,
            meta=meta,
            agent=agent,
            headers={INTEGRATION_HEADER: integration},
            reconnect=opts.reconnect,
            web_socket=opts.web_socket,
            on_stream=self._on_stream,
            on_drain=self._on_drain,
            on_connect=lambda _welcome: self.session.on_welcome(),
            on_reconnect=self._on_reconnect,
            clock=self._clock,
            random=opts.random,
        )
        self.client._on_app_from = self._on_app_from
        self.client._on_disconnect_from = self._on_disconnect_from
        self.client.on("fatal", self._on_client_fatal)
        self.client.on("closed", self._on_client_closed)
        self.handle = ExposeHandle(self)

    async def run(self) -> ExposeHandle:
        start = self._spawn(self.client.start())
        start.add_done_callback(self._on_start_done)
        try:
            return await asyncio.shield(self._started)
        except asyncio.CancelledError:
            self._cancelled()
            raise

    # --- outcomes ----------------------------------------------------------------------------

    def _cancelled(self) -> None:
        """The task awaiting expose() was cancelled (MCPWARP.md section 5.8's abort)."""
        started = self._started
        if not self.settled:
            self.settled = True
            started.cancel()
            self._close_client(take_over=False)
            self._finish(asyncio.CancelledError())
        elif started.done() and not started.cancelled() and started.exception() is None:
            # Resolved, but the caller never received the handle.
            self._spawn(self.handle.close())

    def _settle(self) -> None:
        if not self.settled:
            self.settled = True
            self._started.set_result(self.handle)

    def _finish(self, error: BaseException | None) -> None:
        """The exposure ended: state closed, `close` emitted (last), expose() rejected if it
        has not settled yet."""
        if self.finished:
            return
        self.finished = True
        self.session.mark_closed()
        closed = Closed(error)
        self.observe("close", closed, None)
        self.handle.emit("close", closed)
        if not self.settled:
            self.settled = True
            rejection = (
                error
                if isinstance(error, McpwarpError)
                else McpwarpError(CLOSED, "mcpwarp: closed before the service was registered")
            )
            self._started.set_exception(rejection)

    async def close_impl(self) -> None:
        """MCPWARP.md section 5.7."""
        await self.session.begin_close()
        self._client_closed_by_us = True
        try:
            # Read after the wait: a take-over during it switches to the immediate close.
            if self.session.taken_over:
                await self.client._close_now()
            else:
                await self.client.close()
        except Exception:
            pass
        self._finish(None)

    def _close_client(self, *, take_over: bool) -> None:
        """Closes the client in the background: a fatal failure, a cancelled expose(), or a
        take-over (immediately: error{NO_ERROR} and close 1000, no drain, no grace)."""
        self._client_closed_by_us = True
        # Synchronously, so a replacement dial scheduled in this same delivery step never
        # reaches the connector (MCPWARP.md section 3.3, drain "replaced").
        self.client._stop_dialing()
        task = self._spawn(_swallow(self.client._close_now if take_over else self.client.close))
        if self._hooks.on_client_close is not None:
            try:
                self._hooks.on_client_close(task)
            except Exception:
                pass

    # --- session events ----------------------------------------------------------------------

    def _on_session_event(self, event: str, payload: object, detail: str | None) -> None:
        if event == "retry":
            if isinstance(payload, RetryInfo):
                self._retry(payload, "overloaded" if detail == "overloaded" else "register")
            return
        if event == "conflict" and not self.settled:
            # Taken over before `registered`: the handle could never serve. Settled first, so
            # the take-over close's disconnect cannot read as a lost connection.
            message = (
                f"mcpwarp: service {json.dumps(self._opts.name)} was taken over by another "
                "connection before it registered"
            )
            self._finish(
                McpwarpError(TAKEN_OVER, message, fatal=True, service_name=self._opts.name)
            )
            self._close_client(take_over=True)
            return
        if event == "error":
            if isinstance(payload, BaseException):
                self._report_error(payload, detail)
        else:
            self.observe(event, payload, None)
            self._emit(event, payload)
        if event == "conflict" and not self.finished:
            # A take-over after settling: the handle stays readable, the connection goes now.
            self._close_client(take_over=True)
        if event in ("registered", "disabled"):
            self._settle()

    def _on_session_fatal(self, err: McpwarpError) -> None:
        self._close_client(take_over=False)
        self._finish(err)

    # --- client callbacks --------------------------------------------------------------------

    def _send_app(self, body: dict[str, Any]) -> Awaitable[None]:
        # Bound to the connection live now, not whichever is live when the send runs.
        conn = self.client.current_conn()
        if conn is None:
            raise WsMixerError(ErrorCode.INTERNAL_ERROR, "no live connection")
        return conn.send_app(body)

    def _on_app_from(self, body: dict[str, Any], superseded: bool) -> None:
        # A drained connection's messages after its replacement's welcome are stale.
        if not superseded:
            self.session.on_app(body)

    def _on_disconnect_from(self, reason: DisconnectReason, superseded: bool) -> None:
        # Decided by connection identity: a drained connection ending once its replacement has
        # welcomed (or that replacement's dial failing while the drained one serves) leaves the
        # session alone.
        if not superseded:
            self.session.on_disconnect()
        self.observe("disconnect", reason, "superseded" if superseded else None)
        self._emit("disconnect", reason)
        if (
            not superseded
            and not reason.fatal
            and not self._client_closed_by_us
            and self.client.state == "closed"
        ):
            # The client was closed under the exposure (handle.client.close()): nothing will
            # reconnect, so the handle ends, with no error.
            self._finish(None)

    def _on_client_closed(self, reason: DisconnectReason | None) -> None:
        # The client entered `closed`. A fatal end is _on_client_fatal's; one we started is
        # ours. Otherwise it was closed under the exposure (handle.client.close()): with a live
        # connection, its disconnect (below in _on_disconnect_from) ends the handle; without
        # one (backoff, dialing) nothing else will, so end it now.
        if self._client_closed_by_us or self.finished or reason is not None:
            return
        if self.client.current_conn() is None:
            self._finish(None)

    def _on_start_done(self, fut: asyncio.Future[Any]) -> None:
        if fut.cancelled():
            return
        exc = fut.exception()
        if exc is None or self.finished or self._client_closed_by_us:
            return
        if isinstance(exc, WsMixerError) and not exc.fatal:
            return  # close() won the race to the first welcome: settled elsewhere
        # A fatal start also emits "fatal" (already scheduled); this catches anything else.
        self._loop.call_soon(self._finish, fatal_error(exc, self._opts.name))

    def _on_client_fatal(self, err: WsMixerError) -> None:
        # Deferred: the client reports the fatal disconnect right after this event, and the
        # handle's `close` must come last.
        self._loop.call_soon(self._finish, fatal_error(err, self._opts.name))

    def _on_drain(self, info: DrainInfo) -> None:
        if info.reason != "replaced":
            return
        # A take-over, not a hand-over. The `conflict` emit closes the client (in this same
        # step, so the replacement dial the client just scheduled never starts); a close()
        # already waiting for `unregistered` only gets there after its wait, so close here.
        closing_already = self.handle._close_task is not None
        self.session.on_replaced()
        if not self.finished and closing_already:
            self._close_client(take_over=True)

    def _on_reconnect(self, info: ReconnectInfo) -> None:
        if self.session.taken_over or self.finished:
            return
        if info.cause == "drain":
            # A hand-over, not a failure: the backoff counter is not consulted.
            self._retry(RetryInfo(0, info.delay_ms, "drain"), "drain")
        elif info.cause == "token-refresh":
            self._retry(RetryInfo(1, 0, "token-refresh"), "reconnect")
        else:
            self._retry(RetryInfo(info.attempt, info.delay_ms, info.cause), "reconnect")

    def _on_stream(self, stream: MixerStream) -> None:
        # The edge routes nothing here after a take-over; whatever still arrives is not ours.
        if self.session.state == ExposeState.CONFLICT:
            stream.reset(ErrorCode.CANCEL, TAKEN_OVER_MESSAGE)
            return
        self._spawn(
            serve_stream(
                stream,
                handler=self._handler,
                availability=self._availability,
                max_request_body_bytes=self._opts.max_request_body_bytes,
                clock=self._clock,
                on_error=lambda exc: self._report_error(exc, None),
                handler_timeout_ms=self._opts.handler_timeout_ms,
            )
        )

    def _availability(self) -> Availability:
        state = self.session.state
        if state == ExposeState.REGISTERED:
            return "available"
        if state == ExposeState.DISABLED:
            return "disabled"
        if state in (ExposeState.CLOSING, ExposeState.CLOSED):
            return "closing"
        if state == ExposeState.CONFLICT:
            return "conflict"
        # connecting/registering: after a first registration this is a reconnect or a drain
        # hand-over, and the edge keeps routing to the connection that still holds the name
        # until the new register lands, so those requests are served. Before it, the edge
        # routes nothing here.
        return "available" if self.session.ever_registered else "unavailable"

    # --- reporting ---------------------------------------------------------------------------

    def observe(self, event: str, payload: object, detail: str | None) -> None:
        if self._hooks.observe is None:
            return
        try:
            self._hooks.observe(event, payload, detail)
        except Exception:
            pass

    def _emit(self, event: str, payload: object) -> None:
        # Nothing after `close`.
        if not self.finished:
            self.handle.emit(event, payload)

    def _report_error(self, err: BaseException, origin: ErrorOrigin | str | None) -> None:
        self.observe("error", err, origin)
        self._emit("error", err)

    def _retry(self, info: RetryInfo, source: RetrySource) -> None:
        self.observe("retry", info, source)
        fn = self._opts.on_retry
        if fn is None:
            return
        try:
            result: object = fn(info)
        except Exception as e:
            self._report_error(e, None)
            return
        if inspect.iscoroutine(result):
            result.close()
            logging.getLogger("mcpwarp").warning(
                "on_retry returned a coroutine; it must be a plain function (ignored)"
            )

    def _spawn(self, aw: Awaitable[Any]) -> asyncio.Future[Any]:
        fut = asyncio.ensure_future(aw)
        self._tasks.add(fut)
        fut.add_done_callback(self._tasks.discard)
        return fut


def fatal_error(err: BaseException, service_name: str) -> McpwarpError:
    """A fatal transport failure as the McpwarpError expose() raises / the handle closes with;
    the original is `__cause__`."""
    if isinstance(err, McpwarpError):
        return err
    unauthorized = isinstance(err, WsMixerError) and (
        err.code == ErrorCode.UNAUTHORIZED or err.http_status == 401
    )
    message = err.message if isinstance(err, WsMixerError) else str(err)
    out = McpwarpError(
        UNAUTHORIZED if unauthorized else DISCONNECTED_FATAL,
        message,
        fatal=True,
        service_name=service_name,
    )
    out.__cause__ = err
    return out


async def _swallow(make: Callable[[], Awaitable[None]]) -> None:
    # Takes a factory: a task cancelled before its first step then never created the inner
    # coroutine, so none is left un-awaited.
    try:
        await make()
    except Exception:
        pass


__all__ = [
    "Agent",
    "Closed",
    "Conflict",
    "Disabled",
    "Enabled",
    "ExposeHandle",
    "ExposeHooks",
    "ExposeOptions",
    "ExposeState",
    "Registration",
    "RetryInfo",
    "expose",
    "fatal_error",
    "validate_agent",
    "validate_handler_target",
    "validate_integration",
    "validate_options",
]
