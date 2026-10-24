"""create_tunnel(): a long-lived controller over expose() for hosts whose settings change at
runtime (MCPWARP.md section 6), ported from ws-mixer-js's src/mcpwarp/tunnel.ts.

Call `configure()` on every settings change; it restarts the exposure only when something
relevant changed. Applies run one at a time on a queue, each after the previous connection has
shut down. Every start gets a new generation; events, `on_retry` calls and settlements of an
older one are ignored (section 6.8).

All state lives on the event loop the tunnel is first used on; only `status` may be read from
another thread (an immutable TunnelStatus swapped in one assignment). `configure()`, `stop()`
and `dispose()` do all their work inside the returned coroutine, so a host thread can hand them
to `asyncio.run_coroutine_threadsafe`.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
import logging
import math
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from typing import Any

from .._jscompat import js_number, js_trim
from ..client import ReconnectOptions, TokenProvider
from ..clock import Clock, LoopClock, RandomFn
from ..socket import WebSocketConnector
from . import status as st
from .envelope import DEFAULT_URL, AppErrorCode, RegisterErrorCode, is_valid_service_name
from .errors import DISCONNECTED_FATAL, TAKEN_OVER, UNAUTHORIZED, McpwarpError
from .expose import (
    Agent,
    Closed,
    ExposeHandle,
    ExposeHooks,
    ExposeOptions,
    Exposure,
    validate_agent,
    validate_handler_target,
    validate_integration,
    validate_options,
)
from .http import Handler
from .serve import DEFAULT_MAX_REQUEST_BODY_BYTES
from .session import HELD_DISABLE, Conflict, Disabled, Registration, RetryInfo
from .status import Problem, TunnelState, TunnelStatus

DEFAULT_DISPOSE_TIMEOUT_MS = 5000

_INFO = logging.INFO
_WARN = logging.WARNING
_ERROR = logging.ERROR
_OFF = TunnelStatus(TunnelState.OFF)
# Values that are not "an object" for configure()'s check 1 (JS `typeof config !== "object"`).
_NOT_AN_OBJECT = (str, bytes, bytearray, int, float, complex)


@dataclass(frozen=True)
class TunnelOptions:
    """create_tunnel()'s options: expose()'s minus the per-config ones, plus `logger`.

    Exactly one of `handler` and `target`. None means "not given"."""

    integration: str
    agent: Agent | None = None
    web_socket: WebSocketConnector | None = None
    max_request_body_bytes: int = DEFAULT_MAX_REQUEST_BODY_BYTES
    reconnect: ReconnectOptions | None = None
    on_retry: Callable[[RetryInfo], None] | None = None
    """Called for retries of the current connection only; synchronous."""
    handler: Handler | None = None
    target: str | None = None
    target_timeout_ms: float | None = None
    logger: logging.Logger | None = None
    """Default `logging.getLogger("mcpwarp")`. Never receives the token; messages have no
    "mcpwarp: " prefix. Fatal failures log at ERROR as "CODE: message", retries and non-fatal
    errors at WARNING, progress at INFO."""
    clock: Clock | None = None
    random: RandomFn | None = None
    handler_timeout_ms: float | None = None
    """As ExposeOptions.handler_timeout_ms (an SDK extension), `handler` only."""


@dataclass(frozen=True)
class TunnelConfig:
    """What may change at runtime. Disabled, or a token that trims to "", is "off" whatever the
    other fields hold. None means absent (`url`: the default edge; `kind`: "http";
    `web_socket`: the options' connector, compared with `==` for idempotence).

    `token` is a str or a provider (sync or async callable returning one), called on every
    dial and never cached; a provider is passed through untrimmed and compared with `==`."""

    enabled: bool
    token: TokenProvider
    name: str
    url: str | None = None
    kind: str | None = None
    web_socket: WebSocketConnector | None = None


@dataclass(frozen=True, eq=False)
class _Desired:
    """A config that passed configure()'s checks; compared by identity to tell applies apart."""

    enabled: bool
    token: object
    name: object
    url: str | None
    kind: object
    web_socket: WebSocketConnector | None


@dataclass
class _Active:
    gen: int
    exposure: Exposure
    task: asyncio.Task[ExposeHandle]
    client_closes: list[asyncio.Future[Any]]


_OPTION_FIELDS = tuple(f.name for f in dataclasses.fields(TunnelOptions))


def create_tunnel(options: TunnelOptions | None = None, /, **kwargs: Any) -> Tunnel:
    """Creates a stopped ("off") tunnel. Takes a TunnelOptions or its fields as keyword
    arguments (both: TypeError). Raises TypeError/ValueError right away for an invalid
    `integration`, `agent` or `handler`/`target` choice (MCPWARP.md section 5.2's messages);
    problems with a config (an invalid `name`, say) surface as state "error" instead.

    Synchronous and loop-free: the tunnel binds to the running loop on first use."""
    opts = _collect(options, kwargs)
    validate_integration(opts.integration)
    validate_agent(opts.agent)
    handler = validate_handler_target(
        opts.handler,
        opts.target,
        opts.target_timeout_ms,
        clock=opts.clock,
        handler_timeout_ms=opts.handler_timeout_ms,
    )
    return Tunnel(opts, handler)


def _collect(options: TunnelOptions | None, kwargs: Mapping[str, Any]) -> TunnelOptions:
    if options is not None:
        if kwargs:
            raise TypeError("create_tunnel() takes a TunnelOptions or keyword arguments, not both")
        if not isinstance(options, TunnelOptions):
            raise TypeError("create_tunnel() takes a TunnelOptions or keyword arguments")
        return options
    for key in kwargs:
        if key not in _OPTION_FIELDS:
            raise TypeError(f"create_tunnel() got an unexpected keyword argument {key!r}")
    values = dict(kwargs)
    # Built unchecked: validate_integration() reports a missing one with the spec's message.
    values.setdefault("integration", None)
    return TunnelOptions(**values)


class Tunnel:
    """The controller create_tunnel() returns. See MCPWARP.md section 6."""

    def __init__(self, options: TunnelOptions, handler: Handler) -> None:
        self._base = options
        self._handler = handler
        self._logger = options.logger or logging.getLogger("mcpwarp")
        self._clock: Clock = options.clock or LoopClock()
        self._current = _OFF
        self._listeners: dict[int, Callable[[TunnelStatus], object]] = {}
        self._next_listener = 0
        self._generation = 0
        self._desired: _Desired | None = None
        """The latest config passed to configure(); cleared by stop(), a conflict, an error
        and an invalid config, so the same config then starts again."""
        self._active: _Active | None = None
        self._draining: asyncio.Future[None] | None = None
        """Every background shutdown started so far (None: nothing to wait for)."""
        self._queue: asyncio.Task[None] | None = None
        self._applying: asyncio.Task[None] | None = None
        """The apply of `_desired`, returned to a repeat of it."""
        self._closed = False
        self._closing: asyncio.Task[None] | None = None

    def __repr__(self) -> str:
        return f"<Tunnel {self._current.state.value}>"

    # --- public surface ------------------------------------------------------------------

    @property
    def status(self) -> TunnelStatus:
        """The current status; immutable, safe to read from any thread."""
        return self._current

    def subscribe(self, cb: Callable[[TunnelStatus], None]) -> Callable[[], None]:
        """Calls `cb` now with the current status and on every change; returns the unsubscribe
        function. Each call is its own subscription: the same `cb` subscribed twice gets every
        status twice (JS's Set would deliver once). After dispose(): one call with "off", and a
        no-op unsubscribe."""
        self._notify(cb)
        if self._closed:
            return _noop
        key = self._next_listener
        self._next_listener += 1
        self._listeners[key] = cb

        def unsubscribe() -> None:
            self._listeners.pop(key, None)

        return unsubscribe

    async def configure(self, config: TunnelConfig | Mapping[str, Any] | object) -> None:
        """Applies `config` (a TunnelConfig, a mapping with its field names, or any object
        with those attributes). A no-op when it equals the last one passed (unless a conflict,
        error or stop() came in between); otherwise stops the current connection and, when
        enabled with a token, starts a new one. Returns once the old connection is stopped and
        the new one is dialing (not when online). Applied in call order; the last one wins.
        Never raises: a bad config is state "error", code INVALID_CONFIG."""
        try:
            waiter = self._configure_now(config)
        except Exception as e:
            self._log(_ERROR, f"configure() failed: {_message(e)}")
            return
        if waiter is not None:
            await asyncio.shield(waiter)

    async def stop(self) -> None:
        """Stops (unregister + drain, up to ~7 s) and goes "off"; configure() starts again,
        even with the same config."""
        if self._closed:
            return
        self._desired = None
        self._detach()
        self._set_status(_OFF)
        await asyncio.shield(self._enqueue(self._wait_draining))

    def dispose(self, timeout_ms: float = DEFAULT_DISPOSE_TIMEOUT_MS) -> Coroutine[Any, Any, None]:
        """Final stop: "off" is delivered, listeners are dropped, and the shutdown is awaited
        for at most `timeout_ms` (it then goes on in the background). Afterwards configure()
        and stop() do nothing. Every call returns the first call's result.

        `timeout_ms` is checked here, before the coroutine is created: TypeError for a
        non-number, ValueError for one that is not positive and finite."""
        if isinstance(timeout_ms, bool) or not isinstance(timeout_ms, int | float):
            raise TypeError("mcpwarp: dispose() `timeout_ms` must be a positive finite number")
        if not math.isfinite(timeout_ms) or timeout_ms <= 0:
            raise ValueError("mcpwarp: dispose() `timeout_ms` must be a positive finite number")
        return self._dispose(timeout_ms)

    # --- configure -----------------------------------------------------------------------

    def _configure_now(self, config: object) -> asyncio.Future[None] | None:
        if self._closed:
            return None
        checked = _check_config(config)
        if isinstance(checked, tuple):
            detail, problem = checked
            self._log(_ERROR, f"{problem.code}: {detail}")
            self._desired = None
            self._detach()
            self._set_problem(TunnelState.ERROR, problem)
            return self._enqueue(self._wait_draining)
        if self._same_as_desired(checked):
            return self._applying
        self._desired = checked
        self._applying = self._enqueue(lambda: self._apply(checked))
        return self._applying

    def _same_as_desired(self, b: _Desired) -> bool:
        a = self._desired
        if a is None:
            return False
        try:
            return bool(
                a.enabled == b.enabled
                and _trimmed(a.token) == _trimmed(b.token)
                and a.name == b.name
                and a.url == b.url
                and a.kind == b.kind
                and self._connector(a) == self._connector(b)
            )
        except Exception:
            return False  # an `__eq__` that raises: a change, never a configure() failure

    def _connector(self, cfg: _Desired) -> WebSocketConnector | None:
        return cfg.web_socket if cfg.web_socket is not None else self._base.web_socket

    # --- the apply queue -----------------------------------------------------------------

    def _enqueue(self, task: Callable[[], Coroutine[Any, Any, None]]) -> asyncio.Task[None]:
        """Runs `task` after every earlier one; the chain itself never raises."""
        prev = self._queue

        async def run() -> None:
            if prev is not None:
                await asyncio.shield(prev)
            try:
                await task()
            except Exception as e:
                self._log(_ERROR, f"internal error: {_message(e)}")

        self._queue = asyncio.get_running_loop().create_task(run())
        return self._queue

    async def _wait_draining(self) -> None:
        if self._draining is not None:
            await asyncio.shield(self._draining)

    async def _apply(self, cfg: _Desired) -> None:
        if self._closed or cfg is not self._desired:
            return
        self._detach()
        starts = _will_start(cfg)
        self._set_status(TunnelStatus(TunnelState.CONNECTING) if starts else _OFF)
        await self._wait_draining()
        if self._closed or cfg is not self._desired:
            return
        if starts:
            self._start(cfg)
        else:
            self._log(_INFO, "off")

    # --- one generation ------------------------------------------------------------------

    def _start(self, cfg: _Desired) -> None:
        self._generation += 1
        gen = self._generation

        def live() -> bool:
            return gen == self._generation

        self._set_status(TunnelStatus(TunnelState.CONNECTING))
        via = f" via {cfg.url}" if cfg.url else ""
        self._log(_INFO, f"connecting as {_quote(cfg.name)}{via}")
        # A listener or the logger may have ended this generation just now.
        if not live():
            return

        base = self._base
        on_retry = base.on_retry

        def forward_retry(info: RetryInfo) -> None:
            if not live() or on_retry is None:
                return
            result: object = on_retry(info)
            if inspect.iscoroutine(result):
                result.close()
                self._log(_WARN, "on_retry returned a coroutine; it must be a plain function")

        token = cfg.token
        if isinstance(token, str):
            token = js_trim(token)
        elif not callable(token):  # unreachable: _will_start() lets only these through
            self._fail(cfg, TypeError("`token` must be a string or a callable"), validation=True)
            return
        # name and kind are only type-checked by validate_options() below.
        opts = ExposeOptions(
            token=token,
            name=cfg.name,  # type: ignore[arg-type]
            integration=base.integration,
            url=DEFAULT_URL if cfg.url is None else cfg.url,
            kind="http" if cfg.kind is None else cfg.kind,  # type: ignore[arg-type]
            web_socket=self._connector(cfg),
            agent=base.agent,
            reconnect=base.reconnect,
            max_request_body_bytes=base.max_request_body_bytes,
            on_retry=None if on_retry is None else forward_retry,
            handler=self._handler,
            clock=self._clock,
            random=base.random,
            handler_timeout_ms=base.handler_timeout_ms,
        )
        try:
            validate_options(opts)
        except (TypeError, ValueError) as e:
            self._fail(cfg, e, validation=True)
            return

        client_closes: list[asyncio.Future[Any]] = []

        def observe(event: str, payload: object, detail: str | None) -> None:
            if not live():
                return
            try:
                self._observe(event, payload, detail, cfg)
            except Exception as e:
                self._log(_ERROR, f"internal error: {_message(e)}")

        try:
            exposure = Exposure(
                opts,
                self._handler,
                ExposeHooks(observe=observe, on_client_close=client_closes.append),
            )
            task = asyncio.get_running_loop().create_task(exposure.run())
        except Exception as e:
            self._fail(cfg, e)
            return
        # Every outcome is observed through the hooks; the task's own result settles nothing.
        task.add_done_callback(_retrieve)
        self._active = _Active(gen, exposure, task, client_closes)

    def _observe(self, event: str, payload: object, detail: str | None, cfg: _Desired) -> None:
        """One exposure event of the current generation (JS tunnel.ts observe())."""
        if event == "registered" and isinstance(payload, Registration):
            if detail == HELD_DISABLE:
                return  # the `disabled` right behind it is the one snapshot (T50)
            self._log(_INFO, f"online at {payload.url}")
            self._set_status(TunnelStatus(TunnelState.ONLINE, payload.url))
        elif event == "disabled" and isinstance(payload, Disabled):
            self._log(_WARN, f"the service was disabled ({payload.reason})")
            self._set_problem(TunnelState.DISABLED, st.DISABLED)
        elif event == "enabled":
            self._log(_INFO, "the service was enabled, registering again")
            self._set_status(TunnelStatus(TunnelState.CONNECTING))
        elif event == "conflict" and isinstance(payload, Conflict):
            self._conflicted(cfg)
        elif event == "error" and isinstance(payload, BaseException):
            # Any register refusal is terminal: after an earlier registration nothing retries it.
            if detail == "register_refusal":
                self._fail(cfg, payload)
                return
            self._log(_WARN, _technical(payload))
            # Transient register errors are explained by their retry; handler errors never
            # change the status.
            if detail in ("app", "malformed") and self._current.state == TunnelState.CONNECTING:
                self._set_problem(TunnelState.CONNECTING, st.INTERNAL)
        elif event == "retry" and isinstance(payload, RetryInfo):
            delay = js_number(payload.delay_ms)
            self._log(_WARN, f"retry #{payload.attempt} in {delay} ms: {payload.reason}")
            # While online or disabled this is a drain replacement's register; the old
            # connection still serves.
            if (
                detail in ("register", "overloaded")
                and self._current.state == TunnelState.CONNECTING
            ):
                self._set_problem(TunnelState.CONNECTING, st.RETRY_REGISTER)
        elif event == "disconnect":
            fatal = bool(getattr(payload, "fatal", False))
            if fatal:
                return  # the fatal end arrives as "close"
            message = str(getattr(payload, "message", ""))
            phase = getattr(payload, "phase", None)
            if detail == "superseded":
                if phase == "connected":
                    self._log(_INFO, f"the superseded connection ended: {message}")
                else:
                    self._log(_WARN, f"disconnected: {message}")
                return
            self._log(_WARN, f"disconnected: {message}")
            if phase == "connected":
                self._set_problem(TunnelState.CONNECTING, st.LOST)
            elif (
                self._current.state == TunnelState.CONNECTING
                and self._current.code != st.TunnelStatusCode.DISCONNECTED
            ):
                # After a lost connection the failed redials keep saying so.
                self._set_problem(TunnelState.CONNECTING, st.RETRY_DIAL)
        elif event == "close" and isinstance(payload, Closed):
            error = payload.error
            self._fail(cfg, error if error is not None else Exception("the connection closed"))

    def _conflicted(self, cfg: _Desired) -> None:
        self._log(
            _WARN,
            f"another connection took over the name {_quote(cfg.name)}; not reconnecting "
            "until configure() is called again",
        )
        self._forget(cfg)
        self._detach()
        self._set_problem(TunnelState.CONFLICT, st.TAKEN_OVER)

    def _fail(self, cfg: _Desired, err: BaseException, *, validation: bool = False) -> None:
        """A fatal failure of the current generation, or (`validation`) expose()'s option
        check refusing the config at start."""
        if isinstance(err, McpwarpError) and err.code == TAKEN_OVER:
            self._conflicted(cfg)
            return
        if validation:
            problem = st.INVALID_SETTINGS if is_valid_service_name(cfg.name) else st.INVALID_NAME
        else:
            problem = _describe_problem(err)
        if isinstance(err, McpwarpError):
            self._log(_ERROR, _technical(err))
        else:
            self._log(_ERROR, f"{problem.code}: {_message(err)}")
        self._forget(cfg)
        self._detach()
        self._set_problem(TunnelState.ERROR, problem)

    def _forget(self, cfg: _Desired) -> None:
        if self._desired is cfg:
            self._desired = None

    def _detach(self) -> None:
        """Ends the current generation synchronously and closes its connection in the
        background (tracked in `_draining`)."""
        self._generation += 1
        a, self._active = self._active, None
        if a is None:
            return
        self._log(_INFO, "stopping")
        loop = asyncio.get_running_loop()
        closed = loop.create_task(self._shutdown(a))
        prev = self._draining
        self._draining = loop.create_task(_join(prev, closed))

    async def _shutdown(self, a: _Active) -> None:
        """expose()'s abort (cancelling the task awaiting it), then the handle's close(): a
        handle that resolved for this ended generation is closed, never shown."""
        a.task.cancel()
        await asyncio.wait((a.task,))
        try:
            await a.exposure.handle.close()
            for fut in list(a.client_closes):
                await fut
        except Exception as e:
            self._log(_WARN, f"error while closing: {_message(e)}")

    # --- dispose -------------------------------------------------------------------------

    async def _dispose(self, timeout_ms: float) -> None:
        if self._closing is None:
            # Final at this coroutine's first step, so a configure()/stop() scheduled after it
            # is a no-op; only the bounded wait goes in the task.
            self._closed = True
            self._desired = None
            self._detach()
            self._set_status(_OFF)
            self._listeners.clear()
            loop = asyncio.get_running_loop()
            self._closing = loop.create_task(self._wait_bounded(timeout_ms))
        await asyncio.shield(self._closing)

    async def _wait_bounded(self, timeout_ms: float) -> None:
        loop = asyncio.get_running_loop()
        timed_out: asyncio.Future[None] = loop.create_future()

        def give_up() -> None:
            if not timed_out.done():
                self._log(_WARN, f"dispose() gave up waiting after {js_number(timeout_ms)} ms")
                timed_out.set_result(None)

        timer = self._clock.call_later(timeout_ms, give_up)
        waiter = loop.create_task(self._wait_queue_and_draining())
        try:
            await asyncio.wait((waiter, timed_out), return_when=asyncio.FIRST_COMPLETED)
        finally:
            timer.cancel()
            # The shutdown itself goes on (shielded inside); only this wait for it ends.
            waiter.cancel()

    async def _wait_queue_and_draining(self) -> None:
        if self._queue is not None:
            await asyncio.shield(self._queue)
        await self._wait_draining()

    # --- status --------------------------------------------------------------------------

    def _set_problem(self, state: TunnelState, problem: Problem) -> None:
        self._set_status(TunnelStatus(state, None, problem.code, problem.last_error))

    def _set_status(self, status: TunnelStatus) -> None:
        if status == self._current:
            return
        self._current = status
        for cb in list(self._listeners.values()):
            self._notify(cb)

    def _notify(self, cb: Callable[[TunnelStatus], object]) -> None:
        try:
            result = cb(self._current)
        except Exception as e:
            self._log(_ERROR, f"status listener threw: {_message(e)}")
            return
        if inspect.iscoroutine(result):
            result.close()
            self._log(_WARN, "a status listener returned a coroutine; it must be a plain function")

    def _log(self, level: int, message: str) -> None:
        try:
            self._logger.log(level, "%s", message)
        except Exception:
            pass  # a failing logger has nowhere to report to


# --- config checks (MCPWARP.md section 6.2) ------------------------------------------------


def _field(config: object, key: str) -> object:
    """A config field, None when absent (a missing key or attribute, or None)."""
    try:
        if isinstance(config, Mapping):
            return config.get(key)
        return getattr(config, key, None)
    except Exception:
        return None


def _check_config(config: object) -> _Desired | tuple[str, Problem]:
    """The section 6.2 checks, first failure wins: (log detail, problem), or the config."""
    if config is None or isinstance(config, _NOT_AN_OBJECT):
        return "the config must be an object", st.INVALID_SETTINGS
    enabled = _field(config, "enabled")
    if not isinstance(enabled, bool):
        return "`enabled` must be a boolean", st.INVALID_SETTINGS
    token = _field(config, "token")
    name = _field(config, "name")
    url = _field(config, "url")
    kind = _field(config, "kind")
    web_socket = _field(config, "web_socket")
    # Disabled, or an empty token, is "off" whatever the rest says. A provider is never called
    # or inspected here.
    if enabled and not (isinstance(token, str) and js_trim(token) == ""):
        if not isinstance(token, str) and not callable(token):
            return "`token` must be a string or a callable", st.TOKEN_NOT_SET
        if not isinstance(name, str):
            return "`name` must be a string", st.NAME_NOT_SET
        if url is not None and not isinstance(url, str):
            return "`url` must be a string", st.INVALID_SETTINGS
        if web_socket is not None and not callable(web_socket):
            return "`web_socket` must be callable", st.INVALID_SETTINGS
    return _Desired(
        enabled,
        token,
        name,
        url if isinstance(url, str) else None,
        kind,
        web_socket if callable(web_socket) else None,
    )


def _will_start(cfg: _Desired) -> bool:
    if isinstance(cfg.token, str):
        return cfg.enabled and js_trim(cfg.token) != ""
    return cfg.enabled and callable(cfg.token)


def _trimmed(token: object) -> object:
    """A provider compares by `==` (identity; receiver and function for a bound method), as a
    disabled config's unchecked token does."""
    return js_trim(token) if isinstance(token, str) else token


# --- problems and messages -----------------------------------------------------------------


def _describe_problem(err: BaseException) -> Problem:
    """What a failure means for the user; the technical message goes to the logger."""
    if isinstance(err, McpwarpError):
        code = err.code
        if code in (UNAUTHORIZED, RegisterErrorCode.UNAUTHENTICATED):
            return st.REJECTED
        if code == RegisterErrorCode.FORBIDDEN:
            return st.FORBIDDEN
        if code == TAKEN_OVER:
            return st.TAKEN_OVER
        if code == RegisterErrorCode.INVALID_NAME:
            return st.INVALID_NAME
        if code == RegisterErrorCode.USERNAME_REQUIRED:
            return st.USERNAME_REQUIRED
        if code == RegisterErrorCode.QUOTA_EXCEEDED:
            return st.QUOTA_EXCEEDED
        if code == RegisterErrorCode.CONFLICT:
            return st.CONFLICT
        if code == AppErrorCode.UNSUPPORTED_VERSION:
            return st.UNSUPPORTED_VERSION
        if code == DISCONNECTED_FATAL:
            return st.DISCONNECTED_FATAL
    return st.INTERNAL


def _message(err: BaseException) -> str:
    """For the logger, which tags messages itself."""
    text = str(err)
    return text[len("mcpwarp: ") :] if text.startswith("mcpwarp: ") else text


def _technical(err: BaseException) -> str:
    """_message() with the McpwarpError code."""
    return f"{err.code}: {_message(err)}" if isinstance(err, McpwarpError) else _message(err)


def _quote(name: object) -> str:
    """`JSON.stringify(name)`."""
    try:
        return json.dumps(name, ensure_ascii=False)
    except (TypeError, ValueError):
        return json.dumps(str(name), ensure_ascii=False)


def _noop() -> None:
    pass


def _retrieve(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


async def _join(prev: asyncio.Future[None] | None, closed: asyncio.Future[None]) -> None:
    if prev is not None:
        await asyncio.shield(prev)
    await asyncio.shield(closed)


__all__ = [
    "DEFAULT_DISPOSE_TIMEOUT_MS",
    "Tunnel",
    "TunnelConfig",
    "TunnelOptions",
    "create_tunnel",
]
