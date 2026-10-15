"""The registration state machine for one exposed service (MCPWARP.md sections 2.5-2.7, 3, 5.7).

Sockets and time are injected: `send_app` hands an `app` body to the live connection and the
`Clock` runs every timer, so this module does no I/O of its own. The edge keeps no registration
state across connections, so the session registers on every `welcome` (first connect, reconnect,
drain replacement) until a take-over (`conflict`, or a drain with reason "replaced") hands the
name to another connection for good.

The caller (expose.py) decides which connection an input belongs to: messages and disconnects of
a superseded connection (a drained one whose replacement has received `welcome`) never reach the
session.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal

from ..clock import Clock, TimerHandle
from .envelope import (
    TRANSIENT_REGISTER_ERRORS,
    AppError,
    AppErrorCode,
    ConflictOp,
    Disable,
    Enable,
    Registered,
    RegisterErrorCode,
    ServiceKind,
    Unregistered,
    decode_envelope,
    encode_register,
    encode_unregister,
)
from .errors import McpwarpError

RETRY_BASE_MS = 1000
RETRY_CAP_MS = 30_000
OVERLOAD_PAUSE_MS = 1000
UNREGISTER_BUDGET_MS = 2000
REGISTER_RETRY_PREFIX = "register failed"

RetryKind = Literal["register", "overloaded"]
"""Why a register is re-sent: a transient register error, or `OVERLOADED`."""
ErrorOrigin = Literal["malformed", "register_transient", "register_refusal", "app"]
"""Where a non-fatal `error` came from: an envelope that failed to decode, a transient register
error (retried), a refusal after an earlier registration (nothing retries it), or an `error` op."""


class ExposeState(StrEnum):
    CONNECTING = "connecting"
    REGISTERING = "registering"
    REGISTERED = "registered"
    DISABLED = "disabled"
    CONFLICT = "conflict"
    """Another connection took the name over; sticky until the handle closes."""
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass(frozen=True)
class Registration:
    """One successful register; `created` is True only when it created the service."""

    url: str
    server_id: str
    created: bool


@dataclass(frozen=True)
class Conflict:
    """The `conflict` event: the edge gave the name to another connection.

    `reason` is the `conflict` op's ("taken_over") or "replaced" for a ws-mixer drain with that
    reason. `server_id` is None only for a "replaced" drain when no server id is known.
    """

    server_id: str | None
    name: str
    reason: str


@dataclass(frozen=True)
class Disabled:
    server_id: str | None
    reason: str


@dataclass(frozen=True)
class Enabled:
    server_id: str | None


@dataclass(frozen=True)
class RetryInfo:
    """What `on_retry` gets before every scheduled reconnect and every register re-send."""

    attempt: int
    delay_ms: float
    reason: str


EmitFn = Callable[[str, object, str | None], None]
"""`emit(event, payload, detail)`: events registered (Registration), disabled (Disabled), enabled
(Enabled), conflict (Conflict), error (McpwarpError; detail its ErrorOrigin) and retry
(RetryInfo; detail its RetryKind)."""


class Session:
    def __init__(
        self,
        name: str,
        kind: ServiceKind,
        *,
        send_app: Callable[[dict[str, Any]], Awaitable[None]],
        clock: Clock,
        emit: EmitFn,
        fatal: Callable[[McpwarpError], None],
    ) -> None:
        """`send_app` is called synchronously and must bind to the connection live at that
        moment; a failure (raised, or from the awaitable) is swallowed, since the next
        `welcome` registers again. `fatal` ends the exposure (expose.py closes the client)."""
        self.name = name
        self.kind: ServiceKind = kind
        self.state = ExposeState.CONNECTING
        self.url: str | None = None
        self.server_id: str | None = None
        self.registration: Registration | None = None
        self.ever_registered = False
        self.taken_over = False
        """Set by a take-over; survives reconnects: the session never registers again."""

        self._send_app = send_app
        self._clock = clock
        self._emit = emit
        self._fatal = fatal
        self._connected = False
        self._sent_register_this_conn = False
        self._outstanding: Literal["register", "unregister"] | None = None
        self._transient_failures = 0
        self._overloaded_resends = 0
        """Consecutive OVERLOADED register re-sends since the last successful register."""
        self._retry_timer: TimerHandle | None = None
        self._overload_timer: TimerHandle | None = None
        self._close_waiter: asyncio.Future[None] | None = None
        self._sends: set[asyncio.Future[None]] = set()

    # --- inputs ------------------------------------------------------------------------------

    def on_welcome(self) -> None:
        """A connection received `welcome` and is now the live one."""
        if self._is_ending():
            return
        self._connected = True
        self._sent_register_this_conn = False
        self._cancel_timers()
        if self.taken_over:
            return
        self.state = ExposeState.REGISTERING
        self._send_register()

    def on_disconnect(self) -> None:
        """The live connection ended (never called for a superseded one)."""
        if self.state == ExposeState.CLOSED:
            return
        self._connected = False
        self._sent_register_this_conn = False
        self._outstanding = None
        self._cancel_timers()
        if self.state == ExposeState.CLOSING:
            # `unregistered` can no longer arrive.
            self._end_close_wait()
            return
        if not self.taken_over:
            self.state = ExposeState.CONNECTING

    def on_app(self, body: dict[str, Any]) -> None:
        """An `app` body from the live connection."""
        if self.state == ExposeState.CLOSED:
            return
        try:
            env = decode_envelope(body)
        except McpwarpError as e:
            e.service_name = self.name
            self._emit("error", e, "malformed")
            return
        if isinstance(env, Registered | Disable | Enable) and not self._connected:
            # From a connection that has since ended: ignored (MCPWARP.md §2.7, v0.6.3).
            return
        if isinstance(env, Registered):
            self._on_registered(env)
        elif isinstance(env, Unregistered):
            # The edge answers with no services when this connection held nothing for the name.
            if self._outstanding == "unregister" or any(s.name == self.name for s in env.services):
                if self._outstanding == "unregister":
                    self._outstanding = None
                self._end_close_wait()
        elif isinstance(env, Disable):
            if self._is_ending() or self.taken_over:
                return
            if self.server_id is None or env.id == self.server_id:
                self.server_id = env.id
                self.state = ExposeState.DISABLED
                self._emit("disabled", Disabled(self.server_id, env.reason), None)
        elif isinstance(env, Enable):
            if self._is_ending() or self.taken_over or env.name != self.name:
                return
            self.server_id = env.id
            self._emit("enabled", Enabled(env.id), None)
            self.state = ExposeState.REGISTERING
            if self._connected:
                self._send_register()
        elif isinstance(env, ConflictOp):
            if self.taken_over:
                return
            if env.name != self.name and (self.server_id is None or env.id != self.server_id):
                return
            if self.state == ExposeState.CLOSING:
                self._take_over_while_closing()
                return
            self._take_over(Conflict(env.id, env.name, env.reason))
        elif isinstance(env, AppError):
            self._on_app_error(env.code, env.message)

    def on_replaced(self) -> bool:
        """A ws-mixer drain with reason "replaced": a take-over, like `conflict`.

        False when nothing is emitted: while closing (it only ends close()'s unregister wait), or
        when already taken over or closed.
        """
        if self.state == ExposeState.CLOSING and not self.taken_over:
            self._take_over_while_closing()
            return False
        if self._is_ending() or self.taken_over:
            return False
        self._take_over(Conflict(self.server_id, self.name, "replaced"))
        return True

    async def begin_close(self, budget_ms: float = UNREGISTER_BUDGET_MS) -> None:
        """close() steps 1-2: state closing, timers cancelled; then, if a register went out on
        the live connection, `unregister` and a wait for `unregistered`, at most `budget_ms`.

        The wait also ends on a failed send, a disconnect, or a take-over. Never raises (except
        the caller's own cancellation, which also ends the wait).
        """
        if self._is_ending():
            return
        was_registered = self._connected and self._sent_register_this_conn
        self.state = ExposeState.CLOSING
        self._cancel_timers()
        if not was_registered:
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._close_waiter = fut
        timer = self._clock.call_later(budget_ms, self._end_close_wait)
        self._outstanding = "unregister"
        self._send(encode_unregister(self.name), on_fail=self._end_close_wait)
        try:
            await fut
        finally:
            timer.cancel()
            if self._close_waiter is fut:
                self._close_waiter = None

    def mark_closed(self) -> None:
        """The exposure ended (closed, or failed fatally)."""
        self._cancel_timers()
        self._end_close_wait()
        self.state = ExposeState.CLOSED

    # --- handling ----------------------------------------------------------------------------

    def _on_registered(self, env: Registered) -> None:
        if self._is_ending() or self.taken_over:
            return
        svc = next((s for s in env.services if s.name == self.name), None)
        if svc is not None:
            self._outstanding = None
            self._transient_failures = 0
            self._overloaded_resends = 0
            self.url = svc.url
            self.server_id = svc.id
            self.registration = Registration(svc.url, svc.id, svc.created)
            self.ever_registered = True
            self.state = ExposeState.REGISTERED
            self._emit("registered", self.registration, None)
            return
        err = next((e for e in env.errors if e.name == self.name), None)
        if err is None:
            return
        self._outstanding = None
        if err.code == RegisterErrorCode.SERVER_DISABLED:
            if err.id is not None:
                self.server_id = err.id
            self.state = ExposeState.DISABLED
            self._emit("disabled", Disabled(self.server_id, "disabled"), None)
            return
        if err.code in TRANSIENT_REGISTER_ERRORS:
            error = McpwarpError(err.code, err.message, service_name=self.name)
            self._emit("error", error, "register_transient")
            delay = min(RETRY_BASE_MS * 2 ** min(self._transient_failures, 32), RETRY_CAP_MS)
            self._transient_failures += 1
            info = RetryInfo(
                self._transient_failures,
                delay,
                f"{REGISTER_RETRY_PREFIX}: {err.code}: {err.message}",
            )
            self._emit("retry", info, "register")
            self._clear_retry()
            self._retry_timer = self._clock.call_later(delay, self._on_retry_timer)
            return
        if not self.ever_registered:
            self._fatal(McpwarpError(err.code, err.message, fatal=True, service_name=self.name))
            return
        # Registered before: expose() keeps the handle; the tunnel treats it as terminal.
        refusal = McpwarpError(err.code, err.message, service_name=self.name)
        self._emit("error", refusal, "register_refusal")

    def _on_retry_timer(self) -> None:
        self._retry_timer = None
        if not self._is_ending() and not self.taken_over and self._connected:
            self._send_register()

    def _on_app_error(self, code: str, message: str) -> None:
        if code == AppErrorCode.UNSUPPORTED_VERSION:
            self._fatal(McpwarpError(code, message, fatal=True, service_name=self.name))
            return
        if code == AppErrorCode.OVERLOADED:
            # Re-send what is outstanding once, after a pause (the Go CLI's OverloadPause).
            op = self._outstanding
            if op is None or self._overload_timer is not None:
                return
            self._overload_timer = self._clock.call_later(
                OVERLOAD_PAUSE_MS, lambda: self._on_overload_timer(op)
            )
            if op == "register" and not self._is_ending():
                self._overloaded_resends += 1
                info = RetryInfo(
                    self._overloaded_resends,
                    OVERLOAD_PAUSE_MS,
                    f"{REGISTER_RETRY_PREFIX}: {code}: {message}",
                )
                self._emit("retry", info, "overloaded")
            return
        self._emit("error", McpwarpError(code, message, service_name=self.name), "app")

    def _on_overload_timer(self, op: Literal["register", "unregister"]) -> None:
        self._overload_timer = None
        # Settled meanwhile (e.g. `registered` within the pause): nothing to re-send.
        if not self._connected or self._outstanding != op:
            return
        if op == "register" and not self._is_ending() and not self.taken_over:
            self._send_register()
        elif op == "unregister" and self.state == ExposeState.CLOSING:
            self._send(encode_unregister(self.name))

    def _take_over(self, conflict: Conflict) -> None:
        self.taken_over = True
        self._outstanding = None
        # The edge no longer holds the name for this connection: close() has nothing to
        # unregister.
        self._sent_register_this_conn = False
        self._cancel_timers()
        self.state = ExposeState.CONFLICT
        self._emit("conflict", conflict, None)

    def _take_over_while_closing(self) -> None:
        """close() waits for an `unregistered` that will not come: stop waiting (no event);
        expose.py then closes the connection at once."""
        self.taken_over = True
        self._outstanding = None
        self._end_close_wait()

    # --- plumbing ----------------------------------------------------------------------------

    def _send_register(self) -> None:
        self._outstanding = "register"
        self._sent_register_this_conn = True
        self._send(encode_register(self.name, self.kind))

    def _send(self, body: dict[str, Any], on_fail: Callable[[], None] | None = None) -> None:
        try:
            fut = asyncio.ensure_future(self._send_app(body))
        except Exception:
            if on_fail is not None:
                on_fail()
            return
        self._sends.add(fut)

        def sent(f: asyncio.Future[None]) -> None:
            self._sends.discard(f)
            failed = f.cancelled() or f.exception() is not None
            if failed and on_fail is not None:
                on_fail()

        fut.add_done_callback(sent)

    def _end_close_wait(self) -> None:
        fut, self._close_waiter = self._close_waiter, None
        if fut is not None and not fut.done():
            fut.set_result(None)

    def _is_ending(self) -> bool:
        return self.state in (ExposeState.CLOSING, ExposeState.CLOSED)

    def _clear_retry(self) -> None:
        if self._retry_timer is not None:
            self._retry_timer.cancel()
            self._retry_timer = None

    def _cancel_timers(self) -> None:
        self._clear_retry()
        if self._overload_timer is not None:
            self._overload_timer.cancel()
            self._overload_timer = None
