"""ThreadedTunnel: create_tunnel() on a private loop thread, for hosts with no asyncio loop of
their own (Blender). Hosts that already run a loop (Anki) use `Tunnel` directly.

Nothing happens at import or in `__init__` beyond validating the options; `start()` starts the
thread and never dials. Every method is safe from any thread. `status` is lock-free. Callbacks
from `subscribe()` run through `dispatch` when one is given (e.g. `queue.SimpleQueue().put`
drained by a main-thread timer), otherwise on the loop thread.

`stop()` is bounded, idempotent and safe even if `start()` never ran:
1. `dispose()` with 0.8 x timeout, so the unregister and the close handshake get their chance
   (skipped for a zero timeout);
2. one synchronous teardown step on the loop: `abort()` on every socket this run dialed (a
   no-op for those that already closed), so a hung edge cannot keep a transport open past the
   timeout; every subscription detached from this run's Tunnel; and a final "off" for any
   subscriber that has not seen one yet;
3. `LoopThread.stop()`: cancel what is left, stop the loop, join the thread with the rest.
Anything under ~2.5 s skips part of the graceful unregister; the edge then sees an abrupt
close, which the next register overrides anyway.

Every start() gets a new loop, so nothing in the options may be bound to one: a `LoopClock`
(in `clock`, or inside a `StdlibConnector(clock=...)`) binds to the first loop it is used on
and would break the next run. Both are refused with ValueError; leave `clock` unset.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import dataclasses
import inspect
import logging
import threading
import weakref
from collections.abc import Callable, Mapping
from typing import Any, TypeVar

from .._rfc6455 import StdlibConnector
from ..clock import LoopClock, monotonic_ms
from ..socket import WebSocketConnection, WebSocketConnector, WebSocketInit
from ..threaded import Dispatch, LoopThread, clamp_timeout
from .status import TunnelState, TunnelStatus
from .tunnel import Tunnel, TunnelConfig, TunnelOptions, create_tunnel

T = TypeVar("T")

_OFF = TunnelStatus(TunnelState.OFF)
_CONFIG_FIELDS = tuple(f.name for f in dataclasses.fields(TunnelConfig))
DISPOSE_SHARE = 0.8
"""The share of stop()'s timeout given to the graceful dispose()."""


class _Sockets:
    """The sockets one run dialed; weakly held when the connection type allows it."""

    def __init__(self) -> None:
        self._weak: weakref.WeakSet[WebSocketConnection] = weakref.WeakSet()
        self._strong: list[WebSocketConnection] = []
        self.aborted = False

    def add(self, ws: WebSocketConnection) -> None:
        try:
            self._weak.add(ws)
        except TypeError:
            self._strong.append(ws)

    def all(self) -> list[WebSocketConnection]:
        return [*self._weak, *self._strong]


class _Tracked:
    """A connector that records what it dials. Compares equal by the connector it wraps, so
    configure()'s idempotence is unchanged."""

    def __init__(self, inner: WebSocketConnector, sockets: _Sockets) -> None:
        self.inner = inner
        self._sockets = sockets

    async def __call__(self, url: str, init: WebSocketInit) -> WebSocketConnection:
        ws = await self.inner(url, init)
        self._sockets.add(ws)
        if self._sockets.aborted:
            ws.abort()  # dialed after stop() aborted everything
        return ws

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _Tracked):
            return NotImplemented
        return bool(self.inner == other.inner)

    def __hash__(self) -> int:
        return hash(self.inner)


class _Gen:
    """One start()..stop(): the loop it runs on, its Tunnel (created on that loop) and the
    sockets it dialed."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.tunnel: Tunnel | None = None  # set on the loop
        self.sockets = _Sockets()
        self.torn_down = threading.Event()  # the teardown step ran
        self.finished = threading.Event()  # stop() is done with this run
        self.stop_task: asyncio.Task[None] | None = None  # a stop() from the loop thread


class _Sub:
    def __init__(self, cb: Callable[[TunnelStatus], None]) -> None:
        self.cb = cb
        self.active = True
        self.last: TunnelStatus | None = None  # touched on the loop, or before any start()
        self.gen: _Gen | None = None  # the run this is attached to (loop thread only)
        self.detach: Callable[[], None] | None = None


class ThreadedTunnel:
    """create_tunnel() running on its own `LoopThread`. See the module docstring."""

    def __init__(
        self,
        options: TunnelOptions | None = None,
        /,
        *,
        dispatch: Dispatch | None = None,
        thread_name: str = "mcpwarp",
        **kwargs: Any,
    ) -> None:
        # The same checks and messages as create_tunnel(); the result is thrown away.
        create_tunnel(options, **kwargs)
        self._options = options
        self._kwargs = kwargs
        given = options.web_socket if options is not None else kwargs.get("web_socket")
        clock = options.clock if options is not None else kwargs.get("clock")
        if isinstance(clock, LoopClock) or (
            isinstance(given, StdlibConnector) and isinstance(given._clock, LoopClock)
        ):
            raise ValueError(
                "ThreadedTunnel: a LoopClock binds to one loop and every start() makes a new "
                "one; leave `clock` unset"
            )
        self._connector: WebSocketConnector = given if given is not None else StdlibConnector()
        logger = options.logger if options is not None else kwargs.get("logger")
        self._logger: logging.Logger = logger or logging.getLogger("mcpwarp")
        self._lt = LoopThread(name=thread_name, dispatch=dispatch)
        self._lock = threading.Lock()  # lifecycle and subscriptions; never taken by `status`
        self._gen: _Gen | None = None
        self._closing: _Gen | None = None
        self._subs: dict[int, _Sub] = {}
        self._next_sub = 0

    def __repr__(self) -> str:
        return f"<ThreadedTunnel {self.status.state.value}>"

    # --- lifecycle -----------------------------------------------------------------------

    def start(self) -> None:
        """Starts the thread and a fresh Tunnel on it ("off" until configure()); never dials.
        Idempotent. After stop() it starts again with a new loop and a new Tunnel; existing
        subscriptions carry over. Waits for a stop() still running on another thread."""
        while True:
            with self._lock:
                if self._gen is not None:
                    return
                closing = self._closing
                if closing is None or closing.finished.is_set():
                    self._lt.start()
                    gen = _Gen(self._lt.loop)
                    self._gen = gen
                    gen.loop.call_soon_threadsafe(self._boot, gen)
                    return
            if _on_loop(closing.loop):
                raise RuntimeError("ThreadedTunnel.stop() is still in progress on this thread")
            closing.finished.wait()

    def stop(self, timeout: float = 3.0) -> bool:
        """Disposes the Tunnel (0.8 x `timeout`), aborts any socket still open, cancels what is
        left and joins the thread. True once the thread has exited within `timeout` seconds.

        Safe from any thread, safe twice, safe if start() never ran (True). From the loop thread
        itself (a subscribe() callback without `dispatch`) it schedules the same shutdown and
        returns False; the thread then exits by itself. NaN and negative timeouts count as 0,
        infinite or huge ones as an hour."""
        timeout = clamp_timeout(timeout)
        deadline = monotonic_ms() + timeout * 1000
        with self._lock:
            gen = self._gen
            if gen is not None:
                self._gen = None
                self._closing = gen
            closing = self._closing
        if gen is None:
            if closing is not None and not closing.finished.is_set():
                if _on_loop(closing.loop):
                    return False
                if not closing.finished.wait(_remaining_s(deadline)):
                    return False
            return self._lt.stop(_remaining_s(deadline))
        dispose_ms = DISPOSE_SHARE * timeout * 1000
        if _on_loop(gen.loop):
            gen.stop_task = gen.loop.create_task(self._stop_on_loop(gen, dispose_ms))
            return False
        try:
            if dispose_ms > 0:
                # A little past dispose()'s own bound, which logs when it gives up.
                wait_s = min(dispose_ms / 1000 + 0.05, _remaining_s(deadline))
                self._wait_dispose(gen, dispose_ms, wait_s)
            try:
                # Queued ahead of LoopThread.stop()'s cancellations, so it runs even when the
                # wait below is zero.
                self._lt.call_soon(lambda: self._teardown(gen))
            except RuntimeError:
                pass  # the loop has gone (it crashed); LoopThread.stop() reports the rest
            else:
                gen.torn_down.wait(_remaining_s(deadline))
            joined = self._lt.stop(_remaining_s(deadline))
        finally:
            gen.finished.set()
        if not joined:
            self._logger.warning("stop() timed out after %g s; the thread exits by itself", timeout)
        return joined

    def __enter__(self) -> ThreadedTunnel:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()

    # --- the Tunnel ----------------------------------------------------------------------

    @property
    def status(self) -> TunnelStatus:
        """The current status; lock-free, any thread. "off" before start() and after stop()."""
        gen = self._gen
        tunnel = gen.tunnel if gen is not None else None
        return _OFF if tunnel is None else tunnel.status

    def configure(
        self, config: TunnelConfig | Mapping[str, Any]
    ) -> concurrent.futures.Future[None]:
        """Tunnel.configure() on the loop; the future resolves when it returns (dialing, not
        online). Any thread. RuntimeError when not started."""
        with self._lock:
            gen = self._live()
            return self._lt.submit(self._configure(gen, self._track_config(gen, config)))

    def disconnect(self) -> concurrent.futures.Future[None]:
        """Tunnel.stop(): unregisters, goes "off", and configure() starts again (even with the
        same config). The thread keeps running. Any thread. RuntimeError when not started."""
        with self._lock:
            gen = self._live()
            return self._lt.submit(self._disconnect(gen))

    def subscribe(self, cb: Callable[[TunnelStatus], None]) -> Callable[[], None]:
        """Calls `cb` with the current status now (through `dispatch`) and on every change.
        Any thread, before or after start(); survives stop()/start(). Returns the unsubscribe
        function. Called on the thread the callbacks run on (the dispatch thread, or the loop
        thread without `dispatch`), `cb` is never called after it returns, queued calls
        included; from any other thread a call already running may still finish."""
        sub = _Sub(cb)
        with self._lock:
            key = self._next_sub
            self._next_sub += 1
            self._subs[key] = sub
            gen = self._gen
            attached = False
            if gen is not None:
                try:
                    gen.loop.call_soon_threadsafe(self._attach, gen, sub)
                    attached = True
                except RuntimeError:
                    pass  # the loop is gone
            if not attached:
                sub.last = _OFF
        if not attached:
            self._deliver(sub, _OFF)

        def unsubscribe() -> None:
            with self._lock:
                sub.active = False
                self._subs.pop(key, None)
                attached_to, detach = sub.gen, sub.detach
            if attached_to is not None and detach is not None:
                try:
                    attached_to.loop.call_soon_threadsafe(detach)
                except RuntimeError:
                    pass  # dispose() already dropped every listener

        return unsubscribe

    async def call_dispatched(self, fn: Callable[[], T]) -> T:
        """Runs `fn` through `dispatch` (on the host's thread) and returns its result; await it
        on the loop thread, e.g. from the request handler."""
        return await self._lt.call_dispatched(fn)

    # --- internals -----------------------------------------------------------------------

    def _live(self) -> _Gen:
        gen = self._gen
        if gen is None:
            raise RuntimeError("ThreadedTunnel is not started")
        return gen

    def _boot(self, gen: _Gen) -> None:
        """On the loop: the run's Tunnel, and every subscription attached to it."""
        tracked = _Tracked(self._connector, gen.sockets)
        try:
            if self._options is not None:
                gen.tunnel = create_tunnel(dataclasses.replace(self._options, web_socket=tracked))
            else:
                gen.tunnel = create_tunnel(**{**self._kwargs, "web_socket": tracked})
        except Exception:
            self._logger.exception("could not create the tunnel")
            return
        with self._lock:
            subs = list(self._subs.values())
        for sub in subs:
            self._attach(gen, sub)

    def _attach(self, gen: _Gen, sub: _Sub) -> None:
        """On the loop. Tunnel.subscribe() reports the current status at once; a repeat of what
        `sub` last got (the "off" after a restart) is skipped."""
        tunnel = gen.tunnel
        if not sub.active or sub.gen is gen or tunnel is None or gen.torn_down.is_set():
            return

        def listener(status: TunnelStatus) -> None:
            if sub.active and status != sub.last:
                sub.last = status
                self._deliver(sub, status)

        detach = tunnel.subscribe(listener)
        with self._lock:
            if sub.active:
                sub.gen, sub.detach = gen, detach
                return
        detach()  # unsubscribed while Tunnel.subscribe() ran

    def _deliver(self, sub: _Sub, status: TunnelStatus) -> None:
        def call() -> None:
            if not sub.active:
                return
            result: object = sub.cb(status)
            if inspect.iscoroutine(result):
                result.close()
                self._logger.warning("a status listener returned a coroutine; it must be plain")

        self._lt.dispatch_callback(call)

    def _track_config(
        self, gen: _Gen, config: TunnelConfig | Mapping[str, Any]
    ) -> TunnelConfig | Mapping[str, Any]:
        """The config with its own `web_socket` (if any) recording into this run; anything
        else, invalid values included, goes to the Tunnel unchanged. Any other object with
        the config's attributes becomes a Mapping of them."""
        if isinstance(config, TunnelConfig):
            ws = config.web_socket
            if ws is not None and callable(ws):
                return dataclasses.replace(config, web_socket=_Tracked(ws, gen.sockets))
        elif isinstance(config, Mapping):
            ws = config.get("web_socket")
            if ws is not None and callable(ws):
                return {**config, "web_socket": _Tracked(ws, gen.sockets)}
        else:
            ws = getattr(config, "web_socket", None)
            if ws is not None and callable(ws):
                fields = {k: getattr(config, k, None) for k in _CONFIG_FIELDS}
                return {**fields, "web_socket": _Tracked(ws, gen.sockets)}
        return config

    @staticmethod
    async def _configure(gen: _Gen, config: TunnelConfig | Mapping[str, Any]) -> None:
        if gen.tunnel is not None:
            await gen.tunnel.configure(config)

    @staticmethod
    async def _disconnect(gen: _Gen) -> None:
        if gen.tunnel is not None:
            await gen.tunnel.stop()

    @staticmethod
    async def _dispose(gen: _Gen, timeout_ms: float) -> None:
        if gen.tunnel is not None:
            await gen.tunnel.dispose(timeout_ms)

    def _teardown(self, gen: _Gen) -> None:
        """On the loop, synchronously, before LoopThread.stop() cancels anything: abort every
        socket of the run, detach every subscription from its Tunnel, and give each subscriber
        a final "off" if its last status was something else."""
        try:
            gen.sockets.aborted = True
            for ws in gen.sockets.all():
                try:
                    ws.abort()
                except Exception:
                    self._logger.exception("aborting a socket failed")
            with self._lock:
                subs = list(self._subs.values())
                detaches: list[Callable[[], None]] = []
                for sub in subs:
                    if sub.gen is gen and sub.detach is not None:
                        detaches.append(sub.detach)
                        sub.gen = sub.detach = None
            for detach in detaches:
                detach()
            for sub in subs:
                if sub.active and sub.last != _OFF:
                    sub.last = _OFF
                    self._deliver(sub, _OFF)
        finally:
            gen.torn_down.set()

    async def _stop_on_loop(self, gen: _Gen, dispose_ms: float) -> None:
        try:
            if dispose_ms > 0:
                await self._dispose(gen, dispose_ms)
        finally:
            self._teardown(gen)
            with self._lock:
                # From the loop thread this only schedules the shutdown; start() waits for
                # `finished` (or refuses on this thread), so the run being stopped is ours.
                self._lt.stop(0)
            gen.finished.set()

    def _wait_dispose(self, gen: _Gen, dispose_ms: float, timeout_s: float) -> None:
        try:
            fut = self._lt.submit(self._dispose(gen, dispose_ms))
        except RuntimeError:
            return  # the loop has gone (it crashed); LoopThread.stop() reports the rest
        try:
            fut.result(timeout_s)
        except concurrent.futures.TimeoutError:
            self._logger.warning("stop(): the loop did not finish dispose() in time")
        except concurrent.futures.CancelledError:
            pass
        except Exception:
            self._logger.exception("stop(): dispose() failed")


def _on_loop(loop: asyncio.AbstractEventLoop) -> bool:
    try:
        return asyncio.get_running_loop() is loop
    except RuntimeError:
        return False


def _remaining_s(deadline_ms: float) -> float:
    return max(0.0, (deadline_ms - monotonic_ms()) / 1000)


__all__ = ["ThreadedTunnel"]
