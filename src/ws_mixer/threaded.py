"""A private asyncio loop on a daemon thread, for hosts that have no loop of their own.

Nothing happens at import. `LoopThread` never installs signal handlers, never configures logging
and keeps no module-level state, so several instances (and several vendored copies) can coexist.

Shutdown mirrors `asyncio.run`: cancel every task, let the cancellations finish, stop the loop,
then shut down async generators and the default executor and close the loop, all on the loop's
own thread. A task that swallows `CancelledError` keeps the thread alive: `stop()` gives up after
its timeout and returns False, and the daemon thread finishes the cleanup by itself once that task
ends (or dies with the interpreter).

The default-executor shutdown waits, unbounded, for any blocking `run_in_executor` job still in
flight; `shutdown_default_executor(timeout=)` (3.12+) is deliberately not used, since it only
warns and leaves the worker running. Those workers are not daemon threads, so a hung job also
blocks host interpreter exit: keep executor jobs bounded.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

from .clock import Clock, LoopClock, monotonic_ms

T = TypeVar("T")

Dispatch = Callable[[Callable[[], None]], None]
"""Hands a zero-argument callable to the host, which runs it on a thread of its choosing."""


class _Run:
    """One start()..stop() generation: a fresh loop, its thread and its shutdown state."""

    def __init__(self, loop: asyncio.AbstractEventLoop, clock: LoopClock) -> None:
        self.loop = loop
        self.clock = clock
        self.thread: threading.Thread | None = None
        self.stopping = False
        self.draining = False  # touched only on the loop thread
        self.settled = threading.Event()  # tasks cancelled and finished, or the thread exited
        self.fatal: BaseException | None = None


class LoopThread:
    """A daemon thread running a private asyncio loop.

    `start()` is idempotent and may be called again after `stop()`; every start gets a fresh loop.
    `stop()` is idempotent, bounded by its timeout and safe from any thread, including the loop's
    own thread (where it only schedules the shutdown and returns False).

    `dispatch`, if given, routes host-facing callbacks (`dispatch_callback`, `call_dispatched`)
    to the host's thread, e.g. `queue.SimpleQueue().put` drained by a main-thread timer.
    """

    def __init__(self, *, name: str = "ws-mixer", dispatch: Dispatch | None = None) -> None:
        self._name = name
        self._dispatch = dispatch
        self._lock = threading.Lock()
        self._run_state: _Run | None = None

    # lifecycle

    def start(self) -> None:
        with self._lock:
            cur = self._run_state
            if cur is not None and not cur.stopping:
                return
            loop = asyncio.new_event_loop()
            run = _Run(loop, LoopClock(loop))
            thread = threading.Thread(target=self._main, args=(run,), name=self._name, daemon=True)
            try:
                thread.start()
            except BaseException:
                loop.close()
                raise
            run.thread = thread
            self._run_state = run

    def stop(self, timeout: float = 3.0) -> bool:
        """Cancels every task, stops the loop and joins the thread.

        Returns True once the thread has exited. Returns False if it is still alive after
        `timeout` seconds, and always when called from the loop's own thread (the shutdown is
        scheduled and the thread exits by itself).

        The 3.0 s default leaves room for a graceful mcpwarp close (unregister takes up to 2 s);
        a shorter timeout is fine here but cuts that short in ThreadedTunnel.
        """
        deadline = monotonic_ms() + clamp_timeout(timeout) * 1000
        with self._lock:
            run = self._run_state
            if run is None or run.thread is None:
                return True
            first = not run.stopping
            run.stopping = True
        thread = run.thread
        if thread.ident == threading.get_ident():
            if first:
                try:
                    run.loop.call_soon(self._begin_shutdown, run)
                except RuntimeError:
                    pass  # loop already closed
            return False
        if first:
            try:
                run.loop.call_soon_threadsafe(self._begin_shutdown, run)
            except RuntimeError:
                pass  # loop already closed: the thread has exited or is exiting
            if not run.settled.wait(_remaining_s(deadline)):
                try:
                    run.loop.call_soon_threadsafe(self._request_loop_stop, run)
                except RuntimeError:
                    pass
        thread.join(_remaining_s(deadline))
        return not thread.is_alive()

    def __enter__(self) -> LoopThread:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()

    # state

    @property
    def running(self) -> bool:
        run = self._run_state
        return (
            run is not None
            and not run.stopping
            and run.thread is not None
            and run.thread.is_alive()
        )

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._live().loop

    @property
    def clock(self) -> Clock:
        """A `LoopClock` bound to the current loop."""
        return self._live().clock

    @property
    def fatal_error(self) -> BaseException | None:
        """What escaped the current (or last) loop, if anything; the thread then exited."""
        run = self._run_state
        return None if run is None else run.fatal

    # scheduling

    def submit(self, coro: Coroutine[Any, Any, T]) -> concurrent.futures.Future[T]:
        """Runs `coro` on the loop; safe from any thread. Raises RuntimeError if not running."""
        with self._lock:
            run = self._run_state
            if run is None or run.stopping:
                coro.close()
                raise RuntimeError("LoopThread is not running")
            try:
                return asyncio.run_coroutine_threadsafe(coro, run.loop)
            except RuntimeError:
                coro.close()
                raise

    def call_soon(self, fn: Callable[[], None]) -> None:
        """Runs `fn` on the loop thread; safe from any thread."""
        self._live().loop.call_soon_threadsafe(fn)

    def call_dispatched(self, fn: Callable[[], T]) -> asyncio.Future[T]:
        """Runs `fn` through `dispatch` (inline without one) and resolves on the loop.

        Call from the loop thread and await the result there. If `fn` raises, the future carries
        the exception. If the host runs `fn` after the loop has gone, the result is dropped.
        """
        loop = asyncio.get_running_loop()
        run = self._run_state
        if run is None or run.loop is not loop:
            raise RuntimeError("call_dispatched must be called on this LoopThread's loop")
        fut: asyncio.Future[T] = loop.create_future()

        def deliver(ok: bool, value: Any) -> None:
            if fut.done():
                return
            if ok:
                fut.set_result(value)
            else:
                fut.set_exception(value)

        def job() -> None:
            raised: BaseException | None = None
            try:
                result: Any = fn()
                ok = True
            except BaseException as exc:
                result, ok, raised = exc, False, exc
            try:
                loop.call_soon_threadsafe(deliver, ok, result)
            except RuntimeError:
                pass  # loop closed
            if raised is not None and not isinstance(raised, Exception):
                raise raised

        dispatch = self._dispatch
        if dispatch is None:
            job()
        else:
            try:
                dispatch(job)
            except Exception as exc:
                fut.set_exception(exc)
        return fut

    def dispatch_callback(self, fn: Callable[[], None]) -> None:
        """Hands a host callback to `dispatch` (or runs it inline); exceptions are logged."""

        def guarded() -> None:
            try:
                fn()
            except Exception:
                logging.getLogger("ws_mixer").exception("callback raised")

        dispatch = self._dispatch
        if dispatch is None:
            guarded()
            return
        try:
            dispatch(guarded)
        except Exception:
            logging.getLogger("ws_mixer").exception("dispatch raised")

    # internals

    def _live(self) -> _Run:
        run = self._run_state
        if run is None or run.stopping or run.thread is None or not run.thread.is_alive():
            raise RuntimeError("LoopThread is not running")
        return run

    def _run(self, loop: asyncio.AbstractEventLoop) -> None:
        loop.run_forever()

    def _main(self, run: _Run) -> None:
        loop = run.loop
        try:
            asyncio.set_event_loop(loop)
            try:
                self._run(loop)
            except BaseException as exc:
                run.fatal = exc
                logging.getLogger("ws_mixer").error(
                    "loop thread %r crashed", self._name, exc_info=exc
                )
            with self._lock:
                run.stopping = True
            run.draining = True
            steps: tuple[Callable[[], object], ...] = (
                lambda: _cancel_all_tasks(loop),
                lambda: loop.run_until_complete(loop.shutdown_asyncgens()),
                lambda: loop.run_until_complete(loop.shutdown_default_executor()),
                # Catches tasks created during the steps above: a submit() queued before the
                # thread marked itself stopping, or a task spawned by an async generator finalizer.
                lambda: _cancel_all_tasks(loop),
            )
            for step in steps:
                try:
                    step()
                except BaseException as exc:
                    if run.fatal is None:
                        run.fatal = exc
                    logging.getLogger("ws_mixer").error(
                        "loop thread %r failed to shut down cleanly", self._name, exc_info=exc
                    )
        finally:
            try:
                asyncio.set_event_loop(None)
                loop.close()
            finally:
                run.settled.set()

    def _begin_shutdown(self, run: _Run) -> None:
        if run.draining:
            # The thread's own cleanup owns cancellation now; cancelling here would also cancel
            # the shutdown_asyncgens()/shutdown_default_executor() task it is running.
            return
        loop = run.loop
        tasks = asyncio.all_tasks(loop)
        if not tasks:
            run.settled.set()
            self._request_loop_stop(run)
            return
        for task in tasks:
            task.cancel()
        gathered = asyncio.gather(*tasks, return_exceptions=True)

        def done(_: object) -> None:
            run.settled.set()
            self._request_loop_stop(run)

        gathered.add_done_callback(done)

    @staticmethod
    def _request_loop_stop(run: _Run) -> None:
        # Once run_forever() has returned, the thread drives the loop with run_until_complete();
        # a late stop() would make that raise, so it is skipped.
        if not run.draining:
            run.loop.stop()


MAX_TIMEOUT_S = 3600.0


def clamp_timeout(timeout: float) -> float:
    """A stop() timeout as seconds in [0, MAX_TIMEOUT_S]: NaN and negatives are 0, infinity and
    anything huge are MAX_TIMEOUT_S (threading waits overflow on them)."""
    if not timeout > 0:
        return 0.0
    return min(float(timeout), MAX_TIMEOUT_S)


def _remaining_s(deadline_ms: float) -> float:
    return max(0.0, (deadline_ms - monotonic_ms()) / 1000)


def _cancel_all_tasks(loop: asyncio.AbstractEventLoop) -> None:
    """asyncio.runners._cancel_all_tasks: cancel what is left and wait for it, unbounded."""
    while True:
        pending = asyncio.all_tasks(loop)
        if not pending:
            return
        for task in pending:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        for task in pending:
            if task.cancelled() or task.exception() is None:
                continue
            loop.call_exception_handler(
                {
                    "message": "unhandled exception during LoopThread shutdown",
                    "exception": task.exception(),
                    "task": task,
                }
            )
