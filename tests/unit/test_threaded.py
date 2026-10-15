from __future__ import annotations

import asyncio
import concurrent.futures
import gc
import logging
import math
import queue
import subprocess
import sys
import threading
import time
import warnings
from collections.abc import Callable, Iterator

import pytest

from ws_mixer.threaded import MAX_TIMEOUT_S, LoopThread, clamp_timeout

T = 2.0  # generous upper bound for anything that should be near-instant


@pytest.fixture
def baseline() -> Iterator[set[threading.Thread]]:
    before = set(threading.enumerate())
    yield before
    leaked = [t for t in threading.enumerate() if t not in before and t.is_alive()]
    assert not leaked, leaked


@pytest.fixture
def lt(baseline: set[threading.Thread]) -> Iterator[LoopThread]:
    thread = LoopThread(name="ws-mixer-test")
    yield thread
    assert thread.stop(timeout=T)


def _drain(q: queue.SimpleQueue[Callable[[], None]]) -> None:
    q.get(timeout=T)()


def test_start_submit_result(lt: LoopThread) -> None:
    lt.start()
    assert lt.running

    async def where() -> tuple[int, bool]:
        return threading.get_ident(), asyncio.get_running_loop() is lt.loop

    ident, same_loop = lt.submit(where()).result(T)
    assert ident != threading.get_ident()
    assert same_loop


def test_start_is_idempotent(lt: LoopThread) -> None:
    lt.start()
    loop = lt.loop
    lt.start()
    assert lt.loop is loop


def test_call_soon(lt: LoopThread) -> None:
    lt.start()
    seen: list[int] = []
    done = threading.Event()

    def cb() -> None:
        seen.append(threading.get_ident())
        done.set()

    lt.call_soon(cb)
    assert done.wait(T)
    assert seen and seen[0] != threading.get_ident()


def test_clock_is_bound_to_the_loop(lt: LoopThread) -> None:
    lt.start()
    clock = lt.clock

    async def sample() -> tuple[float, float, float]:
        a = clock.now_ms()
        await asyncio.sleep(0.01)
        b = clock.now_ms()
        return a, b, asyncio.get_running_loop().time() * 1000

    a, b, loop_ms = lt.submit(sample()).result(T)
    assert b >= a + 5
    assert abs(loop_ms - b) < 50

    fired = threading.Event()

    def arm() -> None:
        clock.call_later(5, fired.set)

    lt.call_soon(arm)
    assert fired.wait(T)


def test_dispatch_routes_callbacks_to_the_host_thread(baseline: set[threading.Thread]) -> None:
    q: queue.SimpleQueue[Callable[[], None]] = queue.SimpleQueue()
    lt = LoopThread(dispatch=q.put)
    main = threading.get_ident()
    with lt:
        ran_on: list[int] = []

        async def fire() -> None:
            lt.dispatch_callback(lambda: ran_on.append(threading.get_ident()))

        lt.submit(fire()).result(T)
        assert ran_on == []
        _drain(q)
        assert ran_on == [main]

        async def roundtrip() -> tuple[int, int]:
            return await lt.call_dispatched(lambda: (threading.get_ident(), 42))

        fut = lt.submit(roundtrip())
        _drain(q)
        assert fut.result(T) == (main, 42)

        def boom() -> int:
            raise ValueError("host side")

        async def failing() -> int:
            return await lt.call_dispatched(boom)

        fut2 = lt.submit(failing())
        _drain(q)
        with pytest.raises(ValueError, match="host side"):
            fut2.result(T)


def test_without_dispatch_callbacks_run_inline_and_are_isolated(
    lt: LoopThread, caplog: pytest.LogCaptureFixture
) -> None:
    lt.start()

    def bad() -> None:
        raise RuntimeError("callback bug")

    async def go() -> tuple[int, int, int]:
        hits: list[int] = []
        lt.dispatch_callback(bad)
        lt.dispatch_callback(lambda: hits.append(threading.get_ident()))
        value = await lt.call_dispatched(lambda: 7)
        return hits[0], threading.get_ident(), value

    with caplog.at_level(logging.ERROR, logger="ws_mixer"):
        cb_thread, loop_thread, value = lt.submit(go()).result(T)
    assert cb_thread == loop_thread and value == 7
    assert any(r.name == "ws_mixer" and r.exc_info for r in caplog.records)


def test_call_dispatched_off_loop_raises(lt: LoopThread) -> None:
    lt.start()
    with pytest.raises(RuntimeError):
        lt.call_dispatched(lambda: 1)


def test_stop_joins_and_cancels_tasks(lt: LoopThread) -> None:
    lt.start()
    started = threading.Event()
    cancelled = threading.Event()

    async def forever() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    fut = lt.submit(forever())
    assert started.wait(T)
    assert lt.stop(timeout=T)
    assert not lt.running
    assert cancelled.is_set()
    assert fut.cancelled()
    with pytest.raises(RuntimeError):
        _ = lt.loop
    with pytest.raises(RuntimeError):
        lt.call_soon(lambda: None)


def test_stop_shuts_down_the_default_executor(lt: LoopThread) -> None:
    lt.start()

    async def use_executor() -> int:
        return await asyncio.get_running_loop().run_in_executor(None, lambda: 3)

    assert lt.submit(use_executor()).result(T) == 3
    assert lt.stop(timeout=T)  # the `baseline` fixture checks the executor threads are gone


def test_restart_gets_a_fresh_loop(lt: LoopThread, baseline: set[threading.Thread]) -> None:
    lt.start()
    first = lt.loop
    assert lt.stop(timeout=T)
    assert first.is_closed()
    assert set(threading.enumerate()) == baseline
    lt.start()
    assert lt.running and lt.loop is not first

    async def one() -> int:
        return 1

    assert lt.submit(one()).result(T) == 1
    assert lt.stop(timeout=T)
    assert set(threading.enumerate()) == baseline


def test_stop_before_start_and_twice(baseline: set[threading.Thread]) -> None:
    lt = LoopThread()
    assert lt.stop()
    lt.start()
    assert lt.stop(timeout=T)
    assert lt.stop(timeout=T)
    assert lt.fatal_error is None


def test_concurrent_stop_from_two_threads(lt: LoopThread) -> None:
    lt.start()
    results: list[bool] = []
    go = threading.Barrier(2)

    def stopper() -> None:
        go.wait(T)
        results.append(lt.stop(timeout=T))

    threads = [threading.Thread(target=stopper) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(T)
    assert results == [True, True]


def test_stop_from_the_loop_thread_returns_false_and_thread_exits(lt: LoopThread) -> None:
    lt.start()
    thread = next(t for t in threading.enumerate() if t.name == "ws-mixer-test")

    async def stop_from_inside() -> bool:
        return lt.stop(timeout=T)

    assert lt.submit(stop_from_inside()).result(T) is False
    thread.join(T)
    assert not thread.is_alive()
    assert not lt.running


def test_task_that_swallows_cancellation(lt: LoopThread) -> None:
    lt.start()
    loop = lt.loop
    started = threading.Event()
    gate = asyncio.Event()

    async def stubborn() -> None:
        started.set()
        while True:
            try:
                await gate.wait()
                return
            except asyncio.CancelledError:
                continue

    lt.submit(stubborn())
    assert started.wait(T)
    t0 = time.monotonic()
    assert lt.stop(timeout=0.2) is False
    assert time.monotonic() - t0 < 0.3
    assert not lt.running
    loop.call_soon_threadsafe(gate.set)
    assert lt.stop(timeout=T)
    assert loop.is_closed()


def test_submit_when_not_running_closes_the_coroutine(baseline: set[threading.Thread]) -> None:
    lt = LoopThread()

    async def never() -> None:
        pass

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeError):
            lt.submit(never())
        lt.start()
        assert lt.stop(timeout=T)
        with pytest.raises(RuntimeError):
            lt.submit(never())
        gc.collect()
    assert not [w for w in caught if "never awaited" in str(w.message)]


def test_fatal_error_is_captured(
    baseline: set[threading.Thread],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def boom(self: LoopThread, loop: asyncio.AbstractEventLoop) -> None:
        raise RuntimeError("loop body")

    monkeypatch.setattr(LoopThread, "_run", boom)
    lt = LoopThread()
    with caplog.at_level(logging.ERROR, logger="ws_mixer"):
        lt.start()
        assert lt.stop(timeout=T)
    assert isinstance(lt.fatal_error, RuntimeError)
    assert not lt.running
    with pytest.raises(RuntimeError):
        lt.submit(asyncio.sleep(0))


def test_context_manager(baseline: set[threading.Thread]) -> None:
    with LoopThread() as lt:
        assert lt.running
    assert not lt.running


def test_import_starts_no_thread() -> None:
    probe = "import threading, ws_mixer.threaded; assert threading.active_count() == 1"
    out = subprocess.run(
        [sys.executable, "-I", "-c", probe], capture_output=True, text=True, timeout=30
    )
    assert out.returncode == 0, out.stderr


@pytest.mark.parametrize("timeout", [math.inf, 1e300, math.nan, -1.0])
def test_stop_clamps_non_finite_and_huge_timeouts(lt: LoopThread, timeout: float) -> None:
    lt.start()
    t0 = time.monotonic()
    joined = lt.stop(timeout=timeout)  # no OverflowError from the thread waits
    assert time.monotonic() - t0 < 0.5
    if timeout > 0:
        assert joined
    assert not lt.running


def test_clamp_timeout() -> None:
    assert clamp_timeout(math.inf) == clamp_timeout(1e300) == MAX_TIMEOUT_S
    assert clamp_timeout(math.nan) == clamp_timeout(-1) == clamp_timeout(0) == 0.0
    assert clamp_timeout(2.5) == 2.5


def test_stop_with_zero_timeout_returns_immediately(lt: LoopThread) -> None:
    lt.start()
    started = threading.Event()

    async def forever() -> None:
        started.set()
        await asyncio.Event().wait()

    lt.submit(forever())
    assert started.wait(T)
    t0 = time.monotonic()
    lt.stop(timeout=0)
    assert time.monotonic() - t0 < 0.5
    assert not lt.running


def test_submit_racing_stop_resolves_every_future(lt: LoopThread) -> None:
    lt.start()
    futures: list[concurrent.futures.Future[None]] = []
    go = threading.Event()

    async def forever() -> None:
        await asyncio.Event().wait()

    def spam() -> None:
        while True:
            try:
                futures.append(lt.submit(forever()))
            except RuntimeError:
                return
            go.set()

    spammer = threading.Thread(target=spam)
    spammer.start()
    assert go.wait(T)
    assert lt.stop(timeout=T)
    spammer.join(T)
    assert not spammer.is_alive()
    assert futures
    assert all(f.done() and f.cancelled() for f in futures)


def test_failed_thread_start_leaves_the_instance_reusable(
    baseline: set[threading.Thread], monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(self: threading.Thread) -> None:
        raise RuntimeError("can't start new thread")

    lt = LoopThread()
    with monkeypatch.context() as m:
        m.setattr(threading.Thread, "start", refuse)
        with pytest.raises(RuntimeError, match="can't start"):
            lt.start()
    assert not lt.running
    assert lt.stop(timeout=T)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeError, match="not running"):
            lt.submit(asyncio.sleep(0))
        gc.collect()
    assert not [w for w in caught if "never awaited" in str(w.message)]
    lt.start()
    assert lt.running
    assert lt.stop(timeout=T)


def test_crash_with_a_submit_in_flight_resolves_the_future(
    baseline: set[threading.Thread],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    def crash_later(self: LoopThread, loop: asyncio.AbstractEventLoop) -> None:
        entered.set()
        release.wait(T)
        raise RuntimeError("loop body")

    monkeypatch.setattr(LoopThread, "_run", crash_later)

    async def forever() -> None:
        await asyncio.Event().wait()

    lt = LoopThread()
    with caplog.at_level(logging.ERROR, logger="ws_mixer"):
        lt.start()
        assert entered.wait(T)
        fut = lt.submit(forever())
        release.set()
        assert lt.stop(timeout=T)
    assert fut.done() and fut.cancelled()
    assert isinstance(lt.fatal_error, RuntimeError)
