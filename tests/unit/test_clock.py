from __future__ import annotations

import asyncio

import pytest

from ws_mixer.clock import LoopClock, default_random, monotonic_ms
from ws_mixer.testing import FakeClock, settle


@pytest.mark.asyncio
async def test_loop_clock_binds_lazily_and_fires() -> None:
    clock = LoopClock()
    loop = asyncio.get_running_loop()
    assert abs(clock.now_ms() - loop.time() * 1000) < 50
    fired = asyncio.Event()
    start = clock.now_ms()
    clock.call_later(20, fired.set)
    await fired.wait()
    assert clock.now_ms() - start >= 15


@pytest.mark.asyncio
async def test_loop_clock_cancel_and_negative_delay() -> None:
    clock = LoopClock(asyncio.get_running_loop())
    hits: list[str] = []
    h = clock.call_later(5, lambda: hits.append("cancelled"))
    h.cancel()
    done = asyncio.Event()

    def now() -> None:
        hits.append("now")
        done.set()

    clock.call_later(-10, now)
    await done.wait()
    assert hits == ["now"]


def test_loop_clock_outside_a_loop_reads_monotonic() -> None:
    a = LoopClock().now_ms()
    assert abs(a - monotonic_ms()) < 1000


def test_default_random_range() -> None:
    r = default_random()
    assert all(0 <= r() < 1 for _ in range(100))


@pytest.mark.asyncio
async def test_fake_clock_fires_in_order_with_now_at_due() -> None:
    clock = FakeClock(start_ms=1000)
    log: list[tuple[str, float]] = []
    clock.call_later(30, lambda: log.append(("c", clock.now_ms())))
    clock.call_later(10, lambda: log.append(("a", clock.now_ms())))
    clock.call_later(10, lambda: log.append(("b", clock.now_ms())))
    clock.call_later(31, lambda: log.append(("late", clock.now_ms())))
    assert clock.pending() == 4
    await clock.advance(30)
    assert log == [("a", 1010), ("b", 1010), ("c", 1030)]
    assert clock.now_ms() == 1030
    assert clock.pending() == 1
    assert clock.next_due_ms() == 1031


@pytest.mark.asyncio
async def test_fake_clock_cancel() -> None:
    clock = FakeClock()
    hits: list[int] = []
    h = clock.call_later(5, lambda: hits.append(1))
    h.cancel()
    assert clock.pending() == 0
    await clock.advance(10)
    assert hits == []


@pytest.mark.asyncio
async def test_fake_clock_nested_scheduling_fires_within_same_advance() -> None:
    clock = FakeClock()
    log: list[float] = []

    def first() -> None:
        log.append(clock.now_ms())
        clock.call_later(5, lambda: log.append(clock.now_ms()))
        clock.call_later(0, lambda: log.append(clock.now_ms()))
        clock.call_later(50, lambda: log.append(clock.now_ms()))

    clock.call_later(10, first)
    await clock.advance(20)
    assert log == [10, 10, 15]
    assert clock.pending() == 1
    await clock.run_all()
    assert log == [10, 10, 15, 60]


@pytest.mark.asyncio
async def test_fake_clock_settles_between_timers() -> None:
    """A task woken by the first timer runs before the second timer fires."""
    clock = FakeClock()
    log: list[str] = []
    woke = asyncio.Event()

    async def waiter() -> None:
        await woke.wait()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        log.append("task")

    task = asyncio.create_task(waiter())
    clock.call_later(1, woke.set)
    clock.call_later(2, lambda: log.append("timer2"))
    await clock.advance(2)
    assert log == ["task", "timer2"]
    await task


@pytest.mark.asyncio
async def test_settle_runs_ready_chains() -> None:
    log: list[int] = []

    async def chain() -> None:
        for i in range(20):
            await asyncio.sleep(0)
            log.append(i)

    task = asyncio.create_task(chain())
    await settle()
    assert log == list(range(20))
    await task


@pytest.mark.asyncio
async def test_fake_clock_bounds_self_rescheduling_zero_timers() -> None:
    clock = FakeClock()

    def again() -> None:
        clock.call_later(0, again)

    clock.call_later(0, again)
    with pytest.raises(RuntimeError):
        await clock.advance(1, limit=500)


@pytest.mark.asyncio
async def test_timer_exception_escapes_advance_mid_way() -> None:
    clock = FakeClock()
    log: list[int] = []

    def boom() -> None:
        raise KeyError("x")

    clock.call_later(5, boom)
    clock.call_later(7, lambda: log.append(7))
    with pytest.raises(KeyError):
        await clock.advance(10)
    assert clock.now_ms() == 5
    assert log == [] and clock.pending() == 1


@pytest.mark.asyncio
async def test_settle_raises_on_a_busy_loop() -> None:
    stop = False

    async def spin() -> None:
        while not stop:
            await asyncio.sleep(0)

    task = asyncio.create_task(spin())
    try:
        with pytest.raises(RuntimeError):
            await settle()
    finally:
        stop = True
        await task


@pytest.mark.asyncio
async def test_fake_clock_rejects_negative_advance() -> None:
    with pytest.raises(ValueError):
        await FakeClock().advance(-1)
