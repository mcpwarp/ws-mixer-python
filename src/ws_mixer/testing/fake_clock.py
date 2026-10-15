"""A manually advanced `Clock` for deterministic tests."""

from __future__ import annotations

import asyncio
import heapq
from collections.abc import Callable

_SETTLE_FALLBACK_ROUNDS = 50
_SETTLE_MAX_ROUNDS = 10_000


class FakeTimer:
    """A `TimerHandle` from `FakeClock.call_later`."""

    def __init__(self, due_ms: float, cb: Callable[[], None]) -> None:
        self.due_ms = due_ms
        self.cb = cb
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class FakeClock:
    """Time moves only when `advance()` is awaited.

    Timers fire in due order (ties in scheduling order), each with `now_ms()` equal to its due
    time, and with `settle()` run before each one, so the tasks a timer wakes run before the
    next timer fires. A timer scheduled while advancing fires in the same `advance()` when it
    falls due within it.
    """

    def __init__(self, start_ms: float = 0) -> None:
        self._now = float(start_ms)
        self._seq = 0
        self._heap: list[tuple[float, int, FakeTimer]] = []

    def now_ms(self) -> float:
        return self._now

    def call_later(self, delay_ms: float, cb: Callable[[], None]) -> FakeTimer:
        timer = FakeTimer(self._now + max(0.0, delay_ms), cb)
        heapq.heappush(self._heap, (timer.due_ms, self._seq, timer))
        self._seq += 1
        return timer

    def pending(self) -> int:
        """Timers scheduled and neither fired nor cancelled."""
        return sum(1 for _, _, t in self._heap if not t.cancelled)

    def next_due_ms(self) -> float | None:
        for due, _, t in sorted(self._heap):
            if not t.cancelled:
                return due
        return None

    async def advance(self, ms: float, *, limit: int = 100_000) -> None:
        """Moves time forward by `ms`, firing every timer that falls due, then settles.

        An exception from a timer callback propagates out of `advance()` with `now_ms()` left
        at that timer's due time and later timers unfired. More than `limit` timers firing in
        one call (e.g. a 0 ms timer that reschedules itself) raises RuntimeError.
        """
        if ms < 0:
            raise ValueError("cannot move time backwards")
        target = self._now + ms
        await settle()
        fired = 0
        while self._heap and self._heap[0][0] <= target:
            due, _, timer = heapq.heappop(self._heap)
            if timer.cancelled:
                continue
            fired += 1
            if fired > limit:
                raise RuntimeError(f"more than {limit} timers fired in one advance()")
            self._now = due
            timer.cb()
            await settle()
        self._now = target
        await settle()

    async def settle(self) -> None:
        await settle()

    async def run_all(self, limit: int = 10_000) -> None:
        """Advances to each pending timer in turn until none is left (bounded by `limit`)."""
        for _ in range(limit):
            due = self.next_due_ms()
            if due is None:
                return
            await self.advance(due - self._now)
        raise RuntimeError("timers keep rescheduling themselves")


async def settle() -> None:
    """Yields to the loop until it has nothing ready to run, twice in a row.

    Peeks at the private `loop._ready` (CPython's ready queue); without it, yields 50 times.
    Raises RuntimeError if the loop is still busy after 10 000 yields (a busy-looping task).
    """
    loop = asyncio.get_running_loop()
    ready = getattr(loop, "_ready", None)
    if ready is None:
        for _ in range(_SETTLE_FALLBACK_ROUNDS):
            await asyncio.sleep(0)
        return
    idle = 0
    for _ in range(_SETTLE_MAX_ROUNDS):
        await asyncio.sleep(0)
        if len(ready) == 0:
            idle += 1
            if idle >= 2:
                return
        else:
            idle = 0
    raise RuntimeError(f"the loop never went idle in {_SETTLE_MAX_ROUNDS} yields")
