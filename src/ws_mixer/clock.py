"""Time and randomness, injectable.

This is the only module in the package that reads a clock, schedules a loop timer or touches
`random`: everything else takes a `Clock` and a `RandomFn` so tests can drive them
(`ws_mixer.testing.FakeClock`). A `LoopClock` binds to its loop lazily, on first use, so one
can be built before any loop runs.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable
from typing import Protocol

RandomFn = Callable[[], float]
"""Returns a float in [0, 1)."""


class TimerHandle(Protocol):
    def cancel(self) -> None: ...


class Clock(Protocol):
    def now_ms(self) -> float:
        """Monotonic milliseconds; only differences are meaningful."""
        ...

    def call_later(self, delay_ms: float, cb: Callable[[], None]) -> TimerHandle:
        """Runs `cb` on the loop after `delay_ms` (negative counts as 0)."""
        ...


class LoopClock:
    """A `Clock` over an asyncio loop: `loop.time()` and `loop.call_later()`."""

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._loop = loop

    def _bound(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    def now_ms(self) -> float:
        if self._loop is None:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return monotonic_ms()
        return self._bound().time() * 1000

    def call_later(self, delay_ms: float, cb: Callable[[], None]) -> TimerHandle:
        return self._bound().call_later(max(0.0, delay_ms) / 1000, cb)


def monotonic_ms() -> float:
    return time.monotonic() * 1000


def wall_ms() -> float:
    """Milliseconds since the Unix epoch; only for absolute dates such as Retry-After."""
    return time.time() * 1000


def default_random() -> RandomFn:
    return random.random
