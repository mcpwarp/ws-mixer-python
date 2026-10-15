"""A small event emitter: on/once/off, synchronous fan-out over a snapshot of the listeners.

Unlike JS's (and Node's), a listener that raises does not stop the others: its exception goes
to `_listener_failed`, which by default re-emits it as "error" (and drops it when it came from
an "error" listener, or when nobody listens for "error"). Emitting "error" with no listener is
a no-op, never a raise. Only `Exception` is isolated; `BaseException` (CancelledError,
KeyboardInterrupt) propagates.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

Listener = Callable[[Any], object]


@dataclass(eq=False)
class _Entry:
    fn: Listener
    once: bool


class Emitter:
    def __init__(self) -> None:
        self._events: dict[str, list[_Entry]] = {}

    def on(self, event: str, listener: Listener) -> Callable[[], None]:
        """Registers `listener`; returns a function that unregisters exactly this registration."""
        return self._add(event, listener, once=False)

    def once(self, event: str, listener: Listener) -> Callable[[], None]:
        """Like `on`, but the listener is dropped before its first call."""
        return self._add(event, listener, once=True)

    def off(self, event: str, listener: Listener) -> None:
        """Unregisters the most recent registration of `listener` for `event`, if any."""
        entries = self._events.get(event)
        if not entries:
            return
        for i in range(len(entries) - 1, -1, -1):
            if entries[i].fn == listener:
                del entries[i]
                break
        if not entries:
            del self._events[event]

    def listener_count(self, event: str) -> int:
        return len(self._events.get(event, ()))

    def emit(self, event: str, arg: object = None) -> bool:
        """Calls every listener in registration order with `arg`. True if there was any."""
        entries = self._snapshot(event)
        for entry in entries:
            if entry.once:
                self._drop(event, entry)
            try:
                entry.fn(arg)
            except Exception as e:
                self._listener_failed(event, e)
        return bool(entries)

    def _listener_failed(self, event: str, exc: Exception) -> None:
        if event != "error" and self.listener_count("error") > 0:
            self.emit("error", exc)

    def _snapshot(self, event: str) -> list[_Entry]:
        return list(self._events.get(event, ()))

    def _drop(self, event: str, entry: _Entry) -> None:
        entries = self._events.get(event)
        if entries is None:
            return
        for i, e in enumerate(entries):
            if e is entry:
                del entries[i]
                break
        if not entries:
            del self._events[event]

    def _add(self, event: str, fn: Listener, *, once: bool) -> Callable[[], None]:
        entry = _Entry(fn, once)
        self._events.setdefault(event, []).append(entry)

        def unsubscribe() -> None:
            self._drop(event, entry)

        return unsubscribe
