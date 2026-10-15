"""Scripted in-memory WebSockets for tests: `FakeConnector`, `FakeWebSocket`, `socket_pair()`.

Messages are delivered strictly in order through per-direction asyncio queues; nothing here
touches the network or a clock.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable, Iterable, Mapping

from ..socket import (
    SUBPROTOCOL,
    UpgradeRejected,
    WebSocketClosed,
    WebSocketConnection,
    WebSocketInit,
    check_close_args,
)

_Inbound = bytes | str | WebSocketClosed


def _copy(c: WebSocketClosed) -> WebSocketClosed:
    return WebSocketClosed(c.code, c.reason, c.received)


def _drain(q: asyncio.Queue[_Inbound]) -> None:
    while not q.empty():
        q.get_nowait()


class FakeWebSocket:
    """One scripted connection, handed out by `FakeConnector`.

    The test decides how the dial ends with `open()`, `reject()` or `fail()`, before or after
    the SDK dials. `push()` and `server_close()` drive what `recv()` returns; `sent`
    (also awaitable one by one with `next_sent()`), `close_calls` and `aborted` record what the
    SDK did, and `dial_cancelled` whether it gave up on the dial. With `echo_close` (the
    default) an SDK `close()` ends `recv()` as a well-behaved server would, with its own code
    and `received=True`; without it the server never answers, so `recv()` stays pending until
    the SDK aborts or the test calls `server_close()`/`server_drop()`. `close()` validates its
    arguments like the real connectors (`check_close_args`).
    """

    def __init__(self, *, echo_close: bool = True) -> None:
        self.subprotocol: str | None = None
        self.url: str | None = None
        self.init: WebSocketInit | None = None
        self.sent: list[bytes] = []
        self.close_calls: list[tuple[int, str]] = []
        self.closed = False
        self.aborted = False
        self.dial_cancelled = False
        self._echo_close = echo_close
        self._sent_cursor = 0
        self._sent_waiters: list[asyncio.Future[None]] = []
        self._outcome: tuple[str | None, BaseException | None] | None = None
        self._outcome_set = asyncio.Event()
        self._inbox: asyncio.Queue[_Inbound] = asyncio.Queue()
        self._end: WebSocketClosed | None = None
        self._send_end: WebSocketClosed | None = None
        self._sends_open = asyncio.Event()
        self._sends_open.set()

    def open(self, subprotocol: str | None = SUBPROTOCOL) -> None:
        """Completes the dial with a 101 echoing `subprotocol` (None: no echo)."""
        self._settle_dial((subprotocol, None))

    def reject(self, status: int, headers: Mapping[str, str] | None = None) -> None:
        """Makes the dial raise `UpgradeRejected(status, headers)`."""
        self._settle_dial((None, UpgradeRejected(status, headers)))

    def fail(self, exc: BaseException | None = None) -> None:
        """Makes the dial raise `exc` (default: ConnectionRefusedError, an OSError)."""
        self._settle_dial((None, exc or ConnectionRefusedError("fake dial refused")))

    def _settle_dial(self, outcome: tuple[str | None, BaseException | None]) -> None:
        if self._outcome is not None:
            raise RuntimeError("this FakeWebSocket's dial was already settled")
        self._outcome = outcome
        self._outcome_set.set()

    async def _dial(self, url: str, init: WebSocketInit) -> WebSocketConnection:
        self.url, self.init = url, init
        try:
            await self._outcome_set.wait()
        except asyncio.CancelledError:
            self.dial_cancelled = True
            raise
        assert self._outcome is not None
        subprotocol, exc = self._outcome
        if exc is not None:
            raise exc
        self.subprotocol = subprotocol
        return self

    def push(self, data: bytes | str) -> None:
        """Queues a message from the server (`str` for a text frame)."""
        self._inbox.put_nowait(data)

    def server_close(self, code: int = 1000, reason: str = "") -> None:
        """Queues the server's close frame: `recv()` raises `WebSocketClosed(received=True)`."""
        self._inbox.put_nowait(WebSocketClosed(code, reason, received=True))

    def server_drop(self) -> None:
        """Queues a transport loss with no close frame (1006)."""
        self._inbox.put_nowait(WebSocketClosed(1006))

    def set_echo_close(self, echo: bool) -> None:
        """Changes `echo_close` for SDK `close()` calls made from now on (False: the server
        stops answering them, e.g. while it holds the connection open)."""
        self._echo_close = echo

    def pause_sends(self) -> None:
        """Makes `send()` block (socket backpressure) until `resume_sends()`."""
        self._sends_open.clear()

    def resume_sends(self) -> None:
        self._sends_open.set()

    async def recv(self) -> bytes | str:
        if self._end is not None:
            raise _copy(self._end)
        item = await self._inbox.get()
        if isinstance(item, WebSocketClosed):
            self._end = item
            self.closed = True
            self._send_end = self._send_end or item
            raise _copy(item)
        return item

    async def send(self, data: bytes) -> None:
        if self._send_end is None and not self._sends_open.is_set():
            await self._sends_open.wait()
        if self._send_end is not None:
            raise _copy(self._send_end)
        self.sent.append(bytes(data))
        waiters, self._sent_waiters = self._sent_waiters, []
        for w in waiters:
            if not w.done():
                w.set_result(None)

    async def next_sent(self) -> bytes:
        """The next message the SDK sent that no earlier `next_sent()` returned, once sent."""
        while self._sent_cursor >= len(self.sent):
            w: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._sent_waiters.append(w)
            await w
        self._sent_cursor += 1
        return self.sent[self._sent_cursor - 1]

    async def close(self, code: int = 1000, reason: str = "") -> None:
        check_close_args(code, reason)
        self.close_calls.append((code, reason))
        if self.closed:
            return
        self.closed = True
        self._send_end = WebSocketClosed(code, reason)
        self._sends_open.set()
        if self._echo_close:
            self._inbox.put_nowait(WebSocketClosed(code, reason, received=True))

    def abort(self) -> None:
        self.aborted = True
        self.closed = True
        self._send_end = WebSocketClosed(1006)
        self._sends_open.set()
        _drain(self._inbox)
        self._inbox.put_nowait(WebSocketClosed(1006))


class FakeConnector:
    """A `WebSocketConnector` handing out `FakeWebSocket`s, recording every dial.

    Sockets come from `sockets` (or `add()`) in order, then from `factory`, then fresh
    unsettled `FakeWebSocket()`s; `next_socket()` waits for a dial to pick one up.
    """

    def __init__(
        self,
        sockets: Iterable[FakeWebSocket] = (),
        factory: Callable[[str, WebSocketInit], FakeWebSocket] | None = None,
    ) -> None:
        self.calls: list[tuple[str, WebSocketInit]] = []
        self.sockets: list[FakeWebSocket] = []
        self._queue: deque[FakeWebSocket] = deque(sockets)
        self._factory = factory
        self._waiters: list[asyncio.Future[None]] = []

    @property
    def dials(self) -> int:
        return len(self.calls)

    @property
    def cancelled_dials(self) -> int:
        """Dials the SDK cancelled before they settled (e.g. its connect timeout)."""
        return sum(1 for ws in self.sockets if ws.dial_cancelled)

    def add(self, ws: FakeWebSocket) -> FakeWebSocket:
        self._queue.append(ws)
        return ws

    async def __call__(self, url: str, init: WebSocketInit) -> WebSocketConnection:
        self.calls.append((url, init))
        if self._queue:
            ws = self._queue.popleft()
        elif self._factory is not None:
            ws = self._factory(url, init)
        else:
            ws = FakeWebSocket()
        self.sockets.append(ws)
        waiters, self._waiters = self._waiters, []
        for w in waiters:
            if not w.done():
                w.set_result(None)
        return await ws._dial(url, init)

    async def next_socket(self, index: int | None = None) -> FakeWebSocket:
        """The socket of dial number `index` (0-based; default: the next dial), once dialed."""
        want = len(self.sockets) if index is None else index
        while len(self.sockets) <= want:
            w: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._waiters.append(w)
            await w
        return self.sockets[want]


class _PairEnd:
    def __init__(self, subprotocol: str | None) -> None:
        self.subprotocol = subprotocol
        self.peer: _PairEnd
        self._inbox: asyncio.Queue[_Inbound] = asyncio.Queue()
        self._end: WebSocketClosed | None = None
        self._send_end: WebSocketClosed | None = None

    async def recv(self) -> bytes | str:
        if self._end is not None:
            raise _copy(self._end)
        item = await self._inbox.get()
        if isinstance(item, WebSocketClosed):
            self._end = item
            self._send_end = self._send_end or item
            raise _copy(item)
        return item

    async def send(self, data: bytes) -> None:
        if self._send_end is not None:
            raise _copy(self._send_end)
        self.peer._inbox.put_nowait(bytes(data))

    async def close(self, code: int = 1000, reason: str = "") -> None:
        check_close_args(code, reason)
        if self._send_end is not None:
            return
        self._send_end = WebSocketClosed(code, reason)
        if self.peer._send_end is None:
            self.peer._send_end = WebSocketClosed(code, reason, received=True)
        self.peer._inbox.put_nowait(WebSocketClosed(code, reason, received=True))
        self._inbox.put_nowait(WebSocketClosed(code, reason, received=True))

    def abort(self) -> None:
        self._send_end = WebSocketClosed(1006)
        _drain(self._inbox)
        self._inbox.put_nowait(WebSocketClosed(1006))
        if self.peer._send_end is None:
            self.peer._send_end = WebSocketClosed(1006)
            self.peer._inbox.put_nowait(WebSocketClosed(1006))


def socket_pair(
    subprotocol: str | None = SUBPROTOCOL,
) -> tuple[WebSocketConnection, WebSocketConnection]:
    """Two connected in-memory ends. A `close()` reaches both ends' `recv()` as a received close
    frame (the peer echoes it); `abort()` ends both with 1006."""
    a, b = _PairEnd(subprotocol), _PairEnd(subprotocol)
    a.peer, b.peer = b, a
    return a, b
