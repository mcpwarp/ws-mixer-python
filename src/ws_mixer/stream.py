"""MixerStream: one ws-mixer.v1 byte stream (WIRE.md sections 2.5-2.6).

The client never opens streams; it only gets them from the server's OPEN. How a stream ends,
and what its reader and writer then see:

- CLOSE both ways (`close_write()` plus the peer's CLOSE, in either order): `wait_closed()`
  gives `Finished`; reads drain the buffer, then return b"".
- RESET, from the peer, from the SDK on a stream-scoped violation, or from the app
  (`reset`/`close`/`refuse`): buffered data is discarded, every pending and later read and
  write raises `StreamError(code)`; `wait_closed()` gives `Reset`.
- The connection ends first (`terminate`): buffered data and an already-received CLOSE are
  still delivered, then reads raise the connection's error (never a clean EOF unless the
  peer's CLOSE had arrived); writes fail; `wait_closed()` gives `ConnectionLost`, or `Reset`
  (not by the peer) when the connection hands a stream-scoped `StreamError`.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Literal, Protocol

from ._jscompat import truncate_utf8, utf8_encode
from .errors import ConnError, ErrorCode, StreamError, WsMixerError, code_name, parse_error_code
from .frame import (
    MAX_CHUNK,
    MAX_RESET_MESSAGE,
    MAX_SEND_WINDOW,
    encode_close,
    encode_reset,
    encode_window,
)

StreamState = Literal["open", "half_closed_local", "half_closed_remote", "closed"]

# WIRE.md 2.8's stream column, plus the range left to the layer above.
_STREAM_CODES = frozenset(
    {
        ErrorCode.PROTOCOL_ERROR,
        ErrorCode.INTERNAL_ERROR,
        ErrorCode.STREAM_CLOSED,
        ErrorCode.REFUSED_STREAM,
        ErrorCode.CANCEL,
        ErrorCode.STREAM_LIMIT,
        ErrorCode.ENHANCE_YOUR_CALM,
    }
)
_APP_CODE_MIN = 0x1000_0000
_APP_CODE_MAX = 0xFFFF_FFFF


@dataclass(frozen=True)
class Finished:
    """CLOSE was sent and received."""


@dataclass(frozen=True)
class Reset:
    code: int
    name: str
    message: str
    by_peer: bool


@dataclass(frozen=True)
class ConnectionLost:
    """The connection ended before the stream finished or was reset."""

    error: ConnError


StreamCloseReason = Finished | Reset | ConnectionLost


class StreamHost(Protocol):
    """What a stream needs from its connection."""

    def send_data(self, stream_id: int, chunk: bytes) -> Awaitable[None]:
        """Queues one DATA chunk for round-robin sending; done once handed to the socket."""
        ...

    def send_control_frame(self, frame: bytes) -> None:
        """Queues a WINDOW/CLOSE/RESET frame on the control queue (fire and forget)."""
        ...

    def retire_stream(self, stream_id: int) -> None:
        """The stream finished or was reset: drop it from the table, fail its queued DATA."""
        ...


def _not_sending(stream_id: int) -> StreamError:
    return StreamError(
        ErrorCode.STREAM_CLOSED, stream_id, "write on a stream that is not open for sending"
    )


def _settle(fut: asyncio.Future[None], exc: BaseException | None = None) -> None:
    if fut.done():
        return
    if exc is None:
        fut.set_result(None)
    else:
        fut.set_exception(exc)


class MixerStream:
    """A server-opened stream. Created only by the SDK; use `read`/`write` and friends."""

    def __init__(
        self,
        stream_id: int,
        host: StreamHost,
        initial_recv_window: int,
        initial_send_window: int,
    ) -> None:
        # Stored errors (`_reset_error`, `_lost_error`) are raised from several places; every raise
        # site resets the traceback so it never accumulates frames from earlier raises.
        loop = asyncio.get_running_loop()
        self.id = stream_id
        self.reset_code: int | None = None
        """Set by a RESET in either direction, or a stream-scoped error at connection teardown."""
        self._host = host
        self._state: StreamState = "open"
        self._initial_window = initial_recv_window
        self._recv_window = initial_recv_window
        self._send_window = initial_send_window
        self._unacked = 0
        self._remote_closed = False
        self._local_closing = False
        self._close_write: asyncio.Future[None] | None = None
        self._reset_error: StreamError | None = None
        self._lost_error: ConnError | StreamError | None = None
        self._buffer: deque[bytes] = deque()
        self._readers: deque[tuple[int, asyncio.Future[bytes]]] = deque()
        self._send_waiters: list[asyncio.Future[None]] = []
        self._write_lock = asyncio.Lock()
        self._writes_pending = 0
        self._closed: asyncio.Future[StreamCloseReason] = loop.create_future()

    def __repr__(self) -> str:
        return f"<MixerStream id={self.id} state={self._state}>"

    @property
    def state(self) -> StreamState:
        return self._state

    @property
    def send_window(self) -> int:
        return self._send_window

    @property
    def recv_window(self) -> int:
        return self._recv_window

    # --- reading -------------------------------------------------------------------------

    async def read(self, n: int = -1) -> bytes:
        """The next buffered bytes (at most `n` when `n` >= 0), or b"" at EOF.

        EOF is the peer's CLOSE with the buffer drained. Raises `StreamError` after a RESET
        (buffer discarded), or the connection's `ConnError` once the connection is gone and the
        buffer is drained without a CLOSE. Consumed bytes are credited back to the peer here.
        Concurrent reads are served in call order.
        """
        if n == 0:
            return b""
        if not self._readers:
            ready, value, error = self._next_read(n)
            if ready:
                if error is not None:
                    raise error.with_traceback(None)
                return value
        fut: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        self._readers.append((n, fut))
        return await fut

    def __aiter__(self) -> MixerStream:
        return self

    async def __anext__(self) -> bytes:
        chunk = await self.read()
        if not chunk:
            raise StopAsyncIteration
        return chunk

    def _next_read(self, n: int) -> tuple[bool, bytes, BaseException | None]:
        if self._buffer:
            head = self._buffer[0]
            if n < 0 or n >= len(head):
                self._buffer.popleft()
                taken = head
            else:
                taken = head[:n]
                self._buffer[0] = head[n:]
            self._credit_consumed(len(taken))
            return True, taken, None
        if self._reset_error is not None:
            return True, b"", self._reset_error
        if self._remote_closed:
            return True, b"", None
        if self._lost_error is not None:
            return True, b"", self._lost_error
        return False, b"", None

    def _pump_reads(self) -> None:
        while self._readers:
            n, fut = self._readers[0]
            if fut.done():
                self._readers.popleft()
                continue
            ready, value, error = self._next_read(n)
            if not ready:
                return
            self._readers.popleft()
            if error is not None:
                fut.set_exception(error)
            else:
                fut.set_result(value)

    def _credit_consumed(self, n: int) -> None:
        self._unacked += n
        if (
            2 * self._unacked >= self._initial_window
            and not self._remote_closed
            and self._state != "closed"
        ):
            increment = self._unacked
            self._recv_window += increment
            self._unacked = 0
            self._host.send_control_frame(encode_window(self.id, increment))

    # --- writing -------------------------------------------------------------------------

    async def write(self, data: bytes | bytearray | memoryview | str) -> None:
        """Sends `data` (a str as UTF-8) in DATA frames of at most 16 KiB, waiting for credit.

        Writes are serialized. Returns once every frame was handed to the socket. Raises
        `StreamError(STREAM_CLOSED)` after `close_write()`, `StreamError(code)` after a RESET,
        or the connection's error once the connection is gone.
        """
        failure = self._reset_error or self._lost_error
        if failure is not None:
            raise failure.with_traceback(None)
        if self._local_closing or self._state in ("half_closed_local", "closed"):
            raise _not_sending(self.id)
        payload = utf8_encode(data) if isinstance(data, str) else bytes(data)
        if not payload:
            return
        self._writes_pending += 1
        try:
            async with self._write_lock:
                await self._send_all(payload)
        finally:
            self._writes_pending -= 1

    async def _send_all(self, payload: bytes) -> None:
        view = memoryview(payload)
        try:
            while view:
                n = await self._reserve_send_credit(len(view))
                # The first point that can see a RESET or teardown that happened meanwhile; past
                # here the stream may already be retired and nothing else stops the chunk.
                done, error = self.send_done()
                if done:
                    assert error is not None
                    raise error
                piece, view = bytes(view[:n]), view[n:]
                await self._host.send_data(self.id, piece)
        except Exception:
            failure = self._reset_error or self._lost_error
            if failure is None:
                raise
            raise failure.with_traceback(None) from None

    async def _reserve_send_credit(self, want: int) -> int:
        while True:
            failure = self._reset_error or self._lost_error
            if failure is not None:
                raise failure.with_traceback(None)
            if self._state not in ("open", "half_closed_remote"):
                raise _not_sending(self.id)
            if self._send_window > 0:
                n = min(want, self._send_window, MAX_CHUNK)
                self._send_window -= n
                return n
            fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._send_waiters.append(fut)
            try:
                await fut
            finally:
                if fut in self._send_waiters:
                    self._send_waiters.remove(fut)

    def _wake_send_waiters(self) -> None:
        waiters, self._send_waiters = self._send_waiters, []
        for fut in waiters:
            _settle(fut)

    async def close_write(self) -> None:
        """Half-close: refuse further writes now, let queued writes flush, then send CLOSE.

        Idempotent and never raises; returns once CLOSE is queued, or at once when the stream
        has already ended.
        """
        await asyncio.shield(self._start_close_write())

    def _start_close_write(self) -> asyncio.Future[None]:
        if self._close_write is None:
            self._local_closing = True
            loop = asyncio.get_running_loop()
            if self._writes_pending == 0:
                self._finish_close_write()
                self._close_write = loop.create_future()
                self._close_write.set_result(None)
            else:
                self._close_write = loop.create_task(self._close_write_after_writes())
        return self._close_write

    async def _close_write_after_writes(self) -> None:
        async with self._write_lock:
            self._finish_close_write()

    def _finish_close_write(self) -> None:
        if self._reset_error is not None or self._lost_error is not None:
            return
        if self._state == "open":
            self._state = "half_closed_local"
        elif self._state == "half_closed_remote":
            self._state = "closed"
        else:
            return
        self._host.send_control_frame(encode_close(self.id))
        self._wake_send_waiters()
        if self._state == "closed":
            self._finish()

    def reset(self, code: int | str = ErrorCode.CANCEL, message: str = "") -> None:
        """RESET both directions: discards buffered data, fails pending reads and writes.

        `code` is a stream-scoped ws-mixer.v1 code (PROTOCOL_ERROR, INTERNAL_ERROR, STREAM_CLOSED,
        REFUSED_STREAM, CANCEL, STREAM_LIMIT, ENHANCE_YOUR_CALM), its wire name, or an
        application code in 0x1000_0000..0xFFFF_FFFF (WIRE.md 2.8); anything else, including
        the connection-only codes, raises ValueError. `message` is cut to 256 UTF-8 bytes on a
        character boundary. A no-op once the stream has ended.
        """
        if isinstance(code, str):
            parsed = parse_error_code(code)
            if parsed is None:
                raise ValueError("unknown ws-mixer error code name")
            code = parsed
        code = int(code)
        if code not in _STREAM_CODES and not _APP_CODE_MIN <= code <= _APP_CODE_MAX:
            raise ValueError(f"{code_name(code)} ({code}) is not a stream RESET code")
        if self._state == "closed":
            return
        self._apply_reset(int(code), truncate_utf8(message, MAX_RESET_MESSAGE), True, False)

    def close(self) -> None:
        """RESET(CANCEL) if the peer has not sent CLOSE yet, else `close_write()`."""
        if not self._remote_closed:
            self.reset(ErrorCode.CANCEL, "local close: peer had not finished sending")
            return
        self._start_close_write()

    def refuse(self) -> None:
        """RESET(REFUSED_STREAM): the application will not serve this stream."""
        self.reset(ErrorCode.REFUSED_STREAM, "stream refused by the application")

    async def wait_closed(self) -> StreamCloseReason:
        """How the stream ended, once it has. Never raises (except the caller's cancellation)."""
        return await asyncio.shield(self._closed)

    # --- driven by MixerConn ---------------------------------------------------------------

    def handle_data(self, payload: bytes) -> None:
        """A received DATA frame. Raises ConnError(FLOW_CONTROL_ERROR) past the credit and
        StreamError(STREAM_CLOSED) after the peer's CLOSE."""
        if self._state == "half_closed_remote":
            raise StreamError(
                ErrorCode.STREAM_CLOSED, self.id, "DATA received after this stream's CLOSE"
            )
        if self._state == "closed":
            raise StreamError(ErrorCode.STREAM_CLOSED, self.id, "DATA received on a closed stream")
        n = len(payload)
        if n > self._recv_window:
            raise ConnError(
                ErrorCode.FLOW_CONTROL_ERROR,
                f"stream {self.id}: received {n} DATA bytes with "
                f"{self._recv_window} credit remaining",
            )
        self._recv_window -= n
        if n == 0:
            return
        self._buffer.append(bytes(payload))
        self._pump_reads()

    def handle_window(self, increment: int) -> None:
        """A received WINDOW. Tolerated on any state; only a cumulative overflow is an error."""
        new_window = self._send_window + increment
        if new_window > MAX_SEND_WINDOW:
            raise ConnError(
                ErrorCode.FLOW_CONTROL_ERROR,
                f"stream {self.id}: WINDOW would push the send window to {new_window}, past 2^31-1",
            )
        if self._state == "closed":
            return
        self._send_window = new_window
        self._wake_send_waiters()

    def handle_close(self) -> None:
        """A received CLOSE: the peer sends no more DATA. The buffer stays readable, then EOF."""
        if self._state == "open":
            self._state = "half_closed_remote"
        elif self._state == "half_closed_local":
            self._state = "closed"
        elif self._state == "half_closed_remote":
            raise StreamError(ErrorCode.STREAM_CLOSED, self.id, "duplicate CLOSE received")
        else:
            return
        self._remote_closed = True
        if self._state == "closed":
            self._finish()
        self._pump_reads()

    def handle_reset(self, code: int, message: str) -> None:
        """A received RESET: any state, buffer dropped, never answered with RESET."""
        if self._state == "closed":
            return
        self._apply_reset(code, message, False, True)

    def abort(self, code: int, message: str) -> None:
        """A stream-scoped violation the SDK detected: like `reset`, but not the app's choice."""
        if self._state == "closed":
            return
        self._apply_reset(code, truncate_utf8(message, MAX_RESET_MESSAGE), True, False)

    def terminate(self, err: ConnError | StreamError) -> None:
        """The connection under this stream ended. Buffered data (and an already-received
        CLOSE) is still delivered, then reads raise `err`; writes fail now. A StreamError ends
        the stream as a local reset that sends nothing."""
        if self._state == "closed":
            return
        self._state = "closed"
        self._lost_error = err
        self._wake_send_waiters()
        self._pump_reads()
        if isinstance(err, StreamError):
            self.reset_code = err.code
            self._settle_closed(Reset(err.code, code_name(err.code), err.message, False))
        else:
            self._settle_closed(ConnectionLost(err))

    def send_done(self) -> tuple[bool, WsMixerError | None]:
        """Whether this stream must send no more DATA, and the error queued DATA fails with."""
        if self._state in ("half_closed_local", "closed"):
            failure = self._reset_error or self._lost_error
            if failure is not None:
                return True, failure
            return True, StreamError(
                ErrorCode.STREAM_CLOSED,
                self.id,
                "DATA not sent: CLOSE already sent on this stream",
            )
        return False, None

    def _apply_reset(self, code: int, message: str, send_frame: bool, by_peer: bool) -> None:
        self._state = "closed"
        self.reset_code = code
        self._reset_error = StreamError(code, self.id, message)
        self._buffer.clear()
        self._wake_send_waiters()
        self._pump_reads()
        if send_frame:
            self._host.send_control_frame(encode_reset(self.id, code, message))
        self._host.retire_stream(self.id)
        self._settle_closed(Reset(code, code_name(code), message, by_peer))

    def _finish(self) -> None:
        self._host.retire_stream(self.id)
        self._settle_closed(Finished())

    def _settle_closed(self, reason: StreamCloseReason) -> None:
        if not self._closed.done():
            self._closed.set_result(reason)
