"""A minimal RFC 6455 client over asyncio streams: the SDK's default `WebSocketConnector`.

Scope is the ws-mixer.v1 wire profile (WIRE.md section 2.1): binary messages sent as single
masked frames, no extensions (a 101 that negotiates one is refused), no redirects, no proxy.
Received messages are reassembled from fragments with control frames interleaved; pings are
answered at once (skipped while the write buffer is above its high-water mark, so a ping flood
cannot grow it), pongs ignored. Text messages are returned as `str` so the layer above can
reject them as a ws-mixer protocol error. No RFC-level pings are sent, by design: the in-band
ws-mixer ping is the liveness authority, and the JS SDK's default socket does not ping either.

Reading is single-consumer: `recv()` owns the stream reader, and `close()` reads (discarding
data) until the peer's close frame only when no `recv()` is running. Cancelling a `recv()`
part-way through a frame leaves the stream unframeable, so it aborts the connection. When a
received frame fails the connection (1002/1007/1009), the close frame goes out and incoming
bytes are discarded unparsed until the peer closes TCP or `close_timeout_ms` passes, so the
peer reads our code rather than a reset. Once the close handshake completes the client closes
TCP at once instead of waiting for the server to close first (RFC 6455 section 7.1.1 SHOULD).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import ssl
import struct
from typing import NoReturn
from urllib.parse import quote, urlsplit

from ._http1 import MAX_HEAD_SIZE, HttpParseError, format_upgrade_request, parse_response_head
from ._tls import default_ssl_context
from .clock import Clock, LoopClock, TimerHandle
from .socket import (
    InvalidHandshake,
    UnexpectedSubprotocol,
    UpgradeRejected,
    WebSocketClosed,
    WebSocketInit,
    check_close_args,
)

_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OP_CONT = 0x0
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA

_MAX_CONTROL_PAYLOAD = 125
_DISCARD_CHUNK = 65_536
_VALID_RECEIVED_CODES = frozenset(
    {1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014}
)
_NETWORK_ERRORS = (asyncio.IncompleteReadError, OSError)


def accept_key(key: str) -> str:
    """Sec-WebSocket-Accept for a Sec-WebSocket-Key (RFC 6455 section 4.2.2)."""
    digest = hashlib.sha1(key.encode("ascii") + _GUID, usedforsecurity=False).digest()
    return base64.b64encode(digest).decode("ascii")


def apply_mask(data: bytes, key: bytes) -> bytes:
    """XORs `data` with the repeated 4-byte `key`, as one big-integer operation."""
    n = len(data)
    if n == 0:
        return b""
    pad = (key * ((n + 3) // 4))[:n]
    return (int.from_bytes(data, "big") ^ int.from_bytes(pad, "big")).to_bytes(n, "big")


def encode_frame(opcode: int, payload: bytes, mask_key: bytes) -> bytes:
    """One final, masked frame (client to server)."""
    n = len(payload)
    b0 = 0x80 | opcode
    if n < 126:
        head = struct.pack("!BB", b0, 0x80 | n)
    elif n < 0x10000:
        head = struct.pack("!BBH", b0, 0x80 | 126, n)
    else:
        head = struct.pack("!BBQ", b0, 0x80 | 127, n)
    return head + mask_key + apply_mask(payload, mask_key)


class _Fail(Exception):
    """A received frame that fails the connection with `code`."""

    def __init__(self, code: int, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason


def _parse_url(url: str) -> tuple[bool, str, int, str, str]:
    """(secure, host, port, Host header value, request target)."""
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in ("ws", "wss"):
        raise ValueError(f"unsupported URL scheme {scheme!r}: expected ws:// or wss://")
    if parts.fragment:
        raise ValueError("a WebSocket URL must not have a fragment")
    if parts.username is not None or parts.password is not None:
        raise ValueError("credentials in a WebSocket URL are not supported")
    host = parts.hostname
    if not host:
        raise ValueError("the WebSocket URL has no host")
    if not host.isascii():
        host = host.encode("idna").decode("ascii")
    secure = scheme == "wss"
    default_port = 443 if secure else 80
    port = parts.port if parts.port is not None else default_port
    authority = f"[{host}]" if ":" in host else host
    host_header = authority if port == default_port else f"{authority}:{port}"
    target = parts.path or "/"
    if parts.query:
        target += "?" + parts.query
    target = "".join(c if "!" <= c <= "~" else quote(c, safe="") for c in target)
    return secure, host, port, host_header, target


def _tokens(fields: dict[str, list[str]], name: str) -> list[str]:
    return [t.strip().lower() for v in fields.get(name, ()) for t in v.split(",")]


def _check_switching(
    fields: dict[str, list[str]], key: str, offered: tuple[str, ...]
) -> str | None:
    """Validates a 101's fields; returns the echoed subprotocol (None when absent)."""
    if "websocket" not in _tokens(fields, "upgrade"):
        raise InvalidHandshake("101 response without Upgrade: websocket")
    if "upgrade" not in _tokens(fields, "connection"):
        raise InvalidHandshake("101 response without Connection: upgrade")
    accept = fields.get("sec-websocket-accept", [])
    if len(accept) != 1 or accept[0] != accept_key(key):
        raise InvalidHandshake("101 response with a wrong Sec-WebSocket-Accept")
    if "sec-websocket-extensions" in fields:
        raise InvalidHandshake("101 response negotiated an extension that was not offered")
    values = fields.get("sec-websocket-protocol")
    if values is None:
        return None
    chosen = [t.strip() for v in values for t in v.split(",")]
    if len(chosen) != 1 or chosen[0] not in offered:
        raise UnexpectedSubprotocol(", ".join(chosen))
    return chosen[0]


class StdlibConnector:
    """The default `WebSocketConnector`: stdlib asyncio streams and `ssl`, no proxy support.

    `open_timeout_ms` bounds TCP connect, TLS and the upgrade, raising `TimeoutError`.
    `close_timeout_ms` bounds how long `close()` waits for the peer's close frame before
    dropping the transport. `ssl_context` defaults to `default_ssl_context()`, created once
    per connector on the first wss:// dial.
    """

    def __init__(
        self,
        *,
        ssl_context: ssl.SSLContext | None = None,
        open_timeout_ms: float | None = None,
        happy_eyeballs_delay: float | None = 0.25,
        close_timeout_ms: float = 1000,
        clock: Clock | None = None,
    ) -> None:
        self._ssl_context = ssl_context
        self._open_timeout_ms = open_timeout_ms
        self._happy_eyeballs_delay = happy_eyeballs_delay
        self._close_timeout_ms = close_timeout_ms
        self._clock = clock

    def _ssl(self) -> ssl.SSLContext:
        if self._ssl_context is None:
            self._ssl_context = default_ssl_context()
        return self._ssl_context

    async def __call__(self, url: str, init: WebSocketInit) -> StdlibWebSocket:
        secure, host, port, host_header, target = _parse_url(url)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = format_upgrade_request(host_header, target, key, init.protocols, init.headers)
        clock = self._clock if self._clock is not None else LoopClock()
        task = asyncio.current_task()
        timer: TimerHandle | None = None
        timed_out = False

        def expire() -> None:
            nonlocal timed_out
            timed_out = True
            if task is not None:
                task.cancel()

        if self._open_timeout_ms is not None:
            timer = clock.call_later(self._open_timeout_ms, expire)
        writer: asyncio.StreamWriter | None = None
        try:
            reader, writer = await asyncio.open_connection(
                host,
                port,
                ssl=self._ssl() if secure else None,
                server_hostname=host if secure else None,
                happy_eyeballs_delay=self._happy_eyeballs_delay,
                limit=MAX_HEAD_SIZE,
            )
            writer.write(request)
            await writer.drain()
            subprotocol = await _read_upgrade_response(reader, key, init.protocols)
        except BaseException as exc:
            if writer is not None:
                writer.transport.abort()
            if isinstance(exc, asyncio.CancelledError) and timed_out and task is not None:
                if task.uncancel() == 0:
                    raise TimeoutError("WebSocket open timed out") from None
            raise
        finally:
            if timer is not None:
                timer.cancel()
        if timed_out:
            # The timer fired after the last await had already completed.
            writer.transport.abort()
            if task is not None:
                task.uncancel()
            raise TimeoutError("WebSocket open timed out")
        return StdlibWebSocket(
            reader, writer, subprotocol, init.max_message, self._close_timeout_ms, clock
        )


async def _read_upgrade_response(
    reader: asyncio.StreamReader, key: str, offered: tuple[str, ...]
) -> str | None:
    try:
        head = await reader.readuntil(b"\r\n\r\n")
    except asyncio.LimitOverrunError:
        raise InvalidHandshake("upgrade response head too large") from None
    except asyncio.IncompleteReadError:
        raise ConnectionError("connection closed before the upgrade response") from None
    try:
        status, _reason, fields, _consumed = parse_response_head(head)
    except HttpParseError as e:
        raise InvalidHandshake(f"malformed upgrade response: {e}") from None
    if status != 101:
        raise UpgradeRejected(status, {k: ", ".join(v) for k, v in fields.items()})
    return _check_switching(fields, key, offered)


class StdlibWebSocket:
    """An open client connection from `StdlibConnector`; implements `WebSocketConnection`."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        subprotocol: str | None,
        max_message: int,
        close_timeout_ms: float,
        clock: Clock,
    ) -> None:
        self.subprotocol = subprotocol
        self._reader = reader
        self._writer = writer
        self._transport = writer.transport
        self._max_message = max_message
        self._close_timeout_ms = close_timeout_ms
        self._clock = clock
        self._send_lock = asyncio.Lock()
        self._sent_close: tuple[int, str] | None = None
        self._closed: WebSocketClosed | None = None
        self._failing: WebSocketClosed | None = None
        self._close_timer: TimerHandle | None = None
        self._reader_owner: str | None = None
        self._waiters: list[asyncio.Future[None]] = []
        self._mid_frame = False
        self._msg_opcode: int | None = None
        self._msg_parts: list[bytes] = []
        self._msg_size = 0

    async def recv(self) -> bytes | str:
        while True:
            if self._closed is not None:
                self._raise_closed()
            if self._reader_owner is None:
                break
            if self._reader_owner == "recv":
                raise RuntimeError("recv() is already running on this connection")
            await self._wait_change()
        self._reader_owner = "recv"
        try:
            return await self._read_message()
        finally:
            self._reader_owner = None
            self._notify()

    async def send(self, data: bytes) -> None:
        async with self._send_lock:
            if self._closed is not None:
                self._raise_closed()
            if self._sent_close is not None:
                raise WebSocketClosed(*self._sent_close, received=False)
            self._write_frame(OP_BINARY, bytes(data))
            try:
                await self._writer.drain()
            except OSError:
                self._terminate(WebSocketClosed(1006), abort=True)
            if self._closed is not None:
                self._raise_closed()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        check_close_args(code, reason)
        if self._closed is not None:
            return
        if self._sent_close is None:
            self._write_close(code, reason)
            self._close_timer = self._clock.call_later(self._close_timeout_ms, self._close_expired)
        try:
            while self._closed is None:
                if self._reader_owner is None:
                    await self._drain_until_closed()
                else:
                    await self._wait_change()
        except asyncio.CancelledError:
            self.abort()
            raise

    def abort(self) -> None:
        self._transport.abort()
        self._terminate(self._failing or WebSocketClosed(1006), abort=True)

    async def _drain_until_closed(self) -> None:
        self._reader_owner = "close"
        try:
            while self._closed is None:
                try:
                    await self._read_message()
                except WebSocketClosed:
                    return
        finally:
            self._reader_owner = None
            self._notify()

    def _close_expired(self) -> None:
        self._close_timer = None
        self._terminate(self._failing or WebSocketClosed(1006), abort=True)

    async def _fail(self, code: int, reason: str) -> None:
        self._failing = WebSocketClosed(code, reason)
        if self._sent_close is None:
            self._write_close(code, reason)
        if self._close_timer is None:
            self._close_timer = self._clock.call_later(self._close_timeout_ms, self._close_expired)
        try:
            while self._closed is None:
                try:
                    chunk = await self._reader.read(_DISCARD_CHUNK)
                except OSError:
                    break
                if not chunk:
                    break
        except asyncio.CancelledError:
            self.abort()
            raise
        self._terminate(self._failing, abort=False)

    async def _read_message(self) -> bytes | str:
        try:
            return await self._next_message()
        except _Fail as f:
            await self._fail(f.code, f.reason)
        except _NETWORK_ERRORS:
            self._terminate(WebSocketClosed(1006), abort=True)
        except asyncio.CancelledError:
            if self._mid_frame:
                self.abort()
            raise
        self._raise_closed()

    async def _next_message(self) -> bytes | str:
        while True:
            fin, opcode, payload = await self._read_frame()
            if opcode == OP_PING:
                if self._sent_close is None and (
                    self._transport.get_write_buffer_size()
                    <= self._transport.get_write_buffer_limits()[1]
                ):
                    self._write_frame(OP_PONG, payload)
                continue
            if opcode == OP_PONG:
                continue
            if opcode == OP_CLOSE:
                code, reason = _parse_close(payload)
                if self._sent_close is None:
                    self._write_close(None if code == 1005 else code, "")
                self._terminate(WebSocketClosed(code, reason, received=True), abort=False)
                self._raise_closed()
            if opcode != OP_CONT:
                self._msg_opcode = opcode
            self._msg_parts.append(payload)
            self._msg_size += len(payload)
            if not fin:
                continue
            message = b"".join(self._msg_parts)
            op = self._msg_opcode
            self._msg_opcode, self._msg_parts, self._msg_size = None, [], 0
            if op == OP_TEXT:
                try:
                    return message.decode("utf-8")
                except UnicodeDecodeError:
                    raise _Fail(1007, "invalid UTF-8 in text message") from None
            return message

    async def _read_frame(self) -> tuple[bool, int, bytes]:
        head = await self._reader.readexactly(2)
        self._mid_frame = True
        b0, b1 = head[0], head[1]
        fin, opcode, n = bool(b0 & 0x80), b0 & 0x0F, b1 & 0x7F
        if b0 & 0x70:
            raise _Fail(1002, "reserved bits set")
        if b1 & 0x80:
            raise _Fail(1002, "masked frame from server")
        if opcode >= OP_CLOSE:
            if opcode not in (OP_CLOSE, OP_PING, OP_PONG):
                raise _Fail(1002, "unknown opcode")
            if not fin:
                raise _Fail(1002, "fragmented control frame")
            if n > _MAX_CONTROL_PAYLOAD:
                raise _Fail(1002, "control frame too long")
        elif opcode == OP_CONT:
            if self._msg_opcode is None:
                raise _Fail(1002, "unexpected continuation frame")
        elif opcode in (OP_TEXT, OP_BINARY):
            if self._msg_opcode is not None:
                raise _Fail(1002, "expected a continuation frame")
        else:
            raise _Fail(1002, "unknown opcode")
        if n == 126:
            (n,) = struct.unpack("!H", await self._reader.readexactly(2))
        elif n == 127:
            (n,) = struct.unpack("!Q", await self._reader.readexactly(8))
            if n >> 63:
                raise _Fail(1002, "invalid frame length")
        if opcode < OP_CLOSE and (self._msg_size if opcode == OP_CONT else 0) + n > (
            self._max_message
        ):
            raise _Fail(1009, "message too big")
        payload = await self._reader.readexactly(n) if n else b""
        self._mid_frame = False
        return fin, opcode, payload

    def _write_frame(self, opcode: int, payload: bytes) -> None:
        if not self._transport.is_closing():
            self._writer.write(encode_frame(opcode, payload, os.urandom(4)))

    def _write_close(self, code: int | None, reason: str) -> None:
        self._sent_close = (1005 if code is None else code, reason)
        payload = b"" if code is None else code.to_bytes(2, "big") + reason.encode("utf-8")
        self._write_frame(OP_CLOSE, payload)

    def _terminate(self, outcome: WebSocketClosed, *, abort: bool) -> None:
        if self._closed is not None:
            return
        self._closed = outcome
        if self._close_timer is not None:
            self._close_timer.cancel()
            self._close_timer = None
        if abort:
            self._transport.abort()
        else:
            self._transport.close()
        self._notify()

    def _raise_closed(self) -> NoReturn:
        c = self._closed
        assert c is not None
        raise WebSocketClosed(c.code, c.reason, c.received)

    def _notify(self) -> None:
        waiters, self._waiters = self._waiters, []
        for w in waiters:
            if not w.done():
                w.set_result(None)

    async def _wait_change(self) -> None:
        w: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(w)
        await w


def _parse_close(payload: bytes) -> tuple[int, str]:
    if not payload:
        return 1005, ""
    if len(payload) == 1:
        raise _Fail(1002, "close frame with a 1-byte payload")
    code = int.from_bytes(payload[:2], "big")
    if code not in _VALID_RECEIVED_CODES and not 3000 <= code <= 4999:
        raise _Fail(1002, "invalid close code")
    # Lossy on purpose: the SDK reports the peer's reason as received (CLIENT-SDK closeReason).
    return code, payload[2:].decode("utf-8", errors="replace")
