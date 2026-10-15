"""Target mode: forward each bridged request to a local HTTP server (MCPWARP.md sections 4.5,
4.6) with a small stdlib HTTP/1.1 client.

One connection per request (`Connection: close`). Redirects are not followed: a 3xx is passed
back to the edge as is, like any other response. Content-Encoding is passed through untouched,
because this client never decodes a body. The response body is streamed back as it arrives.
"""

from __future__ import annotations

import asyncio
import errno
import math
import socket
import ssl
from collections import deque
from typing import Literal
from urllib.parse import urlsplit

from .._http1 import (
    HOP_BY_HOP,
    MAX_HEAD_SIZE,
    ChunkedDecoder,
    HttpParseError,
    connection_tokens,
    has_unrepresentable_value,
    is_bodyless,
    is_token,
    parse_response_head,
    percent_encode_target,
)
from .._tls import default_ssl_context
from ..clock import Clock, LoopClock
from .http import Handler, Request, Response, error_reply

DEFAULT_TARGET_TIMEOUT_MS = 30_000

_READ_SIZE = 65_536
# Request fields the client sets itself, or that only make sense on the edge's hop.
_REPLACED = frozenset(
    {"host", "content-length", "accept-encoding", "x-forwarded-host", "x-forwarded-proto"}
)


class _Unreachable(Exception):
    """The local server's answer could not be read; `code` names the OS error, if any."""

    def __init__(self, code: str | None) -> None:
        super().__init__(code or "local server unreachable")
        self.code = code


def target_handler(
    target: str,
    *,
    timeout_ms: float = DEFAULT_TARGET_TIMEOUT_MS,
    ssl_context: ssl.SSLContext | None = None,
    clock: Clock | None = None,
) -> Handler:
    """A Handler that forwards each request to the local server at `target`.

    The request goes to `target` with the incoming query appended after the target's own; the
    headers are the request's minus `host`, plus `accept-encoding: identity`,
    `x-forwarded-host: <public Host>` and `x-forwarded-proto: https`. Without response
    headers within `timeout_ms` the answer is 504 `local server timed out` and the upstream
    connection is aborted; the body may stream past it. A connection failure is 502
    `local server unreachable (<errno name>)`. Raises ValueError for a target that is not an
    absolute http(s) URL, or a `timeout_ms` that is not a positive number.
    """
    parts = urlsplit(target)
    scheme = parts.scheme.lower()
    try:
        port = parts.port
    except ValueError:
        raise ValueError("target port is invalid") from None
    if scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("target must be an absolute http:// or https:// URL")
    if (
        isinstance(timeout_ms, bool)
        or not isinstance(timeout_ms, int | float)
        or not math.isfinite(timeout_ms)
        or timeout_ms <= 0
    ):
        raise ValueError("timeout_ms must be a positive number")
    secure = scheme == "https"
    host = parts.hostname
    if port is None:
        port = 443 if secure else 80
    host_header = parts.netloc.rpartition("@")[2]
    base_path = percent_encode_target(parts.path or "/")
    base_query = percent_encode_target(parts.query)
    context_holder: list[ssl.SSLContext | None] = [ssl_context]

    def tls() -> ssl.SSLContext | None:
        if not secure:
            return None
        if context_holder[0] is None:
            context_holder[0] = default_ssl_context()
        return context_holder[0]

    async def handler(request: Request) -> Response:
        # Resolved per call: a LoopClock binds to the loop it is first used on.
        _clock = clock if clock is not None else LoopClock()
        query = "&".join(q for q in (base_query, percent_encode_target(request.query)) if q)
        wire = _format_request(request, base_path + ("?" + query if query else ""), host_header)
        loop = asyncio.get_running_loop()
        expired: asyncio.Future[None] = loop.create_future()

        def expire() -> None:
            if not expired.done():
                expired.set_result(None)

        timer = _clock.call_later(timeout_ms, expire)
        attempt = loop.create_task(_exchange(host, port, tls(), wire))
        handed_over = False
        try:
            await asyncio.wait((attempt, expired), return_when=asyncio.FIRST_COMPLETED)
            if not attempt.done():
                attempt.cancel()
                await asyncio.wait((attempt,))
                return error_reply(504, "local server timed out")
            exc = attempt.exception()
            if exc is not None:
                code = exc.code if isinstance(exc, _Unreachable) else _errno_name(exc)
                message = (
                    f"local server unreachable ({code})" if code else "local server unreachable"
                )
                return error_reply(502, message)
            reader, writer, status, reason, fields = attempt.result()
            handed_over = True
            return _response(request.method, reader, writer, status, reason, fields)
        finally:
            timer.cancel()
            if not handed_over:
                _discard(attempt)

    return handler


def _discard(attempt: asyncio.Task[_Exchanged]) -> None:
    """Releases a connect attempt nobody will use: abort its connection if it got one (even
    one that completed just as the caller was cancelled), else cancel it."""
    if not attempt.done():
        attempt.cancel()
    elif not attempt.cancelled() and attempt.exception() is None:
        attempt.result()[1].transport.abort()


def _format_request(request: Request, target: str, host_header: str) -> bytes:
    pairs = request.headers.items()
    listed = connection_tokens(pairs)
    fields = [
        (n, v)
        for n, v in pairs
        if (low := n.lower()) not in HOP_BY_HOP and low not in listed and low not in _REPLACED
    ]
    if not all(is_token(n) for n, _ in fields) or has_unrepresentable_value(fields):
        raise ValueError("request header field cannot be forwarded")
    public_host = request.headers.get("host") or urlsplit(request.url).netloc
    if has_unrepresentable_value([("x-forwarded-host", public_host)]):
        raise ValueError("request host cannot be forwarded")
    fields += [
        ("accept-encoding", "identity"),
        ("x-forwarded-host", public_host),
        ("x-forwarded-proto", "https"),
    ]
    method = request.method
    if not is_token(method):
        raise ValueError("request method cannot be forwarded")
    with_body = method not in ("GET", "HEAD")
    lines = [f"{method} {target} HTTP/1.1", f"Host: {host_header}"]
    lines += [f"{n}: {v}" for n, v in fields]
    lines.append("Connection: close")
    if with_body:
        lines.append(f"Content-Length: {len(request.body)}")
    head = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1", errors="strict")
    return head + request.body if with_body else head


async def _open(
    host: str, port: int, tls: ssl.SSLContext | None
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Connects to each address `host` resolves to in turn; the last failure is raised."""
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    last: OSError | None = None
    for family, _, _, _, addr in infos:
        try:
            return await asyncio.open_connection(
                str(addr[0]),
                int(addr[1]),
                family=family,
                ssl=tls,
                server_hostname=host if tls is not None else None,
                limit=MAX_HEAD_SIZE,
            )
        except OSError as exc:
            last = exc
    raise last if last is not None else OSError("no address to connect to")


_Exchanged = tuple[asyncio.StreamReader, asyncio.StreamWriter, int, str, list[tuple[str, str]]]


async def _exchange(host: str, port: int, tls: ssl.SSLContext | None, wire: bytes) -> _Exchanged:
    """Sends the request and reads up to the final response head (interim 1xx skipped).

    The whole request, body included, is written before the response is read, so a local
    server that answers early and closes while the body is still being written is a 502.
    """
    reader, writer = await _open(host, port, tls)
    try:
        writer.write(wire)
        await writer.drain()
        while True:
            try:
                raw = await reader.readuntil(b"\r\n\r\n")
                status, reason, fields, _ = parse_response_head(raw)
            except (asyncio.LimitOverrunError, asyncio.IncompleteReadError, HttpParseError):
                raise _Unreachable(None) from None
            if status == 101:
                raise _Unreachable(None)
            if status >= 200:
                pairs = [(n, v) for n, values in fields.items() for v in values]
                return reader, writer, status, reason, pairs
    except BaseException:
        writer.transport.abort()
        raise


def _errno_name(exc: BaseException) -> str | None:
    """The `<cause code>` of a 502: any resolver failure is "ENOTFOUND" (as Go and JS report
    it, whatever the EAI_* reason), an OS error its errno name, a TLS failure none."""
    if isinstance(exc, socket.gaierror):
        return "ENOTFOUND"
    if isinstance(exc, ssl.SSLError) or not isinstance(exc, OSError):
        return None
    if isinstance(exc.errno, int):
        return errno.errorcode.get(exc.errno)
    return None


def _response(
    method: str,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    status: int,
    reason: str,
    fields: list[tuple[str, str]],
) -> Response:
    """The upstream response, its body framed by the last Transfer-Encoding coding when that is
    `chunked` (de-chunked; an earlier coding such as `gzip, chunked` is left in the bytes with
    no marker, since Transfer-Encoding is hop-by-hop and not passed on), else by a single valid
    Content-Length (also when a non-chunked Transfer-Encoding is present), else by EOF."""
    if is_bodyless(method, status):
        writer.close()
        return Response(status, fields, None, reason or None)
    te = [v for n, v in fields if n == "transfer-encoding"]
    lengths = [v for n, v in fields if n == "content-length"]
    framing: Literal["chunked", "eof"] | int = "eof"
    if te and te[-1].rsplit(",", 1)[-1].strip(" \t").lower() == "chunked":
        framing = "chunked"
        fields = [(n, v) for n, v in fields if n != "content-length"]
    elif len(lengths) == 1 and lengths[0].isascii() and lengths[0].isdigit():
        framing = int(lengths[0])
    return Response(status, fields, _UpstreamBody(reader, writer, framing), reason or None)


class _UpstreamBody:
    """The local server's response body, read as the edge consumes it; `aclose()` (or the end
    of the body) closes the upstream connection."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        framing: Literal["chunked", "eof"] | int,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._framing = framing
        self._decoder = ChunkedDecoder() if framing == "chunked" else None
        self._pending: deque[bytes] = deque()
        self._remaining = framing if isinstance(framing, int) else 0
        self._closed = False

    def __aiter__(self) -> _UpstreamBody:
        return self

    async def __anext__(self) -> bytes:
        if self._closed:
            raise StopAsyncIteration
        try:
            return await self._next()
        except BaseException:
            self._close(abort=True)
            raise

    async def _next(self) -> bytes:
        if self._decoder is not None:
            while not self._pending:
                if self._decoder.done:
                    self._close(abort=False)
                    raise StopAsyncIteration
                data = await self._reader.read(_READ_SIZE)
                if not data:
                    raise ConnectionError("local server closed the connection inside the body")
                self._pending.extend(self._decoder.feed(data))
            return self._pending.popleft()
        if self._framing == "eof":
            data = await self._reader.read(_READ_SIZE)
        elif self._remaining > 0:
            data = await self._reader.read(min(_READ_SIZE, self._remaining))
            self._remaining -= len(data)
        else:
            data = b""
        if not data:
            # Short of a declared Content-Length, the bridge RESETs the stream.
            self._close(abort=False)
            raise StopAsyncIteration
        return data

    async def aclose(self) -> None:
        self._close(abort=True)

    def _close(self, *, abort: bool) -> None:
        if self._closed:
            return
        self._closed = True
        if abort:
            self._writer.transport.abort()
        else:
            self._writer.close()
