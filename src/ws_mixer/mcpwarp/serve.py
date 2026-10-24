"""Serving one bridged HTTP/1.1 request per stream (MCPWARP.md section 4).

`serve_stream` parses the edge's request, decides who answers it, and writes the response. The
precedence (section 4.4, first match wins):

0. the agent is in conflict: RESET CANCEL, nothing read;
1. the head is invalid, too large or late: 400 / 431 / RESET CANCEL (section 4.2);
2. the path is not exactly `/mcp`: 404, body not read;
3. the agent is disabled, closing or otherwise unavailable: 503, body not read;
4. the body is over the cap: 413, read up to the cap;
5. the body is malformed: 400;
6. the request cannot be represented (a CONNECT, TRACE or TRACK method, an invalid Host, a
   header value holding NUL, CR or LF): 400, handler not called;
7. the handler.

Every SDK refusal written while request bytes are still unread drains the rest of the body
(granting window as it reads) and then CLOSEs; it never RESETs. The request body is always
buffered whole before the handler runs, so the streaming-adapter rows of section 4.2 never
apply: a body over the cap is always a 413 before any response byte.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import AsyncIterable, AsyncIterator, Callable
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from .._http1 import (
    HOP_BY_HOP,
    NEED_MORE,
    ChunkedDecoder,
    RequestHead,
    RequestParseError,
    connection_tokens,
    format_error_reply,
    forwardable_request_headers,
    has_unrepresentable_value,
    is_authority,
    is_bodyless,
    is_field_value,
    is_token,
    parse_request_head,
    reason_phrase,
    request_line_method,
    request_path,
)
from ..clock import Clock
from ..errors import ErrorCode, WsMixerError
from ..stream import Finished, MixerStream
from .http import Handler, Headers, Request, items_of

Availability = Literal["available", "unavailable", "disabled", "closing", "conflict"]
"""The agent's state as the bridge sees it when a request arrives (section 4.4 rows 0 and 3)."""

MCP_PATH = "/mcp"
HEAD_DEADLINE_MS = 10_000
DEFAULT_MAX_REQUEST_BODY_BYTES = 4_194_304
TAKEN_OVER_MESSAGE = "service taken over by another connection"
_UNREPRESENTABLE_METHODS = frozenset({"CONNECT", "TRACE", "TRACK"})

_UNAVAILABLE_MESSAGES = MappingProxyType(
    {"disabled": "service disabled", "closing": "service closing"}
)
_LAST_CHUNK = b"0\r\n\r\n"
_BODY_SHORT = "response body shorter than Content-Length"


class _Incomplete(Exception):
    """The edge ended the stream before the end of the request head."""


class _HandlerTimeout(Exception):
    """The handler returned no response within `handler_timeout_ms`; it was cancelled."""


async def serve_stream(
    stream: MixerStream,
    *,
    handler: Handler,
    availability: Callable[[], Availability],
    max_request_body_bytes: int,
    clock: Clock,
    head_deadline_ms: float = HEAD_DEADLINE_MS,
    on_error: Callable[[BaseException], None] | None = None,
    handler_timeout_ms: float | None = None,
) -> None:
    """Answers the one request on `stream` and ends the stream (CLOSE or RESET).

    `handler_timeout_ms` (an SDK extension, not in MCPWARP.md; None: no deadline) bounds how
    long an async handler may take to return its response: past it the handler is cancelled
    and the answer is 504 `local server timed out`. A response body may stream past it.

    `availability` is read when the stream arrives and again once the head is in. `on_error`
    receives what the handler raised, a non-response it returned, a response body that
    failed, or an internal failure; it must not raise (if it does, that is logged).

    Never raises (an `availability` that raises is answered 500 `internal error`). If the
    stream is RESET or the connection ends before the response is complete, the handler (and
    the response body it returned) is cancelled. Cancelling this coroutine cancels them too and
    waits for their cleanup before re-raising; it does not touch the stream, so the caller must
    RESET it.
    """
    report = _reporter(on_error)
    loop = asyncio.get_running_loop()
    work = loop.create_task(
        _serve(
            stream,
            handler,
            availability,
            max_request_body_bytes,
            clock,
            head_deadline_ms,
            report,
            handler_timeout_ms,
        )
    )
    closed = loop.create_task(stream.wait_closed())
    try:
        await asyncio.wait((work, closed), return_when=asyncio.FIRST_COMPLETED)
        if not work.done():
            if not isinstance(closed.result(), Finished):
                work.cancel()
            await asyncio.wait((work,))
    except asyncio.CancelledError:
        work.cancel()
        await asyncio.wait((work,))
        raise
    finally:
        closed.cancel()
    failure = None if work.cancelled() else work.exception()
    if failure is not None:
        report(failure)


def _availability(availability: Callable[[], Availability]) -> str:
    value = availability()
    return value if isinstance(value, str) else "unavailable"


def _reporter(on_error: Callable[[BaseException], None] | None) -> Callable[[BaseException], None]:
    def report(exc: BaseException) -> None:
        if on_error is None:
            return
        try:
            on_error(exc)
        except Exception:
            logging.getLogger("mcpwarp").exception("on_error callback raised")

    return report


class _Source:
    """Buffered reads of the request bytes."""

    def __init__(self, stream: MixerStream) -> None:
        self._stream = stream
        self.buf = bytearray()
        self.eof = False

    async def fill(self) -> bool:
        if self.eof:
            return False
        chunk = await self._stream.read()
        if not chunk:
            self.eof = True
            return False
        self.buf += chunk
        return True

    def take_all(self) -> bytes:
        data = bytes(self.buf)
        self.buf.clear()
        return data

    async def discard(self) -> None:
        """Reads and drops everything up to the edge's CLOSE; each read credits window back."""
        self.buf.clear()
        try:
            while await self.fill():
                self.buf.clear()
        except WsMixerError:
            pass


class _Out:
    """The stream's write side, remembering whether any response byte was written."""

    def __init__(self, stream: MixerStream) -> None:
        self.stream = stream
        self.wrote = False

    async def write(self, data: bytes) -> None:
        self.wrote = True
        await self.stream.write(data)

    async def reply(self, status: int, message: str, method: str) -> None:
        """An SDK reply to a request whose body is fully read, then CLOSE."""
        try:
            await self.write(format_error_reply(status, message, method))
        except WsMixerError:
            return
        await self.stream.close_write()

    async def refuse(self, src: _Source, status: int, message: str, method: str) -> None:
        """An SDK refusal: the reply, then the unread body drained, then CLOSE (never RESET)."""
        try:
            await self.write(format_error_reply(status, message, method))
        except WsMixerError:
            return
        await src.discard()
        await self.stream.close_write()


async def _serve(
    stream: MixerStream,
    handler: Handler,
    availability: Callable[[], Availability],
    max_body: int,
    clock: Clock,
    head_deadline_ms: float,
    report: Callable[[BaseException], None],
    handler_timeout_ms: float | None = None,
) -> None:
    src = _Source(stream)
    out = _Out(stream)
    method = "GET"

    def head_deadline() -> None:
        stream.reset(ErrorCode.CANCEL, "request head deadline exceeded")

    try:
        if _availability(availability) == "conflict":
            stream.reset(ErrorCode.CANCEL, TAKEN_OVER_MESSAGE)
            return
        timer = clock.call_later(head_deadline_ms, head_deadline)
        try:
            head = await _read_head(src)
        finally:
            timer.cancel()
        method = head.method
        state = _availability(availability)
        if state == "conflict":
            stream.reset(ErrorCode.CANCEL, TAKEN_OVER_MESSAGE)
            return
        if request_path(head.target) != MCP_PATH:
            await out.refuse(src, 404, "not_found", method)
            return
        if state != "available":
            message = _UNAVAILABLE_MESSAGES.get(state, "service unavailable")
            await out.refuse(src, 503, message, method)
            return
        body = await _read_body(src, head, max_body)
        request = _build_request(head, body)
        if request is None:
            await out.reply(400, "request cannot be represented as a web Request", method)
            return
        try:
            result = await _call_handler(handler, request, clock, handler_timeout_ms, report)
        except _HandlerTimeout:
            await out.reply(504, "local server timed out", method)
            return
        except Exception as exc:
            report(exc)
            await out.reply(500, "handler error", method)
            return
        error_message = getattr(result, "mcpwarp_error_message", None)
        status = getattr(result, "status", None)
        if isinstance(error_message, str) and isinstance(status, int) and _is_status(status):
            await _close_body(getattr(result, "body", None))
            await out.reply(status, error_message, method)
            return
        try:
            prepared = _prepare(result)
        except (TypeError, ValueError) as exc:
            await _close_body(getattr(result, "body", None))
            report(exc)
            await out.reply(500, "handler error", method)
            return
        await _write_response(out, prepared, method, report)
    except RequestParseError as exc:
        await out.refuse(src, exc.status, exc.message, exc.method or method)
    except _Incomplete:
        await out.reply(400, "incomplete request", request_line_method(src.buf) or "GET")
    except WsMixerError:
        # The stream was RESET or the connection lost: nobody to answer.
        return
    except Exception as exc:
        report(exc)
        if stream.state == "closed":
            return
        if out.wrote:
            stream.reset(ErrorCode.INTERNAL_ERROR, "internal error")
        else:
            await out.refuse(src, 500, "internal error", method)


async def _read_head(src: _Source) -> RequestHead:
    scanned = 0
    while True:
        head = parse_request_head(src.buf, scan_from=scanned)
        if head is not NEED_MORE:
            del src.buf[: head.consumed]
            return head
        scanned = len(src.buf)
        if not await src.fill():
            raise _Incomplete


async def _read_body(src: _Source, head: RequestHead, max_body: int) -> bytes:
    framing = head.framing
    if framing == "none":
        return b""
    if framing == "chunked":
        decoder = ChunkedDecoder(max_body)
        parts = decoder.feed(src.take_all())
        while not decoder.done:
            if not await src.fill():
                raise RequestParseError(400, "stream ended inside the request body")
            parts += decoder.feed(src.take_all())
        return b"".join(parts)
    if framing > max_body:
        raise RequestParseError(413, "request body too large")
    while len(src.buf) < framing:
        if not await src.fill():
            raise RequestParseError(400, "stream ended inside the request body")
    body = bytes(src.buf[:framing])
    del src.buf[:framing]
    return body


def _build_request(head: RequestHead, body: bytes) -> Request | None:
    """Section 4.6 request -> handler, or None when it cannot be represented (section 4.5): a
    CONNECT, TRACE or TRACK method, a Host that is not a valid authority, or a forwarded value
    holding NUL, CR or LF."""
    if head.method.upper() in _UNREPRESENTABLE_METHODS:
        return None
    fields = forwardable_request_headers(head.headers)
    if not is_authority(head.host) or has_unrepresentable_value(fields):
        return None
    target = head.target
    path = request_path(target)
    query = target.split("#", 1)[0].partition("?")[2]
    return Request(
        method=head.method,
        url=f"https://{head.host}{target}",
        target=target,
        path=path,
        query=query,
        headers=Headers(fields),
        body=b"" if head.method in ("GET", "HEAD") else body,
        http_version=head.http_version,
    )


async def _call_handler(
    handler: Handler,
    request: Request,
    clock: Clock,
    timeout_ms: float | None,
    report: Callable[[BaseException], None],
) -> object:
    """The handler's result. With `timeout_ms`, an awaitable result not done by then is
    cancelled (and waited for) and _HandlerTimeout raised; the timer starts once the handler
    has been called, so a synchronous handler is never cut short."""
    result: object = handler(request)
    if not inspect.isawaitable(result):
        return result
    if timeout_ms is None:
        return await result
    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(result)
    expired: asyncio.Future[None] = loop.create_future()

    def expire() -> None:
        if not expired.done():
            expired.set_result(None)

    timer = clock.call_later(timeout_ms, expire)
    timed_out = False
    try:
        await asyncio.wait((task, expired), return_when=asyncio.FIRST_COMPLETED)
        timed_out = not task.done()
    finally:
        timer.cancel()
        if not task.done():
            task.cancel()
            await asyncio.wait((task,))
    if not timed_out:
        return task.result()
    # A handler that swallowed the cancel and finished anyway: its answer is dropped.
    if not task.cancelled():
        exc = task.exception()
        if exc is not None:
            report(exc)
        else:
            await _close_body(getattr(task.result(), "body", None))
    raise _HandlerTimeout


# --- the response ------------------------------------------------------------------------


@dataclass(frozen=True)
class _Prepared:
    status: int
    reason: str
    headers: list[tuple[str, str]]
    declared_length: int | None
    body: bytes | AsyncIterable[bytes] | None


def _is_status(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_latin1_field(value: str) -> bool:
    try:
        value.encode("latin-1")
    except UnicodeEncodeError:
        return False
    return is_field_value(value)


def _prepare(result: object) -> _Prepared:
    """Validates a handler's result by shape before any byte is written. Raises TypeError for
    something that is not a response, ValueError for a response that cannot be sent."""
    status = getattr(result, "status", None)
    if not _is_status(status) or not hasattr(result, "headers") or not hasattr(result, "body"):
        raise TypeError("handler did not return a response (status, headers, body)")
    assert isinstance(status, int)
    if not 200 <= status <= 599:
        raise ValueError(f"response status {status} cannot be sent (section 4.5: 200..599 only)")
    reason = getattr(result, "reason", None)
    if reason is None or reason == "":
        reason = reason_phrase(status)
    elif not isinstance(reason, str) or not _is_latin1_field(reason):
        raise ValueError("response reason is not a valid reason phrase")
    pairs: list[tuple[str, str]] = []
    for pair in items_of(getattr(result, "headers")):  # noqa: B009
        if not isinstance(pair, tuple) or len(pair) != 2:
            raise TypeError("response headers must be (name, value) pairs")
        name, value = pair
        if not isinstance(name, str) or not isinstance(value, str):
            raise TypeError("response header names and values must be str")
        value = value.strip(" \t")
        if not is_token(name) or not _is_latin1_field(value):
            raise ValueError("response header field cannot be sent")
        pairs.append((name, value))
    listed = connection_tokens(pairs)
    lengths = [v for n, v in pairs if n.lower() == "content-length"]
    declared = None
    if len(lengths) == 1 and lengths[0].isascii() and lengths[0].isdigit():
        declared = int(lengths[0])
    kept = [
        (n, v)
        for n, v in pairs
        if (low := n.lower()) not in HOP_BY_HOP and low not in listed and low != "content-length"
    ]
    body = getattr(result, "body")  # noqa: B009
    if isinstance(body, bytes | bytearray | memoryview):
        body = bytes(body)
    elif body is not None and not callable(getattr(body, "__aiter__", None)):
        raise TypeError("response body must be bytes, an async iterable of bytes, or None")
    return _Prepared(status, reason, kept, declared, body)


def _chunk(data: bytes) -> bytes:
    return b"%x\r\n%b\r\n" % (len(data), data)


async def _close_body(body: object) -> None:
    aclose = getattr(body, "aclose", None)
    if callable(aclose):
        try:
            await aclose()
        except Exception:
            pass


async def _write_response(
    out: _Out, res: _Prepared, method: str, report: Callable[[BaseException], None]
) -> None:
    """Section 4.1 response framing, then CLOSE. A response body that fails RESETs the stream
    with INTERNAL_ERROR; so does one shorter than its declared Content-Length."""
    stream = out.stream
    bodyless = is_bodyless(method, res.status)
    body = res.body
    lines = [f"HTTP/1.1 {res.status} {res.reason}"]
    lines += [f"{n}: {v}" for n, v in res.headers]
    length: int | None = None
    if bodyless:
        pass
    elif body is None:
        length = 0
        lines.append("Content-Length: 0")
    elif res.declared_length is not None:
        length = res.declared_length
        lines.append(f"Content-Length: {length}")
    else:
        lines.append("Transfer-Encoding: chunked")
    head = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")
    iterator: AsyncIterator[bytes] | None = None
    try:
        if bodyless or body is None:
            await out.write(head)
        elif isinstance(body, bytes):
            if length is None:
                await out.write(head + (_chunk(body) if body else b"") + _LAST_CHUNK)
            else:
                await out.write(head + body[:length])
                if len(body) < length:
                    stream.reset(ErrorCode.INTERNAL_ERROR, _BODY_SHORT)
                    return
        else:
            iterator = body.__aiter__()
            await out.write(head)
            remaining = length
            while remaining is None or remaining > 0:
                try:
                    chunk = await iterator.__anext__()
                    if not isinstance(chunk, bytes | bytearray | memoryview):
                        raise TypeError("response body yielded something that is not bytes")
                except StopAsyncIteration:
                    break
                except Exception as exc:
                    stream.reset(ErrorCode.INTERNAL_ERROR, "response body failed")
                    report(exc)
                    return
                data = bytes(chunk)
                if not data:
                    continue
                if remaining is None:
                    await out.write(_chunk(data))
                else:
                    data = data[:remaining]
                    remaining -= len(data)
                    await out.write(data)
            if remaining is None:
                await out.write(_LAST_CHUNK)
            elif remaining > 0:
                stream.reset(ErrorCode.INTERNAL_ERROR, _BODY_SHORT)
                return
        await stream.close_write()
    except WsMixerError:
        # The stream was RESET or the connection lost under the response.
        return
    finally:
        await _close_body(iterator if iterator is not None else body)
