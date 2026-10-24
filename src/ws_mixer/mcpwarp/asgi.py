"""`asgi_handler(app)`: serve an ASGI 3 HTTP application as a bridged handler (MCPWARP.md
section 9, ASGI mapping).

The request body is already buffered whole, so `receive()` yields one `http.request` and then
blocks until the edge goes away or the response is complete, and yields `http.disconnect`.
`http.response.start` is held until the handler returns; each `http.response.body` becomes one
item of the Response's async body, which the bridge writes as soon as it is produced. The
hand-off holds at most one chunk, so a slow edge makes the app's `send()` wait. A status below
200 is not a response: the handler raises and the bridge answers 500 `handler error`; so does
the bridge for a status of 600 or more (MCPWARP.md section 4.5).

The app task outlives the handler's return: it runs while the bridge reads the body. When the
bridge stops reading before the response is complete (edge RESET, connection lost, stop), the
app gets `http.disconnect`, one loop turn to react, and is then cancelled; `send()` from then
on raises OSError (ASGI HTTP spec 2.4). A response is complete once the app sends its last body
message or its declared Content-Length is reached; after that the bridge CLOSEs the stream and
then waits for the app to return (background tasks). Lifespan is never run.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any
from urllib.parse import unquote_to_bytes, urlsplit

from .http import Handler, Headers, Request, Response, error_reply

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

_UNREPRESENTABLE = "request cannot be represented as a web Request"


class _ClientDisconnected(OSError):
    """Raised by `send()` once the edge has gone away."""


def asgi_handler(app: ASGIApp) -> Handler:
    """A Handler that runs the ASGI 3 `app` once per request. Lifespan is the host's job."""
    if not callable(app):
        raise TypeError("app must be an ASGI 3 application")

    async def handle(request: Request) -> Response:
        scope = _scope(request)
        if scope is None:
            return error_reply(400, _UNREPRESENTABLE)
        exchange = _Exchange(request.body)
        task = asyncio.get_running_loop().create_task(exchange.run(app, scope))
        exchange.task = task
        try:
            await asyncio.wait((exchange.started, task), return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            await exchange.abort()
            raise
        if exchange.start_error is not None or not exchange.started.done():
            await exchange.abort()
            raise (
                exchange.start_error
                or exchange.failure
                or RuntimeError("ASGI app returned without sending http.response.start")
            )
        return Response(exchange.status, Headers(exchange.headers), _Body(exchange))

    return handle


def _scope(request: Request) -> Scope | None:
    """The section 9 scope, or None when the request cannot be given to an ASGI app."""
    try:
        raw_path = request.path.encode("ascii")
        path = unquote_to_bytes(request.path).decode("utf-8")
        query = request.query.encode("ascii")
        headers = [
            (name.lower().encode("latin-1"), value.encode("latin-1"))
            for name, value in request.headers.items()
        ]
        if "host" not in request.headers:
            headers.append((b"host", urlsplit(request.url).netloc.encode("latin-1")))
    except (UnicodeError, ValueError):
        return None
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": request.http_version,
        "method": request.method,
        "scheme": "https",
        "path": path,
        "raw_path": raw_path,
        "query_string": query,
        "root_path": "",
        "headers": headers,
        "server": None,
        "client": None,
        "state": {},
    }


class _End:
    """The queue item after the last body chunk."""


class _Exchange:
    """One request/response between the bridge and a running app."""

    def __init__(self, body: bytes) -> None:
        self.task: asyncio.Task[None] | None = None
        self.started: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.status = 500
        self.headers: list[tuple[str, str]] = []
        self.declared_length: int | None = None
        self.queued_length = 0
        self.start_error: Exception | None = None
        self.failure: Exception | None = None
        self.chunks: asyncio.Queue[bytes | _End | Exception] = asyncio.Queue(maxsize=1)
        self.complete = False
        """The response is complete: the last body message, or the declared length reached."""
        self.last_sent = False
        """The app sent its last body message (more_body false)."""
        self.reader_gone = False
        """The bridge stopped reading the body; further chunks are dropped."""
        self.disconnected = False
        self._raised_disconnect = False
        self.gone = asyncio.Event()
        """Set once `receive()` answers http.disconnect: edge gone or response complete."""
        self._body: bytes | None = body

    async def run(self, app: ASGIApp, scope: Scope) -> None:
        try:
            await app(scope, self.receive, self.send)
        except Exception as exc:
            self.failure = exc
            if self.started.done() and not self.complete:
                await self._put(exc)
            elif self.complete:
                logging.getLogger("mcpwarp").error(
                    "ASGI app raised after its response was complete", exc_info=exc
                )
            return
        if self.started.done() and not self.complete:
            await self._put(RuntimeError("ASGI app returned without completing the response"))

    async def receive(self) -> Message:
        body = self._body
        if body is not None:
            self._body = None
            return {"type": "http.request", "body": body, "more_body": False}
        await self.gone.wait()
        return {"type": "http.disconnect"}

    async def send(self, message: Message) -> None:
        if self.disconnected:
            await self._raise_disconnected()
        kind = message.get("type")
        if kind == "http.response.start" and not self.started.done():
            try:
                self.status, self.headers, self.declared_length = _start(message)
            except Exception as exc:
                self.start_error = exc
                raise
            finally:
                self.started.set_result(None)
            if self.declared_length == 0:
                self._completed()
                self.chunks.put_nowait(_End())
            return
        if kind != "http.response.body" or not self.started.done() or self.last_sent:
            raise RuntimeError(f"unexpected ASGI message {kind!r} in this state")
        data = message.get("body", b"")
        if not isinstance(data, bytes | bytearray | memoryview):
            raise TypeError("http.response.body 'body' must be bytes")
        more = bool(message.get("more_body", False))
        self.last_sent = not more
        if self.complete:
            # The declared length is reached: the rest is dropped.
            return
        if data:
            await self._put(bytes(data))
            self.queued_length += len(data)
            declared = self.declared_length
            if declared is not None and self.queued_length >= declared:
                more = False
        if not more:
            self._completed()
            await self._put(_End())
        if self.disconnected:
            await self._raise_disconnected()

    def _completed(self) -> None:
        self.complete = True
        self.gone.set()

    async def _raise_disconnected(self) -> None:
        # The first raise is immediate, within abort()'s one turn. Later ones yield first, so
        # an app that swallows the error cannot spin without letting abort() cancel it.
        if self._raised_disconnect:
            await asyncio.sleep(0)
        self._raised_disconnect = True
        raise _ClientDisconnected("the client disconnected")

    async def _put(self, item: bytes | _End | Exception) -> None:
        if not self.reader_gone:
            await self.chunks.put(item)

    def _drain(self) -> None:
        """Drops what is queued, waking an app blocked in send()."""
        while not self.chunks.empty():
            self.chunks.get_nowait()

    async def abort(self) -> None:
        """http.disconnect to the app, one loop turn for it to react, then cancellation. Waits
        for the app task to end even when cancelled meanwhile, then re-raises the cancel."""
        self.reader_gone = True
        self.disconnected = True
        self.gone.set()
        self._drain()
        task = self.task
        if task is None or task.done():
            return
        cancelled = False
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            cancelled = True
        finally:
            task.cancel()
        while not task.done():
            try:
                await asyncio.wait((task,))
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError

    async def finish(self) -> None:
        """The bridge is done with the body: wait for an app whose response is complete to
        return; abort one whose response is not."""
        self.reader_gone = True
        self._drain()
        if not self.complete:
            await self.abort()
            return
        task = self.task
        if task is None or task.done():
            return
        try:
            await asyncio.wait((task,))
        except asyncio.CancelledError:
            await self.abort()
            raise


def _start(message: Message) -> tuple[int, list[tuple[str, str]], int | None]:
    """Status, headers and the declared Content-Length (as the bridge will read it)."""
    status = message.get("status")
    if not isinstance(status, int) or isinstance(status, bool):
        raise TypeError("http.response.start 'status' must be an int")
    if status < 200:
        raise ValueError(f"ASGI app sent status {status}; 1xx responses cannot be sent")
    headers: list[tuple[str, str]] = []
    for pair in message.get("headers", ()):
        try:
            name, value = pair
        except (TypeError, ValueError):
            raise TypeError("http.response.start headers must be [name, value] pairs") from None
        if not isinstance(name, bytes) or not isinstance(value, bytes):
            raise TypeError("http.response.start header names and values must be bytes")
        headers.append((name.decode("latin-1"), value.decode("latin-1")))
    lengths = [v.strip(" \t") for n, v in headers if n.lower() == "content-length"]
    declared = None
    if len(lengths) == 1 and lengths[0].isascii() and lengths[0].isdigit():
        declared = int(lengths[0])
    return status, headers, declared


class _Body:
    """The Response body: one item per http.response.body message."""

    def __init__(self, exchange: _Exchange) -> None:
        self._exchange = exchange
        self._ended = False

    def __aiter__(self) -> _Body:
        return self

    async def __anext__(self) -> bytes:
        if self._ended:
            raise StopAsyncIteration
        item = await self._exchange.chunks.get()
        if isinstance(item, bytes):
            return item
        self._ended = True
        if isinstance(item, Exception):
            raise item
        raise StopAsyncIteration

    async def aclose(self) -> None:
        self._ended = True
        await self._exchange.finish()
