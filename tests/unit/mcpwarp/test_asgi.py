"""asgi_handler (MCPWARP.md section 9, ASGI mapping): the scope, receive/send, framing through
serve_stream, failures before and after http.response.start, backpressure, and http.disconnect
followed by cancellation when the edge goes away."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.requests import Request as StarletteRequest
from starlette.responses import JSONResponse, PlainTextResponse, StreamingResponse
from starlette.responses import Response as StarletteResponse
from starlette.routing import Route

from ws_mixer.errors import ErrorCode
from ws_mixer.frame import FrameType, decode_frame, reset_code, reset_message
from ws_mixer.mcpwarp import Headers, Request, Response, asgi_handler
from ws_mixer.mcpwarp.asgi import Message, Receive, Scope, Send
from ws_mixer.mcpwarp.http import ErrorReply, Handler
from ws_mixer.mcpwarp.serve import serve_stream
from ws_mixer.stream import MixerStream
from ws_mixer.testing import FakeClock, settle

pytestmark = pytest.mark.asyncio

BIG = 64 * 1024 * 1024
HOST = "notes-tester.mcpwarp.test"


@pytest_asyncio.fixture(autouse=True)
async def no_leaked_tasks() -> AsyncIterator[None]:
    before = asyncio.all_tasks()
    yield
    await settle()
    leaked = asyncio.all_tasks() - before - {asyncio.current_task()}
    assert not leaked, leaked


def make_request(
    method: str = "POST",
    target: str = "/mcp",
    body: bytes = b"",
    headers: list[tuple[str, str]] | None = None,
    http_version: Any = "1.1",
) -> Request:
    fields = headers if headers is not None else [("host", HOST)]
    before_fragment = target.split("#", 1)[0]
    return Request(
        method=method,
        url=f"https://{HOST}{target}",
        target=target,
        path=before_fragment.split("?", 1)[0],
        query=before_fragment.partition("?")[2],
        headers=Headers(fields),
        body=body,
        http_version=http_version,
    )


async def aclose(body: object) -> None:
    close = getattr(body, "aclose", None)
    assert callable(close)
    await close()


async def read_all(res: Response) -> list[bytes]:
    """What serve.py does with a streamed body: iterate it, then aclose it."""
    body = res.body
    assert body is not None and not isinstance(body, bytes | bytearray | memoryview)
    parts: list[bytes] = []
    try:
        async for chunk in body:
            parts.append(chunk)
    finally:
        await aclose(body)
    return parts


async def first_chunk(res: Response) -> bytes:
    body = res.body
    assert body is not None and not isinstance(body, bytes | bytearray | memoryview)
    return await body.__aiter__().__anext__()


# --- raw ASGI apps ---------------------------------------------------------------------------


async def echo_app(scope: Scope, receive: Receive, send: Send) -> None:
    message = await receive()
    ct = dict(scope["headers"]).get(b"content-type", b"application/octet-stream")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", ct)]})
    await send({"type": "http.response.body", "body": message["body"]})


class Recorder:
    """An app that records what it saw, for disconnect and cancellation tests."""

    def __init__(self) -> None:
        self.scope: Scope | None = None
        self.messages: list[Message] = []
        self.events: list[str] = []


# --- the scope -------------------------------------------------------------------------------


async def test_scope_is_the_section_9_mapping() -> None:
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        rec.scope = scope
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body"})

    req = make_request(
        target="/a%20b/%E2%9C%93/x%2Fy?q=%20&r=1",
        headers=[("Host", HOST), ("Content-Type", "application/json"), ("X-Trace", "a, b")],
        http_version="1.0",
    )
    res = await asgi_handler(app)(req)
    assert await read_all(res) == []
    assert rec.scope == {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.0",
        "method": "POST",
        "scheme": "https",
        "path": "/a b/✓/x/y",
        "raw_path": b"/a%20b/%E2%9C%93/x%2Fy",
        "query_string": b"q=%20&r=1",
        "root_path": "",
        "headers": [
            (b"host", HOST.encode()),
            (b"content-type", b"application/json"),
            (b"x-trace", b"a, b"),
        ],
        "server": None,
        "client": None,
        "state": {},
    }


async def test_host_is_added_when_missing() -> None:
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        rec.scope = scope
        await send({"type": "http.response.start", "status": 204})
        await send({"type": "http.response.body"})

    res = await asgi_handler(app)(make_request(method="GET", headers=[]))
    await read_all(res)
    assert rec.scope is not None
    assert rec.scope["headers"] == [(b"host", HOST.encode())]


async def test_path_that_is_not_utf8_is_the_400_error_reply() -> None:
    called: list[bool] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        called.append(True)

    res = await asgi_handler(app)(make_request(target="/m%FFcp"))
    assert isinstance(res, ErrorReply)
    assert res.status == 400
    assert res.mcpwarp_error_message == "request cannot be represented as a web Request"
    assert called == []


async def test_bad_arguments() -> None:
    with pytest.raises(TypeError):
        asgi_handler("not an app")  # type: ignore[arg-type]


# --- receive ---------------------------------------------------------------------------------


async def test_receive_is_one_request_then_disconnect_after_the_response() -> None:
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        rec.messages.append(await receive())
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"ok"})
        rec.messages.append(await receive())

    res = await asgi_handler(app)(make_request(body=b'{"a":1}'))
    assert await read_all(res) == [b"ok"]
    assert rec.messages == [
        {"type": "http.request", "body": b'{"a":1}', "more_body": False},
        {"type": "http.disconnect"},
    ]


# --- the response ----------------------------------------------------------------------------


async def test_echo_status_headers_and_body() -> None:
    req = make_request(body=b"hello", headers=[("host", HOST), ("content-type", "text/x")])
    res = await asgi_handler(echo_app)(req)
    assert res.status == 200
    assert res.headers == Headers([("content-type", "text/x")])
    assert await read_all(res) == [b"hello"]


async def test_app_returning_before_start_raises_from_the_handler() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await receive()

    with pytest.raises(RuntimeError, match="without sending http.response.start"):
        await asgi_handler(app)(make_request())


async def test_app_raising_before_start_propagates() -> None:
    class Boom(Exception):
        pass

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        raise Boom

    with pytest.raises(Boom):
        await asgi_handler(app)(make_request())


async def test_app_raising_mid_stream_fails_the_body() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"one", "more_body": True})
        raise ValueError("mid-stream")

    res = await asgi_handler(app)(make_request())
    body = res.body
    assert body is not None and not isinstance(body, bytes | bytearray | memoryview)
    it = body.__aiter__()
    assert await it.__anext__() == b"one"
    with pytest.raises(ValueError, match="mid-stream"):
        await it.__anext__()
    with pytest.raises(StopAsyncIteration):
        await it.__anext__()
    await aclose(body)


async def test_app_returning_without_completing_the_response_fails_the_body() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"x", "more_body": True})

    res = await asgi_handler(app)(make_request())
    with pytest.raises(RuntimeError, match="without completing the response"):
        await read_all(res)


@pytest.mark.parametrize("status", [100, 101, 103, 199])
async def test_1xx_status_raises(status: int) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": status})

    with pytest.raises(ValueError, match="1xx"):
        await asgi_handler(app)(make_request())


@pytest.mark.parametrize(
    "start",
    [
        {"status": "200"},
        {"status": True},
        {"status": 200, "headers": [("content-type", "text/plain")]},
        {"status": 200, "headers": [(b"content-type", "text/plain")]},
        {"status": 200, "headers": [(b"a", b"b", b"c")]},
        {"status": 200, "headers": [b"ab"]},
    ],
)
async def test_invalid_start_raises_type_error_in_app_and_handler(start: dict[str, Any]) -> None:
    seen: list[BaseException] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await send({"type": "http.response.start", **start})
        except TypeError as exc:
            seen.append(exc)
            raise

    with pytest.raises(TypeError):
        await asgi_handler(app)(make_request())
    assert len(seen) == 1


async def test_protocol_errors_raise_in_the_app() -> None:
    seen: list[str] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        bad_messages: list[Message] = [
            {"type": "http.response.body", "body": b"x"},
            {"type": "nope"},
        ]
        for bad in bad_messages:
            try:
                await send(bad)
            except RuntimeError:
                seen.append(bad["type"])
        await send({"type": "http.response.start", "status": 200})
        try:
            await send({"type": "http.response.start", "status": 200})
        except RuntimeError:
            seen.append("second start")
        try:
            await send({"type": "http.response.body", "body": "str"})
        except TypeError:
            seen.append("str body")
        await send({"type": "http.response.body"})
        try:
            await send({"type": "http.response.body", "body": b"late"})
        except RuntimeError:
            seen.append("after complete")

    res = await asgi_handler(app)(make_request())
    assert await read_all(res) == []
    assert seen == ["http.response.body", "nope", "second start", "str body", "after complete"]


async def test_body_message_without_body_key_ends_the_response() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"a", "more_body": True})
        await send({"type": "http.response.body", "body": bytearray(b"b"), "more_body": True})
        await send({"type": "http.response.body"})

    assert await read_all(await asgi_handler(app)(make_request())) == [b"a", b"b"]


async def test_sse_chunks_arrive_before_the_app_finishes() -> None:
    step = asyncio.Event()
    finished: list[bool] = []

    async def events() -> AsyncIterator[str]:
        for i in range(3):
            yield f"data: {i}\n\n"
            if i < 2:
                await step.wait()
                step.clear()
        finished.append(True)

    async def endpoint(request: StarletteRequest) -> StreamingResponse:
        return StreamingResponse(events(), media_type="text/event-stream")

    app = Starlette(routes=[Route("/mcp", endpoint, methods=["GET"])])
    res = await asgi_handler(app)(make_request(method="GET"))
    assert res.status == 200
    assert Headers(res.headers).get("content-type") == "text/event-stream; charset=utf-8"
    body = res.body
    assert body is not None and not isinstance(body, bytes | bytearray | memoryview)
    it = body.__aiter__()
    got = [await it.__anext__()]
    assert got == [b"data: 0\n\n"] and finished == []
    step.set()
    got.append(await it.__anext__())
    assert got[-1] == b"data: 1\n\n" and finished == []
    step.set()
    async for chunk in it:
        got.append(chunk)
    await aclose(body)
    assert got == [b"data: 0\n\n", b"data: 1\n\n", b"data: 2\n\n"]
    assert finished == [True]


async def test_send_is_bounded_to_one_queued_chunk() -> None:
    sent: list[int] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200})
        for i in range(5):
            await send({"type": "http.response.body", "body": b"%d" % i, "more_body": True})
            sent.append(i)
        await send({"type": "http.response.body"})

    res = await asgi_handler(app)(make_request())
    await settle()
    assert sent == [0]
    assert await first_chunk(res) == b"0"
    await settle()
    assert sent == [0, 1]
    assert await read_all(res) == [b"1", b"2", b"3", b"4"]


# --- disconnect and cancellation -------------------------------------------------------------


async def test_early_aclose_delivers_disconnect_then_cancels() -> None:
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await receive()
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"first", "more_body": True})
        message = await receive()
        rec.events.append(message["type"])
        try:
            await send({"type": "http.response.body", "body": b"x", "more_body": True})
        except OSError:
            rec.events.append("send raised OSError")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            rec.events.append("cancelled")
            raise

    res = await asgi_handler(app)(make_request())
    assert await first_chunk(res) == b"first"
    await settle()
    assert rec.events == []
    await aclose(res.body)
    assert rec.events == ["http.disconnect", "send raised OSError", "cancelled"]


async def test_app_blocked_in_send_sees_oserror_on_disconnect() -> None:
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200})
        try:
            while True:
                await send({"type": "http.response.body", "body": b"x", "more_body": True})
        except OSError:
            rec.events.append("OSError")

    res = await asgi_handler(app)(make_request())
    assert await first_chunk(res) == b"x"
    await aclose(res.body)
    assert rec.events == ["OSError"]


async def test_app_that_returns_on_disconnect_is_not_cancelled() -> None:
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await receive()
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"a", "more_body": True})
        if (await receive())["type"] == "http.disconnect":
            rec.events.append("clean exit")

    res = await asgi_handler(app)(make_request())
    assert await first_chunk(res) == b"a"
    await aclose(res.body)
    assert rec.events == ["clean exit"]


async def test_cancelling_the_handler_before_start_disconnects_then_cancels() -> None:
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await receive()
        rec.events.append((await receive())["type"])
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            rec.events.append("cancelled")
            raise

    handler = asgi_handler(app)

    async def call() -> Response:
        return await handler(make_request())

    task = asyncio.create_task(call())
    await settle()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert rec.events == ["http.disconnect", "cancelled"]


async def test_repeated_cancels_during_abort_still_cancel_and_await_the_app() -> None:
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"a", "more_body": True})
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            rec.events.append("cancelled")
            raise

    res = await asgi_handler(app)(make_request())
    assert await first_chunk(res) == b"a"
    closing = asyncio.create_task(aclose(res.body))
    for _ in range(3):
        await asyncio.sleep(0)
        closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert rec.events == ["cancelled"]


async def test_send_after_disconnect_yields_so_a_swallowing_app_is_cancelled() -> None:
    rec = Recorder()
    swallowed: list[int] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"a", "more_body": True})
        try:
            for i in range(10_000):
                try:
                    await send({"type": "http.response.body", "body": b"x", "more_body": True})
                except OSError:
                    swallowed.append(i)
        except asyncio.CancelledError:
            rec.events.append("cancelled")
            raise

    res = await asgi_handler(app)(make_request())
    assert await first_chunk(res) == b"a"
    await aclose(res.body)
    assert rec.events == ["cancelled"]
    assert len(swallowed) == 1


async def test_after_a_complete_response_aclose_waits_for_background_work() -> None:
    release = asyncio.Event()
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"done"})
        await release.wait()
        rec.events.append("background done")

    res = await asgi_handler(app)(make_request())
    body = res.body
    assert body is not None and not isinstance(body, bytes | bytearray | memoryview)
    assert [c async for c in body] == [b"done"]
    closing = asyncio.create_task(aclose(body))
    await settle()
    assert not closing.done() and rec.events == []
    release.set()
    await closing
    assert rec.events == ["background done"]


# --- end to end through serve_stream ---------------------------------------------------------


class Host:
    """A StreamHost recording DATA/CLOSE/RESET (as in test_serve.py)."""

    def __init__(self) -> None:
        self.events: list[tuple[str, Any]] = []

    def send_data(self, stream_id: int, chunk: bytes) -> asyncio.Future[None]:
        self.events.append(("DATA", bytes(chunk)))
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        return fut

    def send_control_frame(self, frame: bytes) -> None:
        f = decode_frame(frame)
        if f.type == FrameType.CLOSE:
            self.events.append(("CLOSE", None))
        elif f.type == FrameType.RESET:
            self.events.append(("RESET", (reset_code(f), reset_message(f))))

    def retire_stream(self, stream_id: int) -> None:
        pass

    def written(self) -> bytes:
        return b"".join(v for k, v in self.events if k == "DATA")

    def data(self) -> list[bytes]:
        return [v for k, v in self.events if k == "DATA"]

    def ends(self) -> list[tuple[str, Any]]:
        return [e for e in self.events if e[0] in ("CLOSE", "RESET")]


class Bridge:
    def __init__(self, handler: Handler) -> None:
        self.host = Host()
        self.stream = MixerStream(1, self.host, BIG, BIG)
        self.errors: list[BaseException] = []
        self.task = asyncio.create_task(
            serve_stream(
                self.stream,
                handler=handler,
                availability=lambda: "available",
                max_request_body_bytes=4_194_304,
                clock=FakeClock(),
                on_error=self.errors.append,
            )
        )

    def send(self, data: str) -> None:
        self.stream.handle_data(data.encode("latin-1"))
        self.stream.handle_close()

    async def request(self, data: str) -> str:
        self.send(data)
        await asyncio.wait_for(self.task, 5)
        return self.host.written().decode("latin-1")


def http(method: str = "POST", body: str = "", fields: str = "") -> str:
    framing = f"Content-Length: {len(body)}\r\n" if body else ""
    return f"{method} /mcp HTTP/1.1\r\nHost: {HOST}\r\n{fields}{framing}\r\n{body}"


async def test_e2e_echo_is_chunked() -> None:
    b = Bridge(asgi_handler(echo_app))
    out = await b.request(http(body='{"x":1}', fields="Content-Type: application/json\r\n"))
    assert out == (
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n"
        'Transfer-Encoding: chunked\r\n\r\n7\r\n{"x":1}\r\n0\r\n\r\n'
    )
    assert b.host.ends() == [("CLOSE", None)]
    assert b.errors == []


def starlette_app() -> Starlette:
    async def mcp(request: StarletteRequest) -> StarletteResponse:
        if request.method == "POST":
            return JSONResponse({"echo": json.loads(await request.body())})
        if status := request.query_params.get("status"):
            return StarletteResponse(status_code=int(status), headers={"etag": '"v1"'})
        return PlainTextResponse("hello")

    return Starlette(routes=[Route("/mcp", mcp, methods=["GET", "POST"])])


async def test_e2e_starlette_json_keeps_content_length() -> None:
    b = Bridge(asgi_handler(starlette_app()))
    out = await b.request(http(body='{"id":1}', fields="Content-Type: application/json\r\n"))
    body = '{"echo":{"id":1}}'
    assert out == (
        f"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n\r\n{body}"
    )
    assert b.host.ends() == [("CLOSE", None)]


async def test_e2e_head_is_bodyless() -> None:
    b = Bridge(asgi_handler(starlette_app()))
    out = await b.request(http("HEAD"))
    assert out == "HTTP/1.1 200 OK\r\ncontent-type: text/plain; charset=utf-8\r\n\r\n"
    assert b.host.ends() == [("CLOSE", None)]
    assert b.errors == []


async def test_e2e_head_to_a_streaming_app_cancels_it() -> None:
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200})
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            rec.events.append("cancelled")
            raise

    b = Bridge(asgi_handler(app))
    assert await b.request(http("HEAD")) == "HTTP/1.1 200 OK\r\n\r\n"
    assert rec.events == ["cancelled"]
    assert b.host.ends() == [("CLOSE", None)]


async def test_e2e_brackets_reach_the_scope_unchanged() -> None:
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        rec.scope = scope
        await send({"type": "http.response.start", "status": 204})
        await send({"type": "http.response.body"})

    b = Bridge(asgi_handler(app))
    await b.request(http("GET").replace("/mcp", "/mcp?a[]=1&k=[x]", 1))
    assert rec.scope is not None
    assert (rec.scope["path"], rec.scope["raw_path"]) == ("/mcp", b"/mcp")
    assert rec.scope["query_string"] == b"a[]=1&k=[x]"
    assert b.errors == []


@pytest.mark.parametrize(
    ("method", "fields"), [("CONNECT", ""), ("trace", ""), ("POST", "x-nul: a\x00b\r\n")]
)
async def test_e2e_unrepresentable_request_is_400_and_the_app_is_not_called(
    method: str, fields: str
) -> None:
    called: list[bool] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        called.append(True)

    b = Bridge(asgi_handler(app))
    out = await b.request(http(method, body="{}", fields=fields))
    body = '{"error":"request cannot be represented as a web Request"}'
    assert out == (
        "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n\r\n{body}"
    )
    assert called == []
    assert b.host.ends() == [("CLOSE", None)]
    assert b.errors == []


@pytest.mark.parametrize(("status", "reason"), [(204, "No Content"), (304, "Not Modified")])
async def test_e2e_204_and_304_are_bodyless(status: int, reason: str) -> None:
    b = Bridge(asgi_handler(starlette_app()))
    out = await b.request(http("GET").replace("/mcp", f"/mcp?status={status}", 1))
    assert out == f'HTTP/1.1 {status} {reason}\r\netag: "v1"\r\n\r\n'
    assert b.host.ends() == [("CLOSE", None)]
    assert b.errors == []


async def test_e2e_close_comes_before_background_tasks_finish() -> None:
    release = asyncio.Event()
    done: list[str] = []

    async def background() -> None:
        await release.wait()
        done.append("background")

    async def endpoint(request: StarletteRequest) -> StarletteResponse:
        return PlainTextResponse("ok", background=BackgroundTask(background))

    app = Starlette(routes=[Route("/mcp", endpoint, methods=["POST"])])
    b = Bridge(asgi_handler(app))
    b.send(http())
    await settle()
    assert b.host.written().endswith(b"\r\n\r\nok")
    assert b.host.ends() == [("CLOSE", None)]
    assert done == [] and not b.task.done()
    release.set()
    await asyncio.wait_for(b.task, 5)
    assert done == ["background"]
    assert b.errors == []


async def test_e2e_declared_length_reached_completes_without_cancelling_the_app() -> None:
    release = asyncio.Event()
    done: list[str] = []

    async def chunks() -> AsyncIterator[bytes]:
        yield b"ab"
        yield b"c"
        await release.wait()
        done.append("after last yield")

    async def background() -> None:
        done.append("background")

    async def endpoint(request: StarletteRequest) -> StarletteResponse:
        return StreamingResponse(
            chunks(), headers={"content-length": "3"}, background=BackgroundTask(background)
        )

    app = Starlette(routes=[Route("/mcp", endpoint, methods=["GET"])])
    b = Bridge(asgi_handler(app))
    b.send(http("GET"))
    await settle()
    assert b.host.written() == b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc"
    assert b.host.ends() == [("CLOSE", None)]
    assert done == []
    release.set()
    await asyncio.wait_for(b.task, 5)
    assert done == ["after last yield", "background"]
    assert b.errors == []


async def test_e2e_declared_length_0_completes_at_start() -> None:
    release = asyncio.Event()
    done: list[str] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"0")]}
        )
        await release.wait()
        await send({"type": "http.response.body", "body": b"dropped"})
        done.append("app done")

    b = Bridge(asgi_handler(app))
    b.send(http())
    await settle()
    assert b.host.written() == b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
    assert b.host.ends() == [("CLOSE", None)]
    release.set()
    await asyncio.wait_for(b.task, 5)
    assert done == ["app done"]


async def test_e2e_app_raising_before_start_is_500_handler_error() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        raise KeyError("boom")

    b = Bridge(asgi_handler(app))
    out = await b.request(http())
    assert out.startswith("HTTP/1.1 500 Internal Server Error\r\n")
    assert out.endswith('{"error":"handler error"}')
    assert [type(e) for e in b.errors] == [KeyError]


async def test_e2e_1xx_is_500_handler_error() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 103})

    b = Bridge(asgi_handler(app))
    out = await b.request(http())
    assert out.endswith('{"error":"handler error"}')
    assert [type(e) for e in b.errors] == [ValueError]


async def test_e2e_app_raising_after_start_resets() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"part", "more_body": True})
        raise KeyError("late")

    b = Bridge(asgi_handler(app))
    out = await b.request(http())
    assert out == "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\npart\r\n"
    assert b.host.ends() == [("RESET", (ErrorCode.INTERNAL_ERROR, "response body failed"))]
    assert [type(e) for e in b.errors] == [KeyError]


async def test_e2e_body_shorter_than_declared_resets() -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"9")]}
        )
        await send({"type": "http.response.body", "body": b"abc"})

    b = Bridge(asgi_handler(app))
    assert await b.request(http()) == "HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nabc"
    assert b.host.ends() == [
        ("RESET", (ErrorCode.INTERNAL_ERROR, "response body shorter than Content-Length"))
    ]


async def test_e2e_sse_streams_each_event_and_reset_disconnects_the_app() -> None:
    step = asyncio.Event()
    rec = Recorder()

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await receive()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/event-stream")],
            }
        )
        await send({"type": "http.response.body", "body": b"data: 1\n\n", "more_body": True})
        await step.wait()
        await send({"type": "http.response.body", "body": b"data: 2\n\n", "more_body": True})
        rec.events.append((await receive())["type"])
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            rec.events.append("cancelled")
            raise

    b = Bridge(asgi_handler(app))
    b.send(http("GET"))
    await settle()
    assert b.host.data()[-1] == b"9\r\ndata: 1\n\n\r\n"
    step.set()
    await settle()
    assert b.host.data()[-1] == b"9\r\ndata: 2\n\n\r\n"
    assert rec.events == []
    b.stream.handle_reset(ErrorCode.CANCEL, "client went away")
    await asyncio.wait_for(b.task, 5)
    assert rec.events == ["http.disconnect", "cancelled"]
    assert b.errors == []


async def test_e2e_starlette_sse_completes() -> None:
    async def events() -> AsyncIterator[bytes]:
        for i in range(3):
            yield b"data: %d\n\n" % i

    async def endpoint(request: StarletteRequest) -> StreamingResponse:
        return StreamingResponse(events(), media_type="text/event-stream")

    app = Starlette(routes=[Route("/mcp", endpoint, methods=["GET"])])
    b = Bridge(asgi_handler(app))
    out = await b.request(http("GET"))
    assert out.endswith(
        "\r\n\r\n9\r\ndata: 0\n\n\r\n9\r\ndata: 1\n\n\r\n9\r\ndata: 2\n\n\r\n0\r\n\r\n"
    )
    assert "Transfer-Encoding: chunked\r\n" in out
    assert b.host.ends() == [("CLOSE", None)]


# --- the mcp SDK's StreamableHTTP app ---------------------------------------------------------


def mcp_server(*, stateless: bool, json_response: bool) -> tuple[Any, Any]:
    """(server, ASGI app) for mcp 2.x (MCPServer) or 1.x (FastMCP), with an `add` tool."""
    server: Any
    try:  # mcp 2.x
        from mcp.server.mcpserver import (  # type: ignore[import-not-found, unused-ignore]
            MCPServer,
        )

        server = MCPServer("smoke")
        options = {"stateless_http": stateless, "json_response": json_response}
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import (  # type: ignore[import-not-found, unused-ignore]
            FastMCP,
        )

        server = FastMCP("smoke", stateless_http=stateless, json_response=json_response)
        options = {}

    def add(a: int, b: int) -> int:
        return a + b

    server.tool()(add)
    return server, server.streamable_http_app(**options)


def mcp_payload(res: Response, raw: bytes) -> Any:
    """The JSON-RPC message of a JSON or an SSE response (the last `data:` line)."""
    if (Headers(res.headers).get("content-type") or "").startswith("text/event-stream"):
        data = [ln[5:].strip() for ln in raw.decode().splitlines() if ln.startswith("data:")]
        return json.loads(data[-1])
    return json.loads(raw)


# Run only these with `-k smoke`: every test id contains "mcpwarp" (the directory), so
# `-k mcp` selects the whole file. They need the mcp package, which is not a dev dependency:
#   uv run --with mcp pytest -q tests/unit/mcpwarp/test_asgi.py -k smoke
#   uv run --with 'mcp<2' pytest -q tests/unit/mcpwarp/test_asgi.py -k smoke
@pytest.mark.parametrize(
    ("stateless", "json_response"),
    [(True, True), (True, False), (False, False)],
    ids=["smoke-json-stateless", "smoke-sse-stateless", "smoke-sse-stateful"],
)
async def test_mcp_streamable_http(stateless: bool, json_response: bool) -> None:
    pytest.importorskip("mcp")
    server, app = mcp_server(stateless=stateless, json_response=json_response)
    handler = asgi_handler(app)
    session: list[tuple[str, str]] = []

    async def call(message: dict[str, Any]) -> tuple[Response, bytes]:
        req = make_request(
            body=json.dumps(message).encode(),
            headers=[
                ("host", "localhost:8000"),
                ("content-type", "application/json"),
                ("accept", "application/json, text/event-stream"),
                ("mcp-protocol-version", "2025-06-18"),
                *session,
            ],
        )
        res = await handler(req)
        return res, b"".join(await read_all(res))

    initialize = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "t", "version": "0"},
        },
    }
    async with server.session_manager.run():
        res, raw = await call(initialize)
        assert res.status == 200, raw
        assert mcp_payload(res, raw)["result"]["serverInfo"]["name"] == "smoke"
        session_id = Headers(res.headers).get("mcp-session-id")
        assert (session_id is None) == stateless
        if session_id is not None:
            session.append(("mcp-session-id", session_id))
        res, raw = await call({"jsonrpc": "2.0", "method": "notifications/initialized"})
        assert res.status == 202, raw
        tool_call = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "add", "arguments": {"a": 2, "b": 3}},
        }
        res, raw = await call(tool_call)
        assert res.status == 200, raw
        result = mcp_payload(res, raw)["result"]
        assert result["content"][0]["text"] == "5", result
    # sse_starlette starts one process-lifetime shutdown watcher per loop (it polls for
    # uvicorn's exit); it is not ours, so stop it before the leak check.
    watchers = [
        t
        for t in asyncio.all_tasks()
        if getattr(t.get_coro(), "__qualname__", None) == "_shutdown_watcher"
    ]
    for t in watchers:
        t.cancel()
    await asyncio.gather(*watchers, return_exceptions=True)
