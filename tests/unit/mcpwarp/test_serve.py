"""serve_stream (MCPWARP.md section 4) against a standalone MixerStream whose host records what
the agent sends: precedence, refusals (reply, drain, CLOSE), the head deadline, body framing and
cap, the response writer, handler failures, and edge cancellation."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from ws_mixer.errors import ErrorCode
from ws_mixer.frame import FrameType, decode_frame, reset_code, reset_message, window_increment
from ws_mixer.mcpwarp import Headers, Request, Response, sync_handler
from ws_mixer.mcpwarp.http import Handler, error_reply
from ws_mixer.mcpwarp.serve import Availability, serve_stream
from ws_mixer.stream import Finished, MixerStream, Reset
from ws_mixer.testing import FakeClock, settle

pytestmark = pytest.mark.asyncio

BIG = 64 * 1024 * 1024
HOST = "Host: notes-tester.mcpwarp.test\r\n"


class Host:
    """A StreamHost recording DATA/WINDOW/CLOSE/RESET."""

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
        elif f.type == FrameType.WINDOW:
            self.events.append(("WINDOW", window_increment(f)))

    def retire_stream(self, stream_id: int) -> None:
        pass

    def written(self) -> bytes:
        return b"".join(v for k, v in self.events if k == "DATA")

    def ends(self) -> list[tuple[str, Any]]:
        return [e for e in self.events if e[0] in ("CLOSE", "RESET")]


class Bridge:
    """One stream being served; the test plays the edge."""

    def __init__(
        self,
        handler: Handler | None = None,
        *,
        state: Availability | str = "available",
        max_body: int = 4_194_304,
        recv_window: int = BIG,
        on_error: Callable[[BaseException], None] | None = None,
        handler_timeout_ms: float | None = None,
    ) -> None:
        self.clock = FakeClock()
        self.host = Host()
        self.stream = MixerStream(1, self.host, recv_window, BIG)
        self.errors: list[BaseException] = []
        self.requests: list[Request] = []
        self.state = state
        self.task = asyncio.create_task(
            serve_stream(
                self.stream,
                handler=handler if handler is not None else self.echo,
                availability=lambda: self.state,  # type: ignore[arg-type, return-value]
                max_request_body_bytes=max_body,
                clock=self.clock,
                on_error=on_error if on_error is not None else self.errors.append,
                handler_timeout_ms=handler_timeout_ms,
            )
        )

    async def echo(self, request: Request) -> Response:
        self.requests.append(request)
        ct = request.headers.get("content-type")
        return Response(200, [("content-type", ct)] if ct else [], request.body)

    def feed(self, data: bytes | str) -> None:
        raw = data.encode("latin-1") if isinstance(data, str) else data
        for i in range(0, len(raw), 16_384):
            self.stream.handle_data(raw[i : i + 16_384])

    async def request(self, data: bytes | str) -> str:
        """Writes the whole request, CLOSE, and waits for serve_stream to finish."""
        self.feed(data)
        self.stream.handle_close()
        await self.done()
        return self.text()

    async def done(self) -> None:
        await asyncio.wait_for(self.task, 5)

    def text(self) -> str:
        return self.host.written().decode("latin-1")


def post(body: str = "", fields: str = "", target: str = "/mcp", method: str = "POST") -> str:
    framing = f"Content-Length: {len(body)}\r\n" if body else ""
    return f"{method} {target} HTTP/1.1\r\n{HOST}{fields}{framing}\r\n{body}"


def reply(status: str, message: str, head_only: bool = False) -> str:
    body = '{"error":"' + message + '"}'
    head = (
        f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n\r\n"
    )
    return head if head_only else head + body


def closed_cleanly(b: Bridge) -> bool:
    return b.host.ends() == [("CLOSE", None)]


# --- section 4.2 head rules ------------------------------------------------------------------


async def test_head_deadline_resets_with_cancel_and_no_response() -> None:
    b = Bridge()
    b.feed("POST /mcp HTTP/1.1\r\nHost: h")
    await b.clock.advance(9_999)
    assert b.host.ends() == []
    await b.clock.advance(1)
    await b.done()
    assert b.host.written() == b""
    assert b.host.ends() == [
        ("RESET", (ErrorCode.CANCEL, "request head deadline exceeded")),
    ]
    assert b.requests == []


async def test_head_deadline_timer_is_cancelled_once_the_head_is_in() -> None:
    b = Bridge()
    b.feed(post("{}"))
    await settle()
    assert b.clock.pending() == 0
    b.stream.handle_close()
    await b.done()
    await b.clock.advance(20_000)
    assert closed_cleanly(b)


async def test_incomplete_request() -> None:
    b = Bridge()
    out = await b.request("POST /mcp HTTP/1.1\r\nHost: h\r\n")
    assert out == reply("400 Bad Request", "incomplete request")
    assert closed_cleanly(b)


async def test_empty_stream_is_an_incomplete_request() -> None:
    b = Bridge()
    assert await b.request(b"") == reply("400 Bad Request", "incomplete request")


async def test_431_drains_the_rest_then_closes() -> None:
    b = Bridge()
    b.feed("POST /mcp HTTP/1.1\r\n" + HOST + "x-pad: " + "a" * 70_000)
    await settle()
    assert b.text() == reply("431 Request Header Fields Too Large", "request head too large")
    assert b.host.ends() == []  # still draining
    b.feed("a" * 1000 + "\r\n\r\n")
    b.stream.handle_close()
    await b.done()
    assert closed_cleanly(b)


async def test_parse_error_reply_to_head_is_head_only() -> None:
    b = Bridge()
    out = await b.request("HEAD /mcp HTTP/1.1\r\n\r\n")
    assert out == reply("400 Bad Request", "missing or duplicate Host header", head_only=True)


async def test_malformed_body_is_400_and_closes() -> None:
    b = Bridge()
    out = await b.request(post(fields="Transfer-Encoding: chunked\r\n") + "zz\r\n")
    assert out == reply("400 Bad Request", "malformed chunk size")
    assert closed_cleanly(b)
    assert b.requests == []


async def test_stream_ended_inside_the_body() -> None:
    b = Bridge()
    out = await b.request(post(fields="Content-Length: 10\r\n") + "12345")
    assert out == reply("400 Bad Request", "stream ended inside the request body")
    b2 = Bridge()
    out = await b2.request(post(fields="Transfer-Encoding: chunked\r\n") + "5\r\nab")
    assert out == reply("400 Bad Request", "stream ended inside the request body")


@pytest.mark.parametrize(
    "fields",
    ["Host: bad host\r\n", "Host: h.example\r\nx-nul: a\x00b\r\n"],
)
async def test_400_request_cannot_be_represented(fields: str) -> None:
    b = Bridge()
    out = await b.request(f"POST /mcp HTTP/1.1\r\n{fields}Content-Length: 2\r\n\r\n{{}}")
    assert out == reply("400 Bad Request", "request cannot be represented as a web Request")
    assert b.requests == []
    assert closed_cleanly(b)


# --- precedence (section 4.4) ----------------------------------------------------------------

OVER_CAP = "x" * 32


@pytest.mark.parametrize(
    ("state", "target", "body", "expected"),
    [
        ("available", "/healthz", "", reply("404 Not Found", "not_found")),
        ("disabled", "/healthz", OVER_CAP, reply("404 Not Found", "not_found")),
        ("closing", "/mcp/", OVER_CAP, reply("404 Not Found", "not_found")),
        ("unavailable", "/MCP", "", reply("404 Not Found", "not_found")),
        ("disabled", "/mcp", OVER_CAP, reply("503 Service Unavailable", "service disabled")),
        ("closing", "/mcp", OVER_CAP, reply("503 Service Unavailable", "service closing")),
        ("unavailable", "/mcp", OVER_CAP, reply("503 Service Unavailable", "service unavailable")),
        ("bogus", "/mcp", "", reply("503 Service Unavailable", "service unavailable")),
        ("available", "/mcp", OVER_CAP, reply("413 Content Too Large", "request body too large")),
    ],
)
async def test_precedence_table(state: str, target: str, body: str, expected: str) -> None:
    b = Bridge(state=state, max_body=16)
    head, _, rest = post(body, target=target).partition("\r\n\r\n")
    # Rows 2-4 answer from the head alone: the reply is out before any body byte arrives.
    b.feed(head + "\r\n\r\n")
    await settle()
    assert b.text() == expected
    b.feed(rest)
    b.stream.handle_close()
    await b.done()
    assert closed_cleanly(b)
    assert b.requests == []


async def test_precedence_head_errors_beat_404_and_503() -> None:
    b = Bridge(state="disabled")
    out = await b.request("GET /healthz HTTP/1.1\r\n\r\n")
    assert out == reply("400 Bad Request", "missing or duplicate Host header")


async def test_precedence_413_beats_a_malformed_body() -> None:
    b = Bridge(max_body=4)
    out = await b.request(post(fields="Transfer-Encoding: chunked\r\n") + "5\r\nabcde\r\nzz\r\n")
    assert out == reply("413 Content Too Large", "request body too large")


async def test_conflict_on_arrival_resets_without_reading() -> None:
    b = Bridge(state="conflict")
    b.feed(post("{}"))
    await b.done()
    assert b.host.written() == b""
    assert b.host.ends() == [
        ("RESET", (ErrorCode.CANCEL, "service taken over by another connection"))
    ]


async def test_conflict_after_the_head_resets() -> None:
    b = Bridge()
    b.feed("POST /healthz HTTP/1.1\r\n")
    await settle()
    b.state = "conflict"
    b.feed(HOST + "\r\n")
    await b.done()
    assert b.host.written() == b""
    assert b.host.ends()[-1][0] == "RESET"


async def test_availability_read_after_the_head() -> None:
    b = Bridge()
    b.feed("POST /mcp HTTP/1.1\r\n")
    await settle()
    b.state = "disabled"
    out = await b.request(HOST + "\r\n")
    assert out == reply("503 Service Unavailable", "service disabled")


# --- ending a refused stream: drain, granting window, then CLOSE (never RESET) --------------


@pytest.mark.parametrize(
    ("state", "target", "max_body", "framing"),
    [
        ("disabled", "/mcp", 4_194_304, "length"),
        ("available", "/healthz", 4_194_304, "length"),
        ("available", "/mcp", 1_000, "length"),
        ("closing", "/mcp", 4_194_304, "chunked"),
        ("available", "/mcp", 1_000, "chunked"),
    ],
)
async def test_refusal_drains_the_body_with_window_then_closes(
    state: str, target: str, max_body: int, framing: str
) -> None:
    window = 1024
    b = Bridge(state=state, max_body=max_body, recv_window=window)
    body = b"y" * 6000
    if framing == "length":
        fields = f"Content-Length: {len(body)}\r\n"
        wire = body
    else:
        fields = "Transfer-Encoding: chunked\r\n"
        wire = b"".join(
            b"%x\r\n%b\r\n" % (len(body[i : i + 500]), body[i : i + 500])
            for i in range(0, len(body), 500)
        )
        wire += b"0\r\n\r\n"
    b.feed(f"POST {target} HTTP/1.1\r\n{HOST}{fields}\r\n")
    sent = 0
    while sent < len(wire):
        await settle()
        n = min(b.stream.recv_window, len(wire) - sent)
        assert n > 0, "the agent stopped granting window"
        b.feed(wire[sent : sent + n])
        sent += n
    await settle()
    assert b.text().startswith("HTTP/1.1 ")
    assert b.host.ends() == []  # no CLOSE before the edge's CLOSE
    b.stream.handle_close()
    await b.done()
    kinds = [k for k, _ in b.host.events]
    assert "WINDOW" in kinds
    assert "RESET" not in kinds
    assert kinds[-1] == "CLOSE"
    # The drain after the reply grants window (row 4 also grants while reading up to the cap).
    assert "WINDOW" in kinds[kinds.index("DATA") :]
    assert isinstance(await b.stream.wait_closed(), Finished)
    assert b.requests == []


async def test_413_by_content_length_is_answered_before_any_body_byte() -> None:
    b = Bridge(max_body=16)
    b.feed(post(fields="Content-Length: 95\r\n"))
    await settle()
    assert b.text() == reply("413 Content Too Large", "request body too large")


async def test_body_cap_counts_decoded_bytes_end_to_end() -> None:
    """Regression (JS port bug): chunk framing and extensions do not count toward the cap."""
    ext = ";" + "e" * 2000
    chunks = "".join(f"1{ext}\r\n{c}\r\n" for c in "0123456789") + f"0{ext}\r\nt: x\r\n\r\n"
    b = Bridge(max_body=10)
    out = await b.request(post(fields="Transfer-Encoding: chunked\r\n") + chunks)
    assert out.startswith("HTTP/1.1 200 OK\r\n")
    assert b.requests[0].body == b"0123456789"
    over = "".join(f"1{ext}\r\n{c}\r\n" for c in "0123456789A") + "0\r\n\r\n"
    b2 = Bridge(max_body=10)
    out = await b2.request(post(fields="Transfer-Encoding: chunked\r\n") + over)
    assert out == reply("413 Content Too Large", "request body too large")


# --- the request the handler sees (section 4.6) ---------------------------------------------


async def test_request_fields() -> None:
    b = Bridge()
    await b.request(
        "POST https://other.example/mcp?x=1&y=%41 HTTP/1.0\r\n"
        + HOST
        + "Connection: close, x-hop\r\nx-hop: 1\r\nKeep-Alive: 5\r\n"
        + "Expect: 100-continue\r\nmcp-session-id: abc\r\nContent-Length: 2\r\n\r\nhi"
    )
    (req,) = b.requests
    assert req.method == "POST"
    assert req.url == "https://notes-tester.mcpwarp.test/mcp?x=1&y=%41"
    assert req.target == "/mcp?x=1&y=%41"
    assert req.path == "/mcp"
    assert req.query == "x=1&y=%41"
    assert req.http_version == "1.0"
    assert req.headers == Headers(
        [("Host", "notes-tester.mcpwarp.test"), ("mcp-session-id", "abc")]
    )
    assert req.body == b"hi"


@pytest.mark.parametrize("method", ["GET", "HEAD"])
async def test_get_and_head_get_no_body(method: str) -> None:
    b = Bridge()
    await b.request(post("abc", method=method))
    assert b.requests[0].body == b""


async def test_expect_continue_never_gets_a_1xx() -> None:
    b = Bridge()
    out = await b.request(post("{}", fields="Expect: 100-continue\r\n"))
    assert out.startswith("HTTP/1.1 200 OK\r\n")
    assert "expect" not in b.requests[0].headers


async def test_sync_handler_through_the_bridge() -> None:
    def fn(req: Request) -> Response:
        return Response(201, {"x-thread": "yes"}, req.body)

    b = Bridge(sync_handler(fn))
    out = await b.request(post("hey"))
    assert out == (
        "HTTP/1.1 201 Created\r\nx-thread: yes\r\nTransfer-Encoding: chunked\r\n\r\n"
        "3\r\nhey\r\n0\r\n\r\n"
    )


# --- the response writer (section 4.1) ------------------------------------------------------


def handler_returning(res: object) -> Handler:
    async def handler(request: Request) -> Response:
        return res  # type: ignore[return-value]

    return handler


async def body_of(*chunks: bytes) -> AsyncIterator[bytes]:
    for c in chunks:
        yield c


async def test_no_length_is_chunked() -> None:
    b = Bridge(handler_returning(Response(200, [("content-type", "text/plain")], b"hello")))
    out = await b.request(post())
    assert out == (
        "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n"
        "5\r\nhello\r\n0\r\n\r\n"
    )
    assert closed_cleanly(b)


async def test_declared_length_is_kept() -> None:
    b = Bridge(handler_returning(Response(200, {"Content-Length": "5"}, b"hello")))
    assert await b.request(post()) == "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"


async def test_declared_length_with_a_streamed_body() -> None:
    res = Response(200, {"content-length": "6"}, body_of(b"abc", b"", b"def"))
    b = Bridge(handler_returning(res))
    assert await b.request(post()) == "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabcdef"
    assert closed_cleanly(b)


async def test_body_longer_than_declared_is_cut() -> None:
    b = Bridge(handler_returning(Response(200, {"content-length": "3"}, b"abcdef")))
    assert await b.request(post()) == "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc"


@pytest.mark.parametrize("bad", ["5, 5", "x", "-1", "²"])
async def test_invalid_declared_length_is_chunked(bad: str) -> None:
    b = Bridge(handler_returning(Response(200, [("content-length", bad)], b"hello")))
    out = await b.request(post())
    assert "Transfer-Encoding: chunked\r\n" in out
    assert "ontent-length" not in out.lower().replace("transfer-encoding", "")


async def test_none_body_is_content_length_0() -> None:
    b = Bridge(handler_returning(Response(200, {"content-length": "9"})))
    assert await b.request(post()) == "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"


async def test_empty_bytes_body_without_length_is_an_empty_chunked_body() -> None:
    b = Bridge(handler_returning(Response(200, (), b"")))
    out = await b.request(post())
    assert out == "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"


@pytest.mark.parametrize("body", [b"abc", "stream"])
async def test_shorter_than_content_length_resets(body: object) -> None:
    payload = b"abc" if body == b"abc" else body_of(b"ab", b"c")
    b = Bridge(handler_returning(Response(200, {"content-length": "10"}, payload)))
    out = await b.request(post())
    assert out == "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc"
    assert b.host.ends() == [
        ("RESET", (ErrorCode.INTERNAL_ERROR, "response body shorter than Content-Length"))
    ]


async def test_each_body_item_is_one_chunk_written_at_once() -> None:
    step = asyncio.Event()
    sent: list[bytes] = []

    async def events() -> AsyncIterator[bytes]:
        yield b"data: 1\n\n"
        await step.wait()
        yield b"data: 2\n\n"

    b = Bridge(handler_returning(Response(200, {"content-type": "text/event-stream"}, events())))
    b.feed(post())
    b.stream.handle_close()
    await settle()
    sent = [v for k, v in b.host.events if k == "DATA"]
    assert sent[-1] == b"9\r\ndata: 1\n\n\r\n"
    step.set()
    await b.done()
    assert b.text().endswith("9\r\ndata: 1\n\n\r\n9\r\ndata: 2\n\n\r\n0\r\n\r\n")


@pytest.mark.parametrize(
    ("method", "status"),
    [("HEAD", 200), ("GET", 204), ("GET", 304), ("HEAD", 404)],
)
async def test_bodyless_responses(method: str, status: int) -> None:
    res = Response(status, {"content-length": "5", "etag": "x"}, body_of(b"never"))
    b = Bridge(handler_returning(res))
    out = await b.request(post(method=method))
    reason = {200: "OK", 204: "No Content", 304: "Not Modified", 404: "Not Found"}[status]
    assert out == f"HTTP/1.1 {status} {reason}\r\netag: x\r\n\r\n"
    assert closed_cleanly(b)


async def test_bodyless_async_body_is_closed() -> None:
    closed: list[bool] = []

    class Body:
        def __aiter__(self) -> Body:
            return self

        async def __anext__(self) -> bytes:
            return b"x"

        async def aclose(self) -> None:
            closed.append(True)

    b = Bridge(handler_returning(Response(200, (), Body())))
    await b.request(post(method="HEAD"))
    assert closed == [True]


@pytest.mark.parametrize("status", [100, 101, 103, 199, 99, 1000])
async def test_1xx_and_out_of_range_statuses_are_never_written(status: int) -> None:
    b = Bridge(handler_returning(Response(status)))
    out = await b.request(post())
    assert out == reply("500 Internal Server Error", "handler error")
    assert len(b.errors) == 1 and isinstance(b.errors[0], ValueError)


async def test_set_cookie_each_on_its_own_line() -> None:
    res = Response(200, [("Set-Cookie", "a=1; Path=/"), ("Set-Cookie", "b=2")], None)
    b = Bridge(handler_returning(res))
    out = await b.request(post())
    assert out == (
        "HTTP/1.1 200 OK\r\nSet-Cookie: a=1; Path=/\r\nSet-Cookie: b=2\r\nContent-Length: 0\r\n\r\n"
    )


async def test_hop_by_hop_and_connection_listed_response_headers_are_dropped() -> None:
    res = Response(
        200,
        [
            ("Connection", "x-private"),
            ("x-private", "1"),
            ("Keep-Alive", "5"),
            ("Transfer-Encoding", "gzip"),
            ("Upgrade", "h2"),
            ("Trailer", "x"),
            ("TE", "x"),
            ("Proxy-Authenticate", "x"),
            ("Proxy-Connection", "x"),
            ("Content-Encoding", "gzip"),
            ("mcp-session-id", "abc"),
        ],
        b"",
    )
    b = Bridge(handler_returning(res))
    out = await b.request(post())
    assert out == (
        "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nmcp-session-id: abc\r\n"
        "Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
    )


async def test_custom_and_default_reason() -> None:
    b = Bridge(handler_returning(Response(299, reason="Fine")))
    assert (await b.request(post())).startswith("HTTP/1.1 299 Fine\r\n")
    b2 = Bridge(handler_returning(Response(299)))
    assert (await b2.request(post())).startswith("HTTP/1.1 299 Unknown\r\n")


async def test_response_recognised_by_shape() -> None:
    class ForeignHeaders:
        def items(self) -> list[tuple[str, str]]:
            return [("x-foreign", "yes")]

    class ForeignResponse:
        status = 202
        headers = ForeignHeaders()
        body = b"ok"
        reason = "Taken"

    b = Bridge(handler_returning(ForeignResponse()))
    out = await b.request(post())
    assert out == (
        "HTTP/1.1 202 Taken\r\nx-foreign: yes\r\nTransfer-Encoding: chunked\r\n\r\n"
        "2\r\nok\r\n0\r\n\r\n"
    )
    assert b.errors == []


async def test_plain_sync_function_returning_a_response_works() -> None:
    def handler(request: Request) -> Response:
        return Response(200, (), b"")

    b = Bridge(handler)  # type: ignore[arg-type]
    assert (await b.request(post())).startswith("HTTP/1.1 200 OK\r\n")


# --- failures --------------------------------------------------------------------------------


async def test_handler_raising_is_500_handler_error_and_reported() -> None:
    boom = RuntimeError("secret detail")

    async def handler(request: Request) -> Response:
        raise boom

    b = Bridge(handler)
    out = await b.request(post("{}"))
    assert out == reply("500 Internal Server Error", "handler error")
    assert "secret" not in out
    assert b.errors == [boom]
    assert closed_cleanly(b)


async def test_on_error_raising_is_contained(caplog: pytest.LogCaptureFixture) -> None:
    async def handler(request: Request) -> Response:
        raise RuntimeError("x")

    def bad_on_error(exc: BaseException) -> None:
        raise ValueError("callback bug")

    b = Bridge(handler, on_error=bad_on_error)
    assert await b.request(post()) == reply("500 Internal Server Error", "handler error")
    assert "on_error callback raised" in caplog.text


@pytest.mark.parametrize(
    "garbage",
    [
        None,
        "a string",
        42,
        {"status": 200, "headers": {}, "body": b""},
        Response(True, (), b""),
        Response(200, [("bad name", "x")], b""),
        Response(200, [("x", "a\r\nInjected: 1")], b""),
        Response(200, [("x", "€")], b""),
        Response(200, [("x", 1)], b""),  # type: ignore[list-item]
        Response(200, "nope", b""),  # type: ignore[arg-type]
        Response(200, (), "str body"),  # type: ignore[arg-type]
        Response(200, (), 123),  # type: ignore[arg-type]
        Response(200, (), b"", reason="bad\r\nreason"),
    ],
)
async def test_handler_returning_garbage_is_500(garbage: object) -> None:
    b = Bridge(handler_returning(garbage))
    out = await b.request(post())
    assert out == reply("500 Internal Server Error", "handler error")
    assert len(b.errors) == 1
    assert closed_cleanly(b)


async def test_internal_failure_before_any_byte_is_500_internal_error() -> None:
    class Exploding:
        status = 200
        body = b""

        @property
        def headers(self) -> object:
            raise RuntimeError("bug")

    b = Bridge(handler_returning(Exploding()))
    out = await b.request(post())
    assert out == reply("500 Internal Server Error", "internal error")
    assert len(b.errors) == 1


async def test_body_failure_resets_and_reports() -> None:
    boom = OSError("disk")

    async def failing() -> AsyncIterator[bytes]:
        yield b"partial"
        raise boom

    b = Bridge(handler_returning(Response(200, (), failing())))
    out = await b.request(post())
    assert out.startswith("HTTP/1.1 200 OK\r\n")
    assert b.host.ends() == [("RESET", (ErrorCode.INTERNAL_ERROR, "response body failed"))]
    assert b.errors == [boom]


async def test_body_yielding_non_bytes_resets() -> None:
    async def wrong() -> AsyncIterator[object]:
        yield "text"

    b = Bridge(handler_returning(Response(200, (), wrong())))  # type: ignore[arg-type]
    await b.request(post())
    assert b.host.ends() == [("RESET", (ErrorCode.INTERNAL_ERROR, "response body failed"))]
    assert isinstance(b.errors[0], TypeError)


async def test_error_reply_from_a_handler_is_byte_exact_also_to_head() -> None:
    b = Bridge(handler_returning(error_reply(504, "local server timed out")))
    out = await b.request(post(method="HEAD"))
    assert out == reply("504 Gateway Timeout", "local server timed out", head_only=True)
    b2 = Bridge(handler_returning(error_reply(502, "local server unreachable")))
    assert await b2.request(post()) == reply("502 Bad Gateway", "local server unreachable")


# --- edge cancellation -----------------------------------------------------------------------


async def test_edge_reset_cancels_the_handler() -> None:
    started = asyncio.Event()
    cancelled: list[bool] = []

    async def handler(request: Request) -> Response:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        raise AssertionError("unreachable")

    b = Bridge(handler)
    b.feed(post("{}"))
    b.stream.handle_close()
    await started.wait()
    b.stream.handle_reset(ErrorCode.CANCEL, "client went away")
    await b.done()
    assert cancelled == [True]
    assert b.host.written() == b""
    assert b.errors == []
    reason = await b.stream.wait_closed()
    assert isinstance(reason, Reset) and reason.by_peer


async def test_edge_reset_closes_a_streaming_body() -> None:
    closed: list[bool] = []

    async def endless() -> AsyncIterator[bytes]:
        try:
            yield b"first"
            await asyncio.Event().wait()
            yield b"never"
        finally:
            closed.append(True)

    b = Bridge(handler_returning(Response(200, (), endless())))
    b.feed(post())
    b.stream.handle_close()
    await settle()
    assert b"first" in b.host.written()
    b.stream.handle_reset(ErrorCode.CANCEL, "")
    await b.done()
    assert closed == [True]
    assert b.errors == []


async def test_connection_loss_cancels_the_handler() -> None:
    from ws_mixer.errors import ConnError

    cancelled: list[bool] = []

    async def handler(request: Request) -> Response:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        raise AssertionError("unreachable")

    b = Bridge(handler)
    b.feed(post())
    b.stream.handle_close()
    await settle()
    b.stream.terminate(ConnError(ErrorCode.GOING_AWAY, "gone"))
    await b.done()
    assert cancelled == [True]


async def test_cancelling_serve_stream_cancels_the_handler() -> None:
    cancelled: list[bool] = []

    async def handler(request: Request) -> Response:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        raise AssertionError("unreachable")

    b = Bridge(handler)
    b.feed(post())
    b.stream.handle_close()
    await settle()
    b.task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await b.task
    await settle()
    assert cancelled == [True]


async def test_cancelling_serve_stream_waits_for_the_handler_cleanup() -> None:
    cleaned: list[str] = []

    async def handler(request: Request) -> Response:
        try:
            await asyncio.Event().wait()
        finally:
            # Cleanup that itself awaits (e.g. aborting an upstream) must finish first.
            for _ in range(3):
                await asyncio.sleep(0)
            cleaned.append("done")
        raise AssertionError("unreachable")

    b = Bridge(handler)
    b.feed(post())
    b.stream.handle_close()
    await settle()
    b.task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await b.task
    assert cleaned == ["done"]
    assert b.host.ends() == []  # the stream is left to the caller


async def test_cancelling_serve_stream_cancels_the_head_deadline() -> None:
    b = Bridge()
    b.feed("POST /mcp HTTP/1.1\r\n")
    await settle()
    assert b.clock.pending() == 1
    b.task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await b.task
    assert b.clock.pending() == 0


@pytest.mark.parametrize("raise_on_call", [1, 2])
async def test_availability_raising_is_500_internal_error(raise_on_call: int) -> None:
    calls = 0

    def availability() -> Availability:
        nonlocal calls
        calls += 1
        if calls == raise_on_call:
            raise RuntimeError("state bug")
        return "available"

    host = Host()
    stream = MixerStream(1, host, BIG, BIG)
    errors: list[BaseException] = []
    task = asyncio.create_task(
        serve_stream(
            stream,
            handler=handler_returning(Response(200)),
            availability=availability,
            max_request_body_bytes=1024,
            clock=FakeClock(),
            on_error=errors.append,
        )
    )
    stream.handle_data(post("{}").encode())
    await settle()
    assert host.written().decode() == reply("500 Internal Server Error", "internal error")
    assert host.ends() == []  # draining the unread body first
    stream.handle_close()
    await asyncio.wait_for(task, 5)
    assert host.ends() == [("CLOSE", None)]
    assert len(errors) == 1 and str(errors[0]) == "state bug"


async def test_head_in_one_byte_frames() -> None:
    b = Bridge()
    raw = ("POST /mcp HTTP/1.1\r\n" + HOST + "x-pad: " + "a" * 70_000).encode()
    for i in range(len(raw)):
        b.stream.handle_data(raw[i : i + 1])
    b.stream.handle_close()
    await b.done()
    assert b.text() == reply("431 Request Header Fields Too Large", "request head too large")


async def test_no_timers_left_behind() -> None:
    for req in (post("{}"), "GET /x HTTP/1.1\r\n" + HOST + "\r\n", "bad\r\n\r\n"):
        b = Bridge()
        await b.request(req)
        assert b.clock.pending() == 0


# --- handler_timeout_ms (SDK extension, docs/DEVIATIONS.md) ---------------------------------

TIMED_OUT = reply("504 Gateway Timeout", "local server timed out")


def stuck_handler(log: list[str]) -> Handler:
    async def handler(request: Request) -> Response:
        log.append("called")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            log.append("cancelled")
            raise
        raise AssertionError("unreachable")

    return handler


async def test_handler_timeout_answers_504_at_exactly_the_deadline() -> None:
    log: list[str] = []
    b = Bridge(stuck_handler(log), handler_timeout_ms=25_000)
    b.feed(post("{}"))
    b.stream.handle_close()
    await settle()
    assert log == ["called"]
    assert b.clock.pending() == 1
    await b.clock.advance(24_999)
    assert b.host.written() == b"" and b.host.ends() == []
    await b.clock.advance(1)
    await b.done()
    assert b.text() == TIMED_OUT
    assert closed_cleanly(b)
    assert log == ["called", "cancelled"]
    assert b.errors == []
    assert b.clock.pending() == 0


async def test_handler_timeout_reply_to_head_is_head_only() -> None:
    b = Bridge(stuck_handler([]), handler_timeout_ms=100)
    b.feed(post(method="HEAD"))
    b.stream.handle_close()
    await settle()
    await b.clock.advance(100)
    await b.done()
    assert b.text() == reply("504 Gateway Timeout", "local server timed out", head_only=True)
    assert closed_cleanly(b)
    assert b.clock.pending() == 0


async def test_handler_answering_just_before_the_deadline_is_served() -> None:
    go = asyncio.Event()

    async def handler(request: Request) -> Response:
        await go.wait()
        return Response(200, [], b"ok")

    b = Bridge(handler, handler_timeout_ms=1_000)
    b.feed(post("{}"))
    b.stream.handle_close()
    await settle()
    await b.clock.advance(999)
    go.set()
    await b.done()
    assert b.text() == "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n"
    assert closed_cleanly(b)
    assert b.clock.pending() == 0


async def test_handler_timeout_does_not_cut_a_streaming_body() -> None:
    more = asyncio.Event()

    async def slow_body() -> AsyncIterator[bytes]:
        yield b"first"
        await more.wait()
        yield b"second"

    b = Bridge(handler_returning(Response(200, (), slow_body())), handler_timeout_ms=100)
    b.feed(post())
    b.stream.handle_close()
    await settle()
    assert b.clock.pending() == 0  # cancelled once the response was returned
    await b.clock.advance(10_000)
    assert b"first" in b.host.written() and b.host.ends() == []
    more.set()
    await b.done()
    assert b.text() == (
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        "5\r\nfirst\r\n6\r\nsecond\r\n0\r\n\r\n"
    )
    assert closed_cleanly(b)


async def test_handler_raising_before_the_deadline_is_still_500() -> None:
    async def handler(request: Request) -> Response:
        raise RuntimeError("boom")

    b = Bridge(handler, handler_timeout_ms=1_000)
    assert await b.request(post()) == reply("500 Internal Server Error", "handler error")
    assert [str(e) for e in b.errors] == ["boom"]
    assert b.clock.pending() == 0


async def test_handler_timeout_leaves_a_sync_result_alone() -> None:
    def handler(request: Request) -> Response:
        return Response(204)

    b = Bridge(handler, handler_timeout_ms=1)  # type: ignore[arg-type]
    assert await b.request(post()) == "HTTP/1.1 204 No Content\r\n\r\n"
    assert b.clock.pending() == 0


async def test_handler_swallowing_the_timeout_cancel_has_its_answer_dropped() -> None:
    closed: list[bool] = []

    async def body() -> AsyncIterator[bytes]:
        try:
            yield b"late"
        finally:
            closed.append(True)

    class Body:
        """An async iterable with aclose(), so the dropped answer can be released."""

        def __init__(self) -> None:
            self.it = body()

        def __aiter__(self) -> AsyncIterator[bytes]:
            return self.it

        async def aclose(self) -> None:
            closed.append(True)

    async def handler(request: Request) -> Response:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        return Response(200, (), Body())

    b = Bridge(handler, handler_timeout_ms=50)
    b.feed(post())
    b.stream.handle_close()
    await settle()
    await b.clock.advance(50)
    await b.done()
    assert b.text() == TIMED_OUT
    assert closed == [True]
    assert b.clock.pending() == 0


async def test_handler_timeout_edge_reset_cancels_the_handler_and_the_timer() -> None:
    log: list[str] = []
    b = Bridge(stuck_handler(log), handler_timeout_ms=1_000)
    b.feed(post())
    b.stream.handle_close()
    await settle()
    b.stream.handle_reset(ErrorCode.CANCEL, "client went away")
    await b.done()
    assert log == ["called", "cancelled"]
    assert b.host.written() == b""
    assert b.clock.pending() == 0
    await b.clock.advance(1_000)
    assert b.host.written() == b""


async def test_handler_timeout_cancelling_serve_stream_cancels_the_handler_and_the_timer() -> None:
    log: list[str] = []
    b = Bridge(stuck_handler(log), handler_timeout_ms=1_000)
    b.feed(post())
    b.stream.handle_close()
    await settle()
    b.task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await b.task
    assert log == ["called", "cancelled"]
    assert b.clock.pending() == 0
    assert b.host.ends() == []  # the stream is left to the caller


async def test_handler_timeout_after_the_edge_reset_its_side_writes_nothing() -> None:
    """The edge RESETs just as the deadline passes: whichever wins, one end, no crash."""
    b = Bridge(stuck_handler([]), handler_timeout_ms=100)
    b.feed(post())
    b.stream.handle_close()
    await settle()
    b.clock.call_later(100, lambda: b.stream.handle_reset(ErrorCode.CANCEL, "gone"))
    await b.clock.advance(100)
    await b.done()
    assert b.errors == []
    assert b.clock.pending() == 0
    assert len(b.host.ends()) <= 1
    assert b.text() in ("", TIMED_OUT)
