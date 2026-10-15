"""target_handler (MCPWARP.md sections 4.5, 4.6) against scripted local servers: what is
forwarded, 502 with the errno name, 504 on the fake clock, pass-through of redirects and
content-encoding, upstream bodies, and cancellation."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterable
from typing import Any

import pytest

from tests.unit.mcpwarp.test_serve import Bridge, post
from ws_mixer.errors import ErrorCode
from ws_mixer.mcpwarp import Headers, Request, target_handler
from ws_mixer.mcpwarp.http import ErrorReply
from ws_mixer.testing import FakeClock, settle

pytestmark = pytest.mark.asyncio


class Upstream:
    """A local server that records each request and answers with `answer` (None: never)."""

    def __init__(self, answer: bytes | None = None, *, close_after: bool = False) -> None:
        self.answer = answer
        self.close_after = close_after
        self.requests: list[bytes] = []
        self.peer_closed = asyncio.Event()
        self.port = 0
        self._server: asyncio.Server | None = None

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def __aenter__(self) -> Upstream:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            length = 0
            for line in head.decode("latin-1").split("\r\n"):
                name, _, value = line.partition(":")
                if name.lower() == "content-length":
                    length = int(value)
            self.requests.append(head + await reader.readexactly(length))
            if self.answer is not None:
                writer.write(self.answer)
                await writer.drain()
                if self.close_after:
                    return
            while await reader.read(1024):
                pass
            self.peer_closed.set()
        except (asyncio.IncompleteReadError, ConnectionError):
            self.peer_closed.set()
        finally:
            writer.transport.abort()

    def head_lines(self, i: int = 0) -> list[str]:
        return self.requests[i].split(b"\r\n\r\n", 1)[0].decode("latin-1").split("\r\n")


def request(
    method: str = "POST",
    query: str = "",
    headers: list[tuple[str, str]] | None = None,
    body: bytes = b"",
) -> Request:
    target = "/mcp" + ("?" + query if query else "")
    return Request(
        method=method,
        url="https://notes-tester.mcpwarp.test" + target,
        target=target,
        path="/mcp",
        query=query,
        headers=Headers(
            headers if headers is not None else [("host", "notes-tester.mcpwarp.test")]
        ),
        body=body if method not in ("GET", "HEAD") else b"",
        http_version="1.1",
    )


async def collect(body: object) -> bytes:
    assert isinstance(body, AsyncIterable)
    return b"".join([chunk async for chunk in body])


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
    return port


OK = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\nok"


async def test_forwards_the_request() -> None:
    async with Upstream(OK) as up:
        handler = target_handler(up.origin + "/base?a=1")
        res = await handler(
            request(
                query="x=2&y=%41",
                headers=[
                    ("host", "notes-tester.mcpwarp.test"),
                    ("Accept-Encoding", "gzip"),
                    ("accept", "application/json"),
                    ("X-Forwarded-Host", "spoofed"),
                    ("mcp-session-id", "abc"),
                    ("x-dup", "1"),
                    ("x-dup", "2"),
                ],
                body=b"{}",
            )
        )
        assert res.status == 200
        assert await collect(res.body) == b"ok"
        assert up.head_lines() == [
            "POST /base?a=1&x=2&y=%41 HTTP/1.1",
            f"Host: 127.0.0.1:{up.port}",
            "accept: application/json",
            "mcp-session-id: abc",
            "x-dup: 1",
            "x-dup: 2",
            "accept-encoding: identity",
            "x-forwarded-host: notes-tester.mcpwarp.test",
            "x-forwarded-proto: https",
            "Connection: close",
            "Content-Length: 2",
        ]
        assert up.requests[0].endswith(b"\r\n\r\n{}")
        assert Headers(res.headers).get("content-type") == "application/json"
        assert Headers(res.headers).get("content-length") == "2"


@pytest.mark.parametrize(
    ("target_query", "incoming", "line"),
    [("", "", "/mcp"), ("a=1", "", "/mcp?a=1"), ("", "x=1", "/mcp?x=1")],
)
async def test_query_merge(target_query: str, incoming: str, line: str) -> None:
    async with Upstream(OK) as up:
        target = up.origin + "/mcp" + ("?" + target_query if target_query else "")
        res = await target_handler(target)(request("GET", query=incoming))
        await collect(res.body)
        assert up.head_lines()[0] == f"GET {line} HTTP/1.1"
        assert not any(h.lower().startswith("content-length") for h in up.head_lines())


async def test_502_with_errno_name() -> None:
    res = await target_handler(f"http://127.0.0.1:{free_port()}/mcp")(request())
    assert isinstance(res, ErrorReply)
    assert res.status == 502
    assert res.mcpwarp_error_message == "local server unreachable (ECONNREFUSED)"


async def test_502_dns_failure_is_enotfound() -> None:
    res = await target_handler("http://does-not-exist.invalid/mcp")(request())
    assert isinstance(res, ErrorReply)
    assert res.mcpwarp_error_message == "local server unreachable (ENOTFOUND)"


@pytest.mark.parametrize(
    "answer",
    [b"garbage\r\n\r\n", b"HTTP/1.1 200 OK\r\n", b"HTTP/1.1 101 Switching Protocols\r\n\r\n"],
)
async def test_502_without_code_when_the_answer_is_unusable(answer: bytes) -> None:
    async with Upstream(answer, close_after=True) as up:
        res = await target_handler(up.origin + "/mcp")(request())
        assert isinstance(res, ErrorReply)
        assert (res.status, res.mcpwarp_error_message) == (502, "local server unreachable")


async def test_504_on_the_fake_clock_aborts_the_upstream() -> None:
    clock = FakeClock()
    async with Upstream(None) as up:
        task = asyncio.ensure_future(
            target_handler(up.origin + "/mcp", timeout_ms=1000, clock=clock)(request())
        )
        while not up.requests:
            await asyncio.sleep(0.002)
        await clock.advance(999)
        assert not task.done()
        await clock.advance(1)
        res = await asyncio.wait_for(task, 5)
        assert isinstance(res, ErrorReply)
        assert (res.status, res.mcpwarp_error_message) == (504, "local server timed out")
        await asyncio.wait_for(up.peer_closed.wait(), 5)
        assert clock.pending() == 0


async def test_504_through_the_bridge_head_only_for_head() -> None:
    async with Upstream(None) as up:
        clock = FakeClock()
        b = Bridge(target_handler(up.origin + "/mcp", timeout_ms=50, clock=clock))
        b.feed(post(method="HEAD"))
        b.stream.handle_close()
        while not up.requests:
            await asyncio.sleep(0.002)
        await clock.advance(50)
        await b.done()
        assert b.text() == (
            "HTTP/1.1 504 Gateway Timeout\r\nContent-Type: application/json\r\n"
            "Content-Length: 34\r\n\r\n"
        )


async def test_body_may_stream_past_the_timeout() -> None:
    clock = FakeClock()
    answer = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nab"
    async with Upstream(answer) as up:
        res = await target_handler(up.origin + "/mcp", timeout_ms=10, clock=clock)(request())
        body = res.body
        assert isinstance(body, AsyncIterable)
        it = body.__aiter__()
        assert await it.__anext__() == b"ab"
        await clock.advance(1000)
        assert clock.pending() == 0
        await it.aclose()  # type: ignore[attr-defined]


async def test_edge_cancel_aborts_the_upstream() -> None:
    async with Upstream(None) as up:
        task = asyncio.ensure_future(target_handler(up.origin + "/mcp")(request()))
        while not up.requests:
            await asyncio.sleep(0.002)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(up.peer_closed.wait(), 5)


async def test_cancel_as_the_attempt_completes_aborts_its_connection() -> None:
    """The edge cancels in the same loop iteration as the connect attempt finishes: the
    handler sees CancelledError although the attempt holds an open connection, which must be
    aborted rather than left for the garbage collector (an unclosed-transport ResourceWarning)."""
    async with Upstream(OK) as up:
        task = asyncio.ensure_future(target_handler(up.origin + "/mcp")(request()))
        await asyncio.sleep(0)
        (attempt,) = [
            t for t in asyncio.all_tasks() if getattr(t.get_coro(), "__name__", "") == "_exchange"
        ]
        attempt.add_done_callback(lambda _: task.cancel())
        with pytest.raises(asyncio.CancelledError):
            await task
        assert attempt.done() and not attempt.cancelled() and attempt.exception() is None
        await asyncio.wait_for(up.peer_closed.wait(), 5)
        assert attempt.result()[1].transport.is_closing()


async def test_redirects_are_passed_through_not_followed() -> None:
    answer = b"HTTP/1.1 302 Found\r\nLocation: /elsewhere\r\nContent-Length: 0\r\n\r\n"
    async with Upstream(answer) as up:
        res = await target_handler(up.origin + "/mcp")(request("GET"))
        assert res.status == 302
        assert Headers(res.headers).get("location") == "/elsewhere"
        assert await collect(res.body) == b""
        assert len(up.requests) == 1


async def test_content_encoding_passes_through_untouched() -> None:
    gz = bytes(range(0x1F, 0x1F + 20))
    answer = b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 20\r\n\r\n" + gz
    async with Upstream(answer) as up:
        b = Bridge(target_handler(up.origin + "/mcp"))
        out = await b.request(post("{}"))
        assert out.encode("latin-1") == (
            b"HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\nContent-Length: 20\r\n\r\n" + gz
        )


async def test_chunked_upstream_is_rechunked_to_the_edge() -> None:
    answer = (
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: text/event-stream\r\n"
        b"\r\n4;x=y\r\nabcd\r\n2\r\nef\r\n0\r\nt: 1\r\n\r\n"
    )
    async with Upstream(answer, close_after=True) as up:
        b = Bridge(target_handler(up.origin + "/mcp"))
        out = await b.request(post("{}"))
        head, _, body = out.partition("\r\n\r\n")
        assert head == (
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nTransfer-Encoding: chunked"
        )
        # One edge chunk per upstream read; the decoded bytes are what matter.
        assert body in ("4\r\nabcd\r\n2\r\nef\r\n0\r\n\r\n", "6\r\nabcdef\r\n0\r\n\r\n")


async def test_upstream_short_of_content_length_resets_the_stream() -> None:
    answer = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc"
    async with Upstream(answer, close_after=True) as up:
        b = Bridge(target_handler(up.origin + "/mcp"))
        out = await b.request(post("{}"))
        assert out == "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc"
        assert b.host.ends() == [
            ("RESET", (ErrorCode.INTERNAL_ERROR, "response body shorter than Content-Length"))
        ]


async def test_upstream_dying_inside_a_chunked_body_is_a_body_failure() -> None:
    answer = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nab"
    async with Upstream(answer, close_after=True) as up:
        b = Bridge(target_handler(up.origin + "/mcp"))
        await b.request(post("{}"))
        assert b.host.ends() == [("RESET", (ErrorCode.INTERNAL_ERROR, "response body failed"))]
        assert len(b.errors) == 1


async def test_unframed_upstream_body_reads_to_eof() -> None:
    answer = b"HTTP/1.1 200 OK\r\n\r\nuntil close"
    async with Upstream(answer, close_after=True) as up:
        res = await target_handler(up.origin + "/mcp")(request())
        assert await collect(res.body) == b"until close"


async def test_interim_1xx_are_skipped() -> None:
    answer = b"HTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\n" + OK
    async with Upstream(answer) as up:
        res = await target_handler(up.origin + "/mcp")(request())
        assert res.status == 200
        assert await collect(res.body) == b"ok"


async def test_head_response_has_no_body_and_closes() -> None:
    answer = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n"
    async with Upstream(answer) as up:
        res = await target_handler(up.origin + "/mcp")(request("HEAD"))
        assert res.status == 200 and res.body is None
        await asyncio.wait_for(up.peer_closed.wait(), 5)


async def test_closing_the_body_early_aborts_the_upstream() -> None:
    answer = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nab"
    async with Upstream(answer) as up:
        res = await target_handler(up.origin + "/mcp")(request())
        assert isinstance(res.body, AsyncIterable)
        it: Any = res.body.__aiter__()
        assert await it.__anext__() == b"ab"
        await it.aclose()
        await asyncio.wait_for(up.peer_closed.wait(), 5)


async def test_unforwardable_header_is_a_handler_error() -> None:
    async with Upstream(OK) as up:
        handler = target_handler(up.origin + "/mcp")
        with pytest.raises(ValueError):
            await handler(request(headers=[("host", "h"), ("x", "a\r\nInjected: 1")]))
        await settle()
        assert up.requests == []


@pytest.mark.parametrize(
    "target", ["ftp://127.0.0.1/x", "/relative", "http://", "http://h:99999/", "localhost:3000"]
)
async def test_invalid_target(target: str) -> None:
    with pytest.raises(ValueError):
        target_handler(target)


@pytest.mark.parametrize("timeout_ms", [0, -1, float("nan"), float("inf"), True, "5"])
async def test_invalid_timeout(timeout_ms: object) -> None:
    with pytest.raises(ValueError):
        target_handler("http://127.0.0.1:3000/mcp", timeout_ms=timeout_ms)  # type: ignore[arg-type]
