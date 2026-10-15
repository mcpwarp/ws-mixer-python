"""Headers, Request/Response and sync_handler (mcpwarp/http.py)."""

from __future__ import annotations

import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from ws_mixer.mcpwarp import Headers, Request, Response, sync_handler
from ws_mixer.mcpwarp.http import ErrorReply, error_reply


def test_headers_are_ordered_case_insensitive_multi_valued() -> None:
    h = Headers([("Set-Cookie", "a=1"), ("content-type", "text/plain"), ("set-cookie", "b=2")])
    assert h.get("SET-COOKIE") == "a=1, b=2"
    assert h.get_all("set-cookie") == ["a=1", "b=2"]
    assert h.get("missing") is None
    assert h.get("missing", "d") == "d"
    assert "Content-Type" in h and "nope" not in h and 3 not in h
    assert list(h) == ["Set-Cookie", "content-type", "set-cookie"]
    assert len(h) == 3
    assert h.items() == [
        ("Set-Cookie", "a=1"),
        ("content-type", "text/plain"),
        ("set-cookie", "b=2"),
    ]


def test_headers_from_mapping_headers_and_items_objects() -> None:
    assert Headers({"A": "1", "b": "2"}).items() == [("A", "1"), ("b", "2")]
    h = Headers([("X", "1")])
    assert Headers(h) == h

    class Foreign:
        def items(self) -> list[tuple[str, str]]:
            return [("x", "1")]

    assert Headers(Foreign()) == h  # type: ignore[arg-type]
    assert hash(Headers([("x", "1")])) == hash(h)


def test_headers_validation_and_immutability() -> None:
    with pytest.raises(TypeError):
        Headers([("a", 1)])  # type: ignore[list-item]
    with pytest.raises(TypeError):
        Headers(["ab"])  # type: ignore[list-item]
    with pytest.raises(TypeError):
        Headers("ab")  # type: ignore[arg-type]
    h = Headers()
    with pytest.raises(AttributeError):
        h.extra = 1


def test_response_defaults() -> None:
    r = Response(204)
    assert (r.status, r.headers, r.body, r.reason) == (204, (), None, None)


def test_error_reply_is_a_plain_response_too() -> None:
    r = error_reply(504, "local server timed out")
    assert isinstance(r, ErrorReply)
    assert r.status == 504
    assert isinstance(r.body, bytes)
    assert json.loads(r.body) == {"error": "local server timed out"}
    assert Headers(r.headers).get("content-length") == str(len(r.body))
    assert r.mcpwarp_error_message == "local server timed out"


def _request() -> Request:
    return Request(
        method="POST",
        url="https://h.example/mcp",
        target="/mcp",
        path="/mcp",
        query="",
        headers=Headers([("host", "h.example")]),
        body=b"x",
        http_version="1.1",
    )


@pytest.mark.asyncio
async def test_sync_handler_runs_in_a_thread() -> None:
    main = threading.get_ident()
    seen: list[int] = []

    def fn(req: Request) -> Response:
        seen.append(threading.get_ident())
        return Response(200, body=req.body)

    res = await sync_handler(fn)(_request())
    assert res.body == b"x"
    assert seen and seen[0] != main


@pytest.mark.asyncio
async def test_sync_handler_uses_the_given_executor() -> None:
    names: list[str] = []

    def fn(req: Request) -> Response:
        names.append(threading.current_thread().name)
        return Response(200)

    with ThreadPoolExecutor(1, thread_name_prefix="bridge-test") as pool:
        await sync_handler(fn, executor=pool)(_request())
    assert names[0].startswith("bridge-test")


@pytest.mark.asyncio
async def test_sync_handler_propagates_exceptions() -> None:
    def fn(req: Request) -> Response:
        raise KeyError("boom")

    with pytest.raises(KeyError):
        await sync_handler(fn)(_request())


@pytest.mark.asyncio
async def test_sync_handler_cancel_does_not_hang() -> None:
    release = threading.Event()

    def fn(req: Request) -> Response:
        release.wait(5)
        return Response(200)

    task = asyncio.ensure_future(sync_handler(fn)(_request()))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
