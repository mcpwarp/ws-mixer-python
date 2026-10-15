from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest
import pytest_asyncio
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response

from ws_mixer.adapters.websockets import websockets_connector
from ws_mixer.socket import UnexpectedSubprotocol, UpgradeRejected, WebSocketClosed, WebSocketInit

PROTO = "ws-mixer.v1"
INIT = WebSocketInit(headers={"Authorization": "Bearer t", "User-Agent": "ua/1"})

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def no_leaked_tasks() -> AsyncIterator[None]:
    before = asyncio.all_tasks()
    yield
    for _ in range(5):
        await asyncio.sleep(0)
    leaked = asyncio.all_tasks() - before - {asyncio.current_task()}
    assert not leaked, leaked


@asynccontextmanager
async def ws_server(
    handler: Callable[[ServerConnection], Awaitable[None]], **kwargs: Any
) -> AsyncIterator[str]:
    kwargs.setdefault("ping_interval", None)
    server = await serve(handler, "127.0.0.1", 0, **kwargs)
    try:
        yield f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1/tunnel"
    finally:
        server.close()
        await server.wait_closed()


async def test_happy_path() -> None:
    seen: dict[str, Any] = {}

    async def echo(ws: ServerConnection) -> None:
        assert ws.request is not None
        seen["ua"] = ws.request.headers.get_all("User-Agent")
        seen["auth"] = ws.request.headers["Authorization"]
        seen["ext"] = ws.request.headers.get("Sec-WebSocket-Extensions")
        async for msg in ws:
            await ws.send(msg)

    async with ws_server(echo, subprotocols=[PROTO]) as url:
        conn = await websockets_connector()(url, INIT)
        try:
            assert conn.subprotocol == PROTO
            await conn.send(b"hi")
            assert await conn.recv() == b"hi"
        finally:
            await conn.close()
    assert seen == {"ua": ["ua/1"], "auth": "Bearer t", "ext": None}


async def test_text_is_str_and_no_echo_is_none() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.send("txt")
        await ws.wait_closed()

    async with ws_server(handler) as url:
        conn = await websockets_connector()(url, INIT)
        try:
            assert conn.subprotocol is None
            assert await conn.recv() == "txt"
        finally:
            await conn.close()


@pytest.mark.parametrize(
    ("status", "extra"),
    [(401, {}), (403, {}), (429, {"Retry-After": "4"}), (302, {"Location": "ws://x/"})],
)
async def test_rejection_maps_to_upgrade_rejected(status: int, extra: dict[str, str]) -> None:
    def reject(conn: ServerConnection, _r: Request) -> Response:
        resp = conn.respond(status, "no\n")
        for k, v in extra.items():
            resp.headers[k] = v
        return resp

    async with ws_server(lambda ws: ws.wait_closed(), process_request=reject) as url:
        with pytest.raises(UpgradeRejected) as ei:
            await websockets_connector()(url, INIT)
    assert ei.value.status == status
    for k, v in extra.items():
        assert ei.value.headers[k.lower()] == v


@pytest.mark.parametrize(
    ("values", "reported"),
    [(["evil.v9"], "evil.v9"), ([PROTO, "evil.v9"], f"{PROTO}, evil.v9")],
)
async def test_unoffered_subprotocol(values: list[str], reported: str) -> None:
    def rewrite(_c: ServerConnection, _r: Request, response: Response) -> Response:
        del response.headers["Sec-WebSocket-Protocol"]
        for v in values:
            response.headers["Sec-WebSocket-Protocol"] = v
        return response

    async with ws_server(
        lambda ws: ws.wait_closed(), subprotocols=[PROTO], process_response=rewrite
    ) as url:
        with pytest.raises(UnexpectedSubprotocol) as ei:
            await websockets_connector()(url, INIT)
    assert not isinstance(ei.value, OSError)
    assert ei.value.subprotocol == reported


async def test_proxy_refusal_is_upgrade_rejected() -> None:
    handlers: list[asyncio.Task[Any]] = []

    async def proxy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        handlers.append(asyncio.current_task())  # type: ignore[arg-type]
        await reader.readuntil(b"\r\n\r\n")
        writer.write(
            b"HTTP/1.1 407 Proxy Authentication Required\r\n"
            b"Proxy-Authenticate: Basic\r\nContent-Length: 0\r\n\r\n"
        )
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(proxy, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        with pytest.raises(UpgradeRejected) as ei:
            await websockets_connector(proxy=f"http://127.0.0.1:{port}")("ws://edge.test/", INIT)
        assert ei.value.status == 407
        assert ei.value.headers["proxy-authenticate"] == "Basic"
    finally:
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers)


async def test_invalid_proxy_is_a_value_error() -> None:
    with pytest.raises(ValueError):
        await websockets_connector(proxy="ftp://nope")("ws://edge.test/", INIT)


async def test_close_args_are_validated() -> None:
    async with ws_server(lambda ws: ws.wait_closed()) as url:
        conn = await websockets_connector()(url, INIT)
        try:
            with pytest.raises(ValueError):
                await conn.close(1006)
        finally:
            await conn.close()


async def test_refuses_websockets_without_process_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from websockets.asyncio.client import connect

    monkeypatch.delattr(connect, "process_redirect")
    with pytest.raises(RuntimeError, match="process_redirect"):
        websockets_connector()


async def test_close_mapping() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.close(4012, "drained")

    async with ws_server(handler) as url:
        conn = await websockets_connector()(url, INIT)
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.reason, ei.value.received) == (4012, "drained", True)


async def test_drop_maps_to_1006_and_abort_is_abnormal() -> None:
    observed: dict[str, Any] = {}

    async def handler(ws: ServerConnection) -> None:
        try:
            await ws.recv()
        except ConnectionClosed as e:
            observed["rcvd"] = e.rcvd

    async with ws_server(handler) as url:
        conn = await websockets_connector()(url, INIT)
        conn.abort()
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.received) == (1006, False)
    assert observed["rcvd"] is None


async def test_client_close_reaches_server() -> None:
    observed: dict[str, Any] = {}

    async def handler(ws: ServerConnection) -> None:
        await ws.wait_closed()
        observed["close"] = (ws.close_code, ws.close_reason)

    async with ws_server(handler) as url:
        conn = await websockets_connector()(url, INIT)
        await conn.close(4014, "x")
    assert observed["close"] == (4014, "x")


async def test_oversized_message_is_1009() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.send(b"\x00" * 65_545)
        await ws.wait_closed()

    async with ws_server(handler) as url:
        conn = await websockets_connector()(url, INIT)
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert ei.value.code == 1009


async def test_pinned_kwargs_are_refused() -> None:
    with pytest.raises(TypeError):
        websockets_connector(compression="deflate")
