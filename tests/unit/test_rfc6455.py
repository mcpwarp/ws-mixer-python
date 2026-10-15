"""StdlibConnector against a real `websockets` server, plus a raw asyncio server for the
malformed cases `websockets` will not produce."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import shutil
import socket
import ssl
import struct
import subprocess
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response
from websockets.typing import Subprotocol

from ws_mixer._rfc6455 import StdlibConnector, apply_mask
from ws_mixer.clock import LoopClock
from ws_mixer.socket import (
    InvalidHandshake,
    UnexpectedSubprotocol,
    UpgradeRejected,
    WebSocketClosed,
    WebSocketInit,
)

PROTO = "ws-mixer.v1"
INIT = WebSocketInit(protocols=(PROTO,), headers={"Authorization": "Bearer t0k"})
GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

Handler = Callable[[ServerConnection], Awaitable[None]]

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
async def ws_server(handler: Handler, **kwargs: Any) -> AsyncIterator[str]:
    kwargs.setdefault("ping_interval", None)
    server = await serve(handler, "127.0.0.1", 0, **kwargs)
    try:
        port = server.sockets[0].getsockname()[1]
        yield f"ws://127.0.0.1:{port}/v1/tunnel"
    finally:
        server.close()
        await server.wait_closed()


def connector(**kwargs: Any) -> StdlibConnector:
    kwargs.setdefault("close_timeout_ms", 500)
    return StdlibConnector(**kwargs)


async def idle(ws: ServerConnection) -> None:
    await ws.wait_closed()


# --- handshake -----------------------------------------------------------------------------


async def test_happy_path_echoes_subprotocol_and_sends_headers() -> None:
    seen: dict[str, Any] = {}

    async def handler(ws: ServerConnection) -> None:
        assert ws.request is not None
        seen["headers"] = ws.request.headers
        seen["path"] = ws.request.path
        await ws.send(await ws.recv())

    async with ws_server(handler, subprotocols=[PROTO]) as url:
        conn = await connector()(url + "?x=1", INIT)
        try:
            assert conn.subprotocol == PROTO
            await conn.send(b"ping")
            assert await conn.recv() == b"ping"
        finally:
            await conn.close()
    h = seen["headers"]
    assert seen["path"] == "/v1/tunnel?x=1"
    assert h["Authorization"] == "Bearer t0k"
    assert h["Sec-WebSocket-Protocol"] == PROTO
    assert h["Sec-WebSocket-Version"] == "13"
    assert "Origin" not in h
    assert "Sec-WebSocket-Extensions" not in h


async def test_no_echo_gives_none_subprotocol() -> None:
    async with ws_server(idle) as url:
        conn = await connector()(url, INIT)
        try:
            assert conn.subprotocol is None
        finally:
            await conn.close()


@pytest.mark.parametrize(
    ("values", "reported"),
    [(["evil.v9"], "evil.v9"), ([PROTO, "evil.v9"], f"{PROTO}, evil.v9"), ([""], "")],
)
async def test_unoffered_subprotocol_echo_fails(values: list[str], reported: str) -> None:
    def rewrite(_c: ServerConnection, _r: Request, response: Response) -> Response:
        del response.headers["Sec-WebSocket-Protocol"]
        for v in values:
            response.headers["Sec-WebSocket-Protocol"] = v
        return response

    async with ws_server(idle, subprotocols=[PROTO], process_response=rewrite) as url:
        with pytest.raises(UnexpectedSubprotocol) as ei:
            await connector()(url, INIT)
    # Fatal UNSUPPORTED, so it must not look like a retryable network error.
    assert not isinstance(ei.value, OSError)
    assert not isinstance(ei.value, InvalidHandshake)
    assert ei.value.subprotocol == reported


@pytest.mark.parametrize(
    ("status", "extra"),
    [
        (401, {}),
        (403, {}),
        (404, {}),
        (429, {"Retry-After": "7"}),
        (302, {"Location": "ws://127.0.0.1:1/elsewhere"}),
        (503, {"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}),
    ],
)
async def test_upgrade_rejected(status: int, extra: dict[str, str]) -> None:
    def reject(conn: ServerConnection, _r: Request) -> Response:
        resp = conn.respond(status, "no\n")
        for k, v in extra.items():
            resp.headers[k] = v
        return resp

    async with ws_server(idle, process_request=reject) as url:
        with pytest.raises(UpgradeRejected) as ei:
            await connector()(url, INIT)
    err = ei.value
    assert err.status == status
    for k, v in extra.items():
        assert err.headers[k.lower()] == v
    if status == 429:
        assert err.retry_after_ms() == 7000
    if status == 503:
        assert err.retry_after_ms() == 0


async def test_rejects_non_ws_scheme() -> None:
    with pytest.raises(ValueError):
        await connector()("http://127.0.0.1:1/", INIT)


async def test_open_timeout_fires_on_silent_server() -> None:
    handlers: list[asyncio.Task[Any]] = []

    async def silent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        handlers.append(asyncio.current_task())  # type: ignore[arg-type]
        await reader.read()
        writer.close()

    server = await asyncio.start_server(silent, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        c = StdlibConnector(open_timeout_ms=150, clock=LoopClock())
        with pytest.raises(TimeoutError):
            await c(f"ws://127.0.0.1:{port}/", INIT)
        assert asyncio.current_task().cancelling() == 0  # type: ignore[union-attr]
    finally:
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers)


async def test_outer_cancel_is_not_turned_into_timeout() -> None:
    handlers: list[asyncio.Task[Any]] = []

    async def silent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        handlers.append(asyncio.current_task())  # type: ignore[arg-type]
        await reader.read()
        writer.close()

    server = await asyncio.start_server(silent, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        c = StdlibConnector(open_timeout_ms=10_000)
        task = asyncio.create_task(c(f"ws://127.0.0.1:{port}/", INIT))
        while not handlers:
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers)


# --- messages ------------------------------------------------------------------------------


async def test_text_frame_returns_str() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.send("héllo")
        await ws.wait_closed()

    async with ws_server(handler) as url:
        conn = await connector()(url, INIT)
        try:
            assert await conn.recv() == "héllo"
        finally:
            await conn.close()


async def test_fragmented_message_is_reassembled() -> None:
    body = os.urandom(100 * 1024)

    async def handler(ws: ServerConnection) -> None:
        await ws.send([body[i : i + 7000] for i in range(0, len(body), 7000)])
        await ws.wait_closed()

    async with ws_server(handler, max_size=None) as url:
        conn = await connector()(url, WebSocketInit(protocols=(PROTO,), max_message=200_000))
        try:
            assert await conn.recv() == body
        finally:
            await conn.close()


async def test_ping_interleaved_mid_message_is_answered() -> None:
    body = os.urandom(30_000)
    pongs: list[Awaitable[float]] = []

    async def handler(ws: ServerConnection) -> None:
        async def parts() -> AsyncIterator[bytes]:
            yield body[:10_000]
            pongs.append(await ws.ping(b"mid"))
            yield body[10_000:20_000]
            pongs.append(await ws.ping(b"mid2"))
            yield body[20_000:]

        await ws.send(parts())
        await asyncio.wait_for(asyncio.gather(*pongs), 2)
        await ws.send(b"after")
        await ws.wait_closed()

    async with ws_server(handler) as url:
        conn = await connector()(url, INIT)
        try:
            assert await conn.recv() == body
            assert await conn.recv() == b"after"
        finally:
            await conn.close()
    assert len(pongs) == 2


async def test_server_close_with_reason() -> None:
    observed: dict[str, Any] = {}

    async def handler(ws: ServerConnection) -> None:
        await ws.close(1000, "bye now")
        observed["rcvd"] = ws.protocol.close_rcvd

    async with ws_server(handler) as url:
        conn = await connector()(url, INIT)
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.reason, ei.value.received) == (1000, "bye now", True)
        with pytest.raises(WebSocketClosed):
            await conn.send(b"x")
        await conn.close()
    assert observed["rcvd"] is not None and observed["rcvd"].code == 1000


async def test_abrupt_server_drop_is_1006() -> None:
    async def handler(ws: ServerConnection) -> None:
        ws.transport.abort()

    async with ws_server(handler) as url:
        conn = await connector()(url, INIT)
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.received) == (1006, False)
        await conn.close()


async def test_oversized_message_closes_1009() -> None:
    observed: dict[str, Any] = {}

    async def handler(ws: ServerConnection) -> None:
        await ws.send(b"\x00" * 65_545)
        try:
            await ws.recv()
        except ConnectionClosed as e:
            observed["rcvd"] = e.rcvd

    async with ws_server(handler) as url:
        conn = await connector()(url, INIT)
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.received) == (1009, False)
    assert observed["rcvd"].code == 1009


async def test_huge_message_still_delivers_our_1009() -> None:
    """The unread 10 MB payload is discarded, not answered with a reset."""
    observed: dict[str, Any] = {}

    async def handler(ws: ServerConnection) -> None:
        try:
            await ws.send(b"\x00" * 10_000_000)
            await ws.recv()
        except ConnectionClosed as e:
            observed["rcvd"] = e.rcvd

    async with ws_server(handler, max_size=None) as url:
        conn = await connector(close_timeout_ms=5000)(url, INIT)
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.received) == (1009, False)
    assert observed["rcvd"] is not None and observed["rcvd"].code == 1009


async def test_exactly_max_message_is_accepted() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.send(b"\x01" * 65_544)
        await ws.wait_closed()

    async with ws_server(handler) as url:
        conn = await connector()(url, INIT)
        try:
            assert len(await conn.recv()) == 65_544
        finally:
            await conn.close()


async def test_client_close_code_and_reason_reach_server() -> None:
    observed: dict[str, Any] = {}

    async def handler(ws: ServerConnection) -> None:
        await ws.wait_closed()
        observed["close"] = (ws.close_code, ws.close_reason)

    async with ws_server(handler) as url:
        conn = await connector()(url, INIT)
        await conn.close(4014, "x")
        await conn.close(4014, "x")
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.received) == (4014, True)
    assert observed["close"] == (4014, "x")


async def test_close_while_recv_pending() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.wait_closed()

    async with ws_server(handler) as url:
        conn = await connector()(url, INIT)
        reader = asyncio.create_task(conn.recv())
        await asyncio.sleep(0)
        await conn.close(1000)
        with pytest.raises(WebSocketClosed) as ei:
            await reader
        assert (ei.value.code, ei.value.received) == (1000, True)


async def test_abort_is_abnormal_for_server() -> None:
    observed: dict[str, Any] = {}

    async def handler(ws: ServerConnection) -> None:
        try:
            await ws.recv()
        except ConnectionClosed as e:
            observed["rcvd"] = e.rcvd

    async with ws_server(handler) as url:
        conn = await connector()(url, INIT)
        conn.abort()
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.received) == (1006, False)
        await conn.close()
    assert observed["rcvd"] is None


@pytest.mark.parametrize("size", [0, 1, 125, 126, 65_535, 65_536, 65_544])
async def test_masked_frames_round_trip(size: int) -> None:
    async def echo(ws: ServerConnection) -> None:
        async for msg in ws:
            await ws.send(msg)

    payload = os.urandom(size)
    async with ws_server(echo) as url:
        conn = await connector()(url, INIT)
        try:
            await conn.send(payload)
            assert await conn.recv() == payload
        finally:
            await conn.close()


# --- TLS -----------------------------------------------------------------------------------


def _self_signed(tmp: Path) -> tuple[Path, Path] | None:
    openssl = shutil.which("openssl")
    if openssl is None:
        return None
    cert, key = tmp / "cert.pem", tmp / "key.pem"
    cmd = [
        openssl, "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
        "-nodes", "-keyout", str(key), "-out", str(cert), "-days", "1", "-subj", "/CN=localhost",
        "-addext", "subjectAltName=IP:127.0.0.1,DNS:localhost",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,digitalSignature,keyCertSign",
    ]  # fmt: skip
    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=20)
    except (OSError, subprocess.SubprocessError):
        return None
    return cert, key


async def test_wss_with_custom_ca(tmp_path: Path) -> None:
    pair = _self_signed(tmp_path)
    if pair is None:
        pytest.skip("openssl cannot generate a test certificate here")
    cert, key = pair
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(cert, key)
    client_ctx = ssl.create_default_context(cafile=str(cert))

    async def echo(ws: ServerConnection) -> None:
        async for msg in ws:
            await ws.send(msg)

    server = await serve(echo, "127.0.0.1", 0, ssl=server_ctx, subprotocols=[Subprotocol(PROTO)])
    port = server.sockets[0].getsockname()[1]
    try:
        conn = await connector(ssl_context=client_ctx)(f"wss://127.0.0.1:{port}/v1/tunnel", INIT)
        try:
            assert conn.subprotocol == PROTO
            await conn.send(b"secure")
            assert await conn.recv() == b"secure"
        finally:
            await conn.close()
        with pytest.raises(ssl.SSLCertVerificationError):
            untrusted = ssl.create_default_context(cafile=None, cadata=None)
            await connector(ssl_context=untrusted)(f"wss://127.0.0.1:{port}/", INIT)
    finally:
        server.close()
        await server.wait_closed()


# --- raw server: malformed handshakes and frames -------------------------------------------

RawScript = Callable[[asyncio.StreamReader, asyncio.StreamWriter, str], Awaitable[None]]


@asynccontextmanager
async def raw_server(script: RawScript) -> AsyncIterator[str]:
    """`script(reader, writer, key)` runs after the request head is read."""
    handlers: list[asyncio.Task[Any]] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        handlers.append(asyncio.current_task())  # type: ignore[arg-type]
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            key = next(
                line.split(b":", 1)[1].strip().decode()
                for line in head.split(b"\r\n")
                if line.lower().startswith(b"sec-websocket-key:")
            )
            await script(reader, writer, key)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        yield f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/"
    finally:
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers)


def switching(key: str, extra: str = "") -> bytes:
    accept = base64.b64encode(hashlib.sha1(key.encode() + GUID).digest()).decode()
    return (
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Protocol: {PROTO}\r\n{extra}\r\n"
    ).encode()


def server_frame(opcode: int, payload: bytes, *, fin: bool = True, rsv: int = 0) -> bytes:
    n = len(payload)
    b0 = (0x80 if fin else 0) | rsv | opcode
    if n < 126:
        return bytes((b0, n)) + payload
    if n < 65_536:
        return struct.pack("!BBH", b0, 126, n) + payload
    return struct.pack("!BBQ", b0, 127, n) + payload


async def read_client_frame(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    b0, b1 = await reader.readexactly(2)
    assert b1 & 0x80, "client frames must be masked"
    n = b1 & 0x7F
    if n == 126:
        (n,) = struct.unpack("!H", await reader.readexactly(2))
    elif n == 127:
        (n,) = struct.unpack("!Q", await reader.readexactly(8))
    key = await reader.readexactly(4)
    return b0 & 0x0F, apply_mask(await reader.readexactly(n), key)


@pytest.mark.parametrize(
    "extra",
    [
        "Sec-WebSocket-Extensions: permessage-deflate\r\n",
        "X-Folded: a\r\n b\r\n",
    ],
)
async def test_bad_101_is_invalid_handshake(extra: str) -> None:
    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        w.write(switching(key, extra))
        await r.read()

    async with raw_server(script) as url:
        with pytest.raises(InvalidHandshake):
            await connector()(url, INIT)


async def test_wrong_accept_is_invalid_handshake() -> None:
    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        w.write(switching(base64.b64encode(b"0" * 16).decode()))
        await r.read()

    async with raw_server(script) as url:
        with pytest.raises(InvalidHandshake):
            await connector()(url, INIT)


async def test_eof_before_response_is_connection_error() -> None:
    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        pass

    async with raw_server(script) as url:
        with pytest.raises(ConnectionError):
            await connector()(url, INIT)


async def test_frames_right_after_101_are_not_lost() -> None:
    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        w.write(switching(key) + server_frame(2, b"early"))
        await r.read()

    async with raw_server(script) as url:
        conn = await connector()(url, INIT)
        assert await conn.recv() == b"early"
        conn.abort()


@pytest.mark.parametrize(
    ("frames", "code"),
    [
        (server_frame(2, b"x", rsv=0x40), 1002),
        (bytes((0x82, 0x81)) + b"\x00\x00\x00\x00x", 1002),
        (server_frame(3, b"x"), 1002),
        (server_frame(0xB, b""), 1002),
        (server_frame(9, b"x" * 126), 1002),
        (server_frame(9, b"x", fin=False), 1002),
        (server_frame(0, b"x"), 1002),
        (server_frame(2, b"a", fin=False) + server_frame(2, b"b"), 1002),
        (server_frame(8, b"\x03"), 1002),
        (server_frame(8, struct.pack("!H", 1005)), 1002),
        (server_frame(1, b"\xff\xfe"), 1007),
        (server_frame(2, b"a" * 40_000, fin=False) + server_frame(0, b"b" * 30_000), 1009),
    ],
    ids=[
        "rsv",
        "masked",
        "opcode3",
        "opcode11",
        "long-ping",
        "fragmented-ping",
        "orphan-cont",
        "nested-start",
        "close-1-byte",
        "close-1005",
        "bad-utf8",
        "too-big-fragmented",
    ],
)
async def test_protocol_violations_fail_the_connection(frames: bytes, code: int) -> None:
    replies: list[tuple[int, bytes]] = []

    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        w.write(switching(key) + frames)
        replies.append(await read_client_frame(r))

    async with raw_server(script) as url:
        conn = await connector(close_timeout_ms=5000)(url, INIT)
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.received) == (code, False)
    assert replies[0][0] == 8
    assert struct.unpack("!H", replies[0][1][:2])[0] == code


async def test_pong_goes_out_before_next_send_and_close_reply_echoes() -> None:
    seen: list[tuple[int, bytes]] = []

    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        w.write(switching(key) + server_frame(9, b"hb") + server_frame(2, b"data"))
        for _ in range(2):
            seen.append(await read_client_frame(r))
        w.write(server_frame(8, struct.pack("!H", 4012) + b"\xe2\x82"))
        seen.append(await read_client_frame(r))
        await r.read()

    async with raw_server(script) as url:
        conn = await connector()(url, INIT)
        assert await conn.recv() == b"data"
        await conn.send(b"reply")
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.received) == (4012, True)
        assert ei.value.reason == "�"
    assert seen[0] == (9 + 1, b"hb")
    assert seen[1] == (2, b"reply")
    assert seen[2] == (8, struct.pack("!H", 4012))


async def test_failure_waits_bounded_for_silent_peer() -> None:
    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        w.write(switching(key) + server_frame(3, b""))
        await r.read()

    async with raw_server(script) as url:
        conn = await connector(close_timeout_ms=100)(url, INIT)
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.received) == (1002, False)


async def test_ping_flood_does_not_grow_the_write_buffer() -> None:
    release = asyncio.Event()

    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        w.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        w.write(switching(key))
        await opened.wait()
        w.write(server_frame(9, b"p" * 125) * 20_000 + server_frame(2, b"end"))
        await w.drain()
        await release.wait()

    opened = asyncio.Event()
    async with raw_server(script) as url:
        try:
            conn = await connector()(url, INIT)
            sock = conn._transport.get_extra_info("socket")
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            opened.set()
            assert await conn.recv() == b"end"
            # asyncio's default high-water mark is 64 KiB; 20 000 unthrottled pongs are 2.6 MB.
            assert conn._transport.get_write_buffer_size() <= 65_536 + 200
            conn.abort()
        finally:
            release.set()


async def test_send_blocked_in_drain_fails_on_abort() -> None:
    release = asyncio.Event()

    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        w.write(switching(key))
        await release.wait()

    async with raw_server(script) as url:
        try:
            conn = await connector()(url, INIT)
            send = asyncio.create_task(conn.send(b"\x00" * 16_000_000))
            await asyncio.sleep(0.05)
            assert not send.done()
            conn.abort()
            with pytest.raises(WebSocketClosed) as ei:
                await send
            assert ei.value.code == 1006
        finally:
            release.set()


async def test_close_timeout_when_peer_never_answers() -> None:
    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        w.write(switching(key))
        await r.read()

    async with raw_server(script) as url:
        conn = await connector(close_timeout_ms=100)(url, INIT)
        await conn.close(1000)
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.received) == (1006, False)


async def test_close_discards_data_until_close_frame() -> None:
    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        w.write(switching(key))
        opcode, payload = await read_client_frame(r)
        assert opcode == 8
        w.write(server_frame(2, b"late") + server_frame(8, payload))
        await r.read()

    async with raw_server(script) as url:
        conn = await connector(close_timeout_ms=5000)(url, INIT)
        await conn.close(4001, "bye")
        with pytest.raises(WebSocketClosed) as ei:
            await conn.recv()
        assert (ei.value.code, ei.value.reason, ei.value.received) == (4001, "bye", True)


async def test_close_rejects_reserved_code_and_long_reason() -> None:
    async def script(r: asyncio.StreamReader, w: asyncio.StreamWriter, key: str) -> None:
        w.write(switching(key))
        await r.read()

    async with raw_server(script) as url:
        conn = await connector()(url, INIT)
        with pytest.raises(ValueError):
            await conn.close(1006)
        with pytest.raises(ValueError):
            await conn.close(4000, "é" * 62)
        conn.abort()
