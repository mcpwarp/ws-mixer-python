from __future__ import annotations

import asyncio

import pytest

from ws_mixer.socket import UpgradeRejected, WebSocketClosed, WebSocketInit
from ws_mixer.testing import FakeConnector, FakeWebSocket, settle, socket_pair

INIT = WebSocketInit(headers={"Authorization": "Bearer t"})
pytestmark = pytest.mark.asyncio


async def test_connector_records_and_opens() -> None:
    ws = FakeWebSocket()
    conn = FakeConnector([ws])
    dial = asyncio.create_task(conn("ws://edge/v1/tunnel", INIT))
    await settle()
    assert not dial.done()
    assert conn.dials == 1
    assert conn.calls == [("ws://edge/v1/tunnel", INIT)]
    assert ws.url == "ws://edge/v1/tunnel" and ws.init is INIT
    ws.open()
    got = await dial
    assert got is ws
    assert got.subprotocol == "ws-mixer.v1"


async def test_pre_settled_outcomes() -> None:
    a, b, c = FakeWebSocket(), FakeWebSocket(), FakeWebSocket()
    a.reject(429, {"Retry-After": "2"})
    b.fail()
    c.open(subprotocol=None)
    conn = FakeConnector([a, b, c])
    with pytest.raises(UpgradeRejected) as ei:
        await conn("ws://e/", INIT)
    assert ei.value.status == 429 and ei.value.retry_after_ms() == 2000
    with pytest.raises(OSError):
        await conn("ws://e/", INIT)
    assert (await conn("ws://e/", INIT)).subprotocol is None
    assert conn.dials == 3
    with pytest.raises(RuntimeError):
        c.open()


async def test_factory_and_next_socket() -> None:
    made: list[FakeWebSocket] = []

    def factory(url: str, init: WebSocketInit) -> FakeWebSocket:
        ws = FakeWebSocket()
        ws.open()
        made.append(ws)
        return ws

    conn = FakeConnector(factory=factory)
    waiter = asyncio.create_task(conn.next_socket())
    await settle()
    assert not waiter.done()
    await conn("ws://e/", INIT)
    assert await waiter is made[0]
    assert await conn.next_socket(0) is made[0]


async def test_default_socket_when_queue_empty() -> None:
    conn = FakeConnector()
    dial = asyncio.create_task(conn("ws://e/", INIT))
    ws = await conn.next_socket(0)
    ws.open()
    assert await dial is ws


async def test_push_recv_send_and_server_close() -> None:
    ws = FakeWebSocket()
    ws.open()
    ws.push(b"one")
    ws.push("two")
    ws.server_close(4012, "drain")
    assert await ws.recv() == b"one"
    assert await ws.recv() == "two"
    for _ in range(2):
        with pytest.raises(WebSocketClosed) as ei:
            await ws.recv()
        assert (ei.value.code, ei.value.reason, ei.value.received) == (4012, "drain", True)
    assert ws.closed
    with pytest.raises(WebSocketClosed):
        await ws.send(b"x")


async def test_sdk_close_echo_and_records() -> None:
    ws = FakeWebSocket()
    await ws.send(b"a")
    await ws.close(4014, "bye")
    await ws.close(1000)
    assert ws.sent == [b"a"]
    assert ws.close_calls == [(4014, "bye"), (1000, "")]
    with pytest.raises(WebSocketClosed) as ei:
        await ws.recv()
    assert (ei.value.code, ei.value.received) == (4014, True)
    with pytest.raises(WebSocketClosed):
        await ws.send(b"b")

    with pytest.raises(ValueError):
        await ws.close(1006)


async def test_silent_server_never_answers_close() -> None:
    quiet = FakeWebSocket(echo_close=False)
    await quiet.close()
    reader = asyncio.create_task(quiet.recv())
    await settle()
    assert not reader.done()
    quiet.abort()
    with pytest.raises(WebSocketClosed) as ei:
        await reader
    assert (ei.value.code, ei.value.received) == (1006, False)


async def test_set_echo_close_silences_a_later_close() -> None:
    ws = FakeWebSocket()
    ws.set_echo_close(False)
    await ws.close()
    reader = asyncio.create_task(ws.recv())
    await settle()
    assert not reader.done()
    ws.server_close(4012)
    with pytest.raises(WebSocketClosed) as ei:
        await reader
    assert (ei.value.code, ei.value.received) == (4012, True)


async def test_next_sent() -> None:
    ws = FakeWebSocket()
    waiter = asyncio.create_task(ws.next_sent())
    await settle()
    assert not waiter.done()
    await ws.send(b"a")
    await ws.send(b"b")
    assert await waiter == b"a"
    assert await ws.next_sent() == b"b"
    assert ws.sent == [b"a", b"b"]


async def test_cancelled_dial_is_recorded() -> None:
    conn = FakeConnector()
    dial = asyncio.create_task(conn("ws://e/", INIT))
    ws = await conn.next_socket(0)
    dial.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dial
    assert ws.dial_cancelled
    assert (conn.dials, conn.cancelled_dials) == (1, 1)


async def test_abort_skips_queued_messages() -> None:
    ws = FakeWebSocket()
    ws.push(b"never")
    ws.abort()
    assert ws.aborted and ws.closed
    with pytest.raises(WebSocketClosed) as ei:
        await ws.recv()
    assert ei.value.code == 1006


async def test_send_backpressure() -> None:
    ws = FakeWebSocket()
    ws.pause_sends()
    send = asyncio.create_task(ws.send(b"x"))
    await settle()
    assert not send.done() and ws.sent == []
    ws.resume_sends()
    await send
    assert ws.sent == [b"x"]


async def test_socket_pair_in_order_and_close() -> None:
    a, b = socket_pair()
    assert a.subprotocol == b.subprotocol == "ws-mixer.v1"
    for i in range(5):
        await a.send(bytes([i]))
    await b.send(b"back")
    assert [await b.recv() for _ in range(5)] == [bytes([i]) for i in range(5)]
    assert await a.recv() == b"back"
    await a.close(4001, "why")
    with pytest.raises(WebSocketClosed) as ei:
        await b.recv()
    assert (ei.value.code, ei.value.reason, ei.value.received) == (4001, "why", True)
    with pytest.raises(WebSocketClosed) as ei:
        await a.recv()
    assert ei.value.code == 4001
    with pytest.raises(WebSocketClosed):
        await b.send(b"late")


async def test_socket_pair_abort() -> None:
    a, b = socket_pair(subprotocol=None)
    await b.send(b"queued")
    a.abort()
    with pytest.raises(WebSocketClosed) as ei:
        await a.recv()
    assert (ei.value.code, ei.value.received) == (1006, False)
    with pytest.raises(WebSocketClosed) as ei:
        await b.recv()
    assert ei.value.code == 1006
