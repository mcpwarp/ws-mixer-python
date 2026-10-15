"""The FakeEdge test double (ws_mixer.testing.fake_edge), driven by a plain MixerClient."""

from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

from ws_mixer.client import MixerClient, ReconnectOptions
from ws_mixer.conn import PongInfo
from ws_mixer.errors import ErrorCode, StreamError, WsMixerError
from ws_mixer.mcpwarp import encode_register, encode_unregister
from ws_mixer.socket import UpgradeRejected, WebSocketInit
from ws_mixer.stream import MixerStream
from ws_mixer.testing import FakeClock, FakeEdge, settle

pytestmark = pytest.mark.asyncio

PUBLIC = "https://anki-tester.mcpwarp.test/mcp"


class Rig:
    def __init__(self, **edge_kw: Any) -> None:
        self.clock = FakeClock()
        self.edge = FakeEdge(clock=self.clock, **edge_kw)
        self.streams: list[MixerStream] = []
        self.apps: list[dict[str, Any]] = []
        self.client: MixerClient | None = None

    async def connect(self, token: str = "tok", **kw: Any) -> MixerClient:
        await self.edge.start()
        self.client = MixerClient(
            self.edge.url,
            token=token,
            web_socket=self.edge.web_socket,
            clock=self.clock,
            random=lambda: 0.5,
            on_stream=self.streams.append,
            on_app=self.apps.append,
            **kw,
        )
        await asyncio.wait_for(self.client.start(), 1)
        return self.client

    async def register(self, name: str = "anki") -> None:
        assert self.client is not None
        await self.client.send_app(encode_register(name))
        await settle()


@pytest_asyncio.fixture
async def baseline() -> AsyncIterator[set[asyncio.Task[Any]]]:
    yield set(asyncio.all_tasks())


async def finish(rig: Rig, baseline: set[asyncio.Task[Any]]) -> None:
    if rig.client is not None:
        closing = asyncio.create_task(rig.client.close())
        await settle()
        await rig.clock.advance(10_000)
        await asyncio.wait_for(closing, 1)
    await rig.edge.stop()
    await settle()
    assert rig.clock.pending() == 0
    leaked = set(asyncio.all_tasks()) - baseline - {asyncio.current_task()}
    assert not leaked, leaked


async def serve_echo(stream: MixerStream) -> None:
    """A minimal agent: reads the whole request, answers 200 with the bytes it read."""
    data = b""
    async for chunk in stream:
        data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    reply = b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%b" % (len(head), head)
    await stream.write(reply)
    await stream.close_write()


async def test_records_the_upgrade_and_hello_and_welcomes(
    baseline: set[asyncio.Task[Any]],
) -> None:
    rig = Rig()
    client = await rig.connect(headers={"X-Mcpwarp-Integration": "abc"})
    up = rig.edge.upgrades[0]
    assert up.url == "wss://connect.mcpwarp.test"
    assert up.protocols == ("ws-mixer.v1",)
    assert up.headers["Authorization"] == "Bearer tok"
    assert up.headers["X-Mcpwarp-Integration"] == "abc"
    assert rig.edge.hellos[0]["token"] == "tok"
    conn = client.current_conn()
    assert conn is not None and conn.session == "fake-1"
    assert len(rig.edge.connections) == 1 and rig.edge.connections[0].hello is not None
    await finish(rig, baseline)


async def test_answers_ping_with_pong(baseline: set[asyncio.Task[Any]]) -> None:
    rig = Rig()
    client = await rig.connect()
    pongs: list[PongInfo] = []
    client.on("pong", pongs.append)
    await rig.clock.advance(15_000)  # the first ping goes out at random() * interval
    assert len(pongs) == 1
    await rig.clock.advance(120_000)  # pongs keep the keepalive watchdog quiet
    assert client.state == "connected"
    await finish(rig, baseline)


async def test_rejects_a_bad_token_with_4011(baseline: set[asyncio.Task[Any]]) -> None:
    rig = Rig(accept_token=lambda t: t == "good")
    await rig.edge.start()
    client = MixerClient(rig.edge.url, token="bad", web_socket=rig.edge.web_socket, clock=rig.clock)
    with pytest.raises(WsMixerError) as info:
        await asyncio.wait_for(client.start(), 1)
    assert info.value.code == ErrorCode.UNAUTHORIZED and info.value.fatal
    await finish(rig, baseline)


async def test_dials_fail_until_started_and_reject_next_upgrade(
    baseline: set[asyncio.Task[Any]],
) -> None:
    rig = Rig()
    with pytest.raises(ConnectionRefusedError):
        await rig.edge.web_socket(rig.edge.url, _init())
    await rig.edge.start()
    rig.edge.reject_next_upgrade(503, {"Retry-After": "1"})
    with pytest.raises(UpgradeRejected) as info:
        await rig.edge.web_socket(rig.edge.url, _init())
    assert info.value.status == 503 and info.value.retry_after_ms() == 1000
    assert len(rig.edge.upgrades) == 2
    await finish(rig, baseline)


async def test_registry_register_unregister_and_failures(
    baseline: set[asyncio.Task[Any]],
) -> None:
    rig = Rig()
    client = await rig.connect()
    await rig.register()
    assert rig.apps[-1] == {
        "mcpwarp": {
            "v": 1,
            "op": "registered",
            "services": [
                {"name": "anki", "id": "srv_anki", "url": PUBLIC, "created": True},
            ],
            "errors": [],
        }
    }
    assert await rig.edge.wait_for_registered("anki") == ("srv_anki", PUBLIC)
    await rig.register()
    assert rig.apps[-1]["mcpwarp"]["services"][0]["created"] is False
    await client.send_app(encode_unregister("anki"))
    await settle()
    assert rig.apps[-1]["mcpwarp"] == {
        "v": 1,
        "op": "unregistered",
        "services": [{"name": "anki", "id": "srv_anki"}],
    }
    rig.edge.fail_register("anki", {"code": "TIMEOUT"}, times=2)
    await rig.register()
    await rig.register()
    await rig.register()
    errors = [a["mcpwarp"]["errors"] for a in rig.apps[-3:]]
    assert errors[0] == [{"name": "anki", "code": "TIMEOUT", "message": "timeout"}]
    assert errors[1] == errors[0] and errors[2] == []
    rig.edge.disable("anki")
    await rig.register()
    assert rig.apps[-1]["mcpwarp"]["errors"][0]["code"] == "SERVER_DISABLED"
    rig.edge.fail_register("anki", {"code": "X"}, times=math.inf)
    rig.edge.fail_register("anki", None)
    rig.edge.withhold_registered("anki")
    before = len(rig.apps)
    await rig.register()
    assert len(rig.apps) == before  # withheld
    assert [b["mcpwarp"]["op"] for b in rig.edge.app_log].count("register") == 7
    await finish(rig, baseline)


async def test_unknown_op_and_wrong_version_get_error_replies(
    baseline: set[asyncio.Task[Any]],
) -> None:
    rig = Rig()
    client = await rig.connect()
    await client.send_app({"mcpwarp": {"v": 2, "op": "register"}})
    await client.send_app({"mcpwarp": {"v": 1, "op": "dance"}})
    await settle()
    assert [a["mcpwarp"]["code"] for a in rig.apps] == ["UNSUPPORTED_VERSION", "UNKNOWN_OP"]
    await finish(rig, baseline)


async def test_request_is_written_in_go_request_write_shape(
    baseline: set[asyncio.Task[Any]],
) -> None:
    rig = Rig()
    await rig.connect()
    await rig.register()
    pending = asyncio.create_task(
        rig.edge.request(
            "post",
            PUBLIC + "?x=1",
            headers={"Authorization": "Bearer secret", "Connection": "close", "X-A": "1"},
            body="hello",
        )
    )
    await settle()
    serve = asyncio.create_task(serve_echo(rig.streams[0]))
    response = await asyncio.wait_for(pending, 1)
    await serve
    assert response.status == 200
    assert response.body.decode("latin-1").split("\r\n") == [
        "POST /mcp?x=1 HTTP/1.1",
        "Host: anki-tester.mcpwarp.test",
        "User-Agent: Go-http-client/1.1",
        "X-A: 1",
        "content-type: text/plain;charset=UTF-8",
        "Content-Length: 5",
    ]
    assert rig.streams[0].id == 1
    await finish(rig, baseline)


async def test_flow_control_moves_a_mib_each_way(baseline: set[asyncio.Task[Any]]) -> None:
    rig = Rig()
    await rig.connect()
    await rig.register()
    big = bytes(range(256)) * 4096  # 1 MiB

    async def agent(stream: MixerStream) -> None:
        got = b""
        async for chunk in stream:
            got += chunk
        _, _, body = got.partition(b"\r\n\r\n")
        assert body == big
        await stream.write(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
        for i in range(0, len(body), 100_000):
            piece = body[i : i + 100_000]
            await stream.write(b"%x\r\n%b\r\n" % (len(piece), piece))
        await stream.write(b"0\r\n\r\n")
        await stream.close_write()

    pending = asyncio.create_task(rig.edge.request("POST", PUBLIC, body=big))
    await settle()
    served = asyncio.create_task(agent(rig.streams[0]))
    response = await asyncio.wait_for(pending, 5)
    await served
    assert response.body == big
    await finish(rig, baseline)


async def test_drain_with_no_open_streams_closes_4012(baseline: set[asyncio.Task[Any]]) -> None:
    rig = Rig()
    client = await rig.connect(reconnect=ReconnectOptions(max_attempts=0))
    closes: list[Any] = []
    client.on("close", closes.append)
    rig.edge.drain("maintenance", 5000)
    await settle()
    assert rig.edge.connections == []
    assert closes and closes[0].ws_code == 4012
    await finish(rig, baseline)


async def test_drain_deadline_resets_survivors(baseline: set[asyncio.Task[Any]]) -> None:
    rig = Rig()
    await rig.connect(reconnect=ReconnectOptions(max_attempts=0))
    await rig.register()
    pending = asyncio.create_task(rig.edge.request("GET", PUBLIC))
    await settle()
    rig.edge.drain("rollout", 3000)
    await settle()
    assert len(rig.edge.connections) == 1
    await rig.clock.advance(3000)
    with pytest.raises(WsMixerError):
        await asyncio.wait_for(pending, 1)
    assert rig.edge.connections == []
    await finish(rig, baseline)


async def test_request_head_timeout(baseline: set[asyncio.Task[Any]]) -> None:
    rig = Rig(request_timeout_ms=500)
    await rig.connect()
    await rig.register()
    pending = asyncio.create_task(rig.edge.request("GET", PUBLIC))
    await settle()
    await rig.clock.advance(500)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(pending, 1)
    await finish(rig, baseline)


async def test_routing_errors_and_the_disabled_shortcut(
    baseline: set[asyncio.Task[Any]],
) -> None:
    rig = Rig()
    await rig.connect()
    with pytest.raises(ValueError, match="not a service host"):
        await rig.edge.request("GET", "https://elsewhere.test/mcp")
    with pytest.raises(RuntimeError, match="no agent registered"):
        await rig.edge.request("GET", PUBLIC)
    await rig.register()
    rig.edge.disable("anki")
    response = await rig.edge.request("GET", PUBLIC)
    assert (response.status, response.text) == (503, '{"error":"service disabled"}')
    await settle()
    assert rig.apps[-1]["mcpwarp"] == {
        "v": 1,
        "op": "disable",
        "id": "srv_anki",
        "reason": "disabled",
    }
    await finish(rig, baseline)


async def test_conflict_resets_open_requests_and_unroutes(
    baseline: set[asyncio.Task[Any]],
) -> None:
    rig = Rig()
    await rig.connect()
    await rig.register()
    pending = asyncio.create_task(rig.edge.request("GET", PUBLIC))
    await settle()
    rig.edge.conflict("anki")
    with pytest.raises(StreamError):
        await asyncio.wait_for(pending, 1)
    await settle()
    assert rig.apps[-1]["mcpwarp"]["op"] == "conflict"
    with pytest.raises(RuntimeError, match="no agent registered"):
        await rig.edge.request("GET", PUBLIC)
    await finish(rig, baseline)


async def test_wait_for_app_times_out_on_the_clock(baseline: set[asyncio.Task[Any]]) -> None:
    rig = Rig()
    await rig.connect()
    waiting = asyncio.create_task(rig.edge.wait_for_app("register", timeout_ms=100))
    await settle()
    await rig.clock.advance(100)
    with pytest.raises(TimeoutError):
        await waiting
    await rig.register()
    assert (await rig.edge.wait_for_app("register"))["mcpwarp"]["op"] == "register"
    await finish(rig, baseline)


async def test_stop_closes_connections_with_1001(baseline: set[asyncio.Task[Any]]) -> None:
    rig = Rig()
    client = await rig.connect(reconnect=ReconnectOptions(max_attempts=0))
    closes: list[Any] = []
    client.on("close", closes.append)
    await rig.edge.stop()
    await settle()
    assert closes[0].ws_code == 1001
    await finish(rig, baseline)


def _init() -> WebSocketInit:
    return WebSocketInit()
