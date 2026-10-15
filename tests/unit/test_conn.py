"""MixerConn over FakeWebSocket on a FakeClock (plus a few socket_pair end-to-end runs).

Every test ends through `finish()`, which asserts that no timer and no task outlived the
connection.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
import pytest_asyncio

from ws_mixer.conn import (
    CloseInfo,
    DrainInfo,
    HandlerErrorInfo,
    MixerConn,
    PongInfo,
    Timing,
    hello_payload,
)
from ws_mixer.control import AgentInfo, encode_control
from ws_mixer.errors import ConnError, ErrorCode, WsMixerError
from ws_mixer.frame import (
    Frame,
    FrameType,
    decode_frame,
    encode_close,
    encode_data,
    encode_frame,
    encode_open,
    encode_reset,
    encode_window,
    reset_code,
    window_increment,
)
from ws_mixer.socket import SUBPROTOCOL, WebSocketClosed, WebSocketConnection
from ws_mixer.stream import ConnectionLost, Finished, MixerStream, Reset
from ws_mixer.testing import FakeClock, FakeWebSocket, settle, socket_pair

pytestmark = pytest.mark.asyncio

AGENT: AgentInfo = {"sdk": "ws-mixer-py", "sdk_version": "0.0.0"}
WELCOME: dict[str, Any] = {
    "t": "welcome",
    "v": 1,
    "session": "s1",
    "window": 262_144,
    "max_streams": 64,
    "ping_interval": 30_000,
    "ping_timeout": 90_000,
}


def ctl(msg: dict[str, Any]) -> bytes:
    return encode_data(0, encode_control(msg))


def frames(ws: FakeWebSocket) -> list[Frame]:
    return [decode_frame(raw) for raw in ws.sent]


def controls(ws: FakeWebSocket) -> list[dict[str, Any]]:
    out = []
    for f in frames(ws):
        if f.type == FrameType.DATA and f.stream_id == 0:
            out.append(json.loads(f.payload))
    return out


def kinds(ws: FakeWebSocket) -> list[str]:
    """Each sent frame as e.g. "WINDOW:1" or "app" (stream-0 messages by `t`)."""
    out = []
    for f in frames(ws):
        if f.type == FrameType.DATA and f.stream_id == 0:
            out.append(json.loads(f.payload)["t"])
        else:
            out.append(f"{FrameType(f.type).name}:{f.stream_id}")
    return out


class Harness:
    def __init__(self, ws: FakeWebSocket, conn: MixerConn, clock: FakeClock) -> None:
        self.ws = ws
        self.conn = conn
        self.clock = clock
        self.streams: list[MixerStream] = []
        self.closes: list[CloseInfo] = []
        conn.on("close", self.closes.append)
        conn.on_stream = self.streams.append

    def push(self, *msgs: bytes | dict[str, Any]) -> None:
        for m in msgs:
            self.ws.push(ctl(m) if isinstance(m, dict) else m)

    @property
    def info(self) -> CloseInfo:
        assert self.closes, "the connection is still open"
        return self.closes[-1]


def new_conn(
    ws: FakeWebSocket | None = None,
    clock: FakeClock | None = None,
    random: Callable[[], float] = lambda: 0.5,
    **kw: Any,
) -> Harness:
    if ws is None:
        ws = FakeWebSocket()
        ws.subprotocol = SUBPROTOCOL
    clock = clock or FakeClock()
    conn = MixerConn(ws, token="t", agent=AGENT, clock=clock, random=random, **kw)
    return Harness(ws, conn, clock)


@pytest_asyncio.fixture
async def baseline() -> AsyncIterator[set[asyncio.Task[Any]]]:
    yield set(asyncio.all_tasks())


async def connect(welcome: dict[str, Any] | None = None, **kw: Any) -> Harness:
    h = new_conn(**kw)
    hs = asyncio.create_task(h.conn.handshake())
    await settle()
    h.push({**WELCOME, **(welcome or {})})
    await hs
    return h


async def finish(h: Harness, baseline: set[asyncio.Task[Any]]) -> None:
    await finish_conn(h.conn, h.clock, baseline)


async def finish_conn(conn: MixerConn, clock: FakeClock, baseline: set[asyncio.Task[Any]]) -> None:
    if not conn.closed:
        conn.fail(ConnError(ErrorCode.NO_ERROR, "test done"))
    await conn.wait_closed()
    await settle()
    assert clock.pending() == 0, "a timer outlived the connection"
    leaked = set(asyncio.all_tasks()) - baseline - {asyncio.current_task()}
    assert not leaked, leaked


# --- handshake ------------------------------------------------------------------------------


async def test_hello_and_welcome(baseline: set[asyncio.Task[Any]]) -> None:
    welcomes: list[Any] = []
    h = new_conn(window=65_536, max_streams=8, capabilities=["x.y"], meta={"client": "sdk-python"})
    h.conn.on("welcome", welcomes.append)
    hs = asyncio.create_task(h.conn.handshake())
    await settle()
    assert controls(h.ws) == [
        {
            "t": "hello",
            "v": 1,
            "token": "t",
            "agent": dict(AGENT),
            "window": 65_536,
            "max_streams": 8,
            "capabilities": ["x.y"],
            "meta": {"client": "sdk-python"},
        }
    ]
    h.push({**WELCOME, "max_streams": 4, "window": 20_000, "meta": {"m": 1}})
    welcome = await hs
    assert welcomes == [welcome]
    c = h.conn
    assert (c.session, c.peer_window, c.our_window, c.max_streams) == ("s1", 20_000, 65_536, 4)
    assert (c.ping_interval_ms, c.ping_timeout_ms, c.welcome_meta) == (30_000, 90_000, {"m": 1})
    with pytest.raises(RuntimeError):
        await c.handshake()
    await finish(h, baseline)


async def test_constructor_validates_ranges() -> None:
    with pytest.raises(ValueError):
        new_conn(window=100)
    with pytest.raises(ValueError):
        new_conn(max_streams=0)
    with pytest.raises(ValueError, match="stream-0 limit"):
        new_conn(meta={"blob": "x" * 17_000})
    with pytest.raises(ValueError):
        hello_payload(token="t", agent=AGENT, meta={"blob": "x" * 17_000})
    assert json.loads(hello_payload(token="t", agent=AGENT))["window"] == 262_144


async def test_hello_timeout(baseline: set[asyncio.Task[Any]]) -> None:
    h = new_conn(hello_timeout_ms=10_000)
    hs = asyncio.create_task(h.conn.handshake())
    await h.clock.advance(9_999)
    assert not hs.done()
    await h.clock.advance(1)
    with pytest.raises(ConnError) as ei:
        await hs
    assert ei.value.code == ErrorCode.PROTOCOL_ERROR and ei.value.ws_code == 4001
    info = h.info
    assert (info.ws_code, info.error_name, info.phase, info.local) == (
        4001,
        "PROTOCOL_ERROR",
        "handshake",
        True,
    )
    assert [m["t"] for m in controls(h.ws)] == ["hello", "error"]
    assert h.ws.close_calls[0][0] == 4001
    await finish(h, baseline)


async def test_timing_overrides_hello_timeout_and_allows_subfloor(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = await connect(
        {"ping_interval": 100, "ping_timeout": 150},
        _timing=Timing(hello_timeout_ms=5, allow_subfloor=True),
    )
    assert h.conn.ping_interval_ms == 100
    await finish(h, baseline)
    h2 = new_conn(_timing=Timing(hello_timeout_ms=5))
    hs = asyncio.create_task(h2.conn.handshake())
    await h2.clock.advance(5)
    with pytest.raises(ConnError):
        await hs
    await finish(h2, baseline)


async def test_min_ping_interval_floor_is_applied(baseline: set[asyncio.Task[Any]]) -> None:
    h = new_conn(_timing=Timing(allow_subfloor=True, min_ping_interval=200))
    hs = asyncio.create_task(h.conn.handshake())
    await settle()
    h.push({**WELCOME, "ping_interval": 100, "ping_timeout": 1000})
    with pytest.raises(ConnError) as ei:
        await hs
    assert ei.value.code == ErrorCode.PROTOCOL_ERROR
    await finish(h, baseline)


async def test_subprotocol_mismatch_is_fatal_unsupported(baseline: set[asyncio.Task[Any]]) -> None:
    ws = FakeWebSocket()
    ws.subprotocol = "chat"
    h = new_conn(ws)
    fatal: list[WsMixerError] = []
    h.conn.on("fatal", fatal.append)
    with pytest.raises(ConnError) as ei:
        await h.conn.handshake()
    assert ei.value.code == ErrorCode.UNSUPPORTED and ei.value.fatal
    assert fatal == [ei.value]
    assert h.ws.sent == []  # no hello, no error frame
    assert h.ws.close_calls == [(4010, "subprotocol not echoed")]  # still 4010 on the wire
    info = h.info
    assert (info.phase, info.ws_code, info.error_code, info.error_name, info.local) == (
        "dial",
        None,
        ErrorCode.UNSUPPORTED,
        "UNSUPPORTED",
        True,
    )
    assert ei.value.ws_code is None
    await finish(h, baseline)


async def test_peer_error_before_welcome_is_not_answered(baseline: set[asyncio.Task[Any]]) -> None:
    h = new_conn()
    fatal: list[WsMixerError] = []
    h.conn.on("fatal", fatal.append)
    hs = asyncio.create_task(h.conn.handshake())
    await settle()
    h.push({"t": "error", "code": 11, "message": "token rejected"})
    with pytest.raises(ConnError) as ei:
        await hs
    assert ei.value.code == ErrorCode.UNAUTHORIZED
    assert [m["t"] for m in controls(h.ws)] == ["hello"]
    assert h.ws.close_calls == [(4011, "peer error UNAUTHORIZED")]
    info = h.info
    assert (info.ws_code, info.error_code, info.message, info.local, info.received_close) == (
        4011,
        11,
        "token rejected",
        False,
        False,
    )
    assert len(fatal) == 1
    await finish(h, baseline)


async def test_peer_error_with_huge_code_is_clamped_on_the_wire(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = await connect()
    h.push({"t": "error", "code": 0x1000_0001, "message": "app"})
    await settle()
    assert h.info.ws_code == 4000 + 0x1000_0001
    assert h.ws.close_calls[0][0] == 4002
    await finish(h, baseline)


async def test_malformed_peer_error_is_not_answered(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push({"t": "error", "code": "eleven", "message": "x"})
    await settle()
    assert h.info.error_name == "PROTOCOL_ERROR" and h.info.ws_code == 4001
    assert [m["t"] for m in controls(h.ws)] == ["hello"]
    await finish(h, baseline)


async def test_text_frame_is_protocol_error(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.ws.push('{"t":"ping","id":0}')
    await settle()
    assert h.info.error_name == "PROTOCOL_ERROR"
    assert "text frame" in controls(h.ws)[-1]["message"]
    await finish(h, baseline)


async def test_close_before_welcome_is_handshake_phase(baseline: set[asyncio.Task[Any]]) -> None:
    h = new_conn()
    hs = asyncio.create_task(h.conn.handshake())
    await settle()
    h.ws.server_close(4011, "nope")
    with pytest.raises(ConnError) as ei:
        await hs
    assert ei.value.code == ErrorCode.UNAUTHORIZED and ei.value.ws_code == 4011
    info = h.info
    assert (info.phase, info.ws_code, info.error_code, info.close_reason) == (
        "handshake",
        4011,
        None,
        "nope",
    )
    assert info.received_close and not info.local
    await finish(h, baseline)


async def test_cancelled_handshake_tears_down(baseline: set[asyncio.Task[Any]]) -> None:
    h = new_conn()
    hs = asyncio.create_task(h.conn.handshake())
    await settle()
    hs.cancel()
    with pytest.raises(asyncio.CancelledError):
        await hs
    assert h.info.error_code == ErrorCode.NO_ERROR and h.info.ws_code == 1000
    await finish(h, baseline)


@pytest.mark.parametrize(
    "msg",
    [
        {"t": "ping", "id": 0},
        {"t": "drain", "reason": "rollout", "last_stream_id": 0},
        {"t": "app", "body": {}},
        {"t": "hello", "v": 1, "token": "x", "agent": dict(AGENT)},
    ],
)
async def test_control_before_welcome_is_protocol_error(
    msg: dict[str, Any], baseline: set[asyncio.Task[Any]]
) -> None:
    h = new_conn()
    hs = asyncio.create_task(h.conn.handshake())
    await settle()
    h.push(msg)
    with pytest.raises(ConnError) as ei:
        await hs
    assert ei.value.code == ErrorCode.PROTOCOL_ERROR
    assert controls(h.ws)[-1]["t"] == "error"
    await finish(h, baseline)


async def test_send_app_before_welcome_raises(baseline: set[asyncio.Task[Any]]) -> None:
    h = new_conn()
    with pytest.raises(WsMixerError):
        await h.conn.send_app({"x": 1})
    await finish(h, baseline)


# --- dispatch: the connection-level rows of the receive table -------------------------------


async def test_never_opened_and_duplicate_open_are_protocol_errors(
    baseline: set[asyncio.Task[Any]],
) -> None:
    for bad in (encode_window(1, 1), encode_close(5), encode_reset(3, 7), encode_data(9, b"x")):
        h = await connect()
        h.push(bad)
        await settle()
        assert h.info.error_name == "PROTOCOL_ERROR", bad
        await finish(h, baseline)
    h = await connect()
    h.push(encode_open(3), encode_open(1))
    await settle()
    assert h.info.error_name == "PROTOCOL_ERROR"
    await finish(h, baseline)


async def test_window_increment_zero(baseline: set[asyncio.Task[Any]]) -> None:
    zero = encode_frame(FrameType.WINDOW, 1, b"\0\0\0\0")
    h = await connect()
    h.push(zero)  # never opened: connection error
    await settle()
    assert h.info.error_name == "PROTOCOL_ERROR"
    await finish(h, baseline)
    h = await connect()
    h.push(encode_open(1), zero)  # live stream: stream-scoped
    await settle()
    assert not h.conn.closed
    assert h.streams[0].reset_code == ErrorCode.PROTOCOL_ERROR
    assert kinds(h.ws)[-1] == "RESET:1"
    sent = len(h.ws.sent)
    h.push(zero)  # retired now: a stale frame, discarded and counted, nothing sent
    await settle()
    assert not h.conn.closed and len(h.ws.sent) == sent
    assert h.conn.stats().stale_frames == 1
    await finish(h, baseline)


async def test_stale_frames_are_counted(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1), encode_reset(1, 7))
    for f in (encode_data(1, b"x"), encode_window(1, 5), encode_close(1), encode_reset(1, 7)):
        h.push(f)
    h.push(encode_frame(0x05, 1, b"??"))
    await settle()
    assert not h.conn.closed
    stats = h.conn.stats()
    assert stats.stale_frames == 4 and stats.unknown_frame_types == 1
    await finish(h, baseline)


async def test_stream_limit_refuses_and_counts(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect({"max_streams": 1})
    h.push(encode_open(1), encode_open(3), encode_open(5))
    await settle()
    assert not h.conn.closed
    resets = [f for f in frames(h.ws) if f.type == FrameType.RESET]
    assert [(f.stream_id, reset_code(f)) for f in resets] == [(3, 8), (5, 8)]
    assert h.conn.stats().refused_opens == 2 and h.conn.live_stream_count() == 1
    await finish(h, baseline)


async def test_stream_error_on_live_stream_resets_it(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1), encode_close(1), encode_close(1))
    await settle()
    assert not h.conn.closed
    assert h.streams[0].reset_code == ErrorCode.STREAM_CLOSED
    assert kinds(h.ws)[-1] == "RESET:1"
    await finish(h, baseline)


# --- writer ---------------------------------------------------------------------------------


async def test_pong_jumps_ahead_of_queued_window(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect({"window": 262_144})
    h.push(encode_open(1), *(encode_data(1, bytes(50_000)) for _ in range(3)))
    await settle()
    h.ws.pause_sends()
    app = asyncio.create_task(h.conn.send_app({"n": 1}))  # in flight, blocked in send()
    await settle()
    for _ in range(3):
        assert await h.streams[0].read() == bytes(50_000)  # the third queues WINDOW(1)
    h.push({"t": "ping", "id": 7, "ts": 123})
    await settle()
    h.ws.resume_sends()
    await app
    await settle()
    assert kinds(h.ws)[-3:] == ["app", "pong", "WINDOW:1"]
    assert controls(h.ws)[-1] == {"t": "pong", "id": 7, "ts": 123}
    await finish(h, baseline)


async def test_pongs_keep_arrival_order_at_the_head(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.ws.pause_sends()
    first = asyncio.create_task(h.conn.send_app({"n": 1}))  # in flight
    second = asyncio.create_task(h.conn.send_app({"n": 2}))  # queued
    await settle()
    h.push({"t": "ping", "id": 1}, {"t": "ping", "id": 2}, {"t": "ping", "id": 3})
    await settle()
    h.ws.resume_sends()
    await asyncio.gather(first, second)
    tail = controls(h.ws)[-5:]
    assert [(m["t"], m.get("id", m.get("body"))) for m in tail] == [
        ("app", {"n": 1}),
        ("pong", 1),
        ("pong", 2),
        ("pong", 3),
        ("app", {"n": 2}),
    ]
    await finish(h, baseline)


async def test_data_round_robin_one_chunk_per_stream(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1), encode_open(3))
    await settle()
    a, b = h.streams
    h.ws.pause_sends()
    wa = asyncio.create_task(a.write(bytes(40_000)))
    wb = asyncio.create_task(b.write(bytes(20_000)))
    await settle()
    h.ws.resume_sends()
    await asyncio.gather(wa, wb)
    data = [f for f in frames(h.ws) if f.type == FrameType.DATA and f.stream_id]
    assert [(f.stream_id, len(f.payload)) for f in data] == [
        (1, 16_384),
        (3, 16_384),
        (1, 16_384),
        (3, 3616),
        (1, 7232),
    ]
    await finish(h, baseline)


async def test_barrier_flushes_queued_frames_before_retire_close(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = await connect()
    h.ws.pause_sends()
    first = asyncio.create_task(h.conn.send_app({"n": 1}))
    second = asyncio.create_task(h.conn.send_app({"n": 2}))
    await settle()
    h.conn.retire(10_000)  # no streams: barrier, then NO_ERROR
    await settle()
    assert not h.conn.closed
    h.ws.resume_sends()
    await asyncio.gather(first, second)
    await h.conn.wait_closed()
    assert kinds(h.ws)[-3:] == ["app", "app", "error"]
    assert controls(h.ws)[-1]["code"] == 0 and h.ws.close_calls[0][0] == 1000
    await finish(h, baseline)


async def test_send_control_rejects_oversize_and_after_close(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = await connect()
    with pytest.raises(ValueError):
        await h.conn.send_app({"x": "y" * 20_000})
    h.ws.pause_sends()
    blocked = asyncio.create_task(h.conn.send_app({"n": 1}))
    queued = asyncio.create_task(h.conn.send_app({"n": 2}))
    await settle()
    h.conn.fail(ConnError(ErrorCode.INTERNAL_ERROR, "boom"))
    with pytest.raises(ConnError):
        await queued
    h.ws.resume_sends()
    await blocked  # was already in the socket
    with pytest.raises(ConnError):
        await h.conn.send_app({"n": 3})
    await finish(h, baseline)


async def test_socket_send_failure_tears_down(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.ws.abort()
    with pytest.raises(ConnError):
        await h.conn.send_app({"n": 1})
    await settle()
    assert h.info.ws_code == 1006 and not h.info.local
    await finish(h, baseline)


# --- delivery -------------------------------------------------------------------------------


async def test_delivery_is_ordered_serial_and_flushes_after_teardown(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = await connect()
    order: list[str] = []
    gate = asyncio.Event()
    active = 0

    async def slow(name: str) -> None:
        nonlocal active
        active += 1
        assert active == 1, "handlers overlapped"
        await gate.wait()
        order.append(name)
        active -= 1

    async def on_stream(s: MixerStream) -> None:
        await slow(f"stream{s.id}")

    h.conn.on_stream = on_stream
    h.conn.on("app", lambda body: slow(f"app{body['n']}"))
    h.conn.on("drain", lambda d: slow(f"drain:{d.reason}"))
    h.conn.on("close", lambda _: order.append("close"))
    h.push(encode_open(1), {"t": "app", "body": {"n": 1}})
    h.push({"t": "drain", "reason": "rollout", "last_stream_id": 1})
    h.push({"t": "app", "body": {"n": 2}}, {"t": "error", "code": 12, "message": "bye"})
    await settle()
    assert h.conn.closed and order == ["close"]
    gate.set()
    await settle()
    assert order == ["close", "stream1", "app1", "drain:rollout", "app2"]
    await finish(h, baseline)


async def test_cancel_delivery_drops_backlog_and_cancels_handler(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = await connect()
    started: list[int] = []
    cancelled: list[int] = []

    async def handler(body: dict[str, Any]) -> None:
        started.append(body["n"])
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(body["n"])
            raise

    h.conn.on("app", handler)
    h.push(*({"t": "app", "body": {"n": n}} for n in range(3)))
    await settle()
    h.conn.fail(ConnError(ErrorCode.INTERNAL_ERROR, "gone"))
    await h.conn.wait_closed()
    assert started == [0]
    h.conn.cancel_delivery()
    await settle()
    assert cancelled == [0] and started == [0]  # backlog 1, 2 dropped
    h.conn.cancel_delivery()  # idempotent
    await finish(h, baseline)


async def test_handler_errors_are_counted_and_reported(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    seen: list[HandlerErrorInfo] = []
    got: list[int] = []
    h.conn.on("handler_error", seen.append)

    async def bad(body: dict[str, Any]) -> None:
        raise RuntimeError(f"bad {body['n']}")

    h.conn.on("app", bad)
    h.conn.on("app", lambda body: got.append(body["n"]))
    h.conn.once("app", lambda body: got.append(-body["n"]))
    h.push({"t": "app", "body": {"n": 1}}, {"t": "app", "body": {"n": 2}})
    await settle()
    assert got == [1, -1, 2]
    assert [(i.event, str(i.error)) for i in seen] == [("app", "bad 1"), ("app", "bad 2")]
    assert h.conn.stats().handler_errors == 2
    await finish(h, baseline)


async def test_delivery_queue_overflow_is_enhance_your_calm(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = await connect()
    gate = asyncio.Event()
    delivered = 0

    async def blocked(_: object) -> None:
        nonlocal delivered
        await gate.wait()
        delivered += 1

    h.conn.on("app", blocked)
    h.push(*({"t": "app", "body": {"i": i}} for i in range(99)))
    await settle()
    await h.clock.advance(2000)  # refill the stream-0 bucket; the queue is the limit now
    h.push(*({"t": "app", "body": {"i": i}} for i in range(99, 140)))
    await settle()
    assert h.info.error_name == "ENHANCE_YOUR_CALM"
    assert "delivery queue" in h.info.message
    gate.set()
    await settle()
    assert delivered == 129  # the one in the handler plus a full queue of 128
    await finish(h, baseline)


# --- control channel ------------------------------------------------------------------------


async def test_token_bucket_burst(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()  # the welcome took one token
    h.push(*({"t": "app", "body": {"i": i}} for i in range(99)))
    await settle()
    assert not h.conn.closed
    h.push({"t": "app", "body": {"i": 99}})
    await settle()
    assert (h.info.error_name, h.info.ws_code) == ("ENHANCE_YOUR_CALM", 4009)
    await finish(h, baseline)


async def test_token_bucket_refills_at_50_per_second(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(*({"t": "app", "body": {"i": i}} for i in range(99)))
    await settle()
    await h.clock.advance(1000)
    h.push(*({"t": "app", "body": {"i": i}} for i in range(50)))
    await settle()
    assert not h.conn.closed
    h.push({"t": "app", "body": {}})
    await settle()
    assert h.info.error_name == "ENHANCE_YOUR_CALM"
    await finish(h, baseline)


async def test_pong_bookkeeping(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect(random=lambda: 0.0)
    pongs: list[PongInfo] = []
    h.conn.on("pong", pongs.append)
    await h.clock.advance(0)
    await h.clock.advance(30_000)
    assert [m["id"] for m in controls(h.ws) if m["t"] == "ping"] == [0, 1]
    await h.clock.advance(250)
    h.push({"t": "pong", "id": 1}, {"t": "pong", "id": 1}, {"t": "pong", "id": 0})
    await settle()
    assert pongs == [PongInfo(1, 250), PongInfo(0, 30_250)]
    h.push({"t": "pong", "id": 0})
    await settle()
    assert h.conn.stats().duplicate_pongs == 2 and not h.conn.closed
    h.push({"t": "pong", "id": 2})
    await settle()
    assert h.info.error_name == "PROTOCOL_ERROR"
    await finish(h, baseline)


async def test_drain_received(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    drains: list[DrainInfo] = []
    h.conn.on("drain", drains.append)
    h.push(encode_open(1))
    h.push({"t": "drain", "reason": "weird", "last_stream_id": 5, "deadline_ms": 100})
    h.push({"t": "drain", "reason": "rollout", "last_stream_id": 9, "message": "m"})
    await settle()
    assert h.conn.is_draining()
    assert drains == [
        DrainInfo("maintenance", 5, 100, None, None),
        DrainInfo("rollout", 5, None, None, "m"),  # the enforced minimum, not this drain's 9
    ]
    assert h.conn.stats().unknown_drain_reasons == 1
    h.push(encode_open(5))  # at the boundary: fine
    await settle()
    assert not h.conn.closed
    h.push(encode_open(7))  # above the smallest last_stream_id seen
    await settle()
    assert h.info.error_name == "PROTOCOL_ERROR" and h.conn.stats().drain_violations == 1
    await finish(h, baseline)


# --- keepalive ------------------------------------------------------------------------------


async def test_first_ping_jitter_uses_injected_random(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect(random=lambda: 0.25)

    def pings() -> list[int]:
        return [m["id"] for m in controls(h.ws) if m["t"] == "ping"]

    await h.clock.advance(7_499)
    assert pings() == []
    await h.clock.advance(1)
    assert pings() == [0]
    await h.clock.advance(29_999)
    assert pings() == [0]
    await h.clock.advance(1)
    assert pings() == [0, 1]
    assert "ts" not in controls(h.ws)[-1]
    await finish(h, baseline)


async def test_watchdog_fires_only_when_strictly_past_timeout(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = await connect({"ping_interval": 10_000, "ping_timeout": 20_000}, random=lambda: 0.0)
    await h.clock.advance(20_000)
    assert not h.conn.closed  # elapsed == ping_timeout
    await h.clock.advance(1_000)
    assert (h.info.error_name, h.info.ws_code) == ("KEEPALIVE_TIMEOUT", 4013)
    assert controls(h.ws)[-1]["code"] == ErrorCode.KEEPALIVE_TIMEOUT
    await finish(h, baseline)


async def test_pongs_keep_the_watchdog_quiet(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect({"ping_interval": 10_000, "ping_timeout": 20_000}, random=lambda: 0.0)
    for _ in range(10):
        await h.clock.advance(10_000)
        last_ping = [m for m in controls(h.ws) if m["t"] == "ping"][-1]
        h.push({"t": "pong", "id": last_ping["id"]})
        await settle()
    assert not h.conn.closed
    await finish(h, baseline)


# --- close paths ----------------------------------------------------------------------------


async def test_graceful_close_sequence(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1))
    await settle()
    s = h.streams[0]
    closing = asyncio.create_task(h.conn.close())
    await settle()
    assert controls(h.ws)[-1] == {
        "t": "drain",
        "reason": "client_requested",
        "last_stream_id": 0,
        "deadline_ms": 5000,
    }
    h.push(encode_open(3))  # crossed our drain on the wire
    await settle()
    assert not h.conn.closed and kinds(h.ws)[-1] == "RESET:3"
    assert reset_code(frames(h.ws)[-1]) == ErrorCode.REFUSED_STREAM
    h.push(encode_close(1))
    await settle()
    await s.close_write()
    await closing
    assert kinds(h.ws)[-4:] == ["drain", "RESET:3", "CLOSE:1", "error"]
    assert controls(h.ws)[-1] == {"t": "error", "code": 0, "message": "client closing"}
    assert h.ws.close_calls == [(1000, "client closing")]
    assert await s.wait_closed() == Finished()
    assert h.conn.stats().refused_opens == 1
    await finish(h, baseline)


async def test_graceful_close_grace_expires(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1))
    await settle()
    closing = asyncio.create_task(h.conn.close(grace_ms=3000))
    await h.clock.advance(2_999)
    assert not closing.done()
    await h.clock.advance(1)
    await closing
    assert isinstance(await h.streams[0].wait_closed(), ConnectionLost)
    assert h.ws.close_calls[0][0] == 1000
    await h.conn.close()  # idempotent once closed
    await finish(h, baseline)


async def test_close_before_welcome(baseline: set[asyncio.Task[Any]]) -> None:
    h = new_conn()
    hs = asyncio.create_task(h.conn.handshake())
    await settle()
    await h.conn.close()
    with pytest.raises(ConnError):
        await hs
    assert h.info.ws_code == 1000 and h.info.phase == "handshake"
    await finish(h, baseline)


async def test_cancelled_close_then_close_again(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1))
    await settle()
    first = asyncio.create_task(h.conn.close())
    await settle()
    assert controls(h.ws)[-1]["t"] == "drain" and not h.conn.closed
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    # Cancelling skips the rest of the grace period instead of leaving the drain hanging.
    assert h.conn.closed and h.info.error_code == ErrorCode.NO_ERROR
    await h.conn.close()  # completes, no hang
    assert h.ws.close_calls[0][0] == 1000
    assert isinstance(await h.streams[0].wait_closed(), ConnectionLost)
    await finish(h, baseline)


async def test_close_after_shared_close_task_was_cancelled(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = await connect()
    h.push(encode_open(1))
    await settle()
    waiter = asyncio.create_task(h.conn.close())
    await settle()
    shared = h.conn._close_task
    assert shared is not None
    shared.cancel()  # e.g. a host stop cancelling every task
    await waiter  # this caller was not cancelled: clean return
    await h.conn.close()  # and neither is a later one
    assert shared.cancelled() and h.conn.closed
    assert h.info.error_code == ErrorCode.NO_ERROR and h.ws.close_calls[0][0] == 1000
    await finish(h, baseline)


async def test_concurrent_closes_share_one_shutdown(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1))
    await settle()
    a = asyncio.create_task(h.conn.close(grace_ms=4000))
    b = asyncio.create_task(h.conn.close(grace_ms=1))
    await h.clock.advance(3_999)
    assert not a.done() and not b.done()  # the first call's grace applies to both
    await h.clock.advance(1)
    await asyncio.gather(a, b)
    assert [m["t"] for m in controls(h.ws)].count("drain") == 1
    assert len(h.closes) == 1 and h.ws.close_calls[0][0] == 1000
    await finish(h, baseline)


async def test_close_with_message_overrides_pending_graceful_close(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = await connect()
    h.push(encode_open(1))
    await settle()
    graceful = asyncio.create_task(h.conn.close())
    await settle()
    await h.conn.close_with_message("quota")
    await graceful
    assert (h.info.error_code, h.ws.close_calls[0][0]) == (14, 4014)
    await finish(h, baseline)


async def test_close_with_message(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1))
    await settle()
    errors: list[Any] = []
    h.conn.on("error", errors.append)
    msg = "shutting down: " + "é" * 600
    await h.conn.close_with_message(msg)
    sent = controls(h.ws)[-1]
    assert sent["code"] == 14 and sent["message"] == msg[:1024]
    code, reason = h.ws.close_calls[0]
    assert code == 4014 and len(reason.encode()) <= 123 and msg.startswith(reason)
    info = h.info
    assert (info.error_code, info.error_name, info.local) == (14, "APPLICATION_CLOSE", True)
    assert errors == []  # our own close is not an error
    lost = await h.streams[0].wait_closed()
    assert isinstance(lost, ConnectionLost) and lost.error.code == 14
    await finish(h, baseline)


async def test_fail_emits_error_and_streams_lose_the_connection(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = await connect()
    errors: list[WsMixerError] = []
    h.conn.on("error", errors.append)
    h.push(encode_open(1), encode_data(1, b"buffered"))
    await settle()
    s = h.streams[0]
    err = ConnError(ErrorCode.INTERNAL_ERROR, "boom")
    h.conn.fail(err)
    h.conn.fail(ConnError(ErrorCode.PROTOCOL_ERROR, "ignored"))
    assert errors == [err] and len(h.closes) == 1
    assert await s.read() == b"buffered"
    with pytest.raises(ConnError):
        await s.read()
    await finish(h, baseline)


async def test_peer_close_after_welcome(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1), encode_data(1, b"x"), encode_close(1), encode_open(3))
    await settle()
    h.ws.server_close(4012, "draining")
    await settle()
    info = h.info
    assert (info.ws_code, info.error_code, info.close_reason, info.phase) == (
        4012,
        None,
        "draining",
        "connected",
    )
    assert info.received_close and not info.local
    s1, s3 = h.streams
    assert await s1.read() == b"x" and await s1.read() == b""  # CLOSE had arrived
    with pytest.raises(ConnError) as ei:
        await s3.read()
    assert ei.value.code == ErrorCode.GOING_AWAY
    assert [m["t"] for m in controls(h.ws)] == ["hello"]  # nothing answered
    await finish(h, baseline)


async def test_transport_death(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1))
    await settle()
    h.ws.server_drop()
    await settle()
    assert (h.info.ws_code, h.info.received_close, h.info.close_reason) == (1006, False, None)
    with pytest.raises(ConnError):
        await h.streams[0].write(b"x")
    await finish(h, baseline)


# --- retire ---------------------------------------------------------------------------------


async def test_retire_resets_survivors_at_deadline(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1), encode_open(3))
    await settle()
    a, b = h.streams
    h.push(encode_close(3))
    await settle()
    await b.close_write()  # stream 3 finishes in time
    h.conn.retire(6_000)
    await h.clock.advance(5_999)
    assert not h.conn.closed
    await h.clock.advance(1)
    await h.conn.wait_closed()
    assert kinds(h.ws)[-2:] == ["RESET:1", "error"]
    assert reset_code(frames(h.ws)[-2]) == ErrorCode.CANCEL
    assert controls(h.ws)[-1]["code"] == 0 and h.ws.close_calls[0][0] == 1000
    assert await a.wait_closed() == Reset(7, "CANCEL", "connection drained", False)
    assert await b.wait_closed() == Finished()
    await finish(h, baseline)


async def test_retire_closes_early_once_idle(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1))
    await settle()
    h.conn.retire(30_000)
    h.push(encode_reset(1, 7))
    await settle()
    await h.conn.wait_closed()
    assert h.info.error_code == ErrorCode.NO_ERROR and h.clock.now_ms() == 0
    h.conn.retire(1)  # no-op once closed
    await finish(h, baseline)


# --- stats ----------------------------------------------------------------------------------


async def test_bytes_and_protocol_violations(baseline: set[asyncio.Task[Any]]) -> None:
    h = await connect()
    h.push(encode_open(1), encode_data(1, b"12345"))
    await settle()
    stats = h.conn.stats()
    assert stats.bytes_in == len(ctl(WELCOME)) + 8 + 13
    assert stats.bytes_out == sum(len(b) for b in h.ws.sent)
    h.push(encode_data(99, b""))
    await settle()
    assert h.conn.stats().protocol_violations == 1
    assert h.conn.stats().bytes_out == sum(len(b) for b in h.ws.sent)  # error frame included
    await finish(h, baseline)


# --- end to end over socket_pair -------------------------------------------------------------


async def _recv_frame(sock: WebSocketConnection) -> Frame:
    raw = await sock.recv()
    assert isinstance(raw, bytes)
    return decode_frame(raw)


async def _server(sock: WebSocketConnection, script: Callable[[WebSocketConnection], Any]) -> None:
    """A tiny hand-rolled edge: answers hello with WELCOME, then runs `script`."""
    hello = json.loads((await _recv_frame(sock)).payload)
    assert hello["t"] == "hello"
    await sock.send(ctl(WELCOME))
    await script(sock)


async def test_end_to_end_request_response(baseline: set[asyncio.Task[Any]]) -> None:
    client_end, server_end = socket_pair()
    clock = FakeClock()

    async def echo(stream: MixerStream) -> None:
        async def serve() -> None:
            body = b"".join([c async for c in stream])
            await stream.write(body.upper())
            await stream.close_write()

        tasks.append(asyncio.create_task(serve()))

    tasks: list[asyncio.Task[None]] = []
    conn = MixerConn(client_end, token="t", agent=AGENT, clock=clock, random=lambda: 0.5)
    conn.on_stream = echo
    got: list[Frame] = []

    async def script(sock: WebSocketConnection) -> None:
        await sock.send(encode_open(1))
        await sock.send(encode_data(1, b"hello "))
        await sock.send(encode_data(1, b"world"))
        await sock.send(encode_close(1))
        while True:
            f = await _recv_frame(sock)
            got.append(f)
            if f.type == FrameType.CLOSE:
                return

    server = asyncio.create_task(_server(server_end, script))
    await conn.handshake()
    await server
    await asyncio.gather(*tasks)
    assert [(FrameType(f.type).name, f.payload) for f in got] == [
        ("DATA", b"HELLO WORLD"),
        ("CLOSE", b""),
    ]
    closing = asyncio.create_task(conn.close())
    drain = await _recv_frame(server_end)
    error = await _recv_frame(server_end)
    assert json.loads(drain.payload)["t"] == "drain" and json.loads(error.payload)["code"] == 0
    with pytest.raises(WebSocketClosed) as ei:
        await server_end.recv()
    assert ei.value.code == 1000
    await closing
    await finish_conn(conn, clock, baseline)


async def test_end_to_end_flow_control(baseline: set[asyncio.Task[Any]]) -> None:
    client_end, server_end = socket_pair()
    clock = FakeClock()
    conn = MixerConn(
        client_end, token="t", agent=AGENT, clock=clock, random=lambda: 0.5, window=16_384
    )
    streams: list[MixerStream] = []
    conn.on_stream = streams.append
    windows: list[int] = []

    async def script(sock: WebSocketConnection) -> None:
        await sock.send(encode_open(1))
        sent = 0
        credit = 16_384
        while sent < 100_000:
            n = min(4096, credit, 100_000 - sent)
            if n == 0:
                f = await _recv_frame(sock)
                assert f.type == FrameType.WINDOW
                windows.append(window_increment(f))
                credit += window_increment(f)
                continue
            await sock.send(encode_data(1, bytes(n)))
            sent += n
            credit -= n
        await sock.send(encode_close(1))

    server = asyncio.create_task(_server(server_end, script))
    await conn.handshake()
    await settle()
    total = 0
    async for chunk in streams[0]:
        total += len(chunk)
    await server
    assert total == 100_000 and all(w >= 8192 for w in windows)
    await finish_conn(conn, clock, baseline)
