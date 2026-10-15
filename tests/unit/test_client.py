"""MixerClient over FakeConnector on a FakeClock: one test per reconnect-table row and more.

Every test ends through `finish()`, which closes the client and asserts that no timer and no
task outlived it. `random` is pinned to 0.5, so full jitter after the n-th failure is
0.5 * min(cap, base * 2**n).
"""

from __future__ import annotations

import asyncio
import json
import ssl
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

from ws_mixer.client import (
    DisconnectReason,
    MixerClient,
    ReconnectInfo,
    ReconnectOptions,
    _ctx_from_close,
    connect,
    full_jitter_delay,
    token_refresh_requested,
)
from ws_mixer.conn import CloseInfo, Timing
from ws_mixer.control import encode_control
from ws_mixer.errors import ConnError, ErrorCode, TokenUnavailableError, WsMixerError
from ws_mixer.frame import FrameType, decode_frame, encode_data, encode_open, reset_code
from ws_mixer.socket import (
    SUBPROTOCOL,
    ProxyRejected,
    UnexpectedSubprotocol,
    WebSocketConnection,
    WebSocketInit,
)
from ws_mixer.testing import FakeClock, FakeConnector, FakeWebSocket, settle

pytestmark = pytest.mark.asyncio

URL = "wss://edge.test/v1/tunnel"
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


def controls(ws: FakeWebSocket) -> list[dict[str, Any]]:
    out = []
    for raw in ws.sent:
        f = decode_frame(raw)
        if f.type == FrameType.DATA and f.stream_id == 0:
            out.append(json.loads(f.payload))
    return out


class Harness:
    def __init__(self, **kw: Any) -> None:
        self.clock = FakeClock()
        self.fc = FakeConnector()
        self.reports: list[DisconnectReason] = []
        self.retries: list[ReconnectInfo] = []
        self.fatals: list[WsMixerError] = []
        self.superseded: list[bool] = []
        kw.setdefault("token", "tok")
        self.client = MixerClient(
            URL,
            web_socket=self.fc,
            clock=self.clock,
            random=lambda: 0.5,
            on_disconnect=self.reports.append,
            on_reconnect=self.retries.append,
            **kw,
        )
        self.client.on("fatal", self.fatals.append)
        self.client._on_disconnect_from = lambda _r, s: self.superseded.append(s)
        self.start_task: asyncio.Task[None] | None = None

    def start(self) -> asyncio.Task[None]:
        self.start_task = asyncio.create_task(self.client.start())
        return self.start_task

    async def sock(self, i: int) -> FakeWebSocket:
        return await asyncio.wait_for(self.fc.next_socket(i), 1)

    async def welcome(self, i: int, **overrides: Any) -> FakeWebSocket:
        ws = await self.sock(i)
        ws.open()
        await settle()
        ws.push(ctl({**WELCOME, **overrides}))
        await settle()
        return ws

    @property
    def delays(self) -> list[tuple[int, float, str]]:
        return [(r.attempt, r.delay_ms, r.cause) for r in self.retries]


@pytest_asyncio.fixture
async def baseline() -> AsyncIterator[set[asyncio.Task[Any]]]:
    yield set(asyncio.all_tasks())


async def finish(h: Harness, baseline: set[asyncio.Task[Any]]) -> None:
    await h.client.close()
    await settle()
    if h.start_task is not None:
        assert h.start_task.done()
        if not h.start_task.cancelled():
            h.start_task.exception()
    assert h.client.state == "closed"
    assert h.clock.pending() == 0, "a timer outlived the client"
    leaked = set(asyncio.all_tasks()) - baseline - {asyncio.current_task()}
    assert not leaked, leaked


async def started(h: Harness) -> FakeWebSocket:
    t = h.start()
    ws = await h.welcome(0)
    await t
    return ws


# --- dialing, identity, token provider -----------------------------------------------------


async def test_connect_sends_identity_and_resolves_on_welcome(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness(headers={"X-Mcpwarp-Integration": "anki"}, meta={"client": "sdk-python"})
    ws = await started(h)
    assert h.client.state == "connected" and h.client.current_conn() is not None
    _, init = h.fc.calls[0]
    assert init.protocols == ("ws-mixer.v1",)
    assert init.headers["Authorization"] == "Bearer tok"
    assert init.headers["User-Agent"].startswith("ws-mixer-py/")
    assert init.headers["X-Mcpwarp-Integration"] == "anki"
    hello = controls(ws)[0]
    assert hello["token"] == "tok" and hello["agent"]["sdk"] == "ws-mixer-py"
    assert hello["meta"] == {"client": "sdk-python"}
    assert h.client.stats() is not None
    await finish(h, baseline)
    assert len(h.reports) == 1 and not h.reports[0].fatal
    assert (h.reports[0].ws_code, h.reports[0].error_code) == (1000, None)


async def test_connect_function(baseline: set[asyncio.Task[Any]]) -> None:
    clock, fc = FakeClock(), FakeConnector()
    task = asyncio.create_task(connect(URL, token="t", web_socket=fc, clock=clock))
    ws = await fc.next_socket(0)
    ws.open()
    await settle()
    ws.push(ctl(WELCOME))
    client = await task
    assert client.state == "connected"
    await client.close()
    await settle()
    assert clock.pending() == 0


async def test_provider_called_on_every_dial_sync_and_async(
    baseline: set[asyncio.Task[Any]],
) -> None:
    calls: list[bool] = []

    async def provider() -> str:
        calls.append(token_refresh_requested())
        return f"tok{len(calls)}"

    h = Harness(token=provider)
    ws = await started(h)
    ws.server_drop()
    await settle()
    await h.clock.advance(1000)
    await h.welcome(1)
    assert [c[1].headers["Authorization"] for c in h.fc.calls] == ["Bearer tok1", "Bearer tok2"]
    assert calls == [False, False]
    await finish(h, baseline)

    h2 = Harness(token=lambda: "sync")
    await started(h2)
    assert h2.fc.calls[0][1].headers["Authorization"] == "Bearer sync"
    await finish(h2, baseline)


async def test_provider_failure_is_fatal(baseline: set[asyncio.Task[Any]]) -> None:
    boom = RuntimeError("no creds")

    def provider() -> str:
        raise boom

    h = Harness(token=provider)
    with pytest.raises(WsMixerError) as ei:
        await h.start()
    assert ei.value.fatal and ei.value.__cause__ is boom
    assert h.fc.dials == 0
    [r] = h.reports
    assert (r.phase, r.fatal, r.cause, r.error_code, r.message) == (
        "dial",
        True,
        boom,
        None,
        "no creds",
    )
    await finish(h, baseline)


@pytest.mark.parametrize("wrapped", [False, True])
async def test_marked_provider_failure_backs_off(
    baseline: set[asyncio.Task[Any]], wrapped: bool
) -> None:
    n = 0

    def provider() -> str:
        nonlocal n
        n += 1
        if n == 1:
            if wrapped:
                raise ValueError("refresh failed") from TokenUnavailableError("offline")
            raise TokenUnavailableError("offline")
        return "tok"

    h = Harness(token=provider)
    h.start()
    await settle()
    [r] = h.reports
    assert (r.phase, r.fatal) == ("dial", False)
    assert h.delays == [(1, 1000, r.message)]
    assert h.fc.dials == 0
    await h.clock.advance(1000)
    await h.welcome(0)
    assert h.start_task is not None and h.start_task.done()
    await finish(h, baseline)


# --- rejected token -------------------------------------------------------------------------


async def test_401_static_token_is_fatal_at_once(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    h.start()
    (await h.sock(0)).reject(401)
    with pytest.raises(WsMixerError):
        assert h.start_task is not None
        await h.start_task
    [r] = h.reports
    assert (r.phase, r.fatal, r.http_status, r.error_code, r.error_name) == (
        "dial",
        True,
        401,
        None,
        None,
    )
    assert h.fc.dials == 1 and h.retries == []
    await finish(h, baseline)


async def test_401_provider_refreshes_once_then_fatal(baseline: set[asyncio.Task[Any]]) -> None:
    refresh: list[bool] = []

    def provider() -> str:
        refresh.append(token_refresh_requested())
        return "tok"

    h = Harness(token=provider)
    h.start()
    (await h.sock(0)).reject(401)
    (await h.sock(1)).reject(401)
    with pytest.raises(WsMixerError) as ei:
        assert h.start_task is not None
        await h.start_task
    assert ei.value.fatal and ei.value.http_status == 401
    assert refresh == [False, True]
    assert h.delays == [(1, 0, "token-refresh")]
    assert h.fc.dials == 2
    [r] = h.reports
    assert r.fatal and r.http_status == 401
    assert not token_refresh_requested()
    await finish(h, baseline)


async def test_4011_before_welcome_refreshes_then_succeeds(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness(token=lambda: "tok")
    h.start()
    ws = await h.sock(0)
    ws.open()
    await settle()
    ws.push(ctl({"t": "error", "code": 11, "message": "expired"}))
    await h.welcome(1)
    assert h.start_task is not None and h.start_task.done()
    assert h.delays == [(1, 0, "token-refresh")]
    assert h.reports == []
    await finish(h, baseline)


async def test_refresh_budget_rearms_only_at_stability(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness(token=lambda: "tok")
    h.start()
    (await h.sock(0)).reject(401)
    ws = await h.welcome(1)
    # Unstable: the budget stays spent, so the next 401 is fatal.
    ws.server_drop()
    await settle()
    await h.clock.advance(1000)
    (await h.sock(2)).reject(401)
    await settle()
    assert h.fatals and h.reports[-1].fatal and h.fc.dials == 3
    await finish(h, baseline)

    h = Harness(token=lambda: "tok")
    h.start()
    (await h.sock(0)).reject(401)
    ws = await h.welcome(1)
    await h.clock.advance(10_000)
    ws.server_drop()
    await settle()
    assert h.delays[-1] == (1, 1000, "socket closed with code 1006")
    await h.clock.advance(1000)
    (await h.sock(2)).reject(401)
    await h.welcome(3)
    assert h.delays[-1] == (1, 0, "token-refresh") and not h.fatals
    await finish(h, baseline)


async def test_4011_after_welcome_is_fatal(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness(token=lambda: "tok")
    ws = await started(h)
    ws.push(ctl({"t": "error", "code": 11, "message": "revoked"}))
    await settle()
    [r] = h.reports
    assert (r.phase, r.fatal, r.ws_code, r.error_name) == ("connected", True, 4011, "UNAUTHORIZED")
    assert h.client.state == "closed" and h.fc.dials == 1
    assert h.fatals[0].code == ErrorCode.UNAUTHORIZED
    await finish(h, baseline)


# --- dial failures --------------------------------------------------------------------------


@pytest.mark.parametrize("status", [403, 404])
async def test_403_404_fatal(baseline: set[asyncio.Task[Any]], status: int) -> None:
    h = Harness(token=lambda: "tok")
    h.start()
    (await h.sock(0)).reject(status)
    await settle()
    [r] = h.reports
    assert (r.fatal, r.http_status, r.error_code) == (True, status, None)
    assert h.fc.dials == 1
    await finish(h, baseline)


async def test_proxy_rejection_is_fatal(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    h.start()
    ws = await h.sock(0)
    ws.fail(ProxyRejected(407))
    await settle()
    [r] = h.reports
    assert r.fatal and r.http_status == 407 and "proxy" in r.message
    await finish(h, baseline)


async def test_429_honours_retry_after_and_5xx_backs_off(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness()
    h.start()
    (await h.sock(0)).reject(429, {"Retry-After": "7"})
    await settle()
    assert h.delays == [(1, 7000, h.reports[0].message)]
    assert (h.reports[0].fatal, h.reports[0].http_status) == (False, 429)
    await h.clock.advance(7000)
    (await h.sock(1)).reject(503)
    await settle()
    assert h.delays[-1][:2] == (2, 2000)
    await h.clock.advance(2000)
    await h.welcome(2)
    await finish(h, baseline)


@pytest.mark.parametrize("echo", [None, "other"])
async def test_subprotocol_failure_is_fatal_unsupported(
    baseline: set[asyncio.Task[Any]], echo: str | None
) -> None:
    h = Harness()
    h.start()
    ws = await h.sock(0)
    if echo is None:
        ws.open(None)
    else:
        ws.fail(UnexpectedSubprotocol(echo))
    await settle()
    [r] = h.reports
    assert (r.phase, r.fatal, r.ws_code, r.error_code, r.error_name) == (
        "dial",
        True,
        None,
        10,
        "UNSUPPORTED",
    )
    await finish(h, baseline)


async def test_network_error_and_connect_timeout_back_off(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness(reconnect=ReconnectOptions(connect_timeout_ms=3000))
    h.start()
    (await h.sock(0)).fail()
    await settle()
    assert (h.reports[0].phase, h.reports[0].fatal) == ("dial", False)
    await h.clock.advance(1000)
    ws = await h.sock(1)
    await h.clock.advance(3000)
    assert ws.dial_cancelled
    assert h.reports[1].message == "connect timed out after 3000ms"
    assert h.delays[-1][:2] == (2, 2000)
    await h.clock.advance(2000)
    await h.welcome(2)
    await finish(h, baseline)


async def test_hello_timeout_is_handshake_phase(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    h.start()
    (await h.sock(0)).open()
    await h.clock.advance(10_000)
    [r] = h.reports
    assert (r.phase, r.fatal, r.ws_code, r.error_name, r.protocol_error) == (
        "handshake",
        False,
        4001,
        "PROTOCOL_ERROR",
        False,  # a slow server is not an SDK bug
    )
    assert r.close_reason is None
    assert h.delays[0][2].startswith("handshake failed: ")
    await finish(h, baseline)


async def test_malformed_welcome_is_a_protocol_error(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    h.start()
    ws = await h.sock(0)
    ws.open()
    await settle()
    ws.push(ctl({**WELCOME, "window": "big"}))
    await settle()
    [r] = h.reports
    assert (r.phase, r.ws_code, r.protocol_error) == ("handshake", 4001, True)
    await finish(h, baseline)


async def test_tls_verification_failure_is_retryable(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    h.start()
    (await h.sock(0)).fail(ssl.SSLCertVerificationError("certificate verify failed"))
    await settle()
    [r] = h.reports
    assert (r.phase, r.fatal) == ("dial", False)
    assert h.delays[0][:2] == (1, 1000)
    await finish(h, baseline)


async def test_bad_connector_config_is_fatal(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    h.start()
    (await h.sock(0)).fail(ValueError("bad proxy URL"))
    await settle()
    [r] = h.reports
    assert r.fatal and h.retries == []
    await finish(h, baseline)


@pytest.mark.parametrize("how", ["close", "abort"])
async def test_start_after_close_raises(baseline: set[asyncio.Task[Any]], how: str) -> None:
    h = Harness()
    if how == "close":
        await h.client.close()
    else:
        h.client.abort()
    with pytest.raises(WsMixerError) as ei:
        await asyncio.wait_for(h.client.start(), 1)
    assert (ei.value.code, ei.value.fatal, ei.value.message) == (
        ErrorCode.NO_ERROR,
        False,
        "client is closed",
    )
    assert h.fc.dials == 0
    await finish(h, baseline)


# --- connected-phase reconnect table ---------------------------------------------------------


async def _drop_with(ws: FakeWebSocket, kind: str, code: int) -> None:
    if kind == "bare":
        ws.server_close(code, "bye")
    else:
        ws.push(ctl({"t": "error", "code": code - 4000, "message": "bye"}))
    await settle()


@pytest.mark.parametrize(
    ("kind", "code", "delay", "cause"),
    [
        ("bare", 4012, 1000, "going_away"),
        ("bare", 1001, 1000, "going_away"),
        ("error", 4012, 1000, "going_away"),
        ("error", 4009, 30_000, "enhance_your_calm"),
        ("bare", 4014, 30_000, "application_close"),
        ("bare", 4002, 1000, None),
        ("bare", 4001, 1000, None),
        ("bare", 4321, 1000, None),
        ("bare", 1006, 1000, None),
    ],
)
async def test_connected_close_policy(
    baseline: set[asyncio.Task[Any]], kind: str, code: int, delay: float, cause: str | None
) -> None:
    h = Harness()
    ws = await started(h)
    await _drop_with(ws, kind, code)
    [r] = h.reports
    assert (r.phase, r.fatal, r.ws_code) == ("connected", False, code)
    assert r.protocol_error == (code == 4001)
    [(attempt, d, c)] = h.delays
    assert (attempt, d) == (1, delay)
    if cause is not None:
        assert c == cause
    await h.clock.advance(delay)
    await h.welcome(1)
    assert h.client.state == "connected"
    await finish(h, baseline)


async def test_4010_after_welcome_is_fatal(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    ws = await started(h)
    ws.server_close(4010, "nope")
    await settle()
    [r] = h.reports
    assert (r.fatal, r.error_name, r.close_reason) == (True, "UNSUPPORTED", "nope")
    assert h.retries == []
    await finish(h, baseline)


async def test_4013_one_immediate_retry_then_backoff(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    ws = await started(h)
    await _drop_with(ws, "bare", 4013)
    assert h.delays == [(1, 0, "keepalive_timeout")]
    ws = await h.welcome(1)
    await _drop_with(ws, "bare", 4013)
    assert h.delays[-1] == (2, 2000, "keepalive_timeout")
    await h.clock.advance(2000)
    ws = await h.welcome(2)
    await h.clock.advance(10_000)  # stable: budget and attempt re-armed
    await _drop_with(ws, "bare", 4013)
    assert h.delays[-1] == (1, 0, "keepalive_timeout")
    await h.welcome(3)
    await finish(h, baseline)


async def test_stability_resets_attempt_per_connection(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    ws = await started(h)
    for i in range(1, 4):
        await h.clock.advance(9_999)
        ws.server_drop()
        await settle()
        assert h.delays[-1][0] == i
        await h.clock.advance(h.delays[-1][1])
        ws = await h.welcome(i)
    await h.clock.advance(10_000)
    ws.server_drop()
    await settle()
    assert h.delays[-1][:2] == (1, 1000)
    await h.clock.advance(1000)
    await h.welcome(4)
    await finish(h, baseline)


async def test_max_attempts_exhaustion_is_one_merged_report(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness(reconnect=ReconnectOptions(max_attempts=2))
    h.start()
    for i in range(3):
        if i:
            await h.clock.advance(h.delays[-1][1])
        (await h.sock(i)).fail()
        await settle()
    assert [r.fatal for r in h.reports] == [False, False, True]
    assert h.reports[-1].message.startswith("max reconnect attempts (2) exhausted")
    assert h.fc.dials == 3 and len(h.fatals) == 1
    with pytest.raises(WsMixerError):
        assert h.start_task is not None
        await h.start_task
    await finish(h, baseline)


async def test_max_attempts_zero_never_reconnects(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness(reconnect=ReconnectOptions(max_attempts=0))
    ws = await started(h)
    ws.server_drop()
    await settle()
    [r] = h.reports
    assert r.fatal and r.ws_code == 1006 and h.retries == []
    await h.clock.advance(120_000)
    assert h.fc.dials == 1
    await finish(h, baseline)


# --- drain ----------------------------------------------------------------------------------


async def test_drain_hand_over_and_retire_timing(baseline: set[asyncio.Task[Any]]) -> None:
    drains: list[Any] = []
    h = Harness(on_drain=drains.append)
    old = await started(h)
    old_conn = h.client.current_conn()
    assert old_conn is not None
    old.push(encode_open(1))
    await settle()
    old.push(ctl({"t": "drain", "reason": "rollout", "last_stream_id": 1, "deadline_ms": 4000}))
    await settle()
    assert h.delays == [(0, 1000, "drain")] and len(drains) == 1
    await h.clock.advance(1000)
    await h.welcome(1)
    assert h.client.current_conn() is not old_conn
    assert not old_conn.closed
    await h.clock.advance(4999)
    assert not old_conn.closed
    await h.clock.advance(1)
    await settle()
    resets = [decode_frame(m) for m in old.sent if decode_frame(m).type == FrameType.RESET]
    assert [reset_code(f) for f in resets] == [ErrorCode.CANCEL]
    assert old_conn.closed
    [r] = h.reports
    assert (r.fatal, r.ws_code, h.superseded) == (False, 1000, [True])
    assert h.client.state == "connected" and h.client._attempt == 0
    await finish(h, baseline)


async def test_stability_timer_survives_old_conn_retire(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness()
    ws = await started(h)
    ws.server_drop()
    await settle()
    assert h.client._attempt == 1
    await h.clock.advance(h.delays[-1][1])
    c1 = await h.welcome(1)
    c1.push(ctl({"t": "drain", "reason": "rollout", "last_stream_id": 0, "deadline_ms": 0}))
    await settle()
    await h.clock.advance(1000)
    await h.welcome(2)
    await h.clock.advance(1000)  # the old connection retires (deadline 0 + 1000) and closes
    assert c1.closed and h.client._attempt == 1
    await h.clock.advance(9000)
    assert h.client._attempt == 0, "the replacement's stability timer was cancelled"
    await finish(h, baseline)


async def test_drain_then_immediate_4012_is_still_a_drain(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness()
    ws = await started(h)
    ws.push(ctl({"t": "drain", "reason": "rollout", "last_stream_id": 0}))
    ws.server_close(4012, "drained")
    await settle()
    assert h.delays == [(0, 1000, "drain")] and h.client._attempt == 0
    [r] = h.reports
    assert (r.fatal, r.ws_code) == (False, 4012)
    await h.clock.advance(1000)
    await h.welcome(1)
    assert h.fc.dials == 2 and h.client._attempt == 0
    await finish(h, baseline)


async def test_stop_dialing_beats_a_zero_delay_drain_dial(
    baseline: set[asyncio.Task[Any]],
) -> None:
    # As mcpwarp's take-over does: on drain{replaced}, stop dialing synchronously from the
    # handler, then close in the background.
    closing: list[asyncio.Task[None]] = []

    def take_over(info: Any) -> None:
        if info.reason == "replaced":
            h.client._stop_dialing()
            closing.append(asyncio.create_task(h.client._close_now()))

    h = Harness(on_drain=take_over)
    h.client._random = lambda: 0.0
    ws = await started(h)
    ws.push(ctl({"t": "drain", "reason": "replaced", "last_stream_id": 0}))
    await settle()
    assert h.delays == [], "the cancelled replacement dial must not even report"
    await closing[0]
    await h.clock.advance(60_000)
    assert h.fc.dials == 1, "the zero-delay replacement dial reached the connector"
    assert ws.close_calls[0][0] == 1000
    await finish(h, baseline)


async def test_drain_retry_after_is_a_floor_under_the_jitter(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness()
    ws = await started(h)
    ws.push(ctl({"t": "drain", "reason": "rollout", "last_stream_id": 0, "retry_after_ms": 5000}))
    await settle()
    assert h.delays == [(0, 6000, "drain")]
    await h.clock.advance(5999)
    assert h.fc.dials == 1
    await h.clock.advance(1)
    await h.welcome(1)
    await finish(h, baseline)


async def test_handler_errors_are_forwarded(baseline: set[asyncio.Task[Any]]) -> None:
    def bad(_: Any) -> None:
        raise RuntimeError("handler bug")

    h = Harness(on_app=bad)
    infos: list[Any] = []
    h.client.on("handler_error", infos.append)
    ws = await started(h)
    ws.push(ctl({"t": "app", "body": {}}))
    await settle()
    assert [(i.event, str(i.error)) for i in infos] == [("app", "handler bug")]
    assert h.client.stats() is not None and h.client.stats().handler_errors == 1  # type: ignore[union-attr]
    await finish(h, baseline)


async def test_drain_without_deadline_retires_after_30s(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    old = await started(h)
    old_conn = h.client.current_conn()
    assert old_conn is not None
    old.push(encode_open(1))
    old.push(ctl({"t": "drain", "reason": "rollout", "last_stream_id": 1}))
    await settle()
    await h.clock.advance(1000)
    await h.welcome(1)
    await h.clock.advance(29_999)
    assert not old_conn.closed
    await h.clock.advance(1)
    assert old_conn.closed
    await finish(h, baseline)


async def test_server_closes_drained_conn_before_replacement(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness()
    old = await started(h)
    old.push(ctl({"t": "drain", "reason": "rollout", "last_stream_id": 0}))
    await settle()
    old.server_close(4012, "drained")
    await settle()
    [r] = h.reports
    assert (r.fatal, r.ws_code, r.error_name, r.close_reason) == (
        False,
        4012,
        "GOING_AWAY",
        "drained",
    )
    assert h.delays == [(0, 1000, "drain")]
    await h.clock.advance(1000)
    await h.welcome(1)
    assert h.fc.dials == 2 and h.client.state == "connected"
    await finish(h, baseline)


async def test_fatal_close_of_drained_conn_fails_the_replacement_dial(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness()
    old = await started(h)
    old.push(ctl({"t": "drain", "reason": "rollout", "last_stream_id": 0}))
    await settle()
    await h.clock.advance(1000)
    new = await h.sock(1)
    new.open()
    await settle()
    old.push(ctl({"t": "error", "code": 11, "message": "revoked"}))
    await settle()
    new.push(ctl(WELCOME))
    await settle()
    assert [r.fatal for r in h.reports] == [True]
    assert h.client.state == "closed" and h.client.current_conn() is None
    assert new.close_calls and new.close_calls[0][0] == 1000
    await finish(h, baseline)


async def test_drain_with_reconnect_disabled_is_fatal_at_close(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness(reconnect=ReconnectOptions(max_attempts=0))
    ws = await started(h)
    ws.push(ctl({"t": "drain", "reason": "rollout", "last_stream_id": 0}))
    await settle()
    assert h.retries == [] and h.fc.dials == 1
    ws.server_close(4012)
    await settle()
    [r] = h.reports
    assert (r.fatal, r.message) == (True, "drained; reconnect disabled")
    await finish(h, baseline)


async def test_drain_listener_raising_does_not_skip_reconnect(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness()

    def bad(_: Any) -> None:
        raise RuntimeError("listener bug")

    h.client.on("drain", bad)
    ws = await started(h)
    ws.push(ctl({"t": "drain", "reason": "rollout", "last_stream_id": 0}))
    await settle()
    assert h.delays == [(0, 1000, "drain")]
    await finish(h, baseline)


# --- application close, close(), abort() --------------------------------------------------


async def test_application_close_stops_the_loop(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    ws = await started(h)
    await h.client.close("bye now")
    assert ws.close_calls[0] == (4014, "bye now")
    assert controls(ws)[-1] == {"t": "error", "code": 14, "message": "bye now"}
    [r] = h.reports
    assert (r.fatal, r.ws_code, r.error_code, r.error_name, r.close_reason) == (
        False,
        4014,
        14,
        "APPLICATION_CLOSE",
        None,
    )
    await h.clock.advance(120_000)
    assert h.fc.dials == 1 and h.retries == []
    await finish(h, baseline)


async def test_graceful_close_is_idempotent_and_cancellation_safe(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness()
    ws = await started(h)
    ws.push(encode_open(1))
    await settle()
    first = asyncio.create_task(h.client.close())
    second = asyncio.create_task(h.client.close())
    await settle()
    assert controls(ws)[-1]["t"] == "drain"
    first.cancel()
    await settle()
    assert not second.done()
    await h.clock.advance(5000)
    await second
    assert first.cancelled()
    assert ws.close_calls[0][0] == 1000
    assert len(h.reports) == 1 and not h.reports[0].fatal
    await finish(h, baseline)


async def test_close_racing_first_dial(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    h.start()
    ws = await h.sock(0)
    await h.client.close()
    with pytest.raises(WsMixerError) as ei:
        assert h.start_task is not None
        await h.start_task
    assert (ei.value.code, ei.value.fatal) == (ErrorCode.NO_ERROR, False)
    assert ei.value.message == "closed before the first connection completed"
    assert ws.dial_cancelled and h.reports == []
    await finish(h, baseline)


class StubbornConnector:
    """Swallows the first `ignore` cancellations of a dial; after the first, it returns an
    open socket `unwind_ms` later (a connector slow to unwind)."""

    def __init__(self, clock: FakeClock, *, unwind_ms: float, ignore: int = 1) -> None:
        self.clock = clock
        self.unwind_ms = unwind_ms
        self.ignore = ignore
        self.calls = 0
        self.ws: FakeWebSocket | None = None

    async def __call__(self, url: str, init: WebSocketInit) -> WebSocketConnection:
        self.calls += 1
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        swallowed = 0
        timer = None
        while True:
            try:
                await fut
                break
            except asyncio.CancelledError:
                if swallowed >= self.ignore:
                    if timer is not None:
                        timer.cancel()
                    raise
                swallowed += 1
                fut = asyncio.get_running_loop().create_future()

                def unwound(f: asyncio.Future[None] = fut) -> None:
                    if not f.done():
                        f.set_result(None)

                timer = self.clock.call_later(self.unwind_ms, unwound)
        ws = FakeWebSocket()
        ws.subprotocol = SUBPROTOCOL
        self.ws = ws
        return ws


async def test_close_waits_for_a_dial_that_ignores_cancellation(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness()
    stubborn = StubbornConnector(h.clock, unwind_ms=500)
    h.client._connector = stubborn
    h.start()
    await settle()
    assert stubborn.calls == 1
    closing = asyncio.create_task(h.client.close())
    await settle()
    assert not closing.done()
    await h.clock.advance(499)
    assert not closing.done()
    await h.clock.advance(1)
    assert closing.done()
    await closing
    assert stubborn.ws is not None and stubborn.ws.aborted
    assert h.reports == []
    await finish(h, baseline)


async def test_close_wait_for_a_stuck_dial_is_bounded_by_the_connect_timeout(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness(reconnect=ReconnectOptions(connect_timeout_ms=3000))
    stubborn = StubbornConnector(h.clock, unwind_ms=60_000)
    h.client._connector = stubborn
    h.start()
    await h.clock.advance(1000)
    closing = asyncio.create_task(h.client.close())
    await h.clock.advance(1999)
    assert not closing.done()
    await h.clock.advance(1)  # the rest of the connect timeout: cancelled again, honoured
    assert closing.done()
    await closing
    assert stubborn.ws is None
    await finish(h, baseline)


async def test_close_during_a_cooperative_dial_resolves_at_once(
    baseline: set[asyncio.Task[Any]],
) -> None:
    h = Harness()
    h.start()
    ws = await h.sock(0)
    closing = asyncio.create_task(h.client.close())
    await settle()
    assert closing.done() and ws.dial_cancelled
    await closing
    await finish(h, baseline)


async def test_close_during_handshake(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    h.start()
    ws = await h.sock(0)
    ws.open()
    await settle()
    await h.client.close("stop")
    with pytest.raises(WsMixerError):
        assert h.start_task is not None
        await h.start_task
    assert ws.close_calls[0][0] == 4014 and h.reports == []
    await finish(h, baseline)


async def test_close_during_backoff(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    ws = await started(h)
    ws.server_drop()
    await settle()
    assert h.client.state == "backoff"
    await h.client.close()
    await h.clock.advance(60_000)
    assert h.fc.dials == 1 and len(h.reports) == 1
    await finish(h, baseline)


async def test_abort_drops_sockets_and_delivery(baseline: set[asyncio.Task[Any]]) -> None:
    started_handler = asyncio.Event()

    async def slow(_: Any) -> None:
        started_handler.set()
        await asyncio.Event().wait()

    h = Harness(on_stream=slow)
    ws = await started(h)
    ws.push(encode_open(1))
    ws.push(encode_open(3))
    await settle()
    assert started_handler.is_set()
    h.client.abort()
    assert h.clock.pending() == 0
    await settle()
    assert ws.aborted and h.client.state == "closed"
    assert len(h.reports) == 1 and not h.reports[0].fatal
    await finish(h, baseline)


@pytest.mark.parametrize(
    "where",
    ["dialing", "handshake", "backoff", "connected", "drain-jitter", "drain-dialing", "retiring"],
)
async def test_abort_leaves_no_timer_behind(baseline: set[asyncio.Task[Any]], where: str) -> None:
    h = Harness()
    h.start()
    ws = await h.sock(0)
    if where == "handshake":
        ws.open()
        await settle()
    elif where != "dialing":
        ws = await h.welcome(0)
        if where == "backoff":
            ws.server_drop()
            await settle()
        elif where != "connected":
            ws.push(encode_open(1))
            ws.push(ctl({"t": "drain", "reason": "r", "last_stream_id": 1, "deadline_ms": 4000}))
            await settle()
            if where != "drain-jitter":
                await h.clock.advance(1000)
                await h.sock(1)
            if where == "retiring":
                await h.welcome(1)
    assert h.clock.pending() > 0
    h.client.abort()
    assert h.clock.pending() == 0, where
    await settle()
    await finish(h, baseline)


async def test_abort_during_dial_and_cancelled_start(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    h.start()
    await h.sock(0)
    assert h.start_task is not None
    h.start_task.cancel()
    await settle()
    assert h.client.state == "closed" and h.fc.cancelled_dials == 1
    await finish(h, baseline)


async def test_send_app(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    with pytest.raises(WsMixerError):
        await h.client.send_app({"op": "x"})
    ws = await started(h)
    await h.client.send_app({"op": "x"})
    assert controls(ws)[-1] == {"t": "app", "body": {"op": "x"}}
    await finish(h, baseline)


async def test_events_and_timing(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness(_timing=Timing(hello_timeout_ms=500))
    seen: list[str] = []
    for ev in ("welcome", "reconnecting", "close", "app"):

        def record(_x: object, ev: str = ev) -> None:
            seen.append(ev)

        h.client.on(ev, record)
    h.start()
    (await h.sock(0)).open()
    await h.clock.advance(500)
    assert seen == ["close", "reconnecting"]
    await h.clock.advance(1000)
    ws = await h.welcome(1)
    ws.push(ctl({"t": "app", "body": {"k": 1}}))
    await settle()
    assert seen[2:] == ["welcome", "app"]
    await finish(h, baseline)


# --- pure helpers ---------------------------------------------------------------------------


async def test_full_jitter_delay() -> None:
    assert full_jitter_delay(0, 1000, 60_000, lambda: 0.5) == 500
    assert full_jitter_delay(3, 1000, 60_000, lambda: 1.0) == 8000
    assert full_jitter_delay(10_000, 1000, 60_000, lambda: 1.0) == 60_000


async def test_reconnect_options_validation() -> None:
    with pytest.raises(ValueError):
        ReconnectOptions(base_ms=-1)
    with pytest.raises(ValueError):
        ReconnectOptions(connect_timeout_ms=0)
    with pytest.raises(ValueError):
        ReconnectOptions(stable_after_ms=float("nan"))
    with pytest.raises(ValueError):
        ReconnectOptions(max_attempts=-1)
    with pytest.raises(TypeError):
        ReconnectOptions(max_attempts=True)


async def test_constructor_validates_options() -> None:
    with pytest.raises(ValueError):
        MixerClient(URL, token="t", window=1)
    with pytest.raises(ValueError):
        MixerClient(URL, token="t", headers={"Upgrade": "x"})
    with pytest.raises(TypeError):
        MixerClient(URL, token=42)  # type: ignore[arg-type]


def _info(**kw: Any) -> CloseInfo:
    base: dict[str, Any] = {
        "ws_code": None,
        "message": "m",
        "phase": "connected",
        "local": False,
        "received_close": False,
        "error": ConnError(ErrorCode.INTERNAL_ERROR, "m"),
    }
    return CloseInfo(**{**base, **kw})


@pytest.mark.parametrize(
    ("info", "code", "name", "reason"),
    [
        # (1) peer error, unclamped
        (
            {"ws_code": 4000 + 0x1000_0000, "error_code": 0x1000_0000},
            0x1000_0000,
            "INTERNAL_ERROR",
            None,
        ),
        ({"ws_code": 1000, "error_code": 0}, 0, "NO_ERROR", None),
        # (2) bare close in 4001-4999
        ({"ws_code": 4005, "received_close": True, "close_reason": "r"}, 5, "STREAM_CLOSED", "r"),
        ({"ws_code": 4999, "received_close": True}, 999, "INTERNAL_ERROR", None),
        ({"ws_code": 4000, "received_close": True}, None, None, None),
        # never synthesised
        ({"ws_code": 1000, "received_close": True, "close_reason": "ok"}, None, None, "ok"),
        ({"ws_code": 1001, "received_close": True}, None, None, None),
        ({"ws_code": 1009, "received_close": True}, None, None, None),
        ({"ws_code": 1011, "received_close": True}, None, None, None),
        ({"ws_code": 1006}, None, None, None),
        # (3) local error, (4) own application close; own graceful close is neither
        ({"ws_code": 4001, "local": True, "error_code": 1}, 1, "PROTOCOL_ERROR", None),
        (
            {"ws_code": None, "local": True, "error_code": 10, "phase": "dial"},
            10,
            "UNSUPPORTED",
            None,
        ),
        ({"ws_code": 4014, "local": True, "error_code": 14}, 14, "APPLICATION_CLOSE", None),
        ({"ws_code": 1000, "local": True, "error_code": 0}, None, None, None),
    ],
)
async def test_four_sources(
    info: dict[str, Any], code: int | None, name: str | None, reason: str | None
) -> None:
    r = _ctx_from_close(_info(**info)).reason(False)
    assert (r.error_code, r.error_name, r.close_reason) == (code, name, reason)
    assert r.ws_code == info["ws_code"]


async def test_closed_event_once_on_close_in_backoff(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    closed: list[DisconnectReason | None] = []
    h.client.on("closed", closed.append)
    ws = await started(h)
    ws.server_drop()
    await settle()
    assert h.client.state == "backoff"
    await h.client.close()
    await h.client.close()
    assert closed == [None]
    await finish(h, baseline)
    assert closed == [None]


async def test_closed_event_carries_the_fatal_reason(baseline: set[asyncio.Task[Any]]) -> None:
    h = Harness()
    closed: list[DisconnectReason | None] = []
    h.client.on("closed", closed.append)
    h.start()
    (await h.sock(0)).reject(403)
    with pytest.raises(WsMixerError):
        assert h.start_task is not None
        await h.start_task
    assert len(closed) == 1 and closed[0] is h.reports[-1]
    assert closed[0] is not None and closed[0].fatal
    await finish(h, baseline)
    assert len(closed) == 1
