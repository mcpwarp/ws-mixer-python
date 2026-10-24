"""create_tunnel() (MCPWARP.md section 6) beyond the spec fixtures: the rows no fixture reaches
(T9, T25/T26 from a malformed envelope, T36), the section 6.8 generation rule, dispose()'s bound,
configure() on garbage, idempotence with bound-method connectors, listener isolation, the
section 6.9 log lines and `status` reads from another thread.

The edge is the fixture runner's scripted `EdgeSocket` on a FakeClock with `random` pinned to
0.5. Every test ends in `finish()`, which disposes and asserts that no timer and no task outlived
the tunnel.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import Awaitable
from types import SimpleNamespace
from typing import Any

import pytest

from tests.spec.test_tunnel import WELCOME, EdgeSocket
from ws_mixer import TokenUnavailableError, token_refresh_requested
from ws_mixer.errors import ErrorCode
from ws_mixer.frame import FrameType, decode_frame, encode_data, encode_open
from ws_mixer.mcpwarp import (
    Agent,
    Closed,
    McpwarpError,
    Registration,
    Request,
    Response,
    RetryInfo,
    TunnelConfig,
    TunnelOptions,
    TunnelState,
    TunnelStatus,
    TunnelStatusCode,
    create_tunnel,
)
from ws_mixer.mcpwarp import tunnel as tunnel_mod
from ws_mixer.mcpwarp.status import INVALID_NAME, INVALID_SETTINGS
from ws_mixer.socket import WebSocketConnection, WebSocketInit
from ws_mixer.testing import FakeClock, FakeConnector

pytestmark = pytest.mark.asyncio

UUID = "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10"
URL = "https://notes-tester.mcpwarp.test/mcp"
TOKEN = "tok-6f1e9d"
INTERNAL = {
    "state": "error",
    "code": "INTERNAL",
    "lastError": "Something went wrong; the log has the details.",
}
SETTINGS = {
    "state": "error",
    "code": "INVALID_CONFIG",
    "lastError": INVALID_SETTINGS.last_error,
}
LEGAL = {
    ("off", None),
    ("connecting", None),
    ("connecting", "RETRYING"),
    ("connecting", "DISCONNECTED"),
    ("connecting", "INTERNAL"),
    ("online", None),
    ("disabled", "DISABLED"),
    ("conflict", "TAKEN_OVER"),
    *(
        ("error", code)
        for code in (
            "UNAUTHORIZED",
            "INVALID_CONFIG",
            "INVALID_NAME",
            "USERNAME_REQUIRED",
            "QUOTA_EXCEEDED",
            "CONFLICT",
            "UNSUPPORTED_VERSION",
            "DISCONNECTED_FATAL",
            "INTERNAL",
        )
    ),
}


async def handler(request: Request) -> Response:
    return Response(200, body=b"ok")


class Env:
    def __init__(self, **options: Any) -> None:
        self.clock = FakeClock()
        self.connector = FakeConnector(factory=lambda url, init: EdgeSocket())
        self.retries: list[RetryInfo] = []
        self.snaps: list[dict[str, str]] = []
        self.calls: list[asyncio.Future[None]] = []
        self.baseline = set(asyncio.all_tasks())
        kw: dict[str, Any] = {
            "integration": UUID,
            "handler": handler,
            "web_socket": self.connector,
            "on_retry": self.retries.append,
            "clock": self.clock,
            "random": lambda: 0.5,
        }
        kw.update(options)
        self.tunnel = create_tunnel(**kw)
        self.tunnel.subscribe(lambda s: self.snaps.append(s.to_dict()))

    def sock(self, i: int = -1) -> EdgeSocket:
        s = self.connector.sockets[i]
        assert isinstance(s, EdgeSocket)
        return s

    def call(self, aw: Awaitable[None]) -> asyncio.Future[None]:
        fut = asyncio.ensure_future(aw)
        self.calls.append(fut)
        return fut

    async def settle(self) -> None:
        await self.clock.advance(0)

    def configure(self, config: object) -> asyncio.Future[None]:
        return self.call(self.tunnel.configure(config))

    async def online(self, name: str = "notes", **cfg: Any) -> None:
        done = self.configure(TunnelConfig(True, TOKEN, name, **cfg))
        await self.settle()
        self.sock().open()
        await self.settle()
        assert done.done()
        self.sock().control(dict(WELCOME))
        await self.settle()
        registered = {"name": name, "url": URL, "id": f"srv_{name}", "created": True}
        self.sock().app({"op": "registered", "services": [registered]})
        await self.settle()
        assert self.tunnel.status == TunnelStatus(TunnelState.ONLINE, URL)

    async def finish(self) -> None:
        final = asyncio.ensure_future(self.tunnel.dispose(1))
        await self.clock.advance(60_000)
        assert final.done()
        for fut in self.calls:
            assert fut.done(), "a configure/stop/dispose call never finished"
            fut.result()
        assert self.clock.pending() == 0, "a timer outlived the tunnel"
        leaked = set(asyncio.all_tasks()) - self.baseline - {asyncio.current_task()}
        assert not leaked, leaked


def messages(caplog: pytest.LogCaptureFixture, level: int | None = None) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == "mcpwarp" and (level is None or r.levelno == level)
    ]


@pytest.fixture(autouse=True)
def _log_info(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="mcpwarp")


# --- create_tunnel() ----------------------------------------------------------------------------


async def test_create_tunnel_needs_no_loop_and_starts_off() -> None:
    made: list[object] = []

    def build() -> None:  # a thread with no event loop, as a host's UI thread
        made.append(create_tunnel(integration=UUID, handler=handler))
        made.append(create_tunnel(TunnelOptions(integration=UUID, target="http://127.0.0.1:9")))

    thread = threading.Thread(target=build)
    thread.start()
    thread.join(5)
    assert len(made) == 2
    for tunnel in made:
        assert isinstance(tunnel, tunnel_mod.Tunnel)
        assert tunnel.status == TunnelStatus(TunnelState.OFF)
        assert tunnel.status.to_dict() == {"state": "off"}


@pytest.mark.parametrize(
    ("kwargs", "exc", "message"),
    [
        ({"handler": handler}, TypeError, "`integration`"),
        ({"integration": "nope", "handler": handler}, ValueError, "RFC 4122 UUID"),
        ({"integration": UUID, "handler": handler, "agent": "x"}, TypeError, "`agent`"),
        (
            {"integration": UUID, "handler": handler, "agent": Agent(os="\x00")},
            ValueError,
            "agent.os",
        ),
        ({"integration": UUID}, TypeError, "exactly one of `handler` or `target`"),
        (
            {"integration": UUID, "handler": handler, "target": "http://x"},
            TypeError,
            "exactly one",
        ),
        ({"integration": UUID, "target": "ftp://x"}, ValueError, "not a valid URL"),
        ({"integration": UUID, "handler": handler, "bogus": 1}, TypeError, "bogus"),
        (
            {"integration": UUID, "target": "http://x", "handler_timeout_ms": 5},
            TypeError,
            "`handler_timeout_ms` applies only to `handler`",
        ),
        (
            {"integration": UUID, "handler": handler, "handler_timeout_ms": 0},
            ValueError,
            "handler_timeout_ms must be a positive number",
        ),
    ],
)
async def test_create_tunnel_validates_up_front(
    kwargs: dict[str, Any], exc: type[Exception], message: str
) -> None:
    with pytest.raises(exc, match=message):
        create_tunnel(**kwargs)


async def test_handler_timeout_ms_reaches_each_exposure() -> None:
    assert TunnelOptions(integration=UUID).handler_timeout_ms is None
    env = Env(handler_timeout_ms=25_000)
    await env.online()
    active = env.tunnel._active
    assert active is not None
    assert active.exposure._opts.handler_timeout_ms == 25_000
    await env.finish()


async def test_create_tunnel_takes_options_or_kwargs_not_both() -> None:
    with pytest.raises(TypeError, match="not both"):
        create_tunnel(TunnelOptions(integration=UUID, handler=handler), integration=UUID)


# --- configure(): the section 6.2 checks -------------------------------------------------------


@pytest.mark.parametrize(
    ("config", "detail", "last_error"),
    [
        (None, "the config must be an object", INVALID_SETTINGS.last_error),
        (5, "the config must be an object", INVALID_SETTINGS.last_error),
        ("enabled", "the config must be an object", INVALID_SETTINGS.last_error),
        ({}, "`enabled` must be a boolean", INVALID_SETTINGS.last_error),
        ({"enabled": 1, "token": "t", "name": "n"}, "`enabled` must be a boolean", None),
        (
            {"enabled": True, "name": "n"},
            "`token` must be a string or a callable",
            "The token is not set.",
        ),
        (
            {"enabled": True, "token": 7, "name": "n"},
            "`token` must be a string or a callable",
            "The token is not set.",
        ),
        (
            {"enabled": True, "token": object(), "name": "n"},
            "`token` must be a string or a callable",
            "The token is not set.",
        ),
        (
            {"enabled": True, "token": "t"},
            "`name` must be a string",
            "The service name is not set.",
        ),
        (
            SimpleNamespace(enabled=True, token="t", name=3),
            "`name` must be a string",
            "The service name is not set.",
        ),
        ({"enabled": True, "token": "t", "name": "n", "url": 1}, "`url` must be a string", None),
        (
            {"enabled": True, "token": "t", "name": "n", "web_socket": "ws"},
            "`web_socket` must be callable",
            None,
        ),
    ],
)
async def test_configure_refuses_garbage_without_raising(
    caplog: pytest.LogCaptureFixture, config: object, detail: str, last_error: str | None
) -> None:
    env = Env()
    await env.tunnel.configure(config)
    await env.settle()
    expect = {
        "state": "error",
        "code": "INVALID_CONFIG",
        "lastError": last_error or INVALID_SETTINGS.last_error,
    }
    assert env.snaps == [{"state": "off"}, expect]
    assert messages(caplog, logging.ERROR) == [f"INVALID_CONFIG: {detail}"]
    assert env.connector.dials == 0
    await env.finish()


async def test_disabled_or_blank_token_is_off_whatever_else_is_wrong() -> None:
    env = Env()
    await env.tunnel.configure({"enabled": False, "token": 3, "name": None, "url": 5})
    await env.tunnel.configure(SimpleNamespace(enabled=True, token=" 　\t", name=None))
    await env.settle()
    assert env.snaps == [{"state": "off"}]
    assert env.connector.dials == 0
    await env.finish()


async def test_an_invalid_config_while_online_stops_the_connection(
    caplog: pytest.LogCaptureFixture,
) -> None:
    env = Env()
    await env.online()
    await env.tunnel.configure({"enabled": True, "token": TOKEN})
    await env.settle()
    assert env.snaps[-1]["lastError"] == "The service name is not set."
    assert env.sock().closed
    assert "stopping" in messages(caplog, logging.INFO)
    await env.finish()


# --- idempotence ------------------------------------------------------------------------------


class Dialer:
    """A connector exposed as a bound method: every attribute read is a new object."""

    def __init__(self, inner: FakeConnector) -> None:
        self.inner = inner

    async def dial(self, url: str, init: WebSocketInit) -> WebSocketConnection:
        return await self.inner(url, init)


async def test_idempotence_compares_connectors_with_eq() -> None:
    env = Env()
    dialer = Dialer(env.connector)
    assert dialer.dial is not dialer.dial and dialer.dial == dialer.dial
    first = env.configure(TunnelConfig(True, "t", "notes", web_socket=dialer.dial))
    again = env.configure(
        {"enabled": True, "token": " t ", "name": "notes", "web_socket": dialer.dial}
    )
    await env.settle()
    assert first.done() and again.done()
    assert env.connector.dials == 1
    assert env.snaps == [{"state": "off"}, {"state": "connecting"}]
    # The options' connector is the effective one when the config names none.
    env.configure(TunnelConfig(True, "t", "notes", web_socket=env.connector))
    await env.settle()
    env.configure(TunnelConfig(True, "t", "notes"))
    await env.settle()
    assert env.connector.dials == 2
    await env.finish()


async def test_repeat_of_a_pending_configure_returns_once_the_first_applies() -> None:
    env = Env()
    first = env.configure(TunnelConfig(True, "t", "notes"))
    second = env.configure(TunnelConfig(True, "t", "notes", url=None))
    await env.settle()
    assert first.done() and second.done()
    assert env.connector.dials == 1
    await env.finish()


# --- start: T7, T8, T9 -------------------------------------------------------------------------


async def test_t7_t8_log_the_local_validation_failure(caplog: pytest.LogCaptureFixture) -> None:
    env = Env()
    await env.tunnel.configure(TunnelConfig(True, "t", "Bad_Name"))
    await env.tunnel.configure(TunnelConfig(True, "t", "notes", kind="ftp"))
    await env.settle()
    assert env.snaps == [
        {"state": "off"},
        {"state": "connecting"},
        {"state": "error", "code": "INVALID_NAME", "lastError": INVALID_NAME.last_error},
        {"state": "connecting"},
        SETTINGS,
    ]
    errors = messages(caplog, logging.ERROR)
    assert errors == [
        "INVALID_NAME: invalid service `name`: lowercase letters, digits and inner dashes, "
        "at most 30 characters",
        'INVALID_CONFIG: `kind` must be "http" or "stdio"',
    ]
    assert env.connector.dials == 0
    await env.finish()


async def test_t8_options_checked_at_start(caplog: pytest.LogCaptureFixture) -> None:
    env = Env(max_request_body_bytes=0)
    await env.tunnel.configure(TunnelConfig(True, "t", "notes"))
    await env.settle()
    assert env.snaps[-1] == SETTINGS
    assert env.connector.dials == 0
    await env.finish()


async def test_t9_other_synchronous_failure_at_start_is_internal(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*args: object, **kwargs: object) -> None:
        raise RuntimeError("no loop for you")

    monkeypatch.setattr(tunnel_mod, "Exposure", broken)
    env = Env()
    await env.tunnel.configure(TunnelConfig(True, "t", "notes"))
    await env.settle()
    assert env.snaps == [{"state": "off"}, {"state": "connecting"}, INTERNAL]
    assert messages(caplog, logging.ERROR) == ["INTERNAL: no loop for you"]
    assert env.connector.dials == 0
    # The desired config was cleared: the same config starts again (T37).
    monkeypatch.undo()
    await env.tunnel.configure(TunnelConfig(True, "t", "notes"))
    await env.settle()
    assert env.connector.dials == 1
    await env.finish()


# --- T25/T26 from a malformed envelope, T36 ----------------------------------------------------

MALFORMED = {
    "op": "registered",
    "services": [{"name": "notes", "id": "srv_notes", "url": "", "created": True}],
}
MALFORMED_LOG = "MALFORMED_ENVELOPE: registered.services[].url must be a non-empty string"


async def test_t25_malformed_envelope_while_connecting_is_internal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    env = Env()
    env.configure(TunnelConfig(True, "t", "notes"))
    await env.settle()
    env.sock().open()
    await env.settle()
    env.sock().control(dict(WELCOME))
    await env.settle()
    env.sock().app(MALFORMED)
    await env.settle()
    assert env.snaps[-1] == {
        "state": "connecting",
        "code": "INTERNAL",
        "lastError": INTERNAL["lastError"],
    }
    assert MALFORMED_LOG in messages(caplog, logging.WARNING)
    env.sock().app({"op": "registered", "services": [{"name": "notes", "url": URL, "id": "s"}]})
    await env.settle()
    assert env.snaps[-1] == {"state": "online", "url": URL}
    await env.finish()


async def test_t26_malformed_envelope_while_online_or_disabled_changes_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    env = Env()
    await env.online()
    env.sock().app(MALFORMED)
    await env.settle()
    env.sock().app({"op": "disable", "id": "srv_notes"})
    await env.settle()
    env.sock().app(MALFORMED)
    await env.settle()
    assert [s["state"] for s in env.snaps] == ["off", "connecting", "online", "disabled"]
    assert messages(caplog, logging.WARNING).count(MALFORMED_LOG) == 2
    await env.finish()


async def test_t36_client_closed_underneath_is_internal(caplog: pytest.LogCaptureFixture) -> None:
    env = Env()
    await env.online()
    active = env.tunnel._active
    assert active is not None
    await active.exposure.client.close()
    await env.settle()
    assert env.snaps[-1] == INTERNAL
    assert "INTERNAL: the connection closed" in messages(caplog, logging.ERROR)
    await env.clock.advance(120_000)
    assert env.connector.dials == 1
    await env.finish()


async def test_t36_client_closed_while_backing_off_is_internal() -> None:
    env = Env()
    env.configure(TunnelConfig(True, "t", "notes"))
    await env.settle()
    env.sock().fail()
    await env.settle()
    assert env.snaps[-1]["code"] == "RETRYING"
    active = env.tunnel._active
    assert active is not None
    await active.exposure.client.close()
    await env.settle()
    assert env.snaps[-1] == INTERNAL
    await env.clock.advance(120_000)
    assert env.connector.dials == 1
    await env.finish()


# --- section 6.8: generations ------------------------------------------------------------------


async def _end_by(env: Env, how: str) -> TunnelStatus:
    if how == "configure":
        env.configure(TunnelConfig(True, TOKEN, "other"))
    elif how == "stop":
        env.call(env.tunnel.stop())
    elif how == "dispose":
        env.call(env.tunnel.dispose())
    elif how == "conflict":
        env.sock().app({"op": "conflict", "id": "srv_notes", "name": "notes"})
    elif how == "fatal":
        env.sock().server_close(4011)
    await env.settle()
    return env.tunnel.status


@pytest.mark.parametrize("how", ["configure", "stop", "dispose", "conflict", "fatal"])
async def test_an_ended_generation_never_moves_status(how: str) -> None:
    env = Env()
    await env.online()
    old = env.tunnel._active
    assert old is not None
    after = await _end_by(env, how)
    retries = len(env.retries)
    snaps = len(env.snaps)
    # Events, on_retry calls and settlements of the old exposure, as its hooks deliver them.
    old.exposure.observe("registered", Registration(URL, "srv_notes", False), None)
    old.exposure.observe("error", McpwarpError("QUOTA_EXCEEDED", "late"), "register_refusal")
    old.exposure.observe("close", Closed(McpwarpError("UNAUTHORIZED", "late")), None)
    old.exposure._retry(RetryInfo(1, 500, "late"), "reconnect")
    await env.settle()
    assert env.tunnel.status == after
    assert len(env.snaps) == snaps
    assert len(env.retries) == retries
    await env.finish()


async def test_stale_redials_are_neither_forwarded_nor_dialed_after_stop() -> None:
    env = Env()
    env.configure(TunnelConfig(True, "t", "notes"))
    await env.settle()
    env.sock().fail()
    await env.settle()
    assert len(env.retries) == 1
    await env.tunnel.stop()
    await env.clock.advance(120_000)
    assert len(env.retries) == 1
    assert env.connector.dials == 1
    assert env.snaps[-1] == {"state": "off"}
    await env.finish()


async def test_a_stop_during_the_raw_dial_never_shows_the_late_socket() -> None:
    env = Env()
    env.configure(TunnelConfig(True, "t", "notes"))
    await env.settle()
    stopping = env.call(env.tunnel.stop())
    await env.settle()
    assert stopping.done()
    late = env.sock()
    assert late.dial_cancelled
    late.open()
    await env.settle()
    assert env.snaps[-1] == {"state": "off"}
    await env.finish()


def sent_controls(sock: EdgeSocket) -> list[dict[str, Any]]:
    out = []
    for raw in sock.sent:
        f = decode_frame(raw)
        if f.type == FrameType.DATA and f.stream_id == 0:
            out.append(json.loads(f.payload))
    return out


@pytest.mark.parametrize("registered", [True, False])
@pytest.mark.parametrize("how", ["conflict", "replaced"])
async def test_a_take_over_closes_at_once_with_no_error_and_1000(
    registered: bool, how: str
) -> None:
    release = asyncio.Event()
    calls: list[Request] = []

    async def gated(request: Request) -> Response:
        calls.append(request)
        await release.wait()
        return Response(200, body=b"late")

    env = Env(handler=gated)
    if registered:
        await env.online()
        env.sock().push(encode_open(1))
        env.sock().push(
            encode_data(1, b"POST /mcp HTTP/1.1\r\nHost: notes-tester.mcpwarp.test\r\n\r\n")
        )
        await env.settle()
        assert len(calls) == 1  # in flight: a graceful close would wait for it
    else:
        env.configure(TunnelConfig(True, TOKEN, "notes"))
        await env.settle()
        env.sock().open()
        await env.settle()
        env.sock().control(dict(WELCOME))
        await env.settle()
    sock = env.sock()
    if how == "conflict":
        sock.app({"op": "conflict", "id": "srv_notes", "name": "notes", "reason": "taken_over"})
    else:
        sock.control({"t": "drain", "reason": "replaced", "last_stream_id": int(registered)})
    await env.settle()  # no time passes
    sent = sent_controls(sock)
    assert sent[-1]["t"] == "error" and sent[-1]["code"] == ErrorCode.NO_ERROR
    assert "drain" not in [m["t"] for m in sent]
    assert [m["body"]["mcpwarp"]["op"] for m in sent if m["t"] == "app"] == ["register"]
    assert [code for code, _ in sock.close_calls] == [1000]
    assert env.tunnel.status.state == TunnelState.CONFLICT
    await env.clock.advance(120_000)
    assert env.connector.dials == 1
    release.set()
    await env.finish()


# --- dispose() ----------------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [0, -1, float("inf"), float("nan")])
async def test_dispose_rejects_a_bad_timeout_synchronously(bad: float) -> None:
    tunnel = create_tunnel(integration=UUID, handler=handler)
    with pytest.raises(ValueError, match="positive finite"):
        _ = tunnel.dispose(bad)


@pytest.mark.parametrize("bad", [True, "5", None])
async def test_dispose_rejects_a_non_number_synchronously(bad: Any) -> None:
    tunnel = create_tunnel(integration=UUID, handler=handler)
    with pytest.raises(TypeError, match="positive finite"):
        _ = tunnel.dispose(bad)


async def test_dispose_gives_up_after_its_timeout_while_the_close_goes_on(
    caplog: pytest.LogCaptureFixture,
) -> None:
    env = Env()
    await env.online()
    env.sock().hold()
    disposing = env.call(env.tunnel.dispose(timeout_ms=100))
    again = env.call(env.tunnel.dispose(timeout_ms=60_000))
    await env.settle()
    assert env.snaps[-1] == {"state": "off"}
    await env.clock.advance(99)
    assert not disposing.done()
    await env.clock.advance(1)
    assert disposing.done() and again.done()
    assert messages(caplog, logging.WARNING) == ["dispose() gave up waiting after 100 ms"]
    assert not env.sock().aborted
    # The background close still runs its own budget: unregister wait, then the close bound.
    await env.clock.advance(7000)
    assert env.sock().closed
    await env.finish()


async def test_after_dispose_subscribe_gets_off_once_and_calls_do_nothing() -> None:
    env = Env()
    await env.online()
    await env.tunnel.dispose()
    seen: list[TunnelStatus] = []
    unsubscribe = env.tunnel.subscribe(seen.append)
    unsubscribe()
    await env.tunnel.configure(TunnelConfig(True, "t2", "notes"))
    await env.tunnel.stop()
    await env.clock.advance(120_000)
    assert seen == [TunnelStatus(TunnelState.OFF)]
    assert env.snaps[-1] == {"state": "off"}
    assert env.connector.dials == 1
    await env.finish()


async def test_a_configure_scheduled_right_after_dispose_is_a_no_op(
    caplog: pytest.LogCaptureFixture,
) -> None:
    env = Env()
    env.call(env.tunnel.dispose())
    env.configure(None)  # would be error/INVALID_CONFIG if dispose() were not final yet
    env.configure(TunnelConfig(True, "t", "notes"))
    env.call(env.tunnel.stop())
    await env.settle()
    assert env.snaps == [{"state": "off"}]
    assert env.tunnel.status == TunnelStatus(TunnelState.OFF)
    assert messages(caplog, logging.ERROR) == []
    assert env.connector.dials == 0
    await env.finish()


# --- status and listeners ----------------------------------------------------------------------


async def test_a_throwing_listener_is_logged_and_the_rest_still_run(
    caplog: pytest.LogCaptureFixture,
) -> None:
    env = Env()
    later: list[str] = []

    def bad(status: TunnelStatus) -> None:
        if status.state != TunnelState.OFF:
            raise RuntimeError("boom")

    env.tunnel.subscribe(bad)
    env.tunnel.subscribe(lambda s: later.append(s.state.value))
    await env.tunnel.configure(TunnelConfig(True, "t", "notes"))
    assert later == ["off", "connecting"]
    assert messages(caplog, logging.ERROR) == ["status listener threw: boom"]
    await env.finish()


async def test_unsubscribe_stops_deliveries_and_dedup_skips_repeats() -> None:
    env = Env()
    seen: list[str] = []
    unsubscribe = env.tunnel.subscribe(lambda s: seen.append(s.state.value))
    await env.tunnel.configure(TunnelConfig(False, "t", "notes"))
    await env.tunnel.stop()
    unsubscribe()
    await env.tunnel.configure(TunnelConfig(True, "t", "notes"))
    assert seen == ["off"]
    assert env.snaps == [{"state": "off"}, {"state": "connecting"}]
    await env.finish()


async def test_status_reads_from_another_thread_see_whole_snapshots() -> None:
    env = Env()
    stop = threading.Event()
    seen: set[tuple[str, str | None, bool, bool]] = set()
    bad: list[object] = []

    def reader() -> None:
        # Short waits keep this thread from starving the loop's thread of the GIL.
        while not stop.wait(0.0001):
            for _ in range(50):
                s = env.tunnel.status
                if not isinstance(s, TunnelStatus):
                    bad.append(s)
                    continue
                code = None if s.code is None else s.code.value
                seen.add((s.state.value, code, s.url is not None, s.last_error is not None))

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    try:
        for i in range(10):
            await env.online(name=f"svc{i}")
            env.sock().server_drop()
            await env.settle()
            await asyncio.sleep(0.001)  # let the reader catch the DISCONNECTED status too
    finally:
        stop.set()
        thread.join(5)
    assert not thread.is_alive()
    assert not bad
    assert len(seen) >= 2
    for state, code, has_url, has_error in seen:
        assert (state, code) in LEGAL
        assert has_url == (state == "online")
        assert has_error == (code is not None)
    await env.finish()


# --- section 6.9 logging -----------------------------------------------------------------------


async def test_log_lines(caplog: pytest.LogCaptureFixture) -> None:
    env = Env()
    await env.online(url="wss://edge.example")
    # A drain hand-over: the drained connection ends once the replacement has its welcome (T38).
    env.sock().control({"t": "drain", "reason": "rollout", "last_stream_id": 0})
    await env.settle()
    await env.clock.advance(env.retries[-1].delay_ms)
    env.sock().open()
    await env.settle()
    env.sock().control(dict(WELCOME))
    await env.settle()
    registered = {"name": "notes", "url": URL, "id": "srv_notes", "created": False}
    env.sock().app({"op": "registered", "services": [registered]})
    await env.settle()
    s = env.sock()
    s.app({"op": "disable", "id": "srv_notes", "reason": "abuse"})
    await env.settle()
    s.app({"op": "enable", "id": "srv_notes", "name": "notes"})
    await env.settle()
    s.app({"op": "error", "code": "CONNECTION_LIMIT", "message": "too many"})
    await env.settle()
    s.app(
        {"op": "registered", "errors": [{"name": "notes", "code": "UNAVAILABLE", "message": "x"}]}
    )
    await env.settle()
    await env.clock.advance(1000)
    s.server_drop()
    await env.settle()
    await env.clock.advance(env.retries[-1].delay_ms)
    env.sock().open()
    await env.settle()
    env.sock().control(dict(WELCOME))
    await env.settle()
    env.sock().app({"op": "conflict", "id": "srv_notes", "name": "notes"})
    await env.settle()
    await env.tunnel.configure(TunnelConfig(False, TOKEN, "notes"))
    await env.tunnel.configure(TunnelConfig(True, TOKEN, "notes"))
    await env.settle()
    env.sock().open()
    await env.settle()
    env.sock().control(dict(WELCOME))
    await env.settle()
    quota = {"name": "notes", "code": "QUOTA_EXCEEDED", "message": "service quota exceeded"}
    env.sock().app({"op": "registered", "errors": [quota]})
    await env.settle()

    assert messages(caplog, logging.INFO) == [
        'connecting as "notes" via wss://edge.example',
        f"online at {URL}",
        "the superseded connection ended: superseded by a new connection",
        f"online at {URL}",
        "the service was enabled, registering again",
        "stopping",
        "off",
        'connecting as "notes"',
        "stopping",
    ]
    assert messages(caplog, logging.WARNING) == [
        "retry #0 in 1000 ms: drain",
        "the service was disabled (abuse)",
        "CONNECTION_LIMIT: too many",
        "UNAVAILABLE: x",
        "retry #1 in 1000 ms: register failed: UNAVAILABLE: x",
        "disconnected: socket closed with code 1006",
        "retry #1 in 1000 ms: socket closed with code 1006",
        'another connection took over the name "notes"; not reconnecting until configure() '
        "is called again",
    ]
    assert messages(caplog, logging.ERROR) == ["QUOTA_EXCEEDED: service quota exceeded"]
    assert not any(TOKEN in r.getMessage() for r in caplog.records)
    await env.finish()


async def test_a_custom_logger_gets_the_lines_and_its_failures_are_swallowed() -> None:
    class Broken(logging.Logger):
        def log(self, level: int, msg: object, *args: object, **kwargs: Any) -> None:
            raise RuntimeError("logger down")

    env = Env(logger=Broken("custom"))
    await env.tunnel.configure(None)
    await env.settle()
    assert env.snaps[-1] == SETTINGS
    await env.finish()


async def test_on_retry_returning_a_coroutine_is_closed_and_warned(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def on_retry(info: RetryInfo) -> None:
        raise AssertionError("never awaited")

    env = Env(on_retry=on_retry)
    env.configure(TunnelConfig(True, "t", "notes"))
    await env.settle()
    env.sock().fail()
    await env.settle()
    assert "on_retry returned a coroutine; it must be a plain function" in messages(
        caplog, logging.WARNING
    )
    await env.finish()


async def test_status_codes_are_the_enum() -> None:
    env = Env()
    await env.tunnel.configure(None)
    assert env.tunnel.status.code is TunnelStatusCode.INVALID_CONFIG
    await env.finish()


# --- a token provider as TunnelConfig.token -----------------------------------------------------


class Tokens:
    """A token provider: each call takes the next outcome (a token, or an exception to raise),
    then falls back to `tok-<n>`. Records `token_refresh_requested()` per call."""

    def __init__(self, *outcomes: str | BaseException) -> None:
        self.outcomes = list(outcomes)
        self.refresh: list[bool] = []

    def __call__(self) -> str:
        self.refresh.append(token_refresh_requested())
        out = self.outcomes.pop(0) if self.outcomes else f"tok-{len(self.refresh)}"
        if isinstance(out, BaseException):
            raise out
        return out

    def get(self) -> str:
        return self()

    async def fetch(self) -> str:
        await asyncio.sleep(0)
        return self()


def hello_token(sock: EdgeSocket) -> object:
    hello = json.loads(decode_frame(sock.sent[0]).payload)
    assert hello["t"] == "hello"
    return hello["token"]


def assert_no_token_logged(caplog: pytest.LogCaptureFixture, *tokens: str) -> None:
    for record in caplog.records:
        text = record.getMessage()
        assert not any(t in text for t in tokens), text


@pytest.mark.parametrize("flavour", ["sync", "async"])
async def test_a_token_provider_is_called_on_every_dial(
    caplog: pytest.LogCaptureFixture, flavour: str
) -> None:
    env = Env()
    tokens = Tokens()
    provider = tokens.get if flavour == "sync" else tokens.fetch
    env.configure(TunnelConfig(True, provider, "notes"))
    await env.settle()
    env.sock().open()
    await env.settle()
    assert hello_token(env.sock()) == "tok-1"
    init = env.sock().init
    assert init is not None and init.headers["Authorization"] == "Bearer tok-1"
    env.sock().server_drop()
    await env.settle()
    await env.clock.advance(env.retries[-1].delay_ms)
    env.sock().open()
    await env.settle()
    assert hello_token(env.sock()) == "tok-2"
    env.sock().control(dict(WELCOME))
    await env.settle()
    env.sock().app({"op": "registered", "services": [{"name": "notes", "url": URL, "id": "s"}]})
    await env.settle()
    assert env.snaps[-1] == {"state": "online", "url": URL}
    assert tokens.refresh == [False, False]
    assert_no_token_logged(caplog, "tok-1", "tok-2")
    await env.finish()


async def test_idempotence_compares_providers_with_eq() -> None:
    env = Env()
    tokens = Tokens()
    assert tokens.get is not tokens.get and tokens.get == tokens.get
    env.configure(TunnelConfig(True, tokens.get, "notes"))
    env.configure({"enabled": True, "token": tokens.get, "name": "notes"})
    await env.settle()
    assert env.connector.dials == 1

    def provider() -> str:
        return "t"

    env.configure(TunnelConfig(True, provider, "notes"))
    await env.settle()
    env.configure(TunnelConfig(True, provider, "notes"))
    await env.settle()
    assert env.connector.dials == 2
    env.configure(TunnelConfig(True, lambda: "t", "notes"))
    await env.settle()
    assert env.connector.dials == 3
    # A str token equal to what the provider returns is still a different config.
    env.configure(TunnelConfig(True, "t", "notes"))
    await env.settle()
    assert env.connector.dials == 4
    await env.finish()


async def test_a_provider_whose_eq_raises_still_dials(caplog: pytest.LogCaptureFixture) -> None:
    class Odd:
        def __call__(self) -> str:
            return "tok-odd"

        def __eq__(self, other: object) -> bool:
            raise RuntimeError("no comparing me")

        __hash__ = object.__hash__

    env = Env()
    odd = Odd()
    env.configure(TunnelConfig(True, odd, "notes"))
    await env.settle()
    assert env.connector.dials == 1
    env.sock().open()
    await env.settle()
    assert hello_token(env.sock()) == "tok-odd"
    # Configured again: the comparison raises, which counts as a change.
    env.configure(TunnelConfig(True, odd, "notes"))
    await env.settle()
    assert env.connector.dials == 2
    assert messages(caplog, logging.ERROR) == []
    await env.finish()


async def test_a_provider_is_neither_called_nor_inspected_by_configure() -> None:
    env = Env()
    tokens = Tokens()
    await env.tunnel.configure(TunnelConfig(False, tokens.get, "notes"))
    assert tokens.refresh == []
    assert env.snaps == [{"state": "off"}]
    await env.finish()


async def test_a_temporarily_unavailable_token_retries_the_dial(
    caplog: pytest.LogCaptureFixture,
) -> None:
    env = Env()
    tokens = Tokens(TokenUnavailableError("keychain locked"), "tok-ok")
    env.configure(TunnelConfig(True, tokens, "notes"))
    await env.settle()
    assert env.connector.dials == 0
    assert env.snaps == [
        {"state": "off"},
        {"state": "connecting"},
        {"state": "connecting", "code": "RETRYING", "lastError": "Can't reach mcpwarp; retrying."},
    ]
    assert len(env.retries) == 1
    assert (env.retries[0].attempt, env.retries[0].reason) == (1, "keychain locked")
    assert env.retries[0].delay_ms > 0
    await env.clock.advance(env.retries[0].delay_ms)
    env.sock().open()
    await env.settle()
    assert hello_token(env.sock()) == "tok-ok"
    env.sock().control(dict(WELCOME))
    await env.settle()
    env.sock().app({"op": "registered", "services": [{"name": "notes", "url": URL, "id": "s"}]})
    await env.settle()
    assert env.snaps[-1] == {"state": "online", "url": URL}
    assert "retry #1 in 1000 ms: keychain locked" in messages(caplog, logging.WARNING)
    assert_no_token_logged(caplog, "tok-ok")
    await env.finish()


async def test_a_failing_provider_is_fatal(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    failures: list[BaseException] = []
    fail = tunnel_mod.Tunnel._fail

    def spy(self: tunnel_mod.Tunnel, cfg: Any, err: BaseException, **kw: Any) -> None:
        failures.append(err)
        fail(self, cfg, err, **kw)

    monkeypatch.setattr(tunnel_mod.Tunnel, "_fail", spy)
    env = Env()
    boom = RuntimeError("vault exploded")
    tokens = Tokens(boom)
    env.configure(TunnelConfig(True, tokens, "notes"))
    await env.settle()
    assert env.snaps == [
        {"state": "off"},
        {"state": "connecting"},
        {
            "state": "error",
            "code": "DISCONNECTED_FATAL",
            "lastError": "The connection to mcpwarp failed.",
        },
    ]
    assert messages(caplog, logging.ERROR) == ["DISCONNECTED_FATAL: vault exploded"]
    (err,) = failures
    assert isinstance(err, McpwarpError) and err.code == "DISCONNECTED_FATAL" and err.fatal
    assert err.__cause__ is not None and err.__cause__.__cause__ is boom
    await env.clock.advance(120_000)
    assert env.retries == []
    assert len(tokens.refresh) == 1
    assert env.connector.dials == 0
    # The desired config was cleared: the same config starts again (T37).
    env.configure(TunnelConfig(True, tokens, "notes"))
    await env.settle()
    assert len(tokens.refresh) == 2
    assert env.connector.dials == 1
    await env.finish()


@pytest.mark.parametrize("rejection", ["http-401", "close-4011"])
async def test_a_rejected_token_gets_one_refresh_retry_then_is_unauthorized(
    caplog: pytest.LogCaptureFixture, rejection: str
) -> None:
    env = Env()
    tokens = Tokens("tok-stale", "tok-fresh")
    env.configure(TunnelConfig(True, tokens, "notes"))
    await env.settle()

    if rejection == "http-401":
        env.sock().reject(401)
    else:
        env.sock().open()
        await env.settle()
        env.sock().server_close(4011)
    await env.settle()
    assert tokens.refresh == [False, True]
    assert env.retries == [RetryInfo(1, 0, "token-refresh")]
    assert env.connector.dials == 2
    # T48: the rejected dial is a failed dial, as T27.
    assert env.snaps[-1] == {
        "state": "connecting",
        "code": "RETRYING",
        "lastError": "Can't reach mcpwarp; retrying.",
    }
    if rejection == "close-4011":
        assert hello_token(env.sock(0)) == "tok-stale"
        env.sock().open()
        await env.settle()
        assert hello_token(env.sock()) == "tok-fresh"
        env.sock().server_close(4011)
    else:
        init = env.sock().init
        assert init is not None and init.headers["Authorization"] == "Bearer tok-fresh"
        env.sock().reject(401)
    await env.settle()
    assert env.snaps[-1] == {
        "state": "error",
        "code": "UNAUTHORIZED",
        "lastError": "The token was rejected.",
    }
    await env.clock.advance(120_000)
    assert env.connector.dials == 2
    assert len(tokens.refresh) == 2
    assert_no_token_logged(caplog, "tok-stale", "tok-fresh")
    await env.finish()
