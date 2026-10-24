"""Every spec/fixtures/mcpwarp/tunnel/*.json case (MCPWARP.md section 10.4), ported from
ws-mixer-js's test/spec-tunnel-fixtures.test.ts.

create_tunnel() runs over a scripted fake edge sitting below the ws-mixer client (one
`EdgeSocket` per dial, so every fatal verdict is the client's own) on a FakeClock, with
`random` pinned to 0.5. The fake decodes what the SDK sends: it records the service name of each
connection's `register`, answers `ping` with `pong`, and answers `unregister` with
`unregistered` unless the connection is under `hold` (which also leaves the SDK's close
unanswered). After every event the loop runs until idle and zero-delay timers have fired; only
`retry` and `tick` move the clock. Every `disconnect` event also checks the `fatal` verdict the
SDK itself reached for that close against the fixture's (a spy on the exposure's disconnect
hook). The same spy gives the `post-disconnect-delivery` runnerRequirement: a disconnect's
`flush_after` ops reach the exposure's app hook right after the SDK has reported that close,
as the client's delivery queue would hand them over. In a fixture with a `token_provider` event
every token is a provider whose calls take the queued results.

At the end: every configure/stop/dispose call has finished, the primary listener's snapshots
(plus each `subscribe` event's one) equal `expect` exactly as `TunnelStatus.to_dict()`, the
connector was called `expectDials` times, and after a final dispose nothing is left running.

The `ms_*` group fixtures are skipped: host guidance (MCPWARP.md section 10.6 Fixture status).
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import _cases
from ws_mixer._jscompat import js_trim
from ws_mixer.client import DisconnectReason
from ws_mixer.control import encode_control
from ws_mixer.errors import TokenUnavailableError
from ws_mixer.frame import FrameType, decode_frame, encode_data
from ws_mixer.mcpwarp import (
    Request,
    Response,
    RetryInfo,
    Tunnel,
    TunnelConfig,
    create_tunnel,
)
from ws_mixer.mcpwarp.expose import Exposure
from ws_mixer.socket import WebSocketInit
from ws_mixer.testing import FakeClock, FakeConnector, FakeWebSocket

# The fixture count at spec.pin v0.8.0: 01-45 without 33, plus ms_01-ms_14.
TUNNEL_FIXTURES = 58
PROFILE_SKIP = "host guidance, §10.6 Fixture status"
# Section 10.6: group fixtures for the multi-service controller (section 6A).
MULTI_SERVICE_SKIPPED = (
    "ms_01_batch_all_ok",
    "ms_02_partial_errors",
    "ms_03_too_many_services_whole_frame",
    "ms_04_overloaded_resend_backoff",
    "ms_05_conflict_per_name",
    "ms_06_dashboard_disable_enable",
    "ms_07_subset_add_remove",
    "ms_08_reconnect_reregisters_full_list",
    "ms_09_fatal_first_reply",
    "ms_10_server_disabled_only_first_reply",
    "ms_11_first_reply_disabled_and_refused_not_fatal",
    "ms_12_overloaded_reserved_slot_fold_in",
    "ms_13_transient_then_overloaded_separate_counters",
    "ms_14_overloaded_all_removed_no_resend",
)
INTEGRATION = "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10"
WELCOME = {
    "t": "welcome",
    "v": 1,
    "session": "fixture",
    "window": 262_144,
    "max_streams": 100,
    "ping_interval": 3_600_000,
    "ping_timeout": 7_200_000,
}

# Error codes that name no frame; every other one answers the outstanding frame (§2.6).
NOT_ANSWERING_ERRORS = {"OVERLOADED", "CONNECTION_LIMIT"}
EDGE_MESSAGES = ("registered", "register_error", "app_error", "disabled", "enabled", "conflict")
SUPPORTED_REQUIREMENTS = {"post-disconnect-delivery"}

_CASES = _cases("mcpwarp/tunnel", "*.json")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "tunnel_case" in metafunc.fixturenames:
        if _CASES:
            metafunc.parametrize("tunnel_case", [p for p, _ in _CASES], ids=[i for _, i in _CASES])
        else:
            metafunc.parametrize("tunnel_case", [None], ids=["spec-unavailable"])


class EdgeSocket(FakeWebSocket):
    """One dial, scripted by the fixture: pending until `connected` / `dial_failed`."""

    def __init__(self) -> None:
        super().__init__()
        self.name: str | None = None
        """The service name this connection registered."""
        self.held = False

    def hold(self) -> None:
        self.held = True
        self.set_echo_close(False)  # the SDK's close goes unanswered too

    async def send(self, data: bytes) -> None:
        await super().send(data)
        frame = decode_frame(data)
        if frame.stream_id != 0 or frame.type != FrameType.DATA:
            return
        msg = json.loads(frame.payload)
        loop = asyncio.get_running_loop()
        if msg["t"] == "ping":
            loop.call_soon(self.control, {"t": "pong", "id": msg["id"]})
            return
        if msg["t"] != "app":
            return
        env = msg["body"].get("mcpwarp") or {}
        services = env.get("services") or [{}]
        if env.get("op") == "register":
            self.name = services[0].get("name")
        if env.get("op") == "unregister" and not self.held:
            name = services[0].get("name")
            unregistered = {"op": "unregistered", "services": [{"name": name, "id": f"srv_{name}"}]}
            loop.call_soon(self.app, unregistered)

    def control(self, msg: dict[str, Any]) -> None:
        self.push(encode_data(0, encode_control(msg)))

    def app(self, mcpwarp: dict[str, Any]) -> None:
        self.control({"t": "app", "body": {"mcpwarp": {"v": 1, **mcpwarp}}})

    def disconnect(self, ws_code: int | None) -> None:
        if ws_code is None or ws_code == 1006:
            self.server_drop()  # no close frame: the transport died
        else:
            self.server_close(ws_code)


def merged(base: dict[str, Any], over: dict[str, Any]) -> TunnelConfig | dict[str, Any]:
    """`over` shallowly over `base`, None = absent; a TunnelConfig when it can be one."""
    out = dict(base)
    for key, value in over.items():
        if value is None:
            out.pop(key, None)
        else:
            out[key] = value
    if {"enabled", "token", "name"} <= out.keys():
        return TunnelConfig(**out)
    return out


async def handler(request: Request) -> Response:
    return Response(200, body=b"ok")


class Runner:
    def __init__(self, fx: dict[str, Any], verdicts: list[bool] | None = None) -> None:
        """`verdicts`: the `fatal` of every disconnect the SDK reports, appended by a spy
        (see test_tunnel_fixture); None skips the README's fatal-verdict check."""
        self.fx = fx
        self.verdicts = verdicts
        self.clock = FakeClock()
        self.connector = FakeConnector(factory=lambda url, init: self._new_socket(url, init))
        self.retries: list[RetryInfo] = []
        self.server_ids: dict[str, str] = {}
        self.snapshots: list[dict[str, str]] = []
        self.calls: list[asyncio.Task[None]] = []
        self.overloaded_attempt = 0
        self.flushed: list[dict[str, Any]] = []
        """`disconnect.flush_after` app bodies, delivered once the SDK reports that close."""
        self.retry_after_ms: int | None = None
        """The last drain's retry_after_ms, a floor for the next `retry {cause: "drain"}`."""
        # A fixture with a `token_provider` event configures a provider in place of each
        # non-blank token (one per token string, so idempotence is unchanged); each call takes
        # the next queued result, "ok" when none is queued.
        self.uses_provider = any("token_provider" in ev for ev in fx["events"])
        self.providers: dict[str, Callable[[], str]] = {}
        self.token_results: list[str] = []
        self.tunnel: Tunnel = create_tunnel(
            integration=INTEGRATION,
            handler=handler,
            web_socket=self.connector,
            on_retry=self.retries.append,
            clock=self.clock,
            random=lambda: 0.5,
        )
        self.tunnel.subscribe(lambda s: self.snapshots.append(s.to_dict()))

    def _new_socket(self, url: str, init: WebSocketInit) -> FakeWebSocket:
        return EdgeSocket()

    def sock(self, payload: dict[str, Any]) -> EdgeSocket:
        sockets = self.connector.sockets
        conn = payload.get("conn")
        if conn is None:
            assert sockets, "no dial yet"
            s = sockets[-1]
        else:
            assert 1 <= conn <= len(sockets), f"no dial #{conn}"
            s = sockets[conn - 1]
        assert isinstance(s, EdgeSocket)
        return s

    def name_of(self, s: EdgeSocket) -> str:
        return s.name if s.name is not None else str(self.fx["config"]["name"])

    def server_id(self, s: EdgeSocket) -> str:
        name = self.name_of(s)
        return self.server_ids.get(name, f"srv_{name}")

    def track(self, aw: Awaitable[None]) -> None:
        self.calls.append(asyncio.ensure_future(aw))

    def provider(self, token: str) -> Callable[[], str]:
        token = js_trim(token)
        if token not in self.providers:

            def provide() -> str:
                result = self.token_results.pop(0) if self.token_results else "ok"
                if result == "unavailable":
                    raise TokenUnavailableError("token unavailable")
                if result == "error":
                    raise RuntimeError("token provider failed")
                return token

            self.providers[token] = provide
        return self.providers[token]

    def config(self, payload: dict[str, Any]) -> TunnelConfig | dict[str, Any]:
        cfg = merged(self.fx["config"], payload)
        token = cfg.token if isinstance(cfg, TunnelConfig) else cfg.get("token")
        if not self.uses_provider or not isinstance(token, str) or js_trim(token) == "":
            return cfg
        if isinstance(cfg, TunnelConfig):
            return dataclasses.replace(cfg, token=self.provider(token))
        return {**cfg, "token": self.provider(token)}

    def call(self, ev: dict[str, Any]) -> None:
        ((kind, payload),) = ev.items()
        if kind == "configure":
            self.track(self.tunnel.configure(self.config(payload)))
        elif kind == "stop":
            self.track(self.tunnel.stop())
        else:
            raise AssertionError(f"burst: unexpected {kind}")

    async def settle(self) -> None:
        # Runs the loop until idle and fires zero-delay timers (vi.advanceTimersByTimeAsync(0)).
        await self.clock.advance(0)

    async def step(self, ev: dict[str, Any]) -> None:
        ((kind, payload),) = ev.items()
        if kind in ("configure", "stop"):
            self.call(ev)
        elif kind == "burst":
            for inner in payload:
                self.call(inner)
        elif kind == "dispose":
            timeout = payload.get("timeoutMs")
            self.track(self.tunnel.dispose() if timeout is None else self.tunnel.dispose(timeout))
        elif kind == "subscribe":
            self.tunnel.subscribe(lambda s: self.snapshots.append(s.to_dict()))()
        elif kind == "connected":
            self.sock(payload).open()
        elif kind == "dial_failed":
            status = payload.get("httpStatus")
            if status is None:
                self.sock(payload).fail(ConnectionRefusedError("connection refused"))
            else:
                self.sock(payload).reject(status)
        elif kind == "welcome":
            self.sock(payload).control(dict(WELCOME))
        elif kind in EDGE_MESSAGES:
            s = self.sock(payload)
            s.app(self.edge_op(kind, payload, s))
        elif kind == "disconnect":
            await self.disconnect(ev, payload)
        elif kind == "drain":
            msg = {"t": "drain", "reason": payload.get("reason", "rollout"), "last_stream_id": 0}
            if "retry_after_ms" in payload:
                msg["retry_after_ms"] = self.retry_after_ms = payload["retry_after_ms"]
            self.sock(payload).control(msg)
        elif kind == "hold":
            self.sock(payload).hold()
        elif kind == "token_provider":
            result = payload["result"]
            assert result in ("ok", "unavailable", "error"), f"token_provider: {result!r}"
            self.token_results.append(result)
        elif kind == "retry":
            await self.retry(payload["delayMs"], payload["cause"])
        elif kind == "tick":
            self.retries.clear()
            await self.clock.advance(payload["ms"])
        else:
            raise AssertionError(f"unknown fixture event {kind}")
        await self.settle()

    def edge_op(self, kind: str, payload: dict[str, Any], s: EdgeSocket) -> dict[str, Any]:
        """The mcpwarp op the edge sends on `s` for an edge-message event."""
        body: dict[str, Any]
        if kind == "registered":
            for svc in payload["services"]:
                self.server_ids[self.name_of(s)] = svc["id"]
            services = [{"name": self.name_of(s), **svc} for svc in payload["services"]]
            self.overloaded_attempt = 0
            return {"op": "registered", "services": services}
        if kind == "register_error":
            self.overloaded_attempt = 0  # any reply but OVERLOADED ends the streak (v0.7.1)
            entry: dict[str, Any] = {"name": self.name_of(s), "code": payload["code"]}
            if "message" in payload:
                entry["message"] = payload["message"]
            if "id" in payload:
                entry["id"] = payload["id"]
                self.server_ids[self.name_of(s)] = payload["id"]
            return {"op": "registered", "errors": [entry]}
        if kind == "app_error":
            if payload["code"] not in NOT_ANSWERING_ERRORS:
                self.overloaded_attempt = 0
            body = {"op": "error", "code": payload["code"]}
            if "message" in payload:
                body["message"] = payload["message"]
            return body
        if kind == "disabled":
            body = {"op": "disable", "id": payload.get("id", self.server_id(s))}
        elif kind == "enabled":
            return {"op": "enable", "id": self.server_id(s), "name": self.name_of(s)}
        else:
            body = {"op": "conflict", "id": self.server_id(s), "name": self.name_of(s)}
        if "reason" in payload:
            body["reason"] = payload["reason"]
        return body

    def take_flushed(self) -> list[dict[str, Any]]:
        flushed, self.flushed = self.flushed, []
        return flushed

    async def disconnect(self, ev: dict[str, Any], payload: dict[str, Any]) -> None:
        """README "disconnect.fatal": the SDK's own verdict for this close must equal the
        fixture's. A connection the SDK closed before the event (a retired drained one, or a
        held stale one) reports nothing further, and nothing is expected then."""
        s = self.sock(payload)
        closed_by_sdk = s.closed or s.aborted
        before = None if self.verdicts is None else len(self.verdicts)
        for inner in payload.get("flush_after", ()):
            ((kind, op),) = inner.items()
            self.flushed.append({"mcpwarp": {"v": 1, **self.edge_op(kind, op, s)}})
        s.disconnect(payload.get("wsCode"))
        if self.verdicts is None or before is None:
            return
        await self.settle()
        got = self.verdicts[before:]
        if not got:
            assert closed_by_sdk, f"{ev}: the SDK reported no disconnect"
        assert all(fatal == payload["fatal"] for fatal in got), f"{ev}: SDK verdicts {got}"

    async def retry(self, max_delay: float, cause: str) -> None:
        """README "Running the fixtures": exactly one matching on_retry call was recorded since
        the last check; the clock then moves by the delay the SDK announced."""
        pending, self.retries[:] = list(self.retries), []
        what = f"retry ({cause})"
        assert len(pending) == 1, f"{what}: expected exactly one on_retry call, got {pending}"
        info = pending[0]
        assert info.delay_ms <= max_delay, f"{what}: delay {info.delay_ms} > {max_delay}"
        reason = info.reason
        if cause == "register":
            assert info.delay_ms == max_delay, f"{what}: delay {info.delay_ms}"
            assert reason.startswith("register failed: "), f"{what}: {reason!r}"
            assert not reason.startswith("register failed: OVERLOADED: "), f"{what}: {reason!r}"
        elif cause == "overloaded":
            self.overloaded_attempt += 1
            assert info.delay_ms == max_delay == 1000, f"{what}: delay {info.delay_ms}"
            assert reason.startswith("register failed: OVERLOADED: "), f"{what}: {reason!r}"
            assert info.attempt == self.overloaded_attempt, f"{what}: attempt {info.attempt}"
        elif cause == "drain":
            assert (info.attempt, reason) == (0, "drain"), f"{what}: {info}"
            assert self.retry_after_ms is None or info.delay_ms >= self.retry_after_ms, (
                f"{what}: delay {info.delay_ms} < retry_after_ms {self.retry_after_ms}"
            )
            self.retry_after_ms = None
        elif cause == "token-refresh":
            expected = (1, 0, "token-refresh")
            assert (info.attempt, info.delay_ms, reason) == expected, f"{what}: {info}"
        elif cause in ("dial", "lost"):
            assert reason != "drain" and not reason.startswith("register failed"), (
                f"{what}: {reason!r}"
            )
        else:
            raise AssertionError(f"unknown retry cause {cause}")
        await self.clock.advance(info.delay_ms)


def test_tunnel_fixture_count(spec_fixtures: Path) -> None:
    files = sorted((spec_fixtures / "mcpwarp" / "tunnel").glob("*.json"))
    assert len(files) == TUNNEL_FIXTURES
    assert len(MULTI_SERVICE_SKIPPED) == 14
    assert set(MULTI_SERVICE_SKIPPED) <= {f.stem for f in files}


@pytest.mark.asyncio
async def test_tunnel_fixture(
    spec_fixtures: Path, tunnel_case: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if tunnel_case.stem in MULTI_SERVICE_SKIPPED:
        pytest.skip(f"{PROFILE_SKIP}: {tunnel_case.stem}")
    fx = json.loads(tunnel_case.read_text(encoding="utf-8"))
    requirement = fx.get("runnerRequirement")
    if requirement is not None and requirement not in SUPPORTED_REQUIREMENTS:
        pytest.skip(f"runnerRequirement {requirement!r}: not supported by this runner")
    baseline = set(asyncio.all_tasks())
    verdicts: list[bool] = []
    reported = Exposure._on_disconnect_from

    def spy(self: Exposure, reason: DisconnectReason, superseded: bool) -> None:
        verdicts.append(reason.fatal)
        reported(self, reason, superseded)
        # Post-disconnect delivery: what the client would hand the layer from its queue.
        for body in runner.take_flushed():
            self._on_app_from(body, False)

    monkeypatch.setattr(Exposure, "_on_disconnect_from", spy)
    runner = Runner(fx, verdicts)
    for ev in fx["events"]:
        await runner.step(ev)

    unsettled = [t for t in runner.calls if not t.done()]
    snapshots = list(runner.snapshots)
    dials = runner.connector.dials

    # Leave nothing running: a final bounded dispose, then let every close budget expire.
    final = asyncio.ensure_future(runner.tunnel.dispose(1))
    await runner.clock.advance(60_000)
    assert final.done()
    for t in runner.calls:
        if t.done():
            t.result()  # configure/stop/dispose never raise

    assert not unsettled, f"{len(unsettled)} configure/stop/dispose calls still pending"
    assert snapshots == fx["expect"]
    assert dials == fx["expectDials"]
    assert runner.clock.pending() == 0, "a timer outlived the tunnel"
    leaked = set(asyncio.all_tasks()) - baseline - {asyncio.current_task()}
    assert not leaked, leaked
