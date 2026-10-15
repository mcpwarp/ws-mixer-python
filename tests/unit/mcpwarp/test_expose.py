"""expose() and ExposeHandle (MCPWARP.md section 5) end to end against the FakeEdge on a FakeClock.

`random` is pinned to 0.5 unless a test says otherwise, so a drain's replacement dial waits
1000 ms and full jitter after the n-th failure is 0.5 * min(cap, base * 2**n). Every test ends
in `finish()`, which closes what is left and asserts that no timer and no task outlived it.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gc
import logging
import math
import platform
import warnings
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
import pytest_asyncio

from ws_mixer import SDK_VERSION
from ws_mixer.client import DisconnectReason, ReconnectOptions
from ws_mixer.errors import ErrorCode, StreamError, WsMixerError
from ws_mixer.mcpwarp import (
    Agent,
    Closed,
    Conflict,
    Disabled,
    Enabled,
    ExposeHandle,
    ExposeOptions,
    ExposeState,
    McpwarpError,
    Registration,
    Request,
    Response,
    RetryInfo,
    expose,
)
from ws_mixer.mcpwarp.expose import ExposeHooks, Exposure, validate_options
from ws_mixer.stream import Finished, Reset
from ws_mixer.testing import EdgeConnection, FakeClock, FakeEdge, settle

pytestmark = pytest.mark.asyncio

UUID = "0189b5d2-3c4e-7a1b-8c2d-0123456789ab"
NAME = "anki"


async def echo(request: Request) -> Response:
    ct = request.headers.get("content-type")
    return Response(200, [("content-type", ct)] if ct else [], request.body)


class Env:
    def __init__(self, *, random: float = 0.5, **edge_kw: Any) -> None:
        self.clock = FakeClock()
        self.edge = FakeEdge(clock=self.clock, **edge_kw)
        self.retries: list[RetryInfo] = []
        self.random = random
        self.handles: list[ExposeHandle] = []

    def opts(self, **kw: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "token": "tok",
            "name": NAME,
            "integration": UUID,
            "url": self.edge.url,
            "web_socket": self.edge.web_socket,
            "handler": echo,
            "clock": self.clock,
            "random": lambda: self.random,
            "on_retry": self.retries.append,
        }
        base.update(kw)
        return base

    def start(self, **kw: Any) -> asyncio.Task[ExposeHandle]:
        return asyncio.create_task(expose(**self.opts(**kw)))

    async def expose(self, **kw: Any) -> ExposeHandle:
        await self.edge.start()
        task = self.start(**kw)
        await settle()
        handle = await asyncio.wait_for(task, 1)
        self.handles.append(handle)
        return handle

    def conn(self, i: int = -1) -> EdgeConnection:
        return self.edge.connections[i]

    def ops(self) -> list[str]:
        return [b["mcpwarp"]["op"] for b in self.edge.app_log]

    async def post(self, body: str = "hi", conn: EdgeConnection | None = None, path: str = "/mcp"):  # type: ignore[no-untyped-def]
        url = f"https://{NAME}-tester.mcpwarp.test{path}"
        return await asyncio.wait_for(self.edge.request("POST", url, body=body, conn=conn), 2)


def record(handle: ExposeHandle, *events: str) -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []

    def listener(event: str) -> Callable[[object], None]:
        return lambda payload: out.append((event, payload))

    for event in events:
        handle.on(event, listener(event))
    return out


def state(handle: ExposeHandle) -> ExposeState:
    """Read through a call, so mypy does not narrow it across awaits."""
    return handle.state


@pytest_asyncio.fixture
async def baseline() -> AsyncIterator[set[asyncio.Task[Any]]]:
    yield set(asyncio.all_tasks())


async def finish(env: Env, baseline: set[asyncio.Task[Any]]) -> None:
    for handle in env.handles:
        closing = asyncio.create_task(handle.close())
        await settle()
        await env.clock.advance(10_000)
        await asyncio.wait_for(closing, 1)
    await env.edge.stop()
    await settle()
    await env.clock.advance(10_000)
    assert env.clock.pending() == 0, "a timer outlived the exposure"
    leaked = set(asyncio.all_tasks()) - baseline - {asyncio.current_task()}
    assert not leaked, leaked


async def rejection(task: asyncio.Task[ExposeHandle]) -> McpwarpError:
    with pytest.raises(McpwarpError) as info:
        await asyncio.wait_for(task, 1)
    return info.value


def controls(conn: EdgeConnection) -> list[str]:
    return [m["t"] for m in conn.controls]


# --- validation (section 5.2) ------------------------------------------------------------------

NAME_MSG = (
    "mcpwarp: invalid service `name`: lowercase letters, digits and inner dashes, "
    "at most 30 characters"
)
INTEGRATION_MISSING = "mcpwarp: `integration` (your integration's registration UUID) is required"
INTEGRATION_BAD = (
    "mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently "
    "ignores anything else"
)
TIMEOUT_MSG = "mcpwarp: target_timeout_ms must be a positive number of at most 2147483647 ms, got "
HANDLER_TIMEOUT_MSG = (
    "mcpwarp: handler_timeout_ms must be a positive number of at most 2147483647 ms, got "
)

SECRET = "Sup3r-Secret-Value"

VALIDATION_ROWS: list[tuple[dict[str, Any], type[Exception], str]] = [
    ({"handler": None}, TypeError, "mcpwarp: pass exactly one of `handler` or `target`"),
    ({"handler": "x"}, TypeError, "mcpwarp: pass exactly one of `handler` or `target`"),
    (
        {"target": "http://127.0.0.1:9"},
        TypeError,
        "mcpwarp: pass exactly one of `handler` or `target`",
    ),
    (
        {"handler": "x", "target": "http://127.0.0.1:9"},
        TypeError,
        "mcpwarp: pass exactly one of `handler` or `target`",
    ),
    ({"target_timeout_ms": 5}, TypeError, "mcpwarp: `target_timeout_ms` applies only to `target`"),
    ({"handler": None, "target": SECRET}, ValueError, "mcpwarp: `target` is not a valid URL"),
    (
        {"handler": None, "target": f"ftp://{SECRET}"},
        ValueError,
        "mcpwarp: `target` is not a valid URL",
    ),
    ({"handler": None, "target": 5}, TypeError, "mcpwarp: `target` is not a valid URL"),
    (
        {"handler": None, "target": "http://x", "target_timeout_ms": 0},
        ValueError,
        TIMEOUT_MSG + "0",
    ),
    (
        {"handler": None, "target": "http://x", "target_timeout_ms": -1.5},
        ValueError,
        TIMEOUT_MSG + "-1.5",
    ),
    (
        {"handler": None, "target": "http://x", "target_timeout_ms": 3e9},
        ValueError,
        TIMEOUT_MSG + "3000000000",
    ),
    (
        {"handler": None, "target": "http://x", "target_timeout_ms": math.nan},
        ValueError,
        TIMEOUT_MSG + "NaN",
    ),
    (
        {"handler": None, "target": "http://x", "target_timeout_ms": math.inf},
        ValueError,
        TIMEOUT_MSG + "Infinity",
    ),
    (
        {"handler": None, "target": "http://x", "target_timeout_ms": "5"},
        TypeError,
        TIMEOUT_MSG + "str",
    ),
    (
        {"handler": None, "target": "http://x", "target_timeout_ms": True},
        TypeError,
        TIMEOUT_MSG + "bool",
    ),
    (
        {"handler": None, "target": "http://x", "handler_timeout_ms": 5},
        TypeError,
        "mcpwarp: `handler_timeout_ms` applies only to `handler`",
    ),
    ({"handler_timeout_ms": 0}, ValueError, HANDLER_TIMEOUT_MSG + "0"),
    ({"handler_timeout_ms": -1.5}, ValueError, HANDLER_TIMEOUT_MSG + "-1.5"),
    ({"handler_timeout_ms": 3e9}, ValueError, HANDLER_TIMEOUT_MSG + "3000000000"),
    ({"handler_timeout_ms": math.nan}, ValueError, HANDLER_TIMEOUT_MSG + "NaN"),
    ({"handler_timeout_ms": math.inf}, ValueError, HANDLER_TIMEOUT_MSG + "Infinity"),
    ({"handler_timeout_ms": "5"}, TypeError, HANDLER_TIMEOUT_MSG + "str"),
    ({"handler_timeout_ms": True}, TypeError, HANDLER_TIMEOUT_MSG + "bool"),
    ({"name": SECRET}, ValueError, NAME_MSG),
    ({"name": "a" * 31}, ValueError, NAME_MSG),
    ({"name": "-anki"}, ValueError, NAME_MSG),
    ({"name": None}, TypeError, NAME_MSG),
    ({"integration": None}, TypeError, INTEGRATION_MISSING),
    ({"integration": ""}, ValueError, INTEGRATION_MISSING),
    ({"integration": SECRET}, ValueError, INTEGRATION_BAD),
    ({"integration": f" {UUID}"}, ValueError, INTEGRATION_BAD),
    ({"kind": SECRET}, ValueError, 'mcpwarp: `kind` must be "http" or "stdio"'),
    ({"kind": 1}, TypeError, 'mcpwarp: `kind` must be "http" or "stdio"'),
    ({"token": None}, TypeError, "mcpwarp: `token` is required"),
    ({"token": ""}, ValueError, "mcpwarp: `token` is required"),
    ({"token": 12345}, TypeError, "mcpwarp: `token` is required"),
    ({"agent": {"runtime": "x"}}, TypeError, "mcpwarp: `agent` must be an object"),
    (
        {"agent": Agent(runtime="")},
        ValueError,
        "mcpwarp: `agent.runtime` must be 1-64 printable ASCII characters",
    ),
    (
        {"agent": Agent(runtime="x" * 65)},
        ValueError,
        "mcpwarp: `agent.runtime` must be 1-64 printable ASCII characters",
    ),
    (
        {"agent": Agent(os=f"{SECRET}é")},
        ValueError,
        "mcpwarp: `agent.os` must be 1-64 printable ASCII characters",
    ),
    (
        {"agent": Agent(os=5)},  # type: ignore[arg-type]
        TypeError,
        "mcpwarp: `agent.os` must be 1-64 printable ASCII characters",
    ),
    ({"reconnect": {"base_ms": 1}}, TypeError, "mcpwarp: `reconnect` must be a ReconnectOptions"),
    (
        {"max_request_body_bytes": 0},
        ValueError,
        "mcpwarp: `max_request_body_bytes` must be positive",
    ),
]


@pytest.mark.parametrize(("override", "exc", "message"), VALIDATION_ROWS)
async def test_validation_rows(
    override: dict[str, Any], exc: type[Exception], message: str
) -> None:
    env = Env()
    await env.edge.start()
    with pytest.raises(exc) as info:
        await expose(**env.opts(**override))
    assert type(info.value) is exc
    assert str(info.value) == message
    assert SECRET not in str(info.value)
    assert env.edge.upgrades == []


async def test_validation_reconnect_fields_are_checked_again() -> None:
    env = Env()
    bad = ReconnectOptions()
    object.__setattr__(bad, "stable_after_ms", math.inf)
    with pytest.raises(ValueError) as info:
        await expose(**env.opts(reconnect=bad))
    assert str(info.value).startswith("mcpwarp: reconnect.stable_after_ms must be")
    assert env.edge.upgrades == []


async def test_validation_first_failure_wins_in_the_spec_order() -> None:
    env = Env()
    cases: list[tuple[dict[str, Any], str]] = [
        ({"name": "BAD", "target": "http://x"}, "pass exactly one"),
        ({"name": "BAD", "integration": "nope"}, "invalid service `name`"),
        ({"integration": "nope", "kind": "ftp"}, "RFC 4122"),
        ({"kind": "ftp", "token": ""}, "`kind`"),
        ({"token": "", "agent": "x"}, "`token`"),
        ({"agent": "x", "reconnect": 1}, "`agent`"),
    ]
    for override, needle in cases:
        with pytest.raises((TypeError, ValueError)) as info:
            await expose(**env.opts(**override))
        assert needle in str(info.value), (override, info.value)


async def test_options_object_or_keywords_but_not_both() -> None:
    env = Env()
    opts = ExposeOptions(**env.opts())
    with pytest.raises(TypeError, match="not both"):
        await expose(opts, name="anki")
    with pytest.raises(TypeError, match="unexpected keyword argument 'nmae'"):
        await expose(**env.opts(nmae="anki"))
    with pytest.raises(TypeError, match="ExposeOptions"):
        await expose({"name": "anki"})  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="invalid service `name`"):
        await expose(token="t", integration=UUID, handler=echo)  # name missing
    assert validate_options(opts) is echo


async def test_an_options_object_works(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    await env.edge.start()
    handle = await asyncio.wait_for(expose(ExposeOptions(**env.opts())), 1)
    env.handles.append(handle)
    assert state(handle) == ExposeState.REGISTERED
    await finish(env, baseline)


async def test_an_already_cancelled_task_never_dials(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    await env.edge.start()
    task = env.start()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await settle()
    assert env.edge.upgrades == []
    await finish(env, baseline)


# --- identity (sections 3.1, 5.3) ----------------------------------------------------------------


async def test_identity_in_hello_meta_agent_and_headers(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    await env.expose(integration=UUID.upper())
    hello = env.edge.hellos[0]
    meta = hello.get("meta")
    assert meta is not None
    assert meta["client"] == "sdk-python"
    assert meta["integration"] == UUID
    assert isinstance(meta["version"], str) and 0 < len(meta["version"].encode()) <= 100
    assert meta["version"] == SDK_VERSION
    assert meta["os"] == hello["agent"].get("os")
    assert hello["agent"]["sdk"] == "ws-mixer-py"
    assert hello["agent"]["sdk_version"] == SDK_VERSION
    assert hello["agent"].get("runtime") == f"python/{platform.python_version()}"
    assert hello["token"] == "tok"
    headers = env.edge.upgrades[0].headers
    assert headers["X-Mcpwarp-Integration"] == UUID
    assert headers["Authorization"] == "Bearer tok"
    assert headers["User-Agent"].startswith("ws-mixer-py/")
    await finish(env, baseline)


async def test_agent_overrides_runtime_and_os(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    await env.expose(agent=Agent(runtime="anki/25.02", os="Mac OS X"))
    hello = env.edge.hellos[0]
    assert hello["agent"].get("runtime") == "anki/25.02"
    assert hello["agent"].get("os") == "Mac OS X"
    assert hello.get("meta", {})["os"] == "Mac OS X"
    ua = env.edge.upgrades[0].headers["User-Agent"]
    assert ua.endswith("(anki/25.02 Mac OS X)")
    await finish(env, baseline)


async def test_a_token_provider_is_called_on_every_dial(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    calls: list[int] = []

    async def provider() -> str:
        calls.append(1)
        return f"tok{len(calls)}"

    handle = await env.expose(token=provider)
    env.edge.close_connection(4000 + ErrorCode.INTERNAL_ERROR)
    await settle()
    assert state(handle) == ExposeState.CONNECTING
    await env.clock.advance(1000)
    assert state(handle) == ExposeState.REGISTERED
    assert [h["token"] for h in env.edge.hellos] == ["tok1", "tok2"]
    await finish(env, baseline)


# --- resolution (section 5.4) ---------------------------------------------------------------------


async def test_resolves_registered_with_url_registration_and_server_id(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose()
    assert state(handle) == ExposeState.REGISTERED
    assert handle.url == env.edge.public_url(NAME)
    assert handle.server_id == "srv_anki"
    assert handle.registration == Registration(env.edge.public_url(NAME), "srv_anki", True)
    assert handle.path == "/mcp"
    assert handle.client.state == "connected"
    assert env.ops() == ["register"]
    await finish(env, baseline)


async def test_resolves_disabled_on_server_disabled(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    env.edge.disable(NAME)
    handle = await env.expose()
    assert state(handle) == ExposeState.DISABLED
    assert handle.url is None and handle.registration is None
    assert handle.server_id == "srv_anki"
    await finish(env, baseline)


async def test_resolves_disabled_on_a_disable_op(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    env.edge.withhold_registered(NAME)
    await env.edge.start()
    task = env.start()
    await env.edge.wait_for_app("register")
    env.edge.send_app({"mcpwarp": {"v": 1, "op": "disable", "id": "srv_9", "reason": "abuse"}})
    await settle()
    handle = await asyncio.wait_for(task, 1)
    env.handles.append(handle)
    assert state(handle) == ExposeState.DISABLED and handle.server_id == "srv_9"
    await finish(env, baseline)


@pytest.mark.parametrize("code", ["INVALID_NAME", "QUOTA_EXCEEDED", "NOT_IMPLEMENTED", "NEW_CODE"])
async def test_a_refusal_rejects_with_its_code_and_message(
    baseline: set[asyncio.Task[Any]], code: str
) -> None:
    env = Env()
    env.edge.fail_register(NAME, {"code": code, "message": "nope"})
    await env.edge.start()
    err = await rejection(env.start())
    assert (err.code, err.message, err.fatal, err.service_name) == (code, "nope", True, NAME)
    await settle()
    await env.clock.advance(120_000)
    assert env.edge.connections == [] and len(env.edge.upgrades) == 1
    await finish(env, baseline)


async def test_unsupported_version_rejects(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    env.edge.withhold_registered(NAME)
    await env.edge.start()
    task = env.start()
    await env.edge.wait_for_app("register")
    env.edge.app_error("UNSUPPORTED_VERSION", "upgrade")
    err = await rejection(task)
    assert (err.code, err.message, err.fatal) == ("UNSUPPORTED_VERSION", "upgrade", True)
    await finish(env, baseline)


async def test_a_rejected_token_4011_rejects_unauthorized(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env(accept_token=lambda t: t == "good")
    await env.edge.start()
    err = await rejection(env.start())
    assert err.code == "UNAUTHORIZED" and err.fatal
    cause = err.__cause__
    assert isinstance(cause, WsMixerError) and cause.code == ErrorCode.UNAUTHORIZED
    assert len(env.edge.upgrades) == 1
    await finish(env, baseline)


async def test_http_401_rejects_unauthorized_and_403_disconnected_fatal(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    await env.edge.start()
    env.edge.reject_next_upgrade(401)
    err = await rejection(env.start())
    assert err.code == "UNAUTHORIZED"
    assert isinstance(err.__cause__, WsMixerError) and err.__cause__.http_status == 401
    env.edge.reject_next_upgrade(403)
    err = await rejection(env.start())
    assert err.code == "DISCONNECTED_FATAL"
    assert isinstance(err.__cause__, WsMixerError) and err.__cause__.http_status == 403
    await finish(env, baseline)


async def test_exhausted_attempts_before_registration_reject_disconnected_fatal(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()  # not started: every dial is refused
    task = env.start(reconnect=ReconnectOptions(max_attempts=2))
    await settle()
    await env.clock.advance(60_000)
    err = await rejection(task)
    assert err.code == "DISCONNECTED_FATAL" and "exhausted" in err.message
    assert isinstance(err.__cause__, WsMixerError) and err.__cause__.fatal
    assert len(env.edge.upgrades) == 3
    assert [r.attempt for r in env.retries] == [1, 2]
    await finish(env, baseline)


async def test_conflict_before_registered_rejects_taken_over(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    env.edge.withhold_registered(NAME)
    await env.edge.start()
    task = env.start()
    await env.edge.wait_for_app("register")
    conn = env.conn()
    env.edge.conflict(NAME)
    err = await rejection(task)
    assert err.code == "TAKEN_OVER" and err.fatal and err.service_name == NAME
    assert str(err) == (
        'mcpwarp: service "anki" was taken over by another connection before it registered'
    )
    await settle()
    await env.clock.advance(120_000)
    assert conn.closed and controls(conn)[-1] == "error"
    assert conn.controls[-1]["code"] == ErrorCode.NO_ERROR
    assert "drain" not in controls(conn)
    assert len(env.edge.upgrades) == 1 and env.ops() == ["register"]
    await finish(env, baseline)


async def test_drain_replaced_before_registered_rejects_taken_over_with_no_redial(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env(random=0)  # zero drain jitter: the replacement dial would start at once
    env.edge.withhold_registered(NAME)
    await env.edge.start()
    task = env.start()
    await env.edge.wait_for_app("register")
    env.edge.drain("replaced")
    err = await rejection(task)
    assert err.code == "TAKEN_OVER"
    await settle()
    await env.clock.advance(120_000)
    assert len(env.edge.upgrades) == 1
    assert env.retries == []
    await finish(env, baseline)


async def test_close_before_settling_rejects_closed(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    env.edge.withhold_registered(NAME)
    await env.edge.start()
    opts = ExposeOptions(**env.opts())
    exposure = Exposure(opts, validate_options(opts))
    task = asyncio.create_task(exposure.run())
    await env.edge.wait_for_app("register")
    closing = asyncio.create_task(exposure.handle.close())
    await settle()
    await env.clock.advance(2000)
    await asyncio.wait_for(closing, 1)
    err = await rejection(task)
    assert (err.code, str(err)) == ("CLOSED", "mcpwarp: closed before the service was registered")
    assert state(exposure.handle) == ExposeState.CLOSED
    await finish(env, baseline)


async def test_cancelling_before_settling_closes_the_client_and_reraises(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    env.edge.withhold_registered(NAME)
    await env.edge.start()
    seen: list[tuple[str, object, str | None]] = []
    opts = ExposeOptions(**env.opts())
    exposure = Exposure(
        opts, validate_options(opts), ExposeHooks(observe=lambda *a: seen.append(a))
    )
    task = asyncio.create_task(exposure.run())
    await env.edge.wait_for_app("register")
    conn = env.conn()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await settle()
    assert conn.closed
    assert state(exposure.handle) == ExposeState.CLOSED
    closes = [p for e, p, _ in seen if e == "close"]
    assert len(closes) == 1
    assert isinstance(closes[0], Closed) and isinstance(closes[0].error, asyncio.CancelledError)
    await env.clock.advance(120_000)
    assert len(env.edge.upgrades) == 1
    await finish(env, baseline)


async def test_a_background_close_cancelled_before_it_runs_leaves_no_unawaited_coroutine(
    baseline: set[asyncio.Task[Any]],
) -> None:
    """A hard loop stop can cancel the background client close in the very step that spawned
    it; nothing may be left un-awaited."""
    env = Env()
    env.edge.withhold_registered(NAME)
    await env.edge.start()

    def cancel_at_once(task: asyncio.Future[Any]) -> None:
        task.cancel()

    opts = ExposeOptions(**env.opts())
    exposure = Exposure(opts, validate_options(opts), ExposeHooks(on_client_close=cancel_at_once))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        task = asyncio.create_task(exposure.run())
        await env.edge.wait_for_app("register")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await settle()
        gc.collect()
    never_awaited = [w for w in caught if "never awaited" in str(w.message)]
    assert never_awaited == []
    await asyncio.wait_for(exposure.client.close(), 1)  # the cancelled close left it open
    await finish(env, baseline)


async def test_cancelled_after_resolving_closes_the_unreceived_handle(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    await env.edge.start()
    holder: list[asyncio.Task[ExposeHandle]] = []

    def observe(event: str, payload: object, detail: str | None) -> None:
        if event == "registered":
            holder[0].cancel()  # the caller goes away in the very step the handle resolves

    opts = ExposeOptions(**env.opts())
    exposure = Exposure(opts, validate_options(opts), ExposeHooks(observe=observe))
    holder.append(asyncio.create_task(exposure.run()))
    with pytest.raises(asyncio.CancelledError):
        await holder[0]
    await settle()
    assert env.ops() == ["register", "unregister"]
    assert state(exposure.handle) == ExposeState.CLOSED
    await finish(env, baseline)


# --- on_retry (section 5.9) ---------------------------------------------------------------------


async def test_dial_failures_report_on_retry_until_the_edge_is_up(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    task = env.start()
    await settle()
    # Full jitter after the first failure: 0.5 * min(cap, 1000 * 2**1).
    assert env.retries == [RetryInfo(1, 1000.0, "fake edge: not started")]
    await env.edge.start()
    await env.clock.advance(1000)
    handle = await asyncio.wait_for(task, 1)
    env.handles.append(handle)
    assert state(handle) == ExposeState.REGISTERED
    await finish(env, baseline)


async def test_on_retry_for_a_zero_delay_redial(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    handle = await env.expose()
    env.edge.close_connection(4000 + ErrorCode.KEEPALIVE_TIMEOUT)
    await settle()
    assert env.retries == [RetryInfo(1, 0, "keepalive_timeout")]
    assert state(handle) == ExposeState.REGISTERED
    assert env.ops() == ["register", "register"]
    await finish(env, baseline)


async def test_on_retry_for_a_drain_is_attempt_0_with_the_jitter(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    gate = Gate()
    handle = await env.expose(handler=gate)
    inflight = asyncio.create_task(env.post())  # keeps the drained connection open
    await settle()
    env.edge.drain("rollout")
    await settle()
    assert env.retries == [RetryInfo(0, 1000.0, "drain")]
    await env.clock.advance(1000)
    assert state(handle) == ExposeState.REGISTERED
    assert len(env.edge.upgrades) == 2
    gate.release.set()
    assert (await inflight).status == 200
    await finish(env, baseline)


async def test_on_retry_for_the_token_refresh_redial(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    await env.edge.start()
    env.edge.reject_next_upgrade(401)
    handle = await env.expose(token=lambda: "tok")
    assert env.retries == [RetryInfo(1, 0, "token-refresh")]
    assert state(handle) == ExposeState.REGISTERED
    assert len(env.edge.upgrades) == 2
    await finish(env, baseline)


async def test_on_retry_for_overloaded_counts_1_2_3_and_resets(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    env.edge.withhold_registered(NAME, times=3)
    await env.edge.start()
    task = env.start()
    for n in range(1, 4):
        await env.edge.wait_for_app("register", count=n)
        env.edge.app_error("OVERLOADED", "busy")
        await settle()
        await env.clock.advance(1000)
    handle = await asyncio.wait_for(task, 1)
    env.handles.append(handle)
    env.edge.withhold_registered(NAME)
    env.edge.disable(NAME)
    env.edge.enable(NAME)
    await env.edge.wait_for_app("register", count=5)
    env.edge.app_error("OVERLOADED", "busy")
    await settle()
    await env.clock.advance(1000)
    reason = "register failed: OVERLOADED: busy"
    assert env.retries == [RetryInfo(n, 1000, reason) for n in (1, 2, 3, 1)]
    assert state(handle) == ExposeState.REGISTERED
    await finish(env, baseline)


async def test_on_retry_for_transient_register_errors_backs_off(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    env.edge.fail_register(NAME, {"code": "UNAVAILABLE", "message": "busy"}, times=7)
    await env.edge.start()
    task = env.start()
    await settle()
    for _ in range(7):
        await env.clock.advance(env.retries[-1].delay_ms)
    handle = await asyncio.wait_for(task, 1)
    env.handles.append(handle)
    assert [(r.attempt, r.delay_ms) for r in env.retries] == [
        (1, 1000),
        (2, 2000),
        (3, 4000),
        (4, 8000),
        (5, 16000),
        (6, 30000),
        (7, 30000),
    ]
    assert {r.reason for r in env.retries} == {"register failed: UNAVAILABLE: busy"}
    await finish(env, baseline)


async def test_on_retry_raising_is_an_error_event(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    calls: list[RetryInfo] = []

    def bad(info: RetryInfo) -> None:
        calls.append(info)
        raise RuntimeError("listener bug")

    handle = await env.expose(on_retry=bad)
    events = record(handle, "error")
    env.edge.close_connection(4000 + ErrorCode.KEEPALIVE_TIMEOUT)
    await settle()
    assert len(calls) == 1
    assert [str(p) for _, p in events] == ["listener bug"]
    assert state(handle) == ExposeState.REGISTERED
    await finish(env, baseline)


async def test_on_retry_returning_a_coroutine_is_refused(
    baseline: set[asyncio.Task[Any]], caplog: pytest.LogCaptureFixture
) -> None:
    env = Env()

    async def coro(info: RetryInfo) -> None:
        pass

    handle = await env.expose(on_retry=coro)
    with caplog.at_level(logging.WARNING, logger="mcpwarp"):
        env.edge.close_connection(4000 + ErrorCode.KEEPALIVE_TIMEOUT)
        await settle()
    assert "on_retry returned a coroutine" in caplog.text
    assert state(handle) == ExposeState.REGISTERED
    await finish(env, baseline)


# --- drain hand-over (sections 2.7, 5.9) --------------------------------------------------------


class Gate:
    """A handler that holds each request until released."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.seen = 0

    async def __call__(self, request: Request) -> Response:
        self.seen += 1
        await self.release.wait()
        return Response(200, [], request.body)


async def test_hand_over_old_connection_serves_until_done_then_retires(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    gate = Gate()
    handle = await env.expose(handler=gate)
    events = record(handle, "registered", "disconnect")
    old = env.conn()
    inflight = asyncio.create_task(env.post("old"))
    await settle()
    assert gate.seen == 1
    env.edge.drain("rollout")
    await settle()
    await env.clock.advance(1000)
    new = env.conn()
    assert new is not old and not old.closed
    assert state(handle) == ExposeState.REGISTERED
    assert [e for e, _ in events] == ["registered"]
    assert events[0][1] == Registration(env.edge.public_url(NAME), "srv_anki", False)
    gate.release.set()
    response = await inflight
    assert (response.status, response.body, response.end) == (200, b"old", Finished())
    await settle()
    assert old.closed
    assert [e for e, _ in events] == ["registered", "disconnect"]
    assert state(handle) == ExposeState.REGISTERED
    fresh = await env.post("new")
    assert fresh.body == b"new" and env.conn() is new
    await finish(env, baseline)


async def test_regression_superseded_app_messages_are_ignored(
    baseline: set[asyncio.Task[Any]],
) -> None:
    """JS bug: a drained connection's app messages after its replacement's welcome were
    still applied."""
    env = Env()
    gate = Gate()
    handle = await env.expose(handler=gate)
    events = record(handle, "registered", "disabled", "enabled", "conflict", "error")
    old = env.conn()
    inflight = asyncio.create_task(env.post(conn=old))
    await settle()
    env.edge.drain("rollout")
    await settle()
    await env.clock.advance(1000)
    assert env.conn() is not old and state(handle) == ExposeState.REGISTERED
    events.clear()
    svc = {"name": NAME, "id": "srv_stale", "url": "https://stale/mcp"}
    for env_msg in (
        {"op": "registered", "services": [svc]},
        {"op": "disable", "id": "srv_anki"},
        {"op": "enable", "id": "srv_anki", "name": NAME},
        {"op": "conflict", "id": "srv_anki", "name": NAME},
        {"op": "error", "code": "UNSUPPORTED_VERSION"},
    ):
        env.edge.send_app({"mcpwarp": {"v": 1, **env_msg}}, conn=old)
    await settle()
    assert events == []
    assert state(handle) == ExposeState.REGISTERED and handle.server_id == "srv_anki"
    assert env.ops() == ["register", "register"]
    gate.release.set()
    await inflight
    await finish(env, baseline)


async def test_regression_drained_end_does_not_flip_state_while_replacement_registers(
    baseline: set[asyncio.Task[Any]],
) -> None:
    """JS bug: a drained connection's end flipped status to connecting after the replacement
    was already welcomed. Decided by connection identity, also while still registering."""
    env = Env()
    gate = Gate()
    handle = await env.expose(handler=gate)
    disconnects = record(handle, "disconnect")
    old = env.conn()
    inflight = asyncio.create_task(env.post(conn=old))
    await settle()
    env.edge.withhold_registered(NAME)
    env.edge.drain("rollout")
    await settle()
    await env.clock.advance(1000)
    assert state(handle) == ExposeState.REGISTERING  # the replacement's register is pending
    env.edge.close_connection(4000 + ErrorCode.INTERNAL_ERROR, conn=old)
    with pytest.raises(WsMixerError):
        await inflight
    await settle()
    assert len(disconnects) == 1
    assert state(handle) == ExposeState.REGISTERING
    env.edge.send_app(
        {
            "mcpwarp": {
                "v": 1,
                "op": "registered",
                "services": [{"name": NAME, "id": "srv_anki", "url": env.edge.public_url(NAME)}],
            }
        }
    )
    await settle()
    assert state(handle) == ExposeState.REGISTERED
    env.edge.close_connection(4000 + ErrorCode.INTERNAL_ERROR)
    await settle()
    assert state(handle) == ExposeState.CONNECTING  # the live connection's end does count
    await env.clock.advance(1000)
    assert state(handle) == ExposeState.REGISTERED
    await finish(env, baseline)


async def test_drained_end_before_the_replacement_welcome_is_a_lost_connection(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    gate = Gate()
    handle = await env.expose(handler=gate)
    old = env.conn()
    inflight = asyncio.create_task(env.post())
    await settle()
    env.edge.drain("rollout")
    await settle()
    assert env.retries == [RetryInfo(0, 1000.0, "drain")]
    env.edge.close_connection(4000 + ErrorCode.INTERNAL_ERROR, conn=old)  # replacement pending
    with pytest.raises(WsMixerError):
        await inflight
    await settle()
    assert state(handle) == ExposeState.CONNECTING
    await env.clock.advance(1000)
    assert state(handle) == ExposeState.REGISTERED
    assert env.ops() == ["register", "register"] and len(env.edge.upgrades) == 2
    await finish(env, baseline)


# --- take-over (section 3.3) ---------------------------------------------------------------------


async def test_conflict_after_registration_closes_at_once_and_keeps_the_handle(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    gate = Gate()
    handle = await env.expose(handler=gate)
    events = record(handle, "conflict", "disconnect", "close")
    conn = env.conn()
    inflight = asyncio.create_task(env.post())
    await settle()
    env.edge.conflict(NAME)
    late = conn.open_stream()  # queued behind the conflict, like a request racing it
    await settle()
    with pytest.raises(StreamError):
        await inflight
    end = await asyncio.wait_for(late.wait_closed(), 1)
    assert isinstance(end, Reset) and end.code == ErrorCode.CANCEL
    assert end.message == "service taken over by another connection"
    assert state(handle) == ExposeState.CONFLICT
    assert events[0] == ("conflict", Conflict("srv_anki", NAME, "taken_over"))
    assert conn.closed and controls(conn)[-1] == "error"
    assert conn.controls[-1]["code"] == ErrorCode.NO_ERROR and "drain" not in controls(conn)
    assert handle.url == env.edge.public_url(NAME) and handle.server_id == "srv_anki"
    await env.clock.advance(120_000)
    assert len(env.edge.upgrades) == 1
    await handle.close()
    assert env.ops() == ["register"]  # no unregister after a take-over
    assert [e for e, _ in events] == ["conflict", "disconnect", "close"]
    gate.release.set()
    env.handles.clear()
    await finish(env, baseline)


async def test_drain_replaced_after_registration_is_a_take_over(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env(random=0)  # zero drain jitter: the replacement dial would start at once
    gate = Gate()
    handle = await env.expose(handler=gate)
    events = record(handle, "conflict")
    conn = env.conn()
    inflight = asyncio.create_task(env.post())  # cut by the immediate close
    await settle()
    env.edge.drain("replaced")
    await settle()
    with pytest.raises(WsMixerError):
        await inflight
    assert events == [("conflict", Conflict("srv_anki", NAME, "replaced"))]
    assert state(handle) == ExposeState.CONFLICT and conn.closed
    assert controls(conn)[-1] == "error" and conn.controls[-1]["code"] == ErrorCode.NO_ERROR
    assert "drain" not in controls(conn) and env.ops() == ["register"]
    await env.clock.advance(120_000)
    assert len(env.edge.upgrades) == 1 and env.retries == []
    await finish(env, baseline)


async def test_drain_replaced_with_no_server_id_has_none(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    env.edge.fail_register(NAME, {"code": "SERVER_DISABLED", "message": "off"})
    handle = await env.expose()
    assert state(handle) == ExposeState.DISABLED and handle.server_id is None
    events = record(handle, "conflict")
    env.edge.drain("replaced")
    await settle()
    assert events == [("conflict", Conflict(None, NAME, "replaced"))]
    await finish(env, baseline)


@pytest.mark.parametrize("how", ["conflict", "replaced"])
async def test_a_take_over_during_close_ends_the_unregister_wait_and_closes_at_once(
    baseline: set[asyncio.Task[Any]], how: str
) -> None:
    env = Env(random=0)
    gate = Gate()
    handle = await env.expose(handler=gate)
    events = record(handle, "conflict", "close")
    conn = env.conn()
    inflight = asyncio.create_task(env.post())  # keeps the edge from closing on drain
    await settle()
    env.edge.withhold_unregistered(NAME)
    closing = asyncio.create_task(handle.close())
    await settle()
    assert state(handle) == ExposeState.CLOSING
    if how == "conflict":
        env.edge.conflict(NAME)
    else:
        env.edge.drain("replaced")
    await settle()
    await asyncio.wait_for(closing, 1)  # no clock advance: neither budget nor grace is waited
    with pytest.raises(WsMixerError):
        await inflight
    assert state(handle) == ExposeState.CLOSED
    assert [e for e, _ in events] == ["close"]
    assert controls(conn)[-1] == "error" and conn.controls[-1]["code"] == ErrorCode.NO_ERROR
    assert "drain" not in controls(conn)
    assert len(env.edge.upgrades) == 1 and env.retries == []
    env.handles.clear()
    await finish(env, baseline)


# --- close() (section 5.7) -----------------------------------------------------------------------


async def test_close_unregisters_then_closes_gracefully_once(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose()
    events = record(handle, "close", "disconnect")
    conn = env.conn()
    await asyncio.wait_for(handle.close(), 1)
    assert state(handle) == ExposeState.CLOSED
    assert env.ops() == ["register", "unregister"]
    assert [m["t"] for m in conn.controls[-3:]] == ["app", "drain", "error"]
    assert conn.controls[-2]["reason"] == "client_requested"
    assert [e for e, _ in events] == ["disconnect", "close"]
    assert events[-1] == ("close", Closed(None))
    await handle.close()
    async with handle:
        pass
    assert len([e for e, _ in events if e == "close"]) == 1
    env.handles.clear()
    await finish(env, baseline)


async def test_close_waits_at_most_2000ms_for_unregistered(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose()
    env.edge.withhold_unregistered(NAME)
    closing = asyncio.create_task(handle.close())
    await settle()
    await env.clock.advance(1999)
    assert not closing.done() and state(handle) == ExposeState.CLOSING
    response = await env.post(conn=env.conn())
    assert response.status == 503 and response.body == b'{"error":"service closing"}'
    await env.clock.advance(1)
    await asyncio.wait_for(closing, 1)
    assert state(handle) == ExposeState.CLOSED
    env.handles.clear()
    await finish(env, baseline)


async def test_close_is_cancellation_safe(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    handle = await env.expose()
    env.edge.withhold_unregistered(NAME)
    first = asyncio.create_task(handle.close())
    await settle()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    second = asyncio.create_task(handle.close())
    await env.clock.advance(2000)
    await asyncio.wait_for(second, 1)
    assert state(handle) == ExposeState.CLOSED
    env.handles.clear()
    await finish(env, baseline)


async def test_close_while_disconnected_sends_no_unregister(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose()
    await env.edge.stop()
    await settle()
    assert state(handle) == ExposeState.CONNECTING
    await asyncio.wait_for(handle.close(), 1)
    assert env.ops() == ["register"]
    env.handles.clear()
    await finish(env, baseline)


async def test_close_after_server_disabled_does_not_wait_the_budget(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    env.edge.disable(NAME)
    handle = await env.expose()
    await asyncio.wait_for(handle.close(), 1)
    assert state(handle) == ExposeState.CLOSED
    env.handles.clear()
    await finish(env, baseline)


# --- availability in the bridge (section 4.4 row 3) ---------------------------------------------


async def test_503_service_unavailable_before_the_first_registration(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    env.edge.withhold_registered(NAME)
    await env.edge.start()
    task = env.start()
    await env.edge.wait_for_app("register")
    response = await env.post(conn=env.conn())
    assert (response.status, response.body) == (503, b'{"error":"service unavailable"}')
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await finish(env, baseline)


async def test_503_service_disabled(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    seen: list[Request] = []

    async def handler(request: Request) -> Response:
        seen.append(request)
        return Response(200)

    handle = await env.expose(handler=handler)
    events = record(handle, "disabled", "enabled", "registered")
    env.edge.send_app({"mcpwarp": {"v": 1, "op": "disable", "id": "srv_anki"}})
    await settle()
    assert state(handle) == ExposeState.DISABLED
    response = await env.post(conn=env.conn())
    assert (response.status, response.body) == (503, b'{"error":"service disabled"}')
    assert seen == []
    env.edge.enable(NAME)
    await settle()
    assert state(handle) == ExposeState.REGISTERED
    assert (await env.post()).status == 200
    assert [p for _, p in events[:2]] == [Disabled("srv_anki", "disabled"), Enabled("srv_anki")]
    await finish(env, baseline)


async def test_requests_during_a_re_registration_are_served(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose()
    env.edge.withhold_registered(NAME)
    env.edge.close_connection(4000 + ErrorCode.INTERNAL_ERROR)
    await settle()
    await env.clock.advance(1000)
    assert state(handle) == ExposeState.REGISTERING
    response = await env.post(conn=env.conn())
    assert response.status == 200
    await finish(env, baseline)


# --- errors, listeners, fatal paths ------------------------------------------------------------


async def test_listener_exceptions_are_isolated(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    handle = await env.expose()
    got: list[str] = []

    def boom(_: object) -> None:
        raise RuntimeError("first listener")

    handle.on("disabled", boom)
    handle.on("disabled", lambda _p: got.append("second"))
    handle.on("error", lambda e: got.append(f"error:{e}"))

    def bad_error_listener(_: object) -> None:
        raise RuntimeError("dropped")

    handle.on("error", bad_error_listener)
    unsubscribe = handle.on("disabled", lambda _p: got.append("unsubscribed"))
    unsubscribe()
    handle.once("disabled", lambda _p: got.append("once"))
    env.edge.send_app({"mcpwarp": {"v": 1, "op": "disable", "id": "srv_anki"}})
    await settle()
    assert got == ["error:first listener", "second", "once"]
    assert state(handle) == ExposeState.DISABLED
    await finish(env, baseline)


async def test_malformed_envelopes_and_error_ops_are_non_fatal(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose()
    errors = record(handle, "error")
    env.edge.send_app(
        {"mcpwarp": {"v": 1, "op": "registered", "services": [{"name": NAME, "id": "x"}]}}
    )
    env.edge.app_error("CONNECTION_LIMIT", "too many")
    await settle()
    codes = [p.code for _, p in errors if isinstance(p, McpwarpError)]
    assert codes == ["MALFORMED_ENVELOPE", "CONNECTION_LIMIT"]
    assert all(not p.fatal for _, p in errors)
    assert state(handle) == ExposeState.REGISTERED
    await finish(env, baseline)


async def test_a_refusal_after_registration_is_an_error_event(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose()
    errors = record(handle, "error")
    env.edge.fail_register(NAME, {"code": "QUOTA_EXCEEDED", "message": "full"})
    env.edge.close_connection(4000 + ErrorCode.INTERNAL_ERROR)
    await settle()
    await env.clock.advance(1000)
    assert [(p.code, p.fatal) for _, p in errors] == [("QUOTA_EXCEEDED", False)]
    assert state(handle) == ExposeState.REGISTERING
    assert handle.client.state == "connected"
    await finish(env, baseline)


async def test_a_fatal_close_after_registration_closes_the_handle_last(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose()
    events = record(handle, "disconnect", "close")
    env.edge.close_connection(4000 + ErrorCode.UNAUTHORIZED, ErrorCode.UNAUTHORIZED, "revoked")
    await settle()
    assert [e for e, _ in events] == ["disconnect", "close"]
    reason = events[0][1]
    assert isinstance(reason, DisconnectReason) and reason.fatal
    closed = events[1][1]
    assert isinstance(closed, Closed) and isinstance(closed.error, McpwarpError)
    assert closed.error.code == "UNAUTHORIZED" and closed.error.fatal
    assert isinstance(closed.error.__cause__, WsMixerError)
    assert state(handle) == ExposeState.CLOSED
    await finish(env, baseline)


async def test_exhausted_attempts_after_registration_close_the_handle(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose(reconnect=ReconnectOptions(max_attempts=1))
    events = record(handle, "close")
    await env.edge.stop()
    await settle()
    await env.clock.advance(60_000)
    assert state(handle) == ExposeState.CLOSED
    closed = events[0][1]
    assert isinstance(closed.error, McpwarpError) and closed.error.code == "DISCONNECTED_FATAL"
    await finish(env, baseline)


async def test_unsupported_version_after_registration_closes_the_handle(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose()
    events = record(handle, "close")
    env.edge.app_error("UNSUPPORTED_VERSION")
    await settle()
    assert state(handle) == ExposeState.CLOSED
    assert isinstance(events[0][1].error, McpwarpError)
    assert events[0][1].error.code == "UNSUPPORTED_VERSION"
    await finish(env, baseline)


async def test_the_client_closed_under_the_handle_closes_it_without_error(
    baseline: set[asyncio.Task[Any]],
) -> None:
    """T36's source: the connection closed with no error, not by the exposure."""
    env = Env()
    handle = await env.expose()
    events = record(handle, "close")
    await asyncio.wait_for(handle.client.close(), 1)
    await settle()
    assert state(handle) == ExposeState.CLOSED
    assert events == [("close", Closed(None))]
    await finish(env, baseline)


async def test_a_raising_handler_is_a_500_and_an_error_event(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()

    async def handler(request: Request) -> Response:
        raise ValueError("handler bug")

    handle = await env.expose(handler=handler)
    errors = record(handle, "error")
    response = await env.post()
    assert (response.status, response.body) == (500, b'{"error":"handler error"}')
    assert [str(p) for _, p in errors] == ["handler bug"]
    assert state(handle) == ExposeState.REGISTERED
    await finish(env, baseline)


# --- end to end ------------------------------------------------------------------------------


async def test_end_to_end_post_mcp_echo(baseline: set[asyncio.Task[Any]]) -> None:
    env = Env()
    await env.expose()
    response = await env.post('{"jsonrpc":"2.0"}')
    assert response.status == 200 and response.reason == "OK"
    assert response.body == b'{"jsonrpc":"2.0"}'
    assert response.header("content-type") == "text/plain;charset=UTF-8"
    assert response.end == Finished()
    missing = await env.post(path="/healthz")
    assert (missing.status, missing.body) == (404, b'{"error":"not_found"}')
    await finish(env, baseline)


async def test_regression_the_body_cap_counts_decoded_bytes(
    baseline: set[asyncio.Task[Any]],
) -> None:
    """JS bug: the request body cap counted the chunked framing bytes."""
    env = Env()
    await env.expose(max_request_body_bytes=100)
    url = env.edge.public_url(NAME)
    exact = await env.edge.request("POST", url, body=b"x" * 100, chunked=True)
    assert exact.status == 200 and len(exact.body) == 100
    over = await env.edge.request("POST", url, body=b"x" * 101, chunked=True)
    assert over.status == 413
    await finish(env, baseline)


async def test_target_mode_forwards_to_the_local_server(
    baseline: set[asyncio.Task[Any]],
) -> None:
    seen: list[bytes] = []

    async def local(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = await reader.readuntil(b"\r\n\r\n")
        body = await reader.readexactly(2)
        seen.append(head + body)
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\npong")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(local, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    env = Env()
    try:
        handle = await env.expose(
            handler=None, target=f"http://127.0.0.1:{port}/base", target_timeout_ms=5000
        )
        assert state(handle) == ExposeState.REGISTERED
        url = env.edge.public_url(NAME) + "?q=1"
        response = await asyncio.wait_for(env.edge.request("POST", url, body="hi"), 5)
        assert (response.status, response.body) == (200, b"pong")
        request = seen[0].decode("latin-1")
        assert request.startswith("POST /base?q=1 HTTP/1.1\r\n")
        assert f"x-forwarded-host: {NAME}-tester.mcpwarp.test" in request.lower()
        assert request.endswith("\r\n\r\nhi")
    finally:
        server.close()
        await server.wait_closed()
    await finish(env, baseline)


# --- handler_timeout_ms (SDK extension, docs/DEVIATIONS.md) ---------------------------------


async def test_handler_timeout_ms_defaults_to_none_and_accepts_valid_values() -> None:
    assert ExposeOptions(token="t", name=NAME, integration=UUID).handler_timeout_ms is None
    for ok in (1, 0.5, 25_000, 2_147_483_647):
        opts = ExposeOptions(token="t", name=NAME, integration=UUID, handler=echo)
        assert validate_options(dataclasses.replace(opts, handler_timeout_ms=ok)) is echo


async def test_handler_timeout_ms_answers_504_before_the_edge_gives_up(
    baseline: set[asyncio.Task[Any]],
) -> None:
    cancelled: list[bool] = []

    async def stuck(request: Request) -> Response:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        raise AssertionError("unreachable")

    env = Env()
    handle = await env.expose(handler=stuck, handler_timeout_ms=25_000)
    errors = record(handle, "error")
    inflight = asyncio.create_task(env.edge.request("POST", env.edge.public_url(NAME), body="x"))
    await settle()
    await env.clock.advance(24_999)
    assert not inflight.done()
    await env.clock.advance(1)
    response = await asyncio.wait_for(inflight, 1)
    assert (response.status, response.body) == (504, b'{"error":"local server timed out"}')
    assert response.end == Finished()
    assert cancelled == [True]
    assert errors == []
    await finish(env, baseline)


async def test_without_handler_timeout_ms_a_stuck_handler_meets_the_edge_timeout(
    baseline: set[asyncio.Task[Any]],
) -> None:
    async def stuck(request: Request) -> Response:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    env = Env()
    await env.expose(handler=stuck)
    inflight = asyncio.create_task(env.edge.request("POST", env.edge.public_url(NAME), body="x"))
    await settle()
    await env.clock.advance(30_000)
    with pytest.raises(TimeoutError, match="no response head"):
        await asyncio.wait_for(inflight, 1)
    await finish(env, baseline)


# --- review follow-ups -------------------------------------------------------------------------


async def test_the_client_closed_under_the_handle_in_backoff_closes_it(
    baseline: set[asyncio.Task[Any]],
) -> None:
    """T36's source with no live connection: nothing would report a disconnect."""
    env = Env()
    handle = await env.expose()
    events = record(handle, "close")
    await env.edge.stop()
    await settle()
    assert handle.client.state == "backoff" and state(handle) == ExposeState.CONNECTING
    await asyncio.wait_for(handle.client.close(), 1)
    await settle()
    assert state(handle) == ExposeState.CLOSED
    assert events == [("close", Closed(None))]
    await env.clock.advance(120_000)
    assert len(env.edge.upgrades) == 1
    await finish(env, baseline)


async def test_a_disable_right_before_the_close_does_not_disable_the_handle(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose()
    events = record(handle, "disabled")
    env.edge.send_app({"mcpwarp": {"v": 1, "op": "disable", "id": "srv_anki"}})
    env.edge.close_connection(4000 + ErrorCode.INTERNAL_ERROR)
    await settle()
    assert events == []
    assert state(handle) == ExposeState.CONNECTING
    await env.clock.advance(1000)
    assert state(handle) == ExposeState.REGISTERED
    await finish(env, baseline)


async def test_an_error_listener_failure_is_dropped_also_for_the_observer(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    await env.edge.start()
    observed: list[object] = []

    def observe(event: str, payload: object, detail: str | None) -> None:
        if event == "error":
            observed.append(payload)

    opts = ExposeOptions(**env.opts())
    exposure = Exposure(opts, validate_options(opts), ExposeHooks(observe=observe))
    handle = await asyncio.wait_for(exposure.run(), 1)
    env.handles.append(handle)

    def bad_error_listener(_: object) -> None:
        raise RuntimeError("from an error listener")

    def bad_disabled_listener(_: object) -> None:
        raise RuntimeError("from a disabled listener")

    handle.on("error", bad_error_listener)
    handle.on("disabled", bad_disabled_listener)
    env.edge.app_error("CONNECTION_LIMIT")
    env.edge.send_app({"mcpwarp": {"v": 1, "op": "disable", "id": "srv_anki"}})
    await settle()
    texts = [str(p) for p in observed]
    assert texts == ["", "from a disabled listener"]
    first = observed[0]
    if not isinstance(first, McpwarpError):
        pytest.fail(f"expected a McpwarpError, got {first!r}")
    assert first.code == "CONNECTION_LIMIT"
    await finish(env, baseline)


async def test_an_unexpected_start_failure_rejects_expose(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    await env.edge.start()
    opts = ExposeOptions(**env.opts())
    exposure = Exposure(opts, validate_options(opts))

    async def broken_start() -> None:
        raise RuntimeError("bug in start")

    exposure.client.start = broken_start  # type: ignore[method-assign]
    err = await rejection(asyncio.create_task(exposure.run()))
    assert (err.code, str(err), err.fatal) == ("DISCONNECTED_FATAL", "bug in start", True)
    cause = err.__cause__
    if not isinstance(cause, RuntimeError):
        pytest.fail(f"expected the RuntimeError as __cause__, got {cause!r}")
    assert state(exposure.handle) == ExposeState.CLOSED
    await finish(env, baseline)


async def test_drain_then_an_immediate_4012_on_an_idle_connection_is_still_a_drain(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose()
    env.edge.drain("rollout")  # no open streams: the edge closes 4012 right behind it
    await settle()
    assert env.retries == [RetryInfo(0, 1000.0, "drain")]
    await env.clock.advance(1000)
    assert state(handle) == ExposeState.REGISTERED
    assert len(env.edge.upgrades) == 2 and env.ops() == ["register", "register"]
    await finish(env, baseline)


async def test_a_fatal_close_code_wins_over_exhausted_attempts(
    baseline: set[asyncio.Task[Any]],
) -> None:
    env = Env()
    handle = await env.expose(reconnect=ReconnectOptions(max_attempts=1))
    events = record(handle, "close")
    env.edge.reject_next_upgrade(401)  # the last allowed redial is refused for the token
    env.edge.close_connection(4000 + ErrorCode.INTERNAL_ERROR)
    await settle()
    await env.clock.advance(60_000)
    assert state(handle) == ExposeState.CLOSED and len(env.edge.upgrades) == 2
    closed = events[0][1]
    error = closed.error
    if not isinstance(error, McpwarpError):
        pytest.fail(f"expected a McpwarpError, got {error!r}")
    assert error.code == "UNAUTHORIZED" and "exhausted" not in error.message
    await finish(env, baseline)


async def test_options_are_frozen() -> None:
    opts = ExposeOptions(token="t", name="anki", integration=UUID, handler=echo)
    with pytest.raises(dataclasses.FrozenInstanceError):
        opts.name = "x"  # type: ignore[misc]
