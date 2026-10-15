"""The registration state machine (mcpwarp/session.py) on a FakeClock with a recording send_app.

Rows follow MCPWARP.md sections 2.5-2.7, 3.2, 3.3 and 5.7; `test_regression_*` pin the bugs the
JS port had (peer-answers.md). Every test ends with no timer left on the clock.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from ws_mixer.mcpwarp.errors import McpwarpError
from ws_mixer.mcpwarp.session import (
    Conflict,
    Disabled,
    Enabled,
    ExposeState,
    Registration,
    RetryInfo,
    Session,
)
from ws_mixer.testing import FakeClock, settle

pytestmark = pytest.mark.asyncio

URL = "https://anki-u.x/mcp"


class Rig:
    def __init__(self, *, fail_sends: bool = False, raise_sends: bool = False) -> None:
        self.clock = FakeClock()
        self.sent: list[dict[str, Any]] = []
        self.events: list[tuple[str, object, str | None]] = []
        self.fatals: list[McpwarpError] = []
        self.fail_sends = fail_sends
        self.raise_sends = raise_sends
        self.session = Session(
            "anki",
            "http",
            send_app=self.send_app,
            clock=self.clock,
            emit=lambda e, p, d: self.events.append((e, p, d)),
            fatal=self.fatals.append,
        )

    def send_app(self, body: dict[str, Any]) -> asyncio.Future[None]:
        if self.raise_sends:
            raise RuntimeError("no live connection")
        self.sent.append(body["mcpwarp"])
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if self.fail_sends:
            fut.set_exception(ConnectionError("gone"))
        else:
            fut.set_result(None)
        return fut

    def ops(self) -> list[str]:
        return [m["op"] for m in self.sent]

    def of(self, event: str) -> list[object]:
        return [p for e, p, _ in self.events if e == event]

    def details(self, event: str) -> list[str | None]:
        return [d for e, _, d in self.events if e == event]

    def app(self, **env: Any) -> None:
        self.session.on_app({"mcpwarp": {"v": 1, **env}})

    def registered(self, created: bool = False, name: str = "anki") -> None:
        svc = {"name": name, "id": f"srv_{name}", "url": URL, "created": created}
        self.app(op="registered", services=[svc])

    def reg_error(self, code: str, **extra: Any) -> None:
        self.app(
            op="registered",
            errors=[{"name": "anki", "code": code, "message": code.lower(), **extra}],
        )

    def overloaded(self, message: str = "busy") -> None:
        self.app(op="error", code="OVERLOADED", message=message)

    async def done(self) -> None:
        await settle()
        self.session.mark_closed()
        assert self.clock.pending() == 0, "a session timer outlived it"

    def state(self) -> ExposeState:
        """Read through a call, so mypy does not narrow it between inputs."""
        return self.session.state


# --- registration ------------------------------------------------------------------------------


async def test_registers_on_every_welcome_and_emits_registered_each_time() -> None:
    r = Rig()
    assert r.state() == ExposeState.CONNECTING
    r.session.on_welcome()
    assert r.state() == ExposeState.REGISTERING
    assert r.sent == [{"v": 1, "op": "register", "services": [{"name": "anki", "kind": "http"}]}]
    r.registered(created=True)
    assert r.state() == ExposeState.REGISTERED
    assert r.session.url == URL and r.session.server_id == "srv_anki"
    r.session.on_disconnect()
    assert r.state() == ExposeState.CONNECTING
    r.session.on_welcome()
    r.registered(created=False)
    assert r.ops() == ["register", "register"]
    assert r.of("registered") == [
        Registration(URL, "srv_anki", True),
        Registration(URL, "srv_anki", False),
    ]
    assert r.session.registration == Registration(URL, "srv_anki", False)
    assert r.session.ever_registered
    await r.done()


async def test_kind_goes_into_register() -> None:
    clock = FakeClock()
    sent: list[dict[str, Any]] = []

    def send(body: dict[str, Any]) -> asyncio.Future[None]:
        sent.append(body)
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        return fut

    s = Session("anki", "stdio", send_app=send, clock=clock, emit=lambda *a: None, fatal=print)
    s.on_welcome()
    assert sent[0]["mcpwarp"]["services"] == [{"name": "anki", "kind": "stdio"}]
    s.mark_closed()


async def test_other_names_unknown_ops_and_foreign_bodies_are_ignored() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered(name="other")
    r.app(op="registered", errors=[{"name": "other", "code": "INVALID_NAME"}])
    r.session.on_app({"not": "ours"})
    r.session.on_app({"mcpwarp": [1]})
    r.session.on_app({"mcpwarp": {"v": 2, "op": "registered"}})
    r.session.on_app({"mcpwarp": {"v": True, "op": "conflict", "id": "srv_anki", "name": "anki"}})
    r.app(op="frobnicate")
    assert r.state() == ExposeState.REGISTERING
    assert r.events == [] and r.fatals == []
    await r.done()


async def test_server_disabled_is_disabled_with_the_errors_id() -> None:
    r = Rig()
    r.session.on_welcome()
    r.reg_error("SERVER_DISABLED", id="srv_x")
    assert r.state() == ExposeState.DISABLED
    assert r.session.server_id == "srv_x"
    assert r.of("disabled") == [Disabled("srv_x", "disabled")]
    assert r.fatals == [] and r.of("error") == []
    fresh = Rig()
    fresh.session.on_welcome()
    fresh.reg_error("SERVER_DISABLED")
    assert fresh.of("disabled") == [Disabled(None, "disabled")]
    await r.done()
    await fresh.done()


# --- transient register errors ---------------------------------------------------------------


async def test_transient_errors_back_off_doubling_to_the_30s_cap() -> None:
    r = Rig()
    r.session.on_welcome()
    delays = []
    for n in range(7):
        r.reg_error("UNAVAILABLE")
        info = r.of("retry")[-1]
        assert isinstance(info, RetryInfo)
        assert info.attempt == n + 1
        assert info.reason == "register failed: UNAVAILABLE: unavailable"
        delays.append(info.delay_ms)
        await r.clock.advance(info.delay_ms - 1)
        assert r.ops().count("register") == n + 1
        await r.clock.advance(1)
        assert r.ops().count("register") == n + 2
    assert delays == [1000, 2000, 4000, 8000, 16000, 30000, 30000]
    assert set(r.details("retry")) == {"register"}
    assert set(r.details("error")) == {"register_transient"}
    errors = r.of("error")
    assert all(isinstance(e, McpwarpError) and e.code == "UNAVAILABLE" for e in errors)
    assert all(isinstance(e, McpwarpError) and not e.fatal for e in errors)
    # The error event comes before the retry it causes.
    kinds = [e for e, _, _ in r.events]
    assert kinds[:2] == ["error", "retry"]
    assert r.fatals == []
    await r.done()


async def test_transient_counter_resets_only_on_success_and_carries_across_reconnects() -> None:
    r = Rig()
    r.session.on_welcome()
    r.reg_error("TIMEOUT")
    r.session.on_disconnect()
    r.session.on_welcome()
    r.reg_error("TIMEOUT")
    second = r.of("retry")[-1]
    assert isinstance(second, RetryInfo) and second.attempt == 2 and second.delay_ms == 2000
    await r.clock.advance(2000)
    r.registered()
    r.reg_error("TIMEOUT")
    third = r.of("retry")[-1]
    assert isinstance(third, RetryInfo) and third.attempt == 1 and third.delay_ms == 1000
    await r.done()


async def test_a_disconnect_cancels_a_pending_retry_and_the_next_welcome_registers() -> None:
    r = Rig()
    r.session.on_welcome()
    r.reg_error("INTERNAL")
    r.session.on_disconnect()
    assert r.clock.pending() == 0
    await r.clock.advance(60_000)
    assert r.ops() == ["register"]
    r.session.on_welcome()
    assert r.ops() == ["register", "register"]
    await r.done()


# --- refusals --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "code",
    [
        "INVALID_NAME",
        "USERNAME_REQUIRED",
        "QUOTA_EXCEEDED",
        "CONFLICT",
        "BAD_REQUEST",
        "UNAUTHENTICATED",
        "FORBIDDEN",
        "NOT_IMPLEMENTED",
        "SOMETHING_NEW",
    ],
)
async def test_a_refusal_before_the_first_registration_is_fatal(code: str) -> None:
    r = Rig()
    r.session.on_welcome()
    r.reg_error(code)
    assert len(r.fatals) == 1
    err = r.fatals[0]
    assert (err.code, err.message, err.fatal, err.service_name) == (
        code,
        code.lower(),
        True,
        "anki",
    )
    assert r.of("retry") == [] and r.of("error") == []
    await r.done()


async def test_regression_a_refusal_after_a_success_is_reported_not_swallowed() -> None:
    """JS bug: a refusal after an earlier registration left the tunnel stuck at "connecting".
    The session reports it as a non-fatal error with origin register_refusal, which the tunnel
    turns into a terminal state; nothing retries it."""
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.session.on_disconnect()
    r.session.on_welcome()
    r.reg_error("FORBIDDEN")
    assert r.fatals == []
    assert r.details("error") == ["register_refusal"]
    err = r.of("error")[0]
    assert isinstance(err, McpwarpError) and err.code == "FORBIDDEN" and not err.fatal
    await r.clock.advance(60_000)
    assert r.ops() == ["register", "register"]
    await r.done()


# --- OVERLOADED and error ops ------------------------------------------------------------------


async def test_overloaded_resends_the_outstanding_register_once_after_1s() -> None:
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    r.overloaded()
    assert r.of("retry") == [RetryInfo(1, 1000, "register failed: OVERLOADED: busy")]
    assert r.details("retry") == ["overloaded"]
    assert r.of("error") == []
    await r.clock.advance(999)
    assert r.ops() == ["register"]
    await r.clock.advance(1)
    assert r.ops() == ["register", "register"]
    r.registered()
    r.overloaded()
    await r.clock.advance(5000)
    assert r.ops() == ["register", "register"]  # nothing outstanding
    await r.done()


async def test_overloaded_resend_is_dropped_when_registered_arrives_within_the_pause() -> None:
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    await r.clock.advance(500)
    r.registered()
    await r.clock.advance(500)
    assert r.ops() == ["register"]  # no second register, so no duplicate `registered`
    assert len(r.of("registered")) == 1
    await r.done()


async def test_regression_overloaded_attempt_counts_1_2_3_and_resets_on_success() -> None:
    """JS bug: the OVERLOADED re-send attempt was always 1."""
    r = Rig()
    r.session.on_welcome()
    for _ in range(3):
        r.overloaded()
        await r.clock.advance(1000)
    r.registered()
    r.app(op="disable", id="srv_anki")
    r.app(op="enable", id="srv_anki", name="anki")
    r.overloaded()
    await r.clock.advance(1000)
    attempts = [i.attempt for i in r.of("retry") if isinstance(i, RetryInfo)]
    assert attempts == [1, 2, 3, 1]
    assert r.ops() == ["register"] * 6
    await r.done()


async def test_overloaded_while_closing_resends_the_unregister_without_a_retry_event() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    r.overloaded()
    await r.clock.advance(1000)
    assert r.ops() == ["register", "unregister", "unregister"]
    assert r.of("retry") == []
    r.app(op="unregistered", services=[{"name": "anki", "id": "srv_anki"}])
    await closing
    await r.done()


async def test_unsupported_version_is_fatal_other_error_ops_are_not() -> None:
    r = Rig()
    r.session.on_welcome()
    for code in ("CONNECTION_LIMIT", "TOO_MANY_SERVICES", "UNKNOWN_OP", "BAD_REQUEST", "WHAT"):
        r.app(op="error", code=code, message="m")
    codes = [e.code for e in r.of("error") if isinstance(e, McpwarpError)]
    assert codes == ["CONNECTION_LIMIT", "TOO_MANY_SERVICES", "UNKNOWN_OP", "BAD_REQUEST", "WHAT"]
    assert set(r.details("error")) == {"app"}
    assert r.fatals == []
    r.app(op="error", code="UNSUPPORTED_VERSION", message="v2 only")
    assert [(e.code, e.message, e.fatal) for e in r.fatals] == [
        ("UNSUPPORTED_VERSION", "v2 only", True)
    ]
    await r.done()


async def test_a_malformed_envelope_is_a_non_fatal_error_and_changes_nothing() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.app(op="registered", services=[{"name": "anki", "id": "srv_anki", "url": ""}])
    r.app(op="disable", id=5)
    assert r.state() == ExposeState.REGISTERED
    errors = r.of("error")
    assert [e.code for e in errors if isinstance(e, McpwarpError)] == ["MALFORMED_ENVELOPE"] * 2
    assert r.details("error") == ["malformed", "malformed"]
    first = errors[0]
    assert isinstance(first, McpwarpError)
    assert "registered.services[].url" in first.message and first.service_name == "anki"
    assert r.fatals == []
    await r.done()


# --- disable / enable --------------------------------------------------------------------------


async def test_disable_matches_the_server_id_or_applies_before_any_is_known() -> None:
    r = Rig()
    r.session.on_welcome()
    r.app(op="disable", id="srv_early")
    assert r.state() == ExposeState.DISABLED and r.session.server_id == "srv_early"
    r.registered()
    r.app(op="disable", id="srv_someone_else")
    assert r.state() == ExposeState.REGISTERED
    r.app(op="disable", id="srv_anki", reason="abuse")
    assert r.state() == ExposeState.DISABLED
    assert r.of("disabled") == [Disabled("srv_early", "disabled"), Disabled("srv_anki", "abuse")]
    await r.done()


async def test_enable_re_registers_and_adopts_the_id() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.app(op="disable", id="srv_anki")
    r.app(op="enable", id="srv_new", name="other")
    assert r.of("enabled") == []
    r.app(op="enable", id="srv_new", name="anki")
    assert r.of("enabled") == [Enabled("srv_new")]
    assert r.state() == ExposeState.REGISTERING and r.session.server_id == "srv_new"
    assert r.ops() == ["register", "register"]
    await r.done()


async def test_registration_ops_delivered_after_the_disconnect_are_ignored() -> None:
    """A backlog message of an ended connection (e.g. disable, then the close right behind
    it) must not move the state; conflict and error still apply."""
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.session.on_disconnect()
    r.app(op="disable", id="srv_anki")
    r.app(op="enable", id="srv_anki", name="anki")
    r.registered()
    r.reg_error("FORBIDDEN")
    assert r.state() == ExposeState.CONNECTING
    assert r.of("disabled") == [] and r.of("enabled") == [] and len(r.of("registered")) == 1
    assert r.fatals == [] and r.of("error") == [] and r.ops() == ["register"]
    r.app(op="error", code="CONNECTION_LIMIT")
    assert [e.code for e in r.of("error") if isinstance(e, McpwarpError)] == ["CONNECTION_LIMIT"]
    r.app(op="conflict", id="srv_anki", name="anki")
    assert r.state() == ExposeState.CONFLICT
    r.session.on_welcome()
    assert r.ops() == ["register"]
    await r.done()


# --- take-over ---------------------------------------------------------------------------------


async def test_conflict_is_sticky_and_never_registers_again() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.reg_error("INTERNAL")  # a pending retry the take-over must cancel
    r.app(op="conflict", id="srv_anki", name="anki")
    assert r.state() == ExposeState.CONFLICT and r.session.taken_over
    assert r.of("conflict") == [Conflict("srv_anki", "anki", "taken_over")]
    assert r.clock.pending() == 0
    r.app(op="conflict", id="srv_anki", name="anki")
    r.registered()
    r.app(op="disable", id="srv_anki")
    r.app(op="enable", id="srv_anki", name="anki")
    r.session.on_disconnect()
    r.session.on_welcome()
    assert r.state() == ExposeState.CONFLICT
    assert r.ops() == ["register"]
    assert len(r.of("conflict")) == 1 and r.of("enabled") == [] and r.of("disabled") == []
    # The last values stay readable.
    assert r.session.url == URL and r.session.server_id == "srv_anki"
    await r.done()


async def test_conflict_matches_by_id_when_the_name_differs() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.app(op="conflict", id="srv_other", name="other")
    assert r.state() == ExposeState.REGISTERED
    r.app(op="conflict", id="srv_anki", name="renamed", reason="custom")
    assert r.of("conflict") == [Conflict("srv_anki", "renamed", "custom")]
    await r.done()


async def test_a_replaced_drain_takes_over_like_conflict() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    assert r.session.on_replaced() is True
    assert r.of("conflict") == [Conflict("srv_anki", "anki", "replaced")]
    assert r.state() == ExposeState.CONFLICT
    assert r.session.on_replaced() is False
    r.app(op="conflict", id="srv_anki", name="anki")
    assert len(r.of("conflict")) == 1
    r.session.on_welcome()
    assert r.ops() == ["register"]
    await r.done()


async def test_a_replaced_drain_with_no_known_server_id_has_none() -> None:
    r = Rig()
    r.session.on_welcome()
    r.reg_error("SERVER_DISABLED")
    assert r.session.on_replaced() is True
    assert r.of("conflict") == [Conflict(None, "anki", "replaced")]
    await r.done()


# --- begin_close -------------------------------------------------------------------------------


async def test_begin_close_unregisters_and_ends_on_unregistered() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    assert r.state() == ExposeState.CLOSING
    assert r.sent[-1] == {"v": 1, "op": "unregister", "services": [{"name": "anki"}]}
    assert not closing.done()
    r.app(op="unregistered", services=[])  # outstanding: any unregistered ends the wait
    await settle()
    assert closing.done()
    await r.done()


async def test_begin_close_gives_up_after_the_2000ms_budget() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    closing = asyncio.create_task(r.session.begin_close())
    await r.clock.advance(1999)
    assert not closing.done()
    await r.clock.advance(1)
    assert closing.done()
    await r.done()


@pytest.mark.parametrize("how", ["failed", "raised"])
async def test_begin_close_ends_at_once_when_the_unregister_cannot_be_sent(how: str) -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.fail_sends = how == "failed"
    r.raise_sends = how == "raised"
    await asyncio.wait_for(r.session.begin_close(), 1)
    assert r.clock.pending() == 0
    await r.done()


async def test_begin_close_ends_on_a_disconnect() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    r.session.on_disconnect()
    await settle()
    assert closing.done() and r.state() == ExposeState.CLOSING
    await r.done()


async def test_begin_close_sends_nothing_without_a_register_on_the_live_connection() -> None:
    r = Rig()
    await r.session.begin_close()  # never connected
    assert r.sent == [] and r.state() == ExposeState.CLOSING
    r2 = Rig()
    r2.session.on_welcome()
    r2.registered()
    r2.session.on_disconnect()
    await r2.session.begin_close()
    assert r2.ops() == ["register"]
    # Registrations after closing are ignored.
    r2.session.on_welcome()
    r2.registered()
    assert r2.session.state == ExposeState.CLOSING and r2.ops() == ["register"]
    await r.done()
    await r2.done()


async def test_begin_close_after_a_take_over_sends_no_unregister() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.app(op="conflict", id="srv_anki", name="anki")
    await r.session.begin_close()
    assert r.ops() == ["register"]
    await r.done()


@pytest.mark.parametrize("how", ["conflict", "replaced"])
async def test_a_take_over_during_the_unregister_wait_ends_it_without_an_event(how: str) -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    if how == "conflict":
        r.app(op="conflict", id="srv_anki", name="anki")
    else:
        assert r.session.on_replaced() is False
    await settle()
    assert closing.done()
    assert r.session.taken_over and r.state() == ExposeState.CLOSING
    assert r.of("conflict") == []
    await r.done()


async def test_cancelling_begin_close_clears_its_timer() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert r.clock.pending() == 0
    await r.done()


async def test_mark_closed_ignores_everything_after() -> None:
    r = Rig()
    r.session.on_welcome()
    r.reg_error("INTERNAL")
    r.session.mark_closed()
    assert r.clock.pending() == 0
    r.registered()
    r.app(op="error", code="UNSUPPORTED_VERSION")
    r.session.on_welcome()
    r.session.on_disconnect()
    assert r.state() == ExposeState.CLOSED
    assert r.fatals == [] and r.of("registered") == []
    await r.done()
