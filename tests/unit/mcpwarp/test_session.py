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
    r.registered()
    r.events.clear()
    # Nothing outstanding: a reply for other names answers nothing of ours.
    r.registered(name="other")
    r.app(op="registered", errors=[{"name": "other", "code": "INVALID_NAME"}])
    r.session.on_app({"not": "ours"})
    r.session.on_app({"mcpwarp": [1]})
    r.session.on_app({"mcpwarp": {"v": 2, "op": "registered"}})
    r.session.on_app({"mcpwarp": {"v": True, "op": "conflict", "id": "srv_anki", "name": "anki"}})
    r.app(op="frobnicate")
    assert r.state() == ExposeState.REGISTERED and r.ops() == ["register"]
    assert r.events == [] and r.fatals == []
    await r.done()


async def test_a_registered_not_listing_the_name_is_re_sent_with_the_transient_backoff() -> None:
    """Spec text at v0.7.1 (MCPWARP.md §2.3 Empty reply): it answers the outstanding register,
    changes no state, and the register is re-sent after the §3.2 backoff."""
    r = Rig()
    r.session.on_welcome()
    r.registered(name="other")
    reason = "register failed: INTERNAL: unlisted in reply"
    assert r.of("retry") == [RetryInfo(1, 1000, reason)] and r.details("retry") == ["register"]
    assert r.state() == ExposeState.REGISTERING and r.of("error") == []
    await r.clock.advance(999)
    assert r.ops() == ["register"]
    await r.clock.advance(1)
    assert r.ops() == ["register", "register"]
    r.app(op="registered")  # no services, no errors
    assert r.of("retry")[-1] == RetryInfo(2, 2000, reason)
    await r.clock.advance(2000)
    r.registered()
    assert r.state() == ExposeState.REGISTERED and r.ops() == ["register"] * 3
    await r.done()


async def test_a_registered_not_listing_the_name_frees_the_slot() -> None:
    """It answers the register: an `enable` during the backoff registers at once."""
    r = Rig()
    r.session.on_welcome()
    r.app(op="registered")
    r.app(op="enable", id="srv_anki", name="anki")
    assert r.ops() == ["register", "register"]
    r.registered()
    await r.clock.advance(60_000)
    assert r.ops() == ["register", "register"] and r.state() == ExposeState.REGISTERED
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
    """JS bug: the OVERLOADED re-send attempt was always 1. Consecutive re-sends back off
    1 s, 2 s, 4 s (MCPWARP.md §2.6 OVERLOADED re-send)."""
    r = Rig()
    r.session.on_welcome()
    for n in range(3):
        r.overloaded()
        await r.clock.advance(1000 * 2**n - 1)
        assert r.ops() == ["register"] * (n + 1)
        await r.clock.advance(1)
    r.registered()
    r.app(op="disable", id="srv_anki")
    r.app(op="enable", id="srv_anki", name="anki")
    r.overloaded()
    await r.clock.advance(1000)
    retries = [(i.attempt, i.delay_ms) for i in r.of("retry") if isinstance(i, RetryInfo)]
    assert retries == [(1, 1000), (2, 2000), (3, 4000), (1, 1000)]
    assert r.ops() == ["register"] * 6
    await r.done()


async def test_overloaded_backoff_caps_at_30s_and_any_non_transient_reply_resets_it() -> None:
    r = Rig()
    r.session.on_welcome()
    for _ in range(7):
        r.overloaded()
        await r.clock.advance(30_000)
    delays = [i.delay_ms for i in r.of("retry") if isinstance(i, RetryInfo)]
    assert delays == [1000, 2000, 4000, 8000, 16000, 30_000, 30_000]
    r.reg_error("SERVER_DISABLED", id="srv_anki")
    r.app(op="enable", id="srv_anki", name="anki")
    r.overloaded()
    await r.clock.advance(1000)
    last = r.of("retry")[-1]
    assert isinstance(last, RetryInfo) and (last.attempt, last.delay_ms) == (1, 1000)
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


@pytest.mark.parametrize(
    "code", ["TOO_MANY_SERVICES", "UNKNOWN_OP", "BAD_REQUEST", "UNSUPPORTED_VERSION", "NEW_CODE"]
)
async def test_a_whole_frame_error_answers_the_outstanding_register(code: str) -> None:
    """MCPWARP.md §2.6 One in flight: any code but OVERLOADED and CONNECTION_LIMIT, unknown
    ones included, frees the in-flight slot and ends the OVERLOADED streak."""
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    await r.clock.advance(1000)
    assert r.ops() == ["register", "register"]
    r.app(op="error", code=code, message="m")
    assert r.session._outstanding is None
    # The slot is free: an `enable` registers at once instead of waiting for a reply.
    r.app(op="enable", id="srv_anki", name="anki")
    assert r.ops() == ["register"] * 3
    r.overloaded()
    retries = [(i.attempt, i.delay_ms) for i in r.of("retry") if isinstance(i, RetryInfo)]
    assert retries == [(1, 1000), (1, 1000)]
    await r.clock.advance(1000)
    assert r.ops() == ["register"] * 4
    await r.done()


@pytest.mark.parametrize("code", ["BAD_REQUEST", "SOMETHING_NEW"])
async def test_an_error_answering_the_register_sends_the_queued_enable_at_once(code: str) -> None:
    r = Rig()
    r.session.on_welcome()
    r.app(op="enable", id="srv_anki", name="anki")
    assert r.ops() == ["register"]
    r.app(op="error", code=code, message="m")
    assert r.ops() == ["register", "register"] and not r.session._register_deferred
    r.registered()
    assert r.state() == ExposeState.REGISTERED
    await r.done()


async def test_connection_limit_answers_no_frame() -> None:
    r = Rig()
    r.session.on_welcome()
    r.app(op="enable", id="srv_anki", name="anki")
    r.app(op="error", code="CONNECTION_LIMIT", message="m")
    assert r.ops() == ["register"] and r.session._register_deferred
    r.registered()
    assert r.ops() == ["register", "register"]
    await r.done()


async def test_any_other_reply_ends_the_overloaded_streak() -> None:
    """Spec text at v0.7.1 (MCPWARP.md §2.6): the OVERLOADED `n` counts consecutive OVERLOADEDs
    only, so a transient refusal in between starts it over; the transient retry keeps its own
    §3.2 counter."""
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    await r.clock.advance(1000)
    r.reg_error("UNAVAILABLE")
    await r.clock.advance(1000)
    r.overloaded()
    await r.clock.advance(1000)
    r.reg_error("UNAVAILABLE")
    retries = [(i.attempt, i.delay_ms) for i in r.of("retry") if isinstance(i, RetryInfo)]
    assert retries == [(1, 1000), (1, 1000), (1, 1000), (2, 2000)]
    assert r.details("retry") == ["overloaded", "register", "overloaded", "register"]
    await r.clock.advance(2000)
    await r.done()


async def test_an_enable_during_a_pending_register_waits_for_its_reply() -> None:
    """Spec text at v0.7.1 (MCPWARP.md §2.6 One in flight): the `enable` is queued, nothing is
    sent until the outstanding register is answered, then its register goes out."""
    r = Rig()
    r.session.on_welcome()
    r.app(op="enable", id="srv_anki", name="anki")
    assert r.of("enabled") == [Enabled("srv_anki")]
    assert r.ops() == ["register"]
    r.registered()
    assert r.ops() == ["register", "register"]
    assert r.session._outstanding == "register"
    r.registered()
    assert r.ops() == ["register", "register"] and r.state() == ExposeState.REGISTERED
    await r.done()

    # Answered by a transient refusal: the scheduled re-send carries the deferred register.
    r = Rig()
    r.session.on_welcome()
    r.app(op="enable", id="srv_anki", name="anki")
    r.reg_error("UNAVAILABLE")
    assert r.ops() == ["register"]
    await r.clock.advance(1000)
    assert r.ops() == ["register", "register"]
    r.registered()
    assert r.ops() == ["register", "register"]
    await r.done()


async def test_a_register_sent_meanwhile_cancels_the_scheduled_transient_re_send() -> None:
    """An `enable` registers during the transient backoff: that register carries the re-send,
    so the old timer must not send another, whatever the answer (here SERVER_DISABLED)."""
    r = Rig()
    r.session.on_welcome()
    r.reg_error("UNAVAILABLE")
    r.app(op="enable", id="srv_anki", name="anki")
    assert r.ops() == ["register", "register"]
    r.reg_error("SERVER_DISABLED", id="srv_anki")
    await r.clock.advance(60_000)
    assert r.ops() == ["register", "register"] and r.state() == ExposeState.DISABLED
    await r.done()


async def test_a_successful_registered_cancels_the_scheduled_transient_re_send() -> None:
    r = Rig()
    r.session.on_welcome()
    r.reg_error("UNAVAILABLE")
    r.registered()  # a late success during the backoff
    await r.clock.advance(60_000)
    assert r.ops() == ["register"] and r.state() == ExposeState.REGISTERED
    await r.done()


async def test_a_reply_within_the_overloaded_pause_drops_the_re_send() -> None:
    """The edge answered after all; a later register (an `enable`) must not be followed by the
    stale OVERLOADED re-send."""
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    r.registered()
    r.app(op="disable", id="srv_anki")
    r.app(op="enable", id="srv_anki", name="anki")
    await r.clock.advance(60_000)
    assert r.ops() == ["register", "register"]
    await r.done()


async def test_a_whole_frame_error_during_the_overloaded_pause_keeps_the_re_send() -> None:
    """The dropped frame's slot stays reserved: the re-send still goes out instead of leaving
    the session registering with nothing in flight."""
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    r.app(op="error", code="BAD_REQUEST", message="m")
    await r.clock.advance(1000)
    assert r.ops() == ["register", "register"]
    r.registered()
    assert r.ops() == ["register", "register"] and r.state() == ExposeState.REGISTERED
    await r.done()


@pytest.mark.parametrize("reply", ["error", "empty"])
async def test_an_error_or_empty_reply_during_the_overloaded_pause_answers_nothing(
    reply: str,
) -> None:
    """MCPWARP.md §2.6 OVERLOADED re-send (v0.7.2): the re-send and the OVERLOADED counter
    stay, and no transient re-send is scheduled."""
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    if reply == "error":
        r.app(op="error", code="UNKNOWN_OP", message="m")
    else:
        r.app(op="registered", services=[])
    await r.clock.advance(1000)
    assert r.ops() == ["register", "register"]
    r.overloaded()
    retries = [(i.attempt, i.delay_ms) for i in r.of("retry") if isinstance(i, RetryInfo)]
    assert retries == [(1, 1000), (2, 2000)]
    assert r.details("retry") == ["overloaded", "overloaded"]
    await r.clock.advance(2000)
    assert r.ops() == ["register"] * 3
    r.registered()
    assert r.state() == ExposeState.REGISTERED
    await r.done()


async def test_a_registered_answering_no_frame_leaves_the_overloaded_counter() -> None:
    """§2.6 (v0.8.0): while close()'s dropped `unregister` waits for its re-send, a stray
    `registered` answers nothing, so the next OVERLOADED is n = 2 (2000 ms, the whole budget)."""
    r = Rig()
    r.session.on_welcome()
    r.registered()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    r.overloaded()  # drops the unregister: n = 1
    r.registered()
    await r.clock.advance(1000)
    assert r.ops() == ["register", "unregister", "unregister"]
    r.overloaded()  # n = 2: re-sent after 2000 ms, past the budget
    await r.clock.advance(1000)
    await settle()
    assert closing.done() and r.ops() == ["register", "unregister", "unregister"]
    await r.done()


async def test_an_unregistered_during_a_register_pause_leaves_the_overloaded_counter() -> None:
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    r.app(op="unregistered", services=[])
    await r.clock.advance(1000)
    r.overloaded()
    retries = [(i.attempt, i.delay_ms) for i in r.of("retry") if isinstance(i, RetryInfo)]
    assert retries == [(1, 1000), (2, 2000)]
    await r.clock.advance(2000)
    await r.done()


async def test_a_new_connection_starts_a_new_overloaded_counter() -> None:
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    r.session.on_disconnect()
    r.session.on_welcome()
    r.overloaded()
    retries = [(i.attempt, i.delay_ms) for i in r.of("retry") if isinstance(i, RetryInfo)]
    assert retries == [(1, 1000), (1, 1000)]
    await r.clock.advance(1000)
    await r.done()


async def test_a_registered_naming_the_service_during_the_pause_cancels_the_re_send() -> None:
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    r.registered()
    await r.clock.advance(1000)
    assert r.ops() == ["register"] and r.state() == ExposeState.REGISTERED
    # It answered the register, so the OVERLOADED counter starts over.
    r.app(op="enable", id="srv_anki", name="anki")
    r.overloaded()
    last = r.of("retry")[-1]
    assert isinstance(last, RetryInfo) and (last.attempt, last.delay_ms) == (1, 1000)
    await r.clock.advance(1000)
    await r.done()


async def test_a_stray_registered_changes_no_overloaded_counter() -> None:
    """Nothing outstanding and no pause. Unreachable through inputs (every answer already reset
    the counter), so the state is forced, to pin that the success path leaves it alone."""
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.session._overloaded_resends = 1
    r.registered()
    assert r.session._overloaded_resends == 1
    await r.done()


async def test_a_server_disabled_entry_resets_the_transient_counter() -> None:
    """§3.2 Transient retry: a `registered` with a non-transient entry is a successful reply,
    so the next transient refusal starts over at attempt 1 / 1000 ms."""
    r = Rig()
    r.session.on_welcome()
    r.reg_error("UNAVAILABLE")
    await r.clock.advance(1000)
    r.reg_error("SERVER_DISABLED", id="srv_anki")
    r.app(op="enable", id="srv_anki", name="anki")
    r.reg_error("UNAVAILABLE")
    last = r.of("retry")[-1]
    assert isinstance(last, RetryInfo) and (last.attempt, last.delay_ms) == (1, 1000)
    await r.clock.advance(1000)
    await r.done()


async def test_a_refusal_after_a_registration_resets_the_transient_counter() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.session.on_disconnect()
    r.session.on_welcome()
    r.reg_error("INTERNAL")
    await r.clock.advance(1000)
    assert r.ops() == ["register"] * 3
    r.reg_error("CONFLICT")
    r.session.on_disconnect()
    r.session.on_welcome()
    r.reg_error("INTERNAL")
    last = r.of("retry")[-1]
    assert isinstance(last, RetryInfo) and (last.attempt, last.delay_ms) == (1, 1000)
    await r.clock.advance(1000)
    await r.done()


async def test_a_whole_frame_error_does_not_reset_the_transient_counter() -> None:
    r = Rig()
    r.session.on_welcome()
    r.reg_error("INTERNAL")
    await r.clock.advance(1000)
    r.app(op="error", code="UNKNOWN_OP", message="m")
    r.app(op="enable", id="srv_anki", name="anki")
    r.reg_error("INTERNAL")
    last = r.of("retry")[-1]
    assert isinstance(last, RetryInfo) and (last.attempt, last.delay_ms) == (2, 2000)
    await r.clock.advance(2000)
    await r.done()


async def test_any_unregistered_resets_the_transient_counter() -> None:
    r = Rig()
    r.session.on_welcome()
    r.reg_error("UNAVAILABLE")
    await r.clock.advance(1000)
    r.app(op="unregistered", services=[])  # no unregister outstanding
    r.reg_error("UNAVAILABLE")
    last = r.of("retry")[-1]
    assert isinstance(last, RetryInfo) and (last.attempt, last.delay_ms) == (1, 1000)
    await r.clock.advance(1000)
    await r.done()


async def test_a_whole_frame_error_answering_the_unregister_ends_the_close_wait() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    assert r.ops() == ["register", "unregister"]
    r.app(op="error", code="TOO_MANY_SERVICES", message="m")
    await settle()
    assert closing.done()
    await r.done()


@pytest.mark.parametrize("reply", ["registered", "UNKNOWN_OP", "OVERLOADED"])
async def test_a_reply_to_the_register_ahead_of_the_unregister_keeps_the_close_wait(
    reply: str,
) -> None:
    """MCPWARP.md §3.2 Unregister (v0.7.1): the wait ends on the `unregister`'s own reply by
    frame order; a register dropped by OVERLOADED while closing is abandoned, never re-sent."""
    r = Rig()
    r.session.on_welcome()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    assert r.ops() == ["register", "unregister"]
    if reply == "registered":
        r.registered()
        assert r.of("registered") == []
    else:
        r.app(op="error", code=reply, message="m")
    await r.clock.advance(1500)
    assert not closing.done()
    assert r.ops() == ["register", "unregister"] and r.of("retry") == []
    r.app(op="error", code="TOO_MANY_SERVICES", message="m")
    await settle()
    assert closing.done()
    await r.done()


async def test_close_during_the_overloaded_pause_abandons_the_dropped_register() -> None:
    """Registered earlier on this connection, so close() unregisters; no register is ahead of
    the `unregister`, so the next reply is the `unregister`'s own."""
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.app(op="enable", id="srv_anki", name="anki")
    r.overloaded()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    assert r.ops() == ["register", "register", "unregister"]
    r.app(op="error", code="TOO_MANY_SERVICES", message="m")
    await settle()
    assert closing.done()
    assert r.ops() == ["register", "register", "unregister"]
    await r.done()


async def test_close_skips_the_unregister_when_every_register_was_dropped() -> None:
    """MCPWARP.md §3.2 Unregister (v0.7.2): nothing reached the edge, so nothing to unregister;
    the held name of an earlier connection is gone with it."""
    r = Rig()
    r.session.on_welcome()
    r.registered()
    r.session.on_disconnect()
    r.session.on_welcome()
    r.overloaded()
    await r.clock.advance(1000)
    r.overloaded()
    await asyncio.wait_for(r.session.begin_close(), 1)
    assert r.ops() == ["register"] * 3
    await r.done()


async def test_close_unregisters_behind_a_re_sent_register_in_flight() -> None:
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    await r.clock.advance(1000)
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    assert r.ops() == ["register", "register", "unregister"]
    r.app(op="unregistered", services=[])
    await closing
    await r.done()


@pytest.mark.parametrize("reply", ["UNKNOWN_OP", "empty"])
async def test_close_unregisters_after_a_register_answered_by_an_error_or_empty_reply(
    reply: str,
) -> None:
    r = Rig()
    r.session.on_welcome()
    if reply == "empty":
        r.app(op="registered", services=[])
        await r.clock.advance(1000)  # the transient re-send
    else:
        r.app(op="error", code=reply, message="m")
        r.app(op="enable", id="srv_anki", name="anki")
    r.overloaded()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    assert r.ops()[-1] == "unregister"
    r.app(op="unregistered", services=[])
    await closing
    await r.done()


async def test_connection_limit_ahead_of_the_unregister_answers_nothing() -> None:
    r = Rig()
    r.session.on_welcome()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    r.app(op="error", code="CONNECTION_LIMIT", message="m")
    r.registered()  # still settles the register ahead
    await settle()
    assert not closing.done()
    r.app(op="error", code="TOO_MANY_SERVICES", message="m")
    await settle()
    assert closing.done()
    await r.done()


async def test_unsupported_version_ahead_of_the_unregister_is_fatal() -> None:
    r = Rig()
    r.session.on_welcome()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    r.app(op="error", code="UNSUPPORTED_VERSION", message="v2 only")
    assert [e.code for e in r.fatals] == ["UNSUPPORTED_VERSION"]
    assert not r.session._register_ahead
    r.session.mark_closed()  # expose.py's fatal path
    await closing
    await r.done()


async def test_an_error_answering_the_register_ahead_ends_the_overloaded_streak() -> None:
    """§2.6: any non-OVERLOADED reply resets `n`, so the unregister's OVERLOADED is n = 1."""
    r = Rig()
    r.session.on_welcome()
    r.overloaded()
    await r.clock.advance(1000)
    assert r.ops() == ["register", "register"]
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    r.app(op="error", code="BAD_REQUEST", message="m")
    r.overloaded()
    await r.clock.advance(1000)
    assert r.ops() == ["register", "register", "unregister", "unregister"]
    r.app(op="unregistered", services=[])
    await settle()
    assert closing.done()
    await r.done()


async def test_the_abandoned_registers_overloaded_counts_in_the_streak() -> None:
    r = Rig()
    r.session.on_welcome()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    r.overloaded()  # drops the register ahead: n = 1
    r.overloaded()  # drops the unregister: n = 2, re-sent after 2000 ms, the whole budget
    await r.clock.advance(1999)
    assert r.ops() == ["register", "unregister"] and not closing.done()
    await r.clock.advance(1)
    await settle()
    assert closing.done()
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


async def test_disable_matches_the_server_id_and_holds_an_unknown_one() -> None:
    """MCPWARP.md §2.7 Unknown `disable` id (T49): held, never adopted."""
    r = Rig()
    r.session.on_welcome()
    r.app(op="disable", id="srv_early")
    assert r.state() == ExposeState.REGISTERING and r.session.server_id is None
    r.registered()
    r.app(op="disable", id="srv_someone_else")
    assert r.state() == ExposeState.REGISTERED
    r.app(op="disable", id="srv_anki", reason="abuse")
    assert r.state() == ExposeState.DISABLED
    assert r.of("disabled") == [Disabled("srv_anki", "abuse")]
    await r.done()


async def test_a_held_disable_applies_with_the_registered_bringing_its_id() -> None:
    """MCPWARP.md §5.9 Held `disable`: `registered` then `disabled`, back to back, the
    `registered` marked so the tunnel collapses the pair (T50); an `enable` for the id drops
    the held one."""
    r = Rig()
    r.session.on_welcome()
    r.app(op="disable", id="srv_anki", reason="abuse")
    r.registered()
    assert r.state() == ExposeState.DISABLED and r.session.server_id == "srv_anki"
    assert [e for e, _, _ in r.events] == ["registered", "disabled"]
    assert r.details("registered") == ["held_disable"]
    assert r.of("disabled") == [Disabled("srv_anki", "abuse")]
    assert r.session.ever_registered
    await r.done()

    r = Rig()
    r.session.on_welcome()
    r.app(op="disable", id="srv_anki")
    r.app(op="enable", id="srv_anki", name="anki")
    r.registered()
    assert r.state() == ExposeState.REGISTERED and r.of("disabled") == []
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


async def test_enable_matches_by_name_or_server_id() -> None:
    """MCPWARP.md §2.7 enable row: `name` equals the agent's, or `id` its server id."""
    r = Rig()
    r.session.on_welcome()
    r.app(op="enable", id="srv_anki", name="other")  # no server id known yet: not ours
    assert r.of("enabled") == []
    r.registered()
    r.app(op="disable", id="srv_anki")
    r.app(op="enable", id="srv_anki", name="other")  # by id
    assert r.of("enabled") == [Enabled("srv_anki")] and r.state() == ExposeState.REGISTERING
    r.registered()
    r.app(op="enable", id="srv_x", name="anki")  # by name, as before; the id is adopted
    assert r.of("enabled")[-1] == Enabled("srv_x") and r.session.server_id == "srv_x"
    await r.done()


async def test_enable_by_id_drops_a_held_disable_for_that_id() -> None:
    r = Rig()
    r.session.on_welcome()
    r.app(op="disable", id="srv_anki")  # unknown yet: held
    r.app(op="enable", id="srv_anki", name="other")  # not ours (no id known), still drops it
    assert r.of("enabled") == []
    r.registered()
    assert r.state() == ExposeState.REGISTERED and r.of("disabled") == []
    await r.done()


async def test_held_disables_end_with_their_connection() -> None:
    """The next connection's register answer is authoritative: a disconnect (or a take-over)
    drops what was held."""
    r = Rig()
    r.session.on_welcome()
    r.app(op="disable", id="srv_anki")  # unknown yet: held
    r.session.on_disconnect()
    r.session.on_welcome()
    r.registered()
    assert r.state() == ExposeState.REGISTERED and r.of("disabled") == []
    await r.done()

    # A drain hand-over: the replacement's welcome, with no on_disconnect for the old one.
    r = Rig()
    r.session.on_welcome()
    r.app(op="disable", id="srv_anki")
    r.session.on_welcome()
    r.registered()
    assert r.state() == ExposeState.REGISTERED and r.of("disabled") == []
    await r.done()

    r = Rig()
    r.session.on_welcome()
    r.app(op="disable", id="srv_other")
    r.app(op="conflict", id="srv_anki", name="anki")
    assert r.session.taken_over and r.session._held_disables == {}
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


async def test_a_conflict_for_another_service_during_the_unregister_wait_is_ignored() -> None:
    r = Rig()
    r.session.on_welcome()
    r.registered()
    closing = asyncio.create_task(r.session.begin_close())
    await settle()
    r.app(op="conflict", id="srv_other", name="other", reason="taken_over")
    await settle()
    assert not closing.done()
    assert not r.session.taken_over and r.state() == ExposeState.CLOSING
    r.app(op="conflict", id="srv_anki", name="anki", reason="taken_over")
    await settle()
    assert closing.done() and r.session.taken_over
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
