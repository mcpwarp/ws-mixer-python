"""Envelope codec cases the spec fixtures do not pin down."""

from __future__ import annotations

import copy
import pickle
from typing import Any

import pytest

from ws_mixer.control import encode_control
from ws_mixer.mcpwarp import (
    INTEGRATION_ID_RE,
    TRANSIENT_REGISTER_ERRORS,
    AppError,
    AppErrorCode,
    ConflictOp,
    Disable,
    Enable,
    McpwarpError,
    Registered,
    RegisteredService,
    RegisterErrorCode,
    ServiceError,
    Unknown,
    Unregistered,
    UnregisteredService,
    decode_envelope,
    encode_register,
    encode_unregister,
    is_valid_service_name,
)
from ws_mixer.mcpwarp import errors as mcpwarp_errors


def _decode(env: Any) -> object:
    return decode_envelope({"mcpwarp": env})


def test_not_ours() -> None:
    assert decode_envelope({}) is None
    assert decode_envelope({"other": 1}) is None
    assert decode_envelope({"mcpwarp": None}) is None
    assert decode_envelope({"mcpwarp": [1]}) is None
    assert decode_envelope({"mcpwarp": "x"}) is None
    assert decode_envelope(None) is None
    assert decode_envelope([{"mcpwarp": {}}]) is None


def test_bool_v_is_unknown_version() -> None:
    env = {"v": True, "op": "disable", "id": "s"}
    assert _decode(env) == Unknown(raw_op="disable", v=True)


def test_integral_float_v_is_accepted() -> None:
    assert _decode({"v": 1.0, "op": "disable", "id": "s"}) == Disable("s", "disabled")


@pytest.mark.parametrize("v", [None, 2, 1.5, "1", 0, float("nan"), float("inf")])
def test_other_v_is_unknown(v: object) -> None:
    got = _decode({"v": v, "op": "enable", "id": "s", "name": "n"})
    assert isinstance(got, Unknown)
    assert got.raw_op == "enable"


def test_missing_v_is_unknown() -> None:
    assert _decode({"op": "enable"}) == Unknown(raw_op="enable", v=None)


def test_unknown_op_keeps_raw_op() -> None:
    assert _decode({"v": 1, "op": "rebalance"}) == Unknown(raw_op="rebalance", v=1)
    assert _decode({"v": 1, "op": 7}) == Unknown(raw_op=7, v=1)
    assert _decode({"v": 1, "op": "register", "services": []}) == Unknown("register", 1)


def test_unknown_fields_ignored() -> None:
    env = {"v": 1, "op": "conflict", "id": "i", "name": "n", "extra": {"x": 1}}
    assert _decode(env) == ConflictOp("i", "n", "taken_over")


def test_registered_defaults() -> None:
    got = _decode(
        {
            "v": 1,
            "op": "registered",
            "services": [{"name": "a", "id": "i", "url": "https://a", "created": None}],
            "errors": [{"name": "b", "code": "SOMETHING_NEW"}],
        }
    )
    assert got == Registered(
        services=[RegisteredService("a", "i", "https://a", False)],
        errors=[ServiceError("b", "SOMETHING_NEW", "", None)],
    )


def test_registered_absent_lists() -> None:
    assert _decode({"v": 1, "op": "registered"}) == Registered([], [])


@pytest.mark.parametrize(
    ("env", "message"),
    [
        (
            {"v": 1, "op": "registered", "services": [{"name": "a", "id": "i"}]},
            "registered.services[].url must be a non-empty string",
        ),
        (
            {"v": 1, "op": "registered", "services": {}},
            "registered.services must be an array of objects",
        ),
        (
            {"v": 1, "op": "registered", "services": [1]},
            "registered.services must be an array of objects",
        ),
        (
            {
                "v": 1,
                "op": "registered",
                "services": [{"name": "a", "id": "i", "url": "u", "created": 1}],
            },
            "registered.services[].created must be a boolean",
        ),
        (
            {"v": 1, "op": "registered", "errors": [{"name": "a", "code": "X", "id": None}]},
            "registered.errors[].id must be a string",
        ),
        (
            {"v": 1, "op": "registered", "errors": [{"name": "a", "code": 5, "message": 1}]},
            "registered.errors[].code must be a string",
        ),
        (
            # A bad service wins over a bad errors list: services are checked first.
            {
                "v": 1,
                "op": "registered",
                "services": [{"name": "a", "id": "i", "url": ""}],
                "errors": 3,
            },
            "registered.services[].url must be a non-empty string",
        ),
        (
            {"v": 1, "op": "unregistered", "services": [{"name": "a", "id": None}]},
            "unregistered.services[].id must be a string",
        ),
        ({"v": 1, "op": "disable", "id": "i", "reason": None}, "disable.reason must be a string"),
        ({"v": 1, "op": "enable", "id": "i"}, "enable.name must be a string"),
        ({"v": 1, "op": "error", "code": "X", "message": False}, "error.message must be a string"),
    ],
)
def test_malformed(env: dict[str, Any], message: str) -> None:
    with pytest.raises(McpwarpError) as info:
        _decode(env)
    assert info.value.code == "MALFORMED_ENVELOPE"
    assert str(info.value) == message
    assert info.value.fatal is False


def test_url_missing_in_any_entry_rejects_the_whole_message() -> None:
    env = {
        "v": 1,
        "op": "registered",
        "services": [
            {"name": "a", "id": "i", "url": "https://a"},
            {"name": "b", "id": "j", "url": ""},
        ],
    }
    with pytest.raises(McpwarpError, match=r"url must be a non-empty string"):
        _decode(env)


def test_encoders_key_order_and_bytes() -> None:
    reg = encode_register("anki")
    assert list(reg) == ["mcpwarp"]
    assert list(reg["mcpwarp"]) == ["v", "op", "services"]
    assert list(reg["mcpwarp"]["services"][0]) == ["name", "kind"]
    # MCPWARP.md section 2.2's byte-exact register.
    assert encode_control({"t": "app", "body": reg}) == (
        b'{"t":"app","body":{"mcpwarp":{"v":1,"op":"register",'
        b'"services":[{"name":"anki","kind":"http"}]}}}'
    )
    stdio = encode_register("x", "stdio")["mcpwarp"]["services"]
    assert stdio == [{"name": "x", "kind": "stdio"}]
    assert encode_control({"t": "app", "body": encode_unregister("anki")}) == (
        b'{"t":"app","body":{"mcpwarp":{"v":1,"op":"unregister","services":[{"name":"anki"}]}}}'
    )
    # Two calls never share a mutable list.
    a, b = encode_register("a"), encode_register("a")
    assert a["mcpwarp"]["services"] is not b["mcpwarp"]["services"]


def test_encoded_ops_are_not_inbound() -> None:
    # Outbound ops decode as Unknown: the decoder only knows what the edge sends.
    assert decode_envelope(encode_register("anki")) == Unknown(raw_op="register", v=1)
    assert decode_envelope(encode_unregister("anki")) == Unknown(raw_op="unregister", v=1)


@pytest.mark.parametrize(
    ("name", "ok"),
    [
        ("anki", True),
        ("a", True),
        ("a-1", True),
        ("a" * 30, True),
        ("a" * 31, False),
        ("", False),
        ("-a", False),
        ("a-", False),
        ("Anki", False),
        ("an_ki", False),
        ("anki\n", False),
        (None, False),
        (7, False),
    ],
)
def test_is_valid_service_name(name: object, ok: bool) -> None:
    assert is_valid_service_name(name) is ok


def test_integration_id_re() -> None:
    assert INTEGRATION_ID_RE.match("123e4567-e89b-42d3-a456-426614174000")
    assert INTEGRATION_ID_RE.match("00000000-0000-0000-0000-000000000000")
    assert INTEGRATION_ID_RE.match("ffffffff-ffff-ffff-ffff-ffffffffffff")
    assert not INTEGRATION_ID_RE.match("123e4567-e89b-02d3-a456-426614174000")  # version 0
    assert not INTEGRATION_ID_RE.match("123e4567-e89b-42d3-c456-426614174000")  # variant c
    assert not INTEGRATION_ID_RE.match("123E4567-E89B-42D3-A456-426614174000")  # not lowercased
    assert not INTEGRATION_ID_RE.match("123e4567-e89b-42d3-a456-426614174000\n")


def test_code_sets() -> None:
    assert len(RegisterErrorCode) == 17
    assert len(AppErrorCode) == 6
    assert TRANSIENT_REGISTER_ERRORS == {
        "INTERNAL",
        "NO_RESPONDERS",
        "TIMEOUT",
        "ABORTED",
        "CONNECTION_LOST",
        "MALFORMED_REPLY",
        "UNAVAILABLE",
        "SERVICE_ERROR",
    }
    assert all(type(c) is str for c in TRANSIENT_REGISTER_ERRORS)
    assert RegisterErrorCode.SERVER_DISABLED == "SERVER_DISABLED"


def test_mcpwarp_error() -> None:
    cause = OSError("boom")
    try:
        raise McpwarpError("CLOSED", "closed", fatal=True, service_name="anki") from cause
    except McpwarpError as e:
        assert e.code == mcpwarp_errors.CLOSED
        assert str(e) == "closed"
        assert e.message == "closed"
        assert e.fatal is True
        assert e.service_name == "anki"
        assert e.__cause__ is cause
    assert McpwarpError("X", "m").service_name is None
    assert repr(McpwarpError("X", "m", service_name="s")) == (
        "McpwarpError('X', 'm', fatal=False, service_name='s')"
    )
    for name in (
        "MALFORMED_ENVELOPE",
        "CLOSED",
        "UNAUTHORIZED",
        "TAKEN_OVER",
        "DISCONNECTED_FATAL",
    ):
        assert getattr(mcpwarp_errors, name) == name


def test_mcpwarp_error_pickles_and_copies() -> None:
    e = McpwarpError("CONFLICT", "taken", fatal=True, service_name="anki")
    for clone in (pickle.loads(pickle.dumps(e)), copy.copy(e), copy.deepcopy(e)):
        assert type(clone) is McpwarpError
        assert (clone.code, str(clone), clone.fatal, clone.service_name) == (
            "CONFLICT",
            "taken",
            True,
            "anki",
        )


def test_inbound_dataclasses_are_frozen_and_hashable() -> None:
    values = [
        Disable("i", "r"),
        Enable("i", "n"),
        ConflictOp("i", "n", "r"),
        AppError("C", "m"),
        Unknown("x", 2),
        RegisteredService("n", "i", "u", True),
        ServiceError("n", "C", "m", None),
        UnregisteredService("n", ""),
    ]
    for v in values:
        assert hash(v) == hash(type(v)(*vars(v).values()))
        with pytest.raises(AttributeError):
            setattr(v, "name", "other")  # noqa: B010


def test_list_holding_envelopes_are_not_hashable() -> None:
    for v in (Registered([], []), Unregistered([])):
        with pytest.raises(TypeError):
            hash(v)
