"""describe_status() and TunnelStatus beyond the spec fixtures: overrides, odd input, shapes."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any

import pytest

from ws_mixer.mcpwarp import (
    STATUS_LOCALES,
    StatusMessages,
    StatusText,
    TunnelState,
    TunnelStatus,
    TunnelStatusCode,
    describe_status,
)
from ws_mixer.mcpwarp import status as st
from ws_mixer.mcpwarp.status_text import _TABLES


def test_enums_carry_the_js_strings() -> None:
    assert [s.value for s in TunnelState] == [
        "off",
        "connecting",
        "online",
        "disabled",
        "conflict",
        "error",
    ]
    assert len(TunnelStatusCode) == 13
    assert all(c.value == c.name for c in TunnelStatusCode)


def test_tunnel_status_to_dict_drops_none() -> None:
    assert TunnelStatus(TunnelState.OFF).to_dict() == {"state": "off"}
    full = TunnelStatus(TunnelState.ERROR, url=None, code=TunnelStatusCode.INTERNAL, last_error="x")
    d = full.to_dict()
    assert d == {"state": "error", "code": "INTERNAL", "lastError": "x"}
    assert all(type(v) is str for v in d.values())
    online = TunnelStatus(TunnelState.ONLINE, url="https://a/mcp")
    assert online.to_dict() == {"state": "online", "url": "https://a/mcp"}
    # An empty string is a value, not an absent key.
    assert TunnelStatus(TunnelState.OFF, last_error="").to_dict() == {
        "state": "off",
        "lastError": "",
    }


def test_status_dataclasses_hash_and_compare_by_value() -> None:
    a = TunnelStatus(TunnelState.CONNECTING, code=TunnelStatusCode.RETRYING, last_error="r")
    b = TunnelStatus(TunnelState.CONNECTING, code=TunnelStatusCode.RETRYING, last_error="r")
    assert a == b and hash(a) == hash(b)
    assert len({a, b}) == 1
    assert hash(StatusText("x", "y")) == hash(StatusText("x", "y"))
    assert st.RETRY_DIAL == st.Problem(TunnelStatusCode.RETRYING, "Can't reach mcpwarp; retrying.")


def test_problem_table() -> None:
    assert st.INVALID_NAME.last_error == (
        "The service name is invalid: use lowercase letters, digits and dashes "
        "(not at the start or end), at most 30 characters."
    )
    assert st.FORBIDDEN.code == st.REJECTED.code == TunnelStatusCode.UNAUTHORIZED
    invalid = (st.INVALID_SETTINGS, st.TOKEN_NOT_SET, st.NAME_NOT_SET)
    assert {p.code for p in invalid} == {TunnelStatusCode.INVALID_CONFIG}
    problems = [v for v in vars(st).values() if isinstance(v, st.Problem)]
    assert len(problems) == 17
    for p in problems:
        assert p.last_error.endswith(".")
        assert not p.last_error.startswith("mcpwarp: ")


def test_status_text_to_dict() -> None:
    assert StatusText("Off").to_dict() == {"label": "Off"}
    assert StatusText("Error", "Plan limit reached").to_dict() == {
        "label": "Error",
        "detail": "Plan limit reached",
    }


def test_accepts_tunnel_status_and_enums() -> None:
    s = TunnelStatus(TunnelState.ERROR, code=TunnelStatusCode.QUOTA_EXCEEDED, last_error="x")
    assert describe_status(s) == StatusText("Error", "Plan limit reached")
    assert describe_status(s, locale="de") == StatusText("Fehler", "Tariflimit erreicht")
    s = TunnelStatus(TunnelState.ERROR, code=TunnelStatusCode.INVALID_CONFIG, last_error="a: b")
    assert describe_status(s).detail == "Invalid settings: a; b"


def test_mapping_accepts_both_last_error_spellings() -> None:
    want = StatusText("Error", "Invalid settings: The token is not set.")
    for key in ("lastError", "last_error"):
        status = {"state": "error", "code": "INVALID_CONFIG", key: "The token is not set."}
        assert describe_status(status) == want
    assert describe_status({"state": "off", "last_error": "host text"}).detail == "host text"


def test_unknown_state_uses_str() -> None:
    assert describe_status({"state": "paused"}) == StatusText("paused")
    assert describe_status({"state": 7}) == StatusText("7")
    assert describe_status({}).label == "None"


def test_unknown_code_shows_last_error_unchanged() -> None:
    status = {"state": "error", "code": "NEW_THING", "lastError": "a:  b"}
    assert describe_status(status) == StatusText("Error", "a:  b")
    assert describe_status({"state": "error", "code": 5}) == StatusText("Error")
    assert describe_status({"state": "off", "lastError": 5}) == StatusText("Off")


def test_invalid_config_rules() -> None:
    def detail(last_error: object, locale: str = "en") -> str | None:
        status = {"state": "error", "code": "INVALID_CONFIG", "lastError": last_error}
        return describe_status(status, locale=locale).detail

    assert detail("The tunnel settings are invalid.") == "Invalid settings"
    assert detail("Invalid settings, really") == "Invalid settings, really"
    assert detail("") == "Invalid settings"
    assert detail(None) == "Invalid settings"
    assert detail("x:y") == "Invalid settings: x:y"
    assert detail("https://h:\u3000x") == "Invalid settings: https://h; x"
    # Python's whitespace but not JS's: U+001C..U+001F and U+0085 are left alone.
    assert detail("a:\x1cb") == "Invalid settings: a:\x1cb"
    assert detail("a:\x85b") == "Invalid settings: a:\x85b"
    assert detail("x", "fr") == "Param\u00e8tres non valides\u00a0: x"
    assert detail("x", "ja") == "\u8a2d\u5b9a\u304c\u7121\u52b9\u3067\u3059\uff1ax"
    assert detail("x", "zh-CN") == "\u8bbe\u7f6e\u65e0\u6548\uff1ax"


@pytest.mark.parametrize(
    ("locale", "want"),
    [
        ("de", "de"),
        ("DE", "de"),
        ("zh_CN", "zh-CN"),
        ("zh-TW", "zh-CN"),
        ("\u00a0fr\u2028", "fr"),
        ("en-US", "en"),
        ("es_MX", "es"),
        ("ja-JP", "ja"),
        ("pt_BR", "en"),
        ("", "en"),
        ("__proto__", "en"),
        (None, "en"),
        (42, "en"),
        (b"de", "en"),
    ],
)
def test_locale_resolution(locale: Any, want: str) -> None:
    off = _TABLES[want].states["off"]
    assert describe_status({"state": "off"}, locale=locale).label == off


def test_python_only_whitespace_is_not_trimmed_from_locale() -> None:
    # str.strip() would remove U+001C; JS trim() does not, so the tag stays unknown.
    assert describe_status({"state": "off"}, locale="\x1cde").label == "Off"


def test_overrides_known_keys_with_str_values_only() -> None:
    messages: dict[str, Any] = {
        "states": {"error": "Oops", "bogus": "x", "off": 5},
        "codes": {"CONFLICT": "Taken", "NOPE": "y", "INTERNAL": None},
    }
    status = {"state": "error", "code": "CONFLICT"}
    assert describe_status(status, messages=messages) == StatusText("Oops", "Taken")
    assert describe_status({"state": "off"}, messages=messages).label == "Off"
    assert describe_status({"state": "bogus"}, messages=messages).label == "bogus"
    internal = {"state": "error", "code": "INTERNAL"}
    assert describe_status(internal, messages=messages).detail == "Unexpected error"
    nope = {"state": "error", "code": "NOPE", "lastError": "z"}
    assert describe_status(nope, messages=messages).detail == "z"


def test_overrides_apply_to_the_resolved_locale() -> None:
    m = StatusMessages(states=MappingProxyType({"off": "Zu"}))
    assert describe_status({"state": "off"}, locale="de", messages=m).label == "Zu"
    assert describe_status({"state": "online"}, locale="de", messages=m).label == "Online"


def test_overrides_by_shape_from_another_vendored_copy() -> None:
    class ForeignMessages:  # a StatusMessages from a second copy of the package
        states = {"off": "Zu"}
        codes = {"INTERNAL": "Kaputt"}

    m: Any = ForeignMessages()
    assert describe_status({"state": "off"}, messages=m).label == "Zu"
    internal = {"state": "error", "code": "INTERNAL"}
    assert describe_status(internal, messages=m).detail == "Kaputt"
    with pytest.raises(TypeError):
        hash(StatusMessages(states=MappingProxyType({"off": "x"})))


def test_malformed_overrides_are_ignored() -> None:
    for messages in ({"states": 5}, {"codes": "x"}, {}, {"states": None}):
        assert describe_status({"state": "off"}, messages=messages).label == "Off"  # type: ignore[arg-type]


def test_overrides_do_not_mutate_shipped_tables() -> None:
    describe_status({"state": "off"}, messages={"states": {"off": "X"}})
    assert describe_status({"state": "off"}).label == "Off"
    with pytest.raises(TypeError):
        _TABLES["en"].states["off"] = "X"  # type: ignore[index]


def test_detail_omitted_when_equal_to_label() -> None:
    assert describe_status({"state": "disabled", "code": "DISABLED"}) == StatusText(
        "Disabled in the mcpwarp dashboard"
    )
    messages = {"codes": {"RETRYING": "Connecting\u2026"}}
    status = {"state": "connecting", "code": "RETRYING"}
    assert describe_status(status, messages=messages) == StatusText("Connecting\u2026")


def test_every_locale_has_every_state_and_code() -> None:
    for loc in STATUS_LOCALES:
        assert set(_TABLES[loc].states) == {s.value for s in TunnelState}
        assert set(_TABLES[loc].codes) == {c.value for c in TunnelStatusCode}


def test_joiners_are_the_exact_code_points() -> None:
    from ws_mixer.mcpwarp.status_text import _JOINERS

    assert [hex(ord(c)) for c in _JOINERS["fr"]] == ["0xa0", "0x3a", "0x20"]
    assert _JOINERS["ja"] == _JOINERS["zh-CN"] == "\uff1a"
