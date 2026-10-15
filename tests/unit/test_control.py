from __future__ import annotations

import json
import sys
import unicodedata
from typing import Any

import pytest

from ws_mixer._jscompat import JS_WHITESPACE, is_js_integer, js_trim, truncate_utf8, utf8_encode
from ws_mixer.control import encode_control, known_drain_reason, parse_control
from ws_mixer.errors import ConnError, ErrorCode

# Non-ASCII test characters are built with chr() so the source stays plain ASCII.
E_ACUTE = chr(0xE9)
EMOJI = chr(0x1F600)

WELCOME: dict[str, Any] = {
    "t": "welcome",
    "v": 1,
    "session": "s1",
    "window": 262144,
    "max_streams": 64,
    "ping_interval": 30000,
    "ping_timeout": 90000,
}


def _err(raw: str | bytes) -> ConnError:
    with pytest.raises(ConnError) as info:
        parse_control(raw)
    return info.value


def test_bool_is_not_an_integer() -> None:
    e = _err(json.dumps({"t": "ping", "id": True}))
    assert e.code == ErrorCode.PROTOCOL_ERROR and "id" in e.message
    assert _err(json.dumps({**WELCOME, "v": True})).code == ErrorCode.PROTOCOL_ERROR


def test_integral_float_is_an_integer() -> None:
    m = parse_control('{"t":"ping","id":1.0,"ts":5.0}')
    assert m == {"t": "ping", "id": 1, "ts": 5}
    assert m["t"] == "ping" and type(m["id"]) is int
    assert parse_control(json.dumps({**WELCOME, "v": 1.0}))["t"] == "welcome"
    assert _err('{"t":"ping","id":1.5}').code == ErrorCode.PROTOCOL_ERROR
    assert _err('{"t":"ping","id":1e400}').code == ErrorCode.PROTOCOL_ERROR


def test_nan_and_infinity_rejected() -> None:
    for lit in ("NaN", "Infinity", "-Infinity"):
        assert _err('{"t":"ping","id":' + lit + "}").code == ErrorCode.PROTOCOL_ERROR


def test_version_mismatch_is_unsupported() -> None:
    assert _err(json.dumps({**WELCOME, "v": 2})).code == ErrorCode.UNSUPPORTED
    hello = {"t": "hello", "v": 2, "token": "x", "agent": {"sdk": "a", "sdk_version": "1"}}
    assert _err(json.dumps(hello)).code == ErrorCode.UNSUPPORTED


def test_welcome_version_checked_before_missing_fields() -> None:
    assert _err('{"t":"welcome","v":2}').code == ErrorCode.UNSUPPORTED


def test_welcome_floors_and_subfloor() -> None:
    sub = {**WELCOME, "ping_interval": 100, "ping_timeout": 150}
    assert "ping_interval" in _err(json.dumps(sub)).message
    assert parse_control(json.dumps(sub), allow_subfloor=True)["t"] == "welcome"
    short = {**WELCOME, "ping_timeout": 59999}
    assert "ping_timeout" in _err(json.dumps(short)).message


def test_oversize_payload() -> None:
    body = "x" * 16_384
    assert _err(json.dumps({"t": "app", "body": {"a": body}})).code == ErrorCode.ENHANCE_YOUR_CALM


def test_strict_utf8_and_no_bom() -> None:
    assert _err(b'{"t":"app","body":{"a":"\xff"}}').code == ErrorCode.PROTOCOL_ERROR
    assert _err(b'\xef\xbb\xbf{"t":"app","body":{}}').code == ErrorCode.PROTOCOL_ERROR


def test_unknown_type_and_fields() -> None:
    assert "unknown control message type" in _err('{"t":"regsiter"}').message
    m = parse_control('{"t":"pong","id":3,"rtt_ms":4}')
    assert m == {"t": "pong", "id": 3}


def test_length_limits_count_code_points() -> None:
    # 1024 emoji = 1024 code points (2048 UTF-16 units): within error.message's 1024.
    msg = EMOJI * 1024
    assert parse_control(json.dumps({"t": "error", "code": 1, "message": msg}))["t"] == "error"
    assert _err(json.dumps({"t": "error", "code": 1, "message": msg + "x"})).code == 1


def test_empty_error_message_accepted() -> None:
    # ws-mixer-go's Conn.Close(code, "") sends this; rejecting it would answer an error with one.
    assert parse_control('{"t":"error","code":0,"message":""}') == {
        "t": "error",
        "code": 0,
        "message": "",
    }


def test_conn_error_carries_control_type() -> None:
    assert _err('{"t":"error","code":"x","message":"m"}').control_type == "error"
    assert _err('{"t":"ping"}').control_type == "ping"
    assert _err('{"t":"regsiter"}').control_type == "regsiter"
    assert _err('{"v":1}').control_type is None
    assert _err('{"t":1}').control_type is None
    assert _err("[]").control_type is None


def test_echoed_peer_strings_are_capped() -> None:
    long_t = "x" * 10_000
    e = _err(json.dumps({"t": long_t}))
    assert "unknown control message type" in e.message
    assert '"' + "x" * 64 + '"...' in e.message and len(e.message) < 120
    hello = {"t": "hello", "v": 1, "token": "x", "agent": {"sdk": "a", "sdk_version": "1"}}
    e = _err(json.dumps({**hello, "capabilities": ["Z" * 5000]}))
    assert len(e.message) < 140


def test_capability_pattern_is_anchored() -> None:
    hello = {"t": "hello", "v": 1, "token": "x", "agent": {"sdk": "a", "sdk_version": "1"}}
    assert parse_control(json.dumps({**hello, "capabilities": ["a.b-c_d"]}))["t"] == "hello"
    assert _err(json.dumps({**hello, "capabilities": ["abc\n"]})).code == 1
    assert _err(json.dumps({**hello, "capabilities": ["ABC"]})).code == 1


def test_hello_defaults() -> None:
    hello = {"t": "hello", "v": 1, "token": "x", "agent": {"sdk": "a", "sdk_version": "1"}}
    m = parse_control(json.dumps(hello))
    assert m["t"] == "hello" and m["window"] == 262144 and m["max_streams"] == 64


def test_encode_control_t_first_compact() -> None:
    out = encode_control({"id": 1, "t": "ping", "ts": 2})
    assert out == b'{"t":"ping","id":1,"ts":2}'
    word = "h" + E_ACUTE + "llo"
    app = encode_control({"t": "app", "body": {"msg": word}})
    assert app == ('{"t":"app","body":{"msg":"' + word + '"}}').encode()
    assert parse_control(app) == {"t": "app", "body": {"msg": word}}


def test_known_drain_reason() -> None:
    assert known_drain_reason("rollout") and known_drain_reason("client_requested")
    assert not known_drain_reason("v2_capacity_shed")


def test_js_whitespace_set() -> None:
    # ECMAScript: TAB VT FF ZWNBSP + every Zs code point (WhiteSpace), LF CR LS PS (LineTerminator).
    zs = {cp for cp in range(sys.maxunicode + 1) if unicodedata.category(chr(cp)) == "Zs"}
    expected = {0x09, 0x0B, 0x0C, 0xFEFF} | zs | {0x0A, 0x0D, 0x2028, 0x2029}
    assert {ord(c) for c in JS_WHITESPACE} == expected
    assert len(JS_WHITESPACE) == len(expected)


def test_js_trim() -> None:
    padding = "".join(map(chr, (0xA0, 0xFEFF, 0x2028, 0x20)))
    assert js_trim(padding + "tok" + chr(0x3000) + "\t\n") == "tok"
    # Python's str.strip() removes the first three; JS trim() removes none of them.
    for ch in map(chr, (0x1C, 0x1F, 0x85, 0x200B)):
        assert js_trim(ch + "t" + ch) == ch + "t" + ch
        assert ch not in JS_WHITESPACE


def test_is_js_integer() -> None:
    assert is_js_integer(0) and is_js_integer(-3) and is_js_integer(2.0)
    assert not is_js_integer(True) and not is_js_integer(False)
    assert not is_js_integer(1.5) and not is_js_integer(float("inf"))
    assert not is_js_integer(float("nan")) and not is_js_integer("1") and not is_js_integer(None)


def test_truncate_utf8() -> None:
    word = "h" + E_ACUTE + "llo"
    assert truncate_utf8("hello", 10) == "hello"
    assert truncate_utf8(word, 2) == "h"
    assert truncate_utf8(word, 3) == "h" + E_ACUTE
    assert truncate_utf8(EMOJI * 2, 7) == EMOJI
    assert truncate_utf8("abc", 0) == ""


def test_utf8_encode_lone_surrogate() -> None:
    high, low = chr(0xD83D), chr(0xDE00)
    replacement = chr(0xFFFD).encode()
    assert utf8_encode("a" + high + "b") == b"a" + replacement + b"b"
    assert utf8_encode(high + low) == EMOJI.encode()
    assert truncate_utf8("a" + low, 1) == "a"
    # Nothing cut, but the lone surrogate is still sanitized.
    assert truncate_utf8("a" + low, 10) == "a" + chr(0xFFFD)
