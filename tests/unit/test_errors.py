from __future__ import annotations

import pytest

from ws_mixer.errors import (
    ConnError,
    ErrorCode,
    StreamError,
    TokenUnavailableError,
    WsMixerError,
    close_code,
    code_name,
    is_token_unavailable,
    parse_error_code,
    wire_close_code,
)

SPEC_TABLE = [
    (0x00, "NO_ERROR", 1000),
    (0x01, "PROTOCOL_ERROR", 4001),
    (0x02, "INTERNAL_ERROR", 4002),
    (0x03, "FLOW_CONTROL_ERROR", 4003),
    (0x04, "FRAME_SIZE_ERROR", 4004),
    (0x05, "STREAM_CLOSED", 4005),
    (0x06, "REFUSED_STREAM", 4006),
    (0x07, "CANCEL", 4007),
    (0x08, "STREAM_LIMIT", 4008),
    (0x09, "ENHANCE_YOUR_CALM", 4009),
    (0x0A, "UNSUPPORTED", 4010),
    (0x0B, "UNAUTHORIZED", 4011),
    (0x0C, "GOING_AWAY", 4012),
    (0x0D, "KEEPALIVE_TIMEOUT", 4013),
    (0x0E, "APPLICATION_CLOSE", 4014),
]


@pytest.mark.parametrize(("code", "name", "ws"), SPEC_TABLE)
def test_table(code: int, name: str, ws: int) -> None:
    assert ErrorCode[name] == code
    assert code_name(code) == name
    assert parse_error_code(name) == code
    assert close_code(code) == ws


def test_exactly_fifteen_codes() -> None:
    assert len(ErrorCode) == 15


def test_unknown_codes() -> None:
    assert code_name(0x0F) == "INTERNAL_ERROR"
    assert code_name(0x1000_0000) == "INTERNAL_ERROR"
    assert parse_error_code("NOPE") is None
    assert parse_error_code("SCHEMA_ADDITIONAL_PROPERTIES") is None


def test_wire_close_code_clamps() -> None:
    assert wire_close_code(1000) == 1000
    assert wire_close_code(4000) == 4000
    assert wire_close_code(4999) == 4999
    assert wire_close_code(close_code(0x3E7)) == 4999
    assert wire_close_code(close_code(0x3E8)) == 4002
    assert wire_close_code(close_code(0x1000_0000)) == 4002
    assert wire_close_code(1001) == 4002
    assert wire_close_code(3999) == 4002


def test_error_shapes() -> None:
    e = WsMixerError(ErrorCode.UNSUPPORTED, "no", fatal=True, ws_code=4010, http_status=None)
    assert e.code == 10 and type(e.code) is int
    assert e.code_name == "UNSUPPORTED"
    assert e.fatal is True and e.ws_code == 4010 and e.stream_id is None
    assert str(e) == "WsMixerError[UNSUPPORTED]: no"
    assert e.message == "no"

    c = ConnError(ErrorCode.PROTOCOL_ERROR, "bad", last_stream_id=7)
    assert isinstance(c, WsMixerError)
    assert c.last_stream_id == 7 and c.fatal is False
    assert str(c) == "WsMixerError[PROTOCOL_ERROR]: bad"

    s = StreamError(ErrorCode.CANCEL, 5, "gone")
    assert s.stream_id == 5 and s.code_name == "CANCEL" and not isinstance(s, ConnError)

    assert WsMixerError(0x1234_5678, "x").code_name == "INTERNAL_ERROR"


def test_token_unavailable_direct_and_chain() -> None:
    assert is_token_unavailable(TokenUnavailableError("offline"))
    assert not is_token_unavailable(RuntimeError("nope"))
    assert not is_token_unavailable(None)

    try:
        try:
            raise TokenUnavailableError("offline")
        except TokenUnavailableError as inner:
            raise RuntimeError("refresh failed") from inner
    except RuntimeError as outer:
        assert is_token_unavailable(outer)


def test_token_unavailable_ignores_context() -> None:
    try:
        try:
            raise TokenUnavailableError("offline")
        except TokenUnavailableError:
            raise RuntimeError("while handling")  # noqa: B904 - __context__ only, on purpose
    except RuntimeError as outer:
        assert outer.__context__ is not None
        assert not is_token_unavailable(outer)


def test_token_unavailable_depth_limit() -> None:
    def chain(n: int) -> BaseException:
        cur: BaseException = TokenUnavailableError("deep")
        for i in range(n):
            nxt = RuntimeError(f"wrap {i}")
            nxt.__cause__ = cur
            cur = nxt
        return cur

    assert is_token_unavailable(chain(9))  # marker is the 10th link
    assert not is_token_unavailable(chain(10))  # marker is the 11th link


def test_token_unavailable_cycle() -> None:
    a = RuntimeError("a")
    b = RuntimeError("b")
    a.__cause__ = b
    b.__cause__ = a
    assert not is_token_unavailable(a)
