from __future__ import annotations

import pytest

from ws_mixer.errors import ConnError, ErrorCode, StreamError
from ws_mixer.frame import (
    HEADER_SIZE,
    MAX_MESSAGE_SIZE,
    MAX_STREAM_ZERO_PAYLOAD,
    Frame,
    FrameType,
    decode_frame,
    encode_close,
    encode_data,
    encode_frame,
    encode_open,
    encode_reset,
    encode_window,
    frame_type_name,
    reset_code,
    reset_message,
    window_increment,
)


def test_constants() -> None:
    assert HEADER_SIZE == 8
    assert MAX_MESSAGE_SIZE == 65_544
    assert MAX_STREAM_ZERO_PAYLOAD == 16_384


def test_roundtrips() -> None:
    assert decode_frame(encode_open(3)) == Frame(FrameType.OPEN, 0, 3, b"")
    assert decode_frame(encode_data(5, b"hi")) == Frame(FrameType.DATA, 0, 5, b"hi")
    assert decode_frame(encode_close(7)) == Frame(FrameType.CLOSE, 0, 7, b"")
    w = decode_frame(encode_window(9, 0x7FFF_FFFF))
    assert w.type == FrameType.WINDOW and window_increment(w) == 0x7FFF_FFFF
    r = decode_frame(encode_reset(11, ErrorCode.CANCEL, "bye"))
    assert reset_code(r) == 7 and reset_message(r) == "bye"
    assert encode_frame(FrameType.DATA, 1, b"x") == bytes.fromhex("010000000000000178")


def test_reset_with_app_code() -> None:
    r = decode_frame(encode_reset(1, 0x1000_0001))
    assert reset_code(r) == 0x1000_0001 and reset_message(r) == ""


def test_encode_reset_truncates_on_char_boundary() -> None:
    # 255 ASCII bytes then a 4-byte emoji: the emoji does not fit and is dropped whole.
    msg = "a" * 255 + "\U0001f600"
    frame = encode_reset(1, 2, msg)
    assert len(frame) == HEADER_SIZE + 4 + 255
    assert reset_message(decode_frame(frame)) == "a" * 255


def test_reset_message_kept_whole_on_receive() -> None:
    # 256 bytes is a sender SHOULD; a receiver surfaces whatever arrived (as JS and Go do).
    e_acute = chr(0xE9)
    payload = (2).to_bytes(4, "big") + (e_acute * 200).encode()  # 400 bytes of 2-byte chars
    msg = reset_message(decode_frame(encode_frame(FrameType.RESET, 1, payload)))
    assert msg == e_acute * 200


def test_reset_message_invalid_utf8_replaced() -> None:
    payload = (1).to_bytes(4, "big") + b"a\xffb"
    expected = "a" + chr(0xFFFD) + "b"
    assert reset_message(decode_frame(encode_frame(FrameType.RESET, 1, payload))) == expected


def test_frame_type_name() -> None:
    assert frame_type_name(0) == "OPEN"
    assert frame_type_name(4) == "RESET"
    assert frame_type_name(5) == "UNKNOWN(0x05)"
    assert frame_type_name(0xAB) == "UNKNOWN(0xab)"


def test_unknown_type_on_stream_zero_is_ignorable() -> None:
    f = decode_frame(encode_frame(0x7F, 0, b"zz"))
    assert f.type == 0x7F and f.stream_id == 0


def test_rule_order_short_before_anything() -> None:
    with pytest.raises(ConnError) as e:
        decode_frame(b"\x03\x00\x00\x00\x00\x00\x00")
    assert e.value.code == ErrorCode.PROTOCOL_ERROR


def test_window_increment_zero_is_stream_scoped() -> None:
    with pytest.raises(StreamError) as e:
        decode_frame(encode_window(3, 0))
    assert e.value.code == ErrorCode.PROTOCOL_ERROR and e.value.stream_id == 3


def test_stream_zero_data_at_limit() -> None:
    decode_frame(encode_data(0, b"x" * MAX_STREAM_ZERO_PAYLOAD))
    with pytest.raises(ConnError) as e:
        decode_frame(encode_data(0, b"x" * (MAX_STREAM_ZERO_PAYLOAD + 1)))
    assert e.value.code == ErrorCode.ENHANCE_YOUR_CALM


def test_accepts_bytearray_and_memoryview() -> None:
    raw = encode_data(1, b"abc")
    assert decode_frame(bytearray(raw)).payload == b"abc"
    assert decode_frame(memoryview(raw)).payload == b"abc"
