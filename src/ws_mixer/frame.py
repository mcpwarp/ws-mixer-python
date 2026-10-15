"""ws-mixer.v1 mux frame codec (WIRE.md sections 2.2-2.4).

`decode_frame` is stateless and applies the checks in the order the spec's frame fixtures assume.
Stream state (ids already opened, credit) is the connection's job, not this module's.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum

from ._jscompat import truncate_utf8, utf8_encode
from .errors import ConnError, ErrorCode, StreamError


class FrameType(IntEnum):
    OPEN = 0x00
    DATA = 0x01
    WINDOW = 0x02
    CLOSE = 0x03
    RESET = 0x04


HEADER_SIZE = 8
MAX_MESSAGE_SIZE = HEADER_SIZE + 65_536
"""Largest legal WebSocket message: the 8-byte header plus a 64 KiB payload."""
MAX_STREAM_ZERO_PAYLOAD = 16_384
"""Control-channel (stream 0) payload cap."""
MAX_CHUNK = 16_384
"""Recommended DATA chunk size: a sender-side default, not a wire limit."""
MAX_SEND_WINDOW = 0x7FFF_FFFF
"""Largest legal cumulative send-credit window, 2^31-1."""
MAX_RESET_MESSAGE = 256
"""RESET messages are cut to this many UTF-8 bytes on send (a sender SHOULD; receive keeps all)."""

_KNOWN_TYPES = frozenset(int(t) for t in FrameType)
_HIGH_BIT = 0x8000_0000
_HEADER = struct.Struct(">BBHI")
_U32 = struct.Struct(">I")


@dataclass(frozen=True, slots=True)
class Frame:
    type: int
    flags: int
    stream_id: int
    payload: bytes


def frame_type_name(t: int) -> str:
    """A frame type's wire name, or "UNKNOWN(0xNN)" outside the v1 set."""
    try:
        return FrameType(t).name
    except ValueError:
        return f"UNKNOWN(0x{t:02x})"


def decode_frame(msg: bytes | bytearray | memoryview) -> Frame:
    """Decodes one WebSocket message into a Frame.

    Raises ConnError for connection-fatal violations and StreamError (WINDOW increment 0) for
    stream-scoped ones. An unknown frame type decodes fine; the caller ignores and counts it.
    """
    n = len(msg)
    if n < HEADER_SIZE:
        raise ConnError(
            ErrorCode.PROTOCOL_ERROR,
            f"frame header too short: {n} bytes, need at least {HEADER_SIZE}",
        )
    if n > MAX_MESSAGE_SIZE:
        raise ConnError(
            ErrorCode.FRAME_SIZE_ERROR,
            f"message of {n} bytes exceeds the {MAX_MESSAGE_SIZE} byte limit",
        )

    ftype, flags, _reserved, stream_id = _HEADER.unpack_from(msg, 0)
    payload = bytes(msg[HEADER_SIZE:])

    if stream_id & _HIGH_BIT:
        raise ConnError(
            ErrorCode.PROTOCOL_ERROR,
            f"stream id 0x{stream_id:x} has the reserved high bit set",
        )

    frame = Frame(ftype, flags, stream_id, payload)

    if ftype not in _KNOWN_TYPES:
        return frame

    if ftype != FrameType.DATA and stream_id == 0:
        raise ConnError(
            ErrorCode.PROTOCOL_ERROR,
            f"{frame_type_name(ftype)} is not legal on stream 0 (control channel)",
        )

    if ftype == FrameType.DATA:
        if stream_id == 0 and len(payload) > MAX_STREAM_ZERO_PAYLOAD:
            raise ConnError(
                ErrorCode.ENHANCE_YOUR_CALM,
                f"stream 0 payload of {len(payload)} bytes exceeds the "
                f"{MAX_STREAM_ZERO_PAYLOAD} byte control-channel limit",
            )
    elif ftype == FrameType.OPEN:
        if stream_id % 2 == 0:
            raise ConnError(
                ErrorCode.PROTOCOL_ERROR,
                f"OPEN for even stream id {stream_id}: only the server opens streams "
                "and server ids are always odd",
            )
    elif ftype == FrameType.WINDOW:
        if len(payload) != 4:
            raise ConnError(
                ErrorCode.FRAME_SIZE_ERROR,
                f"WINDOW payload is {len(payload)} bytes, must be exactly 4",
            )
        (increment,) = _U32.unpack(payload)
        if increment == 0:
            raise StreamError(
                ErrorCode.PROTOCOL_ERROR,
                stream_id,
                "WINDOW increment of 0 is not legal (range is 1..2^31-1)",
            )
        if increment & _HIGH_BIT:
            raise ConnError(
                ErrorCode.FLOW_CONTROL_ERROR,
                f"WINDOW increment {increment} would push the send window past 2^31-1",
            )
    elif ftype == FrameType.RESET:
        if len(payload) < 4:
            raise ConnError(
                ErrorCode.FRAME_SIZE_ERROR,
                f"RESET payload is {len(payload)} bytes, must be at least 4",
            )

    return frame


def window_increment(frame: Frame) -> int:
    """The increment carried by a WINDOW frame (the caller has checked the type)."""
    return int(_U32.unpack_from(frame.payload, 0)[0])


def reset_code(frame: Frame) -> int:
    """The error code carried by a RESET frame (the caller has checked the type)."""
    return int(_U32.unpack_from(frame.payload, 0)[0])


def reset_message(frame: Frame) -> str:
    """The RESET message, in full; invalid UTF-8 becomes U+FFFD."""
    return frame.payload[4:].decode("utf-8", "replace")


def encode_frame(type: int, stream_id: int, payload: bytes = b"") -> bytes:
    """Serializes a frame: the 8-byte header (flags and reserved written as 0) plus the payload."""
    return _HEADER.pack(type, 0, 0, stream_id) + payload


def encode_open(stream_id: int) -> bytes:
    return encode_frame(FrameType.OPEN, stream_id)


def encode_data(stream_id: int, payload: bytes) -> bytes:
    return encode_frame(FrameType.DATA, stream_id, payload)


def encode_window(stream_id: int, increment: int) -> bytes:
    return encode_frame(FrameType.WINDOW, stream_id, _U32.pack(increment))


def encode_close(stream_id: int) -> bytes:
    return encode_frame(FrameType.CLOSE, stream_id)


def encode_reset(stream_id: int, code: int, message: str = "") -> bytes:
    """A RESET frame; `message` is cut to 256 UTF-8 bytes on a character boundary."""
    msg = utf8_encode(truncate_utf8(message, MAX_RESET_MESSAGE))
    return encode_frame(FrameType.RESET, stream_id, _U32.pack(code) + msg)
