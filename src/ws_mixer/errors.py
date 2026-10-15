"""ws-mixer.v1 error codes (WIRE.md section 2.8) and the SDK's exception types.

One 32-bit code space is shared by stream RESET frames and the connection-level `error`
control message. Unknown codes received on the wire are treated as INTERNAL_ERROR.
"""

from __future__ import annotations

from enum import IntEnum


class ErrorCode(IntEnum):
    NO_ERROR = 0x00
    PROTOCOL_ERROR = 0x01
    INTERNAL_ERROR = 0x02
    FLOW_CONTROL_ERROR = 0x03
    FRAME_SIZE_ERROR = 0x04
    STREAM_CLOSED = 0x05
    REFUSED_STREAM = 0x06
    CANCEL = 0x07
    STREAM_LIMIT = 0x08
    ENHANCE_YOUR_CALM = 0x09
    UNSUPPORTED = 0x0A
    UNAUTHORIZED = 0x0B
    GOING_AWAY = 0x0C
    KEEPALIVE_TIMEOUT = 0x0D
    # Connection-level only and never emitted by ws-mixer itself: the application's own close
    # (WS close 4014), with its reason in error.message and the close reason.
    APPLICATION_CLOSE = 0x0E


_MAX_CAUSE_CHAIN_DEPTH = 10


def code_name(code: int) -> str:
    """The wire name of `code`, e.g. "FLOW_CONTROL_ERROR"; unknown codes give "INTERNAL_ERROR"."""
    try:
        return ErrorCode(code).name
    except ValueError:
        return ErrorCode.INTERNAL_ERROR.name


def parse_error_code(name: str) -> int | None:
    """The code for a wire name such as "STREAM_LIMIT", or None when the name is unknown."""
    member = ErrorCode.__members__.get(name)
    return None if member is None else int(member)


def close_code(code: int) -> int:
    """ws_close = 4000 + error_code, with NO_ERROR mapping to 1000."""
    return 1000 if code == ErrorCode.NO_ERROR else 4000 + code


def wire_close_code(ws_code: int) -> int:
    """Clamps a would-be WS close code to one legal on the wire.

    1000 and 4000-4999 pass through; anything else (e.g. the close code of an application error
    code >= 0x1000_0000) becomes INTERNAL_ERROR's 4002. Only the bytes put on the wire are
    clamped: error.code and the reported ws_code keep the real value.
    """
    if ws_code == 1000 or 4000 <= ws_code <= 4999:
        return ws_code
    return close_code(ErrorCode.INTERNAL_ERROR)


class WsMixerError(Exception):
    """The base of every error this package raises or reports.

    `fatal` marks a failure the client must never reconnect after. `ws_code` and `close_reason`
    are set only when the error was built from an observed WS close frame; `http_status` only for
    a failed upgrade whose status the connector reported. The cause goes in `__cause__`.
    """

    code: int
    message: str
    fatal: bool
    stream_id: int | None
    ws_code: int | None
    close_reason: str | None
    http_status: int | None

    def __init__(
        self,
        code: int,
        message: str,
        *,
        fatal: bool = False,
        stream_id: int | None = None,
        ws_code: int | None = None,
        close_reason: str | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = int(code)
        self.message = message
        self.fatal = fatal
        self.stream_id = stream_id
        self.ws_code = ws_code
        self.close_reason = close_reason
        self.http_status = http_status

    @property
    def code_name(self) -> str:
        return code_name(self.code)

    def __str__(self) -> str:
        return f"WsMixerError[{self.code_name}]: {self.message}"


class ConnError(WsMixerError):
    """A connection-level error: the connection is (or was) torn down with error{code} + WS close.

    Also what every stream still live when the connection ends is failed with. `control_type`
    is set by `parse_control` to the rejected message's `t`, when it got that far.
    """

    last_stream_id: int | None
    control_type: str | None

    def __init__(
        self,
        code: int,
        message: str,
        *,
        fatal: bool = False,
        stream_id: int | None = None,
        last_stream_id: int | None = None,
        ws_code: int | None = None,
        close_reason: str | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(
            code,
            message,
            fatal=fatal,
            stream_id=stream_id,
            ws_code=ws_code,
            close_reason=close_reason,
            http_status=http_status,
        )
        self.last_stream_id = last_stream_id
        self.control_type = None


class StreamError(WsMixerError):
    """A stream-scoped error: RESET(code, message) on one stream; the connection stays up."""

    def __init__(self, code: int, stream_id: int, message: str) -> None:
        super().__init__(code, message, stream_id=stream_id)


class TokenUnavailableError(Exception):
    """Raised by a token provider that could not obtain a token for a temporary reason.

    Raising it (directly, or as the `__cause__` of what the provider raises) turns the dial
    failure into an ordinary non-fatal retry; any other provider exception is fatal. Detection
    is `isinstance` only, never duck-typed, so an unrelated error can never cause endless retries.
    """


def is_token_unavailable(exc: BaseException | None) -> bool:
    """True if `exc` or its `__cause__` chain (at most 10 links, cycle-safe) holds the marker.

    Only `__cause__` is followed, never `__context__`: an exception that merely happened while
    handling a TokenUnavailableError does not inherit its meaning.
    """
    seen: set[int] = set()
    cur = exc
    depth = 0
    while cur is not None and depth < _MAX_CAUSE_CHAIN_DEPTH and id(cur) not in seen:
        if isinstance(cur, TokenUnavailableError):
            return True
        seen.add(id(cur))
        cur = cur.__cause__
        depth += 1
    return False
