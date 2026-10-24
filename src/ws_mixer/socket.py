"""The WebSocket seam (WIRE.md section 2.1): what the SDK needs from a WebSocket and its dialer.

A `WebSocketConnector` opens one connection per call and returns once the upgrade succeeded.
It raises `UpgradeRejected` for a non-101 response, `UnexpectedSubprotocol` for a 101 whose
subprotocol echo was not offered (fatal), `InvalidHandshake` (an `OSError`, retryable) for a
101 that otherwise breaks RFC 6455, and other `OSError`s for network failures. A 101 with no
echo at all is not an error here: `subprotocol` is None and the SDK fails. The default connector is
`ws_mixer._rfc6455.StdlibConnector`; `ws_mixer.adapters.websockets` wraps the `websockets`
package, and `ws_mixer.testing` has scripted fakes.
"""

from __future__ import annotations

import email.utils
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol

from ._http1 import is_field_value, is_token
from ._jscompat import js_trim
from .clock import wall_ms
from .frame import MAX_MESSAGE_SIZE

SUBPROTOCOL = "ws-mixer.v1"
MAX_CLOSE_REASON = 123
"""UTF-8 bytes in a close reason (a control frame payload is at most 125 bytes)."""
MAX_RETRY_AFTER_MS = 2**31 - 1
"""Cap on a server's retry hint (429 `Retry-After`, `drain.retry_after_ms`): the largest JS
timer delay, as the JS SDK clamps it."""

_RESERVED_HEADERS = frozenset({"host", "upgrade", "connection", "content-length"})
# `Number(s)` for a trimmed, non-empty string: a decimal literal or a 0x/0o/0b integer.
_JS_DECIMAL = re.compile(r"^[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$")
_JS_RADIX = re.compile(r"^0(?:[xX][0-9a-fA-F]+|[oO][0-7]+|[bB][01]+)$")


@dataclass(frozen=True)
class WebSocketInit:
    protocols: tuple[str, ...] = (SUBPROTOCOL,)
    headers: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    max_message: int = MAX_MESSAGE_SIZE


class WebSocketConnection(Protocol):
    @property
    def subprotocol(self) -> str | None:
        """The subprotocol the server echoed, or None when it echoed none."""
        ...

    async def recv(self) -> bytes | str:
        """The next message; a text frame arrives as `str`. Raises `WebSocketClosed` once closed."""
        ...

    async def send(self, data: bytes) -> None:
        """Sends one binary message; awaiting it is the socket's backpressure."""
        ...

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Starts the close handshake and waits, bounded, for it to finish. Idempotent."""
        ...

    def abort(self) -> None:
        """Drops the transport now, without a close frame."""
        ...


class WebSocketConnector(Protocol):
    async def __call__(self, url: str, init: WebSocketInit) -> WebSocketConnection: ...


class UpgradeRejected(Exception):
    """The server answered the upgrade with a status other than 101. Redirects are not followed.

    `headers` has lowercase names; repeated fields are joined with ", ".
    """

    status: int
    headers: Mapping[str, str]

    def __init__(self, status: int, headers: Mapping[str, str] | None = None) -> None:
        super().__init__(f"HTTP {status} in response to the WebSocket upgrade")
        self.status = status
        self.headers = MappingProxyType({k.lower(): v for k, v in (headers or {}).items()})

    def retry_after_ms(self, now_ms: float | None = None) -> float | None:
        """Retry-After as a non-negative delay, or None when absent or unparseable.

        `now_ms` (Unix epoch milliseconds) is only used for the HTTP-date form.
        """
        return parse_retry_after(self.headers.get("retry-after"), now_ms)


class ProxyRejected(UpgradeRejected):
    """An HTTP proxy refused the CONNECT tunnel (e.g. 407): the edge was never reached.

    Always fatal: the proxy's status says nothing about the edge, and retrying a 407 forever
    only hides a proxy configuration problem.
    """

    def __init__(self, status: int, headers: Mapping[str, str] | None = None) -> None:
        super().__init__(status, headers)
        self.args = (f"HTTP proxy refused the CONNECT tunnel with HTTP {status}",)


def _js_number(s: str) -> float | None:
    """JavaScript `Number(s)` when finite, else None."""
    t = js_trim(s)
    if not t:
        return 0.0
    if _JS_RADIX.match(t):
        return float(int(t, 0))
    if _JS_DECIMAL.match(t):
        n = float(t)
        return n if math.isfinite(n) else None
    return None


def parse_retry_after(value: str | None, now_ms: float | None = None) -> float | None:
    """Retry-After (RFC 9110 section 10.2.3) in milliseconds, clamped to [0, 2^31-1].

    Numbers follow JavaScript's `Number()` as the JS SDK does ("1e3", "-1" and "" all parse);
    otherwise an HTTP-date, measured from `now_ms` (Unix epoch ms, default the wall clock).
    """
    if value is None:
        return None
    seconds = _js_number(value)
    if seconds is not None:
        ms = seconds * 1000
    else:
        try:
            when = email.utils.parsedate_to_datetime(value.strip())
        except (TypeError, ValueError, IndexError):
            return None
        if when.tzinfo is None:
            return None
        ms = when.timestamp() * 1000 - (wall_ms() if now_ms is None else now_ms)
    return min(max(0.0, ms), float(MAX_RETRY_AFTER_MS))


def check_close_args(code: int, reason: str) -> None:
    """Raises ValueError unless an application may send close `code` with `reason`.

    Allowed codes are 1000, 1001 and 3000-4999; the reason is at most 123 UTF-8 bytes.
    """
    if isinstance(code, bool) or not isinstance(code, int):
        raise TypeError("close code must be an int")
    if not (code in (1000, 1001) or 3000 <= code <= 4999):
        raise ValueError(f"close code {code} cannot be sent by an application")
    if len(reason.encode("utf-8")) > MAX_CLOSE_REASON:
        raise ValueError(f"close reason longer than {MAX_CLOSE_REASON} UTF-8 bytes")


class WebSocketClosed(Exception):
    """The connection is closed.

    `received` is True when the peer's close frame arrived; `code` and `reason` are then its
    values (1005 when it carried no code). Otherwise `code` is 1006 for a transport that died
    without a close frame, or the code this side failed the connection with (1002, 1007, 1009).
    """

    code: int
    reason: str
    received: bool

    def __init__(self, code: int, reason: str = "", received: bool = False) -> None:
        super().__init__(f"WebSocket closed: {code} {reason}".rstrip())
        self.code = code
        self.reason = reason
        self.received = received


class InvalidHandshake(OSError):
    """A response to the upgrade that is not a valid RFC 6455 101 (bad accept key, extensions)."""


class UnexpectedSubprotocol(Exception):
    """The 101 echoed a subprotocol the client did not offer, or several.

    Deliberately not an `OSError`: like a missing echo, this is fatal UNSUPPORTED, never
    retried (WIRE.md section 2.1). `subprotocol` is the echoed value as received.
    """

    def __init__(self, subprotocol: str | None) -> None:
        super().__init__(f"server selected a subprotocol that was not offered: {subprotocol!r}")
        self.subprotocol = subprotocol


def build_headers(
    token: str | None,
    user_agent: str,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """The upgrade's extra request headers: Authorization, User-Agent, then `extra`.

    An `extra` entry replaces an earlier header of the same name (case-insensitively). Headers
    the handshake itself owns (Host, Upgrade, Connection, Sec-WebSocket-*) are refused, as are
    names and values that cannot go on the wire; error messages never include values.
    """
    out: dict[str, str] = {}
    if token is not None:
        out["Authorization"] = f"Bearer {token}"
    out["User-Agent"] = user_agent
    for name, value in (extra or {}).items():
        lower = name.lower()
        if lower in _RESERVED_HEADERS or lower.startswith("sec-websocket-"):
            raise ValueError(f"header {name!r} is set by the WebSocket handshake")
        for existing in [k for k in out if k.lower() == lower]:
            del out[existing]
        out[name] = value
    for name, value in out.items():
        if not is_token(name):
            raise ValueError(f"invalid header name {name!r}")
        if not is_field_value(value):
            raise ValueError(f"invalid value for header {name!r}")
    return out
