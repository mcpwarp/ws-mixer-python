"""HTTP/1.1 message heads (RFC 9112), strict: CRLF line endings only, no obs-fold, 64 KiB cap.

Shared by the WebSocket upgrade and mcpwarp request bridging. Response heads are split by
`_split_head` and their field lines parsed by `_parse_fields`: names come back lowercased with
every value kept in order; values are decoded as Latin-1 (RFC 9110 section 5.5).

Request heads (`parse_request_head`) follow MCPWARP.md section 4.2 instead: each violation maps
to a fixed reply and message, fields keep their order and spelling, and the head is parsed from
a growing buffer (`NEED_MORE` until it is complete).
"""

from __future__ import annotations

import enum
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

MAX_HEAD_SIZE = 65_536
"""Bytes in a head, its terminating CRLFCRLF included."""

_TCHAR = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_FIELD_VALUE_BAD = re.compile(r"[\x00-\x08\x0a-\x1f\x7f]")
_STATUS_LINE = re.compile(r"^HTTP/1\.([01]) ([0-9]{3})(?: ([^\x00-\x08\x0a-\x1f\x7f]*))?$")
_OWS = " \t"


class HttpParseError(ValueError):
    """A malformed or oversized HTTP head."""


def is_token(name: str) -> bool:
    return bool(_TCHAR.match(name))


def is_field_value(value: str) -> bool:
    """True when `value` can go on the wire: no CR, LF, NUL or other controls except HTAB."""
    return not _FIELD_VALUE_BAD.search(value) and value == value.strip(_OWS)


def _split_head(buf: bytes) -> tuple[list[str], int]:
    end = buf.find(b"\r\n\r\n")
    if end < 0:
        if len(buf) >= MAX_HEAD_SIZE:
            raise HttpParseError("head too large")
        raise HttpParseError("incomplete head")
    consumed = end + 4
    if consumed > MAX_HEAD_SIZE:
        raise HttpParseError("head too large")
    raw = buf[:end]
    if b"\n" in raw.replace(b"\r\n", b"") or b"\r" in raw.replace(b"\r\n", b""):
        raise HttpParseError("bare CR or LF in head")
    return raw.decode("latin-1").split("\r\n"), consumed


def _parse_fields(lines: Iterable[str]) -> dict[str, list[str]]:
    fields: dict[str, list[str]] = {}
    for line in lines:
        if line[:1] in (" ", "\t"):
            raise HttpParseError("obsolete line folding")
        name, sep, value = line.partition(":")
        if not sep or not is_token(name):
            raise HttpParseError("malformed header field")
        value = value.strip(_OWS)
        if _FIELD_VALUE_BAD.search(value):
            raise HttpParseError("control character in header value")
        fields.setdefault(name.lower(), []).append(value)
    return fields


def parse_response_head(buf: bytes) -> tuple[int, str, dict[str, list[str]], int]:
    """(status, reason, fields, bytes consumed) for the response head at the start of `buf`."""
    lines, consumed = _split_head(buf)
    m = _STATUS_LINE.match(lines[0])
    if m is None:
        raise HttpParseError("malformed status line")
    return int(m.group(2)), m.group(3) or "", _parse_fields(lines[1:]), consumed


def format_upgrade_request(
    host: str,
    path: str,
    key: str,
    protocols: Iterable[str],
    headers: Mapping[str, str],
) -> bytes:
    """A WebSocket upgrade request (RFC 6455 section 4.1). `host` is the Host header value."""
    if not path.startswith("/") or any(c in path for c in " \r\n\x00"):
        raise ValueError("invalid request target")
    lines = [
        f"GET {path} HTTP/1.1",
        f"Host: {host}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    protos = list(protocols)
    if protos:
        if not all(is_token(p) for p in protos):
            raise ValueError("invalid subprotocol")
        lines.append(f"Sec-WebSocket-Protocol: {', '.join(protos)}")
    for name, value in headers.items():
        if not is_token(name) or not is_field_value(value):
            raise ValueError(f"invalid header field {name!r}")
        lines.append(f"{name}: {value}")
    head = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")
    if len(head) > MAX_HEAD_SIZE:
        raise ValueError("request head too large")
    return head


# --- request side: mcpwarp bridging (MCPWARP.md sections 4.2-4.6) ---------------------------

MAX_CHUNK_LINE = 4096
"""Bytes in one chunk-size or trailer line, CRLF excluded."""

_MAX_SAFE_INTEGER = 2**53 - 1

HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-connection",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)
"""Connection-scoped header names never forwarded across a hop (section 4.6)."""

# RFC 9110 section 15 phrases, plus RFC 6585's 429 and 431. Deliberately not http.HTTPStatus,
# whose phrases differ across Python versions (413, 414, 416, 422).
_REASONS = MappingProxyType(
    {
        100: "Continue",
        101: "Switching Protocols",
        200: "OK",
        201: "Created",
        202: "Accepted",
        203: "Non-Authoritative Information",
        204: "No Content",
        205: "Reset Content",
        206: "Partial Content",
        300: "Multiple Choices",
        301: "Moved Permanently",
        302: "Found",
        303: "See Other",
        304: "Not Modified",
        305: "Use Proxy",
        307: "Temporary Redirect",
        308: "Permanent Redirect",
        400: "Bad Request",
        401: "Unauthorized",
        402: "Payment Required",
        403: "Forbidden",
        404: "Not Found",
        405: "Method Not Allowed",
        406: "Not Acceptable",
        407: "Proxy Authentication Required",
        408: "Request Timeout",
        409: "Conflict",
        410: "Gone",
        411: "Length Required",
        412: "Precondition Failed",
        413: "Content Too Large",
        414: "URI Too Long",
        415: "Unsupported Media Type",
        416: "Range Not Satisfiable",
        417: "Expectation Failed",
        421: "Misdirected Request",
        422: "Unprocessable Content",
        426: "Upgrade Required",
        429: "Too Many Requests",
        431: "Request Header Fields Too Large",
        500: "Internal Server Error",
        501: "Not Implemented",
        502: "Bad Gateway",
        503: "Service Unavailable",
        504: "Gateway Timeout",
        505: "HTTP Version Not Supported",
    }
)

_REQUEST_LINE = re.compile(r"([^ ]+) ([^ ]+) HTTP/1\.([01])")
_ABSOLUTE_TARGET = re.compile(r"https?://[^/?#]*", re.IGNORECASE)
_SCHEME_AUTHORITY = re.compile(r"[a-z][a-z0-9+.-]*://[^/?#]*", re.IGNORECASE)
_AUTHORITY = re.compile(r"(\[[0-9A-Fa-f:.]+\]|[^\x00-\x20\x7f#/:<>?@\[\\\]^|%]+)(?::([0-9]*))?")
_DIGITS = re.compile(r"[0-9]+")
_HEX = re.compile(r"[0-9A-Fa-f]+")
# RFC 3986 unreserved, sub-delims, ":" "@" (pchar), "/" "?" (query) and "%" (escapes are kept,
# never decoded). Any other byte of a request target is percent-encoded once; the fragment is cut
# off before encoding.
_TARGET_SAFE = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~!$&'()*+,;=:@/?%"
)
# What a fetch-style Headers object refuses in a value (and what could split a line on the wire).
_UNREPRESENTABLE_VALUE = re.compile(r"[\x00\r\n]")


def reason_phrase(status: int) -> str:
    """The RFC 9110 reason phrase (413 is "Content Too Large"), else "Unknown"."""
    return _REASONS.get(status, "Unknown")


def is_bodyless(method: str, status: int) -> bool:
    """Responses to HEAD, and 1xx/204/304 responses, never carry a body."""
    return method == "HEAD" or status < 200 or status in (204, 304)


def format_error_reply(status: int, message: str, method: str) -> bytes:
    """The SDK's own reply (section 4.5), byte-exact; to HEAD the same head and no body."""
    body = json.dumps({"error": message}, ensure_ascii=False, separators=(",", ":")).encode()
    head = (
        f"HTTP/1.1 {status} {reason_phrase(status)}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n\r\n"
    ).encode("latin-1")
    return head if is_bodyless(method, status) else head + body


class RequestParseError(HttpParseError):
    """A request the agent answers itself: `status` 400, 413 or 431 with `message` as the
    `{"error": ...}` text. `method` is the request's method once the request line parsed."""

    def __init__(self, status: int, message: str, method: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.message = message
        self.method = method


class NeedMore(enum.Enum):
    NEED_MORE = "need more"


NEED_MORE = NeedMore.NEED_MORE
"""`parse_request_head`'s answer while the head is still incomplete."""

Framing = Literal["none", "chunked"] | int
"""How a request body is framed: not at all, chunked, or by this Content-Length."""


@dataclass(frozen=True)
class RequestHead:
    method: str
    target: str
    """Origin-form path and query; absolute-form is cut to that (not normalised); any fragment
    is dropped; bytes outside the RFC 3986 set are percent-encoded, so it is always ASCII."""
    http_version: Literal["1.0", "1.1"]
    host: str
    headers: tuple[tuple[str, str], ...]
    """Every field line in order, names as received, values with OWS trimmed."""
    framing: Framing
    consumed: int
    """Bytes of the buffer the head took, its CRLFCRLF included; the body starts here."""


def parse_request_head(buf: bytes | bytearray, *, scan_from: int = 0) -> RequestHead | NeedMore:
    """Parses the request head at the start of `buf` (section 4.2).

    Returns `NEED_MORE` until the CRLFCRLF arrives. Raises `RequestParseError`: 431 once the
    head cannot end within 64 KiB, else 400 with the section 4.2 message. At EOF with
    `NEED_MORE` the caller answers 400 "incomplete request". A caller growing `buf` passes the
    length it had at the previous `NEED_MORE` as `scan_from`, so only new bytes (and the 3
    before them) are searched for the terminator.
    """
    end = buf.find(b"\r\n\r\n", max(0, scan_from - 3), MAX_HEAD_SIZE)
    if end < 0:
        if len(buf) >= MAX_HEAD_SIZE:
            raise RequestParseError(431, "request head too large")
        return NEED_MORE
    lines = bytes(buf[:end]).decode("latin-1").split("\r\n")
    m = _REQUEST_LINE.fullmatch(lines[0])
    if m is None or not is_token(m.group(1)):
        raise RequestParseError(400, "malformed request line")
    method = m.group(1)
    target = percent_encode_target(_parse_target(m.group(2), method))
    fields = _parse_request_fields(lines[1:], method)
    hosts = [v for n, v in fields if n.lower() == "host"]
    if len(hosts) != 1 or not hosts[0]:
        raise RequestParseError(400, "missing or duplicate Host header", method)
    return RequestHead(
        method=method,
        target=target,
        http_version="1.1" if m.group(3) == "1" else "1.0",
        host=hosts[0],
        headers=fields,
        framing=_body_framing(fields, method),
        consumed=end + 4,
    )


def _parse_target(raw: str, method: str) -> str:
    """Origin-form or absolute-form, as path and query with any fragment cut off."""
    if raw.startswith("/"):
        return raw.split("#", 1)[0]
    m = _ABSOLUTE_TARGET.match(raw)
    if m is None:
        raise RequestParseError(400, "unsupported request target form", method)
    authority = m.group(0).split("://", 1)[1].rpartition("@")[2]
    if not is_authority(authority):
        raise RequestParseError(400, "malformed request target", method)
    rest = raw[m.end() :].split("#", 1)[0]
    return rest if rest.startswith("/") else "/" + rest


def percent_encode_target(target: str) -> str:
    """`target` with every char outside the RFC 3986 set percent-encoded, once: a char up to
    U+00FF stands for that byte (a Latin-1-decoded head), anything above is encoded as UTF-8."""
    out: list[str] = []
    for c in target:
        if c in _TARGET_SAFE:
            out.append(c)
        else:
            raw = c.encode("utf-8") if ord(c) > 0xFF else bytes([ord(c)])
            out.extend(f"%{b:02X}" for b in raw)
    return "".join(out)


def _parse_request_fields(lines: Iterable[str], method: str) -> tuple[tuple[str, str], ...]:
    out: list[tuple[str, str]] = []
    for line in lines:
        if line[:1] in (" ", "\t"):
            raise RequestParseError(400, "obsolete header line folding", method)
        colon = line.find(":")
        if colon <= 0:
            raise RequestParseError(400, "malformed header line", method)
        name = line[:colon]
        if not is_token(name):
            quoted = json.dumps(name, ensure_ascii=False)
            raise RequestParseError(400, f"invalid header name {quoted}", method)
        out.append((name, line[colon + 1 :].strip(_OWS)))
    return tuple(out)


def _body_framing(fields: Iterable[tuple[str, str]], method: str) -> Framing:
    te = [v for n, v in fields if n.lower() == "transfer-encoding"]
    cl = [v for n, v in fields if n.lower() == "content-length"]
    if te and cl:
        raise RequestParseError(400, "both Transfer-Encoding and Content-Length", method)
    if te:
        if len(te) != 1 or te[0].strip(_OWS).lower() != "chunked":
            raise RequestParseError(400, "unsupported Transfer-Encoding", method)
        return "chunked"
    if cl:
        values = [part.strip(_OWS) for v in cl for part in v.split(",")]
        if not all(_DIGITS.fullmatch(v) for v in values):
            raise RequestParseError(400, "malformed Content-Length", method)
        if len(set(values)) != 1:
            raise RequestParseError(400, "conflicting Content-Length values", method)
        length = int(values[0])
        if length > _MAX_SAFE_INTEGER:
            raise RequestParseError(400, "malformed Content-Length", method)
        return length
    return "none"


def is_authority(value: str) -> bool:
    """`host[:port]` as a URL authority without userinfo: a reg-name or IPv4 address (no
    whitespace, controls or URL delimiters) or a bracketed IPv6 literal, port at most 65535."""
    m = _AUTHORITY.fullmatch(value)
    if m is None:
        return False
    port = m.group(2)
    return not port or int(port) <= 65_535


def has_unrepresentable_value(fields: Iterable[tuple[str, str]]) -> bool:
    """True when a field value holds NUL, CR or LF, which a web Request cannot carry."""
    return any(_UNREPRESENTABLE_VALUE.search(v) for _, v in fields)


def request_path(target: str) -> str:
    """Section 4.3's path of a raw request target: scheme://authority stripped, cut at the first
    "?" or "#", not normalised."""
    m = _SCHEME_AUTHORITY.match(target)
    rest = target[m.end() :] if m else target
    cut = min((i for i in (rest.find("?"), rest.find("#")) if i >= 0), default=len(rest))
    path = rest[:cut]
    return "/" if m and not path else path


def connection_tokens(fields: Iterable[tuple[str, str]]) -> frozenset[str]:
    """The lowercased names a `Connection` header lists (hop-by-hop as well)."""
    out: set[str] = set()
    for name, value in fields:
        if name.lower() == "connection":
            out.update(t for t in (p.strip(_OWS).lower() for p in value.split(",")) if t)
    return frozenset(out)


def forwardable_request_headers(fields: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    """Section 4.6 request -> handler: minus hop-by-hop, Connection-listed names, `expect` and
    `content-length`. `host` stays."""
    pairs = list(fields)
    listed = connection_tokens(pairs)
    return [
        (n, v)
        for n, v in pairs
        if (low := n.lower()) not in HOP_BY_HOP
        and low not in listed
        and low not in ("expect", "content-length")
    ]


class ChunkedDecoder:
    """Incremental RFC 9112 section 7.1 decoder: feed raw bytes, get body bytes.

    Chunk extensions are ignored and trailers read and dropped. Chunk-size and trailer lines
    are capped at 4096 bytes. With `max_body`, the cumulative *decoded* size (declared chunk
    sizes, never the framing) is checked as each chunk-size line arrives, before its data is
    read: over the cap raises `RequestParseError(413)`. Malformed framing raises
    `RequestParseError(400)`.
    """

    def __init__(self, max_body: int | None = None) -> None:
        self._buf = bytearray()
        self._state: Literal["size", "data", "crlf", "trailer", "done"] = "size"
        self._remaining = 0
        self._max_body = max_body
        self.total = 0
        """Decoded body bytes announced so far."""

    @property
    def done(self) -> bool:
        """The last chunk and the trailer section have been read."""
        return self._state == "done"

    def leftover(self) -> bytes:
        """Bytes fed after the end of the chunked body."""
        return bytes(self._buf) if self._state == "done" else b""

    def feed(self, data: bytes) -> list[bytes]:
        self._buf += data
        out: list[bytes] = []
        while self._state != "done":
            if self._state == "size":
                line = self._line()
                if line is None:
                    break
                size_text = line.split(";", 1)[0].strip(_OWS)
                if not _HEX.fullmatch(size_text):
                    raise RequestParseError(400, "malformed chunk size")
                size = int(size_text, 16)
                if size == 0:
                    self._state = "trailer"
                    continue
                if self._max_body is not None and size > self._max_body - self.total:
                    raise RequestParseError(413, "request body too large")
                self.total += size
                self._remaining = size
                self._state = "data"
            elif self._state == "data":
                if not self._buf:
                    break
                n = min(self._remaining, len(self._buf))
                out.append(bytes(self._buf[:n]))
                del self._buf[:n]
                self._remaining -= n
                if self._remaining == 0:
                    self._state = "crlf"
            elif self._state == "crlf":
                if bytes(self._buf[:2]) not in (b"\r\n", b"\r", b""):
                    raise RequestParseError(400, "missing CRLF after chunk data")
                if len(self._buf) < 2:
                    break
                del self._buf[:2]
                self._state = "size"
            else:
                line = self._line()
                if line is None:
                    break
                if line == "":
                    self._state = "done"
        return out

    def _line(self) -> str | None:
        idx = self._buf.find(b"\r\n")
        if idx < 0:
            # A line of exactly MAX_CHUNK_LINE bytes may still be waiting for its CRLF.
            if len(self._buf) > MAX_CHUNK_LINE + 1:
                raise RequestParseError(400, "line too long")
            return None
        if idx > MAX_CHUNK_LINE:
            raise RequestParseError(400, "line too long")
        line = bytes(self._buf[:idx]).decode("latin-1")
        del self._buf[: idx + 2]
        return line
