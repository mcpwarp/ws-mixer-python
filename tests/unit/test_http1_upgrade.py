from __future__ import annotations

import pytest

from ws_mixer._http1 import (
    MAX_HEAD_SIZE,
    HttpParseError,
    format_upgrade_request,
    parse_response_head,
)


def test_format_upgrade_request() -> None:
    raw = format_upgrade_request(
        "edge.example:8443",
        "/v1/tunnel?x=1",
        "dGhlIHNhbXBsZSBub25jZQ==",
        ["ws-mixer.v1"],
        {"Authorization": "Bearer t", "User-Agent": "ua"},
    )
    assert raw == (
        b"GET /v1/tunnel?x=1 HTTP/1.1\r\n"
        b"Host: edge.example:8443\r\n"
        b"Upgrade: websocket\r\n"
        b"Connection: Upgrade\r\n"
        b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        b"Sec-WebSocket-Version: 13\r\n"
        b"Sec-WebSocket-Protocol: ws-mixer.v1\r\n"
        b"Authorization: Bearer t\r\n"
        b"User-Agent: ua\r\n"
        b"\r\n"
    )
    assert b"Origin" not in raw


def test_format_without_protocols() -> None:
    raw = format_upgrade_request("h", "/", "k", [], {})
    assert b"Sec-WebSocket-Protocol" not in raw


@pytest.mark.parametrize(
    ("path", "headers", "protocols"),
    [
        ("/x y", {}, []),
        ("noslash", {}, []),
        ("/", {"X": "a\r\nB: c"}, []),
        ("/", {"Bad Name": "v"}, []),
        ("/", {}, ["has space"]),
    ],
)
def test_format_rejects(path: str, headers: dict[str, str], protocols: list[str]) -> None:
    with pytest.raises(ValueError):
        format_upgrade_request("h", path, "k", protocols, headers)


def test_parse_response_head() -> None:
    buf = (
        b"HTTP/1.1 101 Switching Protocols\r\n"
        b"Upgrade: websocket\r\n"
        b"Set-Cookie: a=1\r\n"
        b"set-cookie: b=2\r\n"
        b"X-Empty:\r\n"
        b"X-Spaced: \t v \t\r\n"
        b"\r\n"
        b"\x82\x00trailing frame bytes"
    )
    status, reason, fields, consumed = parse_response_head(buf)
    assert (status, reason) == (101, "Switching Protocols")
    assert fields == {
        "upgrade": ["websocket"],
        "set-cookie": ["a=1", "b=2"],
        "x-empty": [""],
        "x-spaced": ["v"],
    }
    assert buf[consumed:] == b"\x82\x00trailing frame bytes"


@pytest.mark.parametrize(
    ("line", "status", "reason"),
    [
        (b"HTTP/1.1 404 Not Found", 404, "Not Found"),
        (b"HTTP/1.0 429 ", 429, ""),
        (b"HTTP/1.1 302", 302, ""),
    ],
)
def test_parse_status_lines(line: bytes, status: int, reason: str) -> None:
    assert parse_response_head(line + b"\r\n\r\n")[:2] == (status, reason)


@pytest.mark.parametrize(
    "buf",
    [
        b"HTTP/1.1 101 OK\r\nX: a\r\n b\r\n\r\n",
        b"HTTP/1.1 101 OK\r\nX: a\r\n\tb\r\n\r\n",
        b"HTTP/1.1 101 OK\nX: a\r\n\r\n",
        b"HTTP/1.1 101 OK\r\nX: a\rY\r\n\r\n",
        b"HTTP/1.1 101 OK\r\nX : a\r\n\r\n",
        b"HTTP/1.1 101 OK\r\nNoColon\r\n\r\n",
        b"HTTP/1.1 101 OK\r\nX: a\x00b\r\n\r\n",
        b"HTTP/2 101 OK\r\n\r\n",
        b"HTTP/1.1 1010 OK\r\n\r\n",
        b"ICY 200 OK\r\n\r\n",
        b"HTTP/1.1 101 OK\r\n",
    ],
)
def test_parse_rejects(buf: bytes) -> None:
    with pytest.raises(HttpParseError):
        parse_response_head(buf)


def test_head_cap_includes_terminator() -> None:
    start = b"HTTP/1.1 200 OK\r\nX: "
    fill = MAX_HEAD_SIZE - len(start) - 4
    exact = start + b"a" * fill + b"\r\n\r\n"
    assert len(exact) == MAX_HEAD_SIZE
    assert parse_response_head(exact)[3] == MAX_HEAD_SIZE
    with pytest.raises(HttpParseError, match="too large"):
        parse_response_head(start + b"a" * (fill + 1) + b"\r\n\r\n")
    with pytest.raises(HttpParseError, match="too large"):
        parse_response_head(start + b"a" * MAX_HEAD_SIZE)
