"""The request side of _http1 (MCPWARP.md sections 4.2-4.6): head parsing with every section 4.2
message, the 64 KiB head cap, body framing, the chunked decoder and its decoded-size cap, the
path rule, forwardable headers and the byte-exact error reply."""

from __future__ import annotations

import pytest

from ws_mixer._http1 import (
    MAX_CHUNK_LINE,
    MAX_HEAD_SIZE,
    NEED_MORE,
    ChunkedDecoder,
    RequestHead,
    RequestParseError,
    format_error_reply,
    forwardable_request_headers,
    is_authority,
    parse_request_head,
    percent_encode_target,
    reason_phrase,
    request_line_method,
    request_path,
)

HOST = "Host: notes-tester.mcpwarp.test\r\n"


def head(start: str = "POST /mcp HTTP/1.1", fields: str = HOST) -> bytes:
    return f"{start}\r\n{fields}\r\n".encode("latin-1")


def parsed(raw: bytes) -> RequestHead:
    result = parse_request_head(raw)
    assert isinstance(result, RequestHead)
    return result


def rejected(raw: bytes) -> RequestParseError:
    with pytest.raises(RequestParseError) as info:
        parse_request_head(raw)
    return info.value


def test_parses_a_request_head() -> None:
    raw = head(
        fields=HOST + "accept: application/json\r\nX-Two: a\r\nx-two: b\r\nContent-Length: 4\r\n"
    )
    h = parsed(raw + b"body")
    assert h.method == "POST"
    assert h.target == "/mcp"
    assert h.http_version == "1.1"
    assert h.host == "notes-tester.mcpwarp.test"
    assert h.headers == (
        ("Host", "notes-tester.mcpwarp.test"),
        ("accept", "application/json"),
        ("X-Two", "a"),
        ("x-two", "b"),
        ("Content-Length", "4"),
    )
    assert h.framing == 4
    assert h.consumed == len(raw)


@pytest.mark.parametrize(
    ("buf", "method"),
    [
        (b"HEAD /mcp HTTP/1.1\r\nHost: h", "HEAD"),
        (b"HEAD /mcp HTTP/1.1", None),  # no CRLF yet
        (b"HEAD /mcp HTTP/1.1 garbage\r\n", None),
        (b"HE@D /mcp HTTP/1.1\r\n", None),  # not a token
    ],
)
def test_request_line_method(buf: bytes, method: str | None) -> None:
    assert request_line_method(buf) == method


def test_http_1_0_and_optional_whitespace() -> None:
    h = parsed(head("GET /mcp HTTP/1.0", HOST + "a:   x y \t\r\n"))
    assert h.http_version == "1.0"
    assert ("a", "x y") in h.headers
    assert h.framing == "none"


def test_need_more_until_the_blank_line() -> None:
    raw = head()
    for n in range(len(raw)):
        assert parse_request_head(raw[:n]) is NEED_MORE
    assert isinstance(parse_request_head(raw), RequestHead)


def test_scan_from_finds_a_terminator_split_across_reads() -> None:
    raw = head()
    buf = bytearray()
    scanned = 0
    for i, byte in enumerate(raw):
        buf.append(byte)
        result = parse_request_head(buf, scan_from=scanned)
        if i < len(raw) - 1:
            assert result is NEED_MORE
            scanned = len(buf)
    assert isinstance(result, RequestHead) and result.consumed == len(raw)


# --- the 64 KiB cap includes the CRLFCRLF --------------------------------------------------


def test_head_of_exactly_64_kib_is_accepted() -> None:
    base = head(fields=HOST + "x-pad: \r\n")
    pad = MAX_HEAD_SIZE - len(base)
    raw = head(fields=HOST + "x-pad: " + "a" * pad + "\r\n")
    assert len(raw) == MAX_HEAD_SIZE
    assert parsed(raw).consumed == MAX_HEAD_SIZE


def test_head_one_byte_over_64_kib_is_431() -> None:
    base = head(fields=HOST + "x-pad: \r\n")
    raw = head(fields=HOST + "x-pad: " + "a" * (MAX_HEAD_SIZE - len(base) + 1) + "\r\n")
    assert len(raw) == MAX_HEAD_SIZE + 1
    err = rejected(raw)
    assert (err.status, err.message) == (431, "request head too large")


def test_431_as_soon_as_64_kib_arrived_without_a_blank_line() -> None:
    assert parse_request_head(b"a" * (MAX_HEAD_SIZE - 1)) is NEED_MORE
    err = rejected(b"a" * MAX_HEAD_SIZE)
    assert (err.status, err.message) == (431, "request head too large")
    # A terminator past the cap does not rescue it.
    assert rejected(b"a" * MAX_HEAD_SIZE + b"\r\n\r\n").status == 431


# --- every 400 message ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (head("POST /mcp HTTP/1.2"), "malformed request line"),
        (head("POST  /mcp HTTP/1.1"), "malformed request line"),
        (head("POST /mcp HTTP/1.1 "), "malformed request line"),
        (head("PO(T /mcp HTTP/1.1"), "malformed request line"),
        (head("POST /mcp"), "malformed request line"),
        (head(""), "malformed request line"),
        (head("POST * HTTP/1.1"), "unsupported request target form"),
        (head("CONNECT h.example:443 HTTP/1.1"), "unsupported request target form"),
        (head("POST \\mcp HTTP/1.1"), "unsupported request target form"),
        (head("POST ftp://h.example/mcp HTTP/1.1"), "unsupported request target form"),
        (head("POST http:///mcp HTTP/1.1"), "malformed request target"),
        (head("POST http://h<x/mcp HTTP/1.1"), "malformed request target"),
        (head("POST http://h:99999/mcp HTTP/1.1"), "malformed request target"),
        (head(fields=""), "missing or duplicate Host header"),
        (head(fields="Host: \r\n"), "missing or duplicate Host header"),
        (head(fields=HOST + "host: other\r\n"), "missing or duplicate Host header"),
        (head(fields=HOST + " folded\r\n"), "obsolete header line folding"),
        (head(fields=HOST + "\tfolded\r\n"), "obsolete header line folding"),
        (head(fields=HOST + "no-colon\r\n"), "malformed header line"),
        (head(fields=HOST + ": no name\r\n"), "malformed header line"),
        (head(fields=HOST + "bad name: x\r\n"), 'invalid header name "bad name"'),
        (head(fields=HOST + "béd: x\r\n"), 'invalid header name "béd"'),
        (
            head(fields=HOST + "Transfer-Encoding: chunked\r\nContent-Length: 1\r\n"),
            "both Transfer-Encoding and Content-Length",
        ),
        (head(fields=HOST + "Transfer-Encoding: gzip\r\n"), "unsupported Transfer-Encoding"),
        (
            head(fields=HOST + "Transfer-Encoding: gzip, chunked\r\n"),
            "unsupported Transfer-Encoding",
        ),
        (
            head(fields=HOST + "Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n"),
            "unsupported Transfer-Encoding",
        ),
        (head(fields=HOST + "Content-Length: 5, 6\r\n"), "conflicting Content-Length values"),
        (
            head(fields=HOST + "Content-Length: 5\r\nContent-Length: 6\r\n"),
            "conflicting Content-Length values",
        ),
        (head(fields=HOST + "Content-Length: abc\r\n"), "malformed Content-Length"),
        (head(fields=HOST + "Content-Length: -1\r\n"), "malformed Content-Length"),
        (head(fields=HOST + "Content-Length: \r\n"), "malformed Content-Length"),
        (head(fields=HOST + "Content-Length: 9007199254740992\r\n"), "malformed Content-Length"),
    ],
)
def test_400_messages(raw: bytes, message: str) -> None:
    err = rejected(raw)
    assert (err.status, err.message) == (400, message)


def test_parse_error_carries_the_method_once_known() -> None:
    assert rejected(head("HEAD /mcp HTTP/1.1", "")).method == "HEAD"
    assert rejected(head("HE(D /mcp HTTP/1.1")).method is None


def test_framing() -> None:
    assert parsed(head(fields=HOST + "Transfer-Encoding:  Chunked \r\n")).framing == "chunked"
    assert parsed(head(fields=HOST + "Content-Length: 7, 7\r\n")).framing == 7
    assert parsed(head(fields=HOST + "content-length: 0\r\n")).framing == 0
    assert parsed(head()).framing == "none"


# --- targets -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw_target", "target"),
    [
        ("/mcp?x=1", "/mcp?x=1"),
        ("https://h.example/mcp?x=1", "/mcp?x=1"),
        ("HTTP://H.example:8080/mcp#frag", "/mcp"),
        ("http://user@h.example/mcp", "/mcp"),
        ("http://h.example", "/"),
        ("http://h.example?x", "/?x"),
        ("/a/../mcp", "/a/../mcp"),
        ("/m%41p", "/m%41p"),
        ("/mcp?a[]=1&c=%41", "/mcp?a[]=1&c=%41"),
        ("/café", "/caf%E9"),
        ("/a\x7fb", "/a%7Fb"),
        ("/mcp#frag", "/mcp"),
        ("/mcp?x=1#frag#more", "/mcp?x=1"),
        ("https://h.example/mcp?x=1#f?g", "/mcp?x=1"),
    ],
)
def test_target_forms(raw_target: str, target: str) -> None:
    assert parsed(head(f"GET {raw_target} HTTP/1.1")).target == target


def test_percent_encode_target_is_ascii_once() -> None:
    assert percent_encode_target("/a b%20é€#") == "/a%20b%20%E9%E2%82%AC%23"


@pytest.mark.parametrize(
    ("raw", "encoded"),
    [
        ("aZ09-._~", "aZ09-._~"),
        ("!$&'()*+,;=", "!$&'()*+,;="),
        (":@/?", ":@/?"),
        ("[]", "[]"),
        ("a[]=1&c=%41", "a[]=1&c=%41"),
        ("%41", "%41"),
        ("%", "%"),
        ("%e9%zz%", "%e9%zz%"),
        ("é", "%E9"),
        ("cafÃ©", "caf%C3%A9"),
        (" ", "%20"),
        ("\x01", "%01"),
        ("\t", "%09"),
        ("\x7f", "%7F"),
        ('{}|^\\"`<>', "%7B%7D%7C%5E%5C%22%60%3C%3E"),
    ],
)
def test_percent_encode_target_allowed_set(raw: str, encoded: str) -> None:
    assert percent_encode_target(raw) == encoded
    assert percent_encode_target(encoded) == encoded


@pytest.mark.parametrize(
    ("target", "path"),
    [
        ("/mcp", "/mcp"),
        ("/mcp?x=1", "/mcp"),
        ("/mcp#f", "/mcp"),
        ("https://h.example/mcp?x=1", "/mcp"),
        ("https://h.example?x=1", "/"),
        ("/mcp/", "/mcp/"),
        ("/MCP", "/MCP"),
        ("/", "/"),
        ("/healthz", "/healthz"),
        ("/a/../mcp", "/a/../mcp"),
        ("\\mcp", "\\mcp"),
    ],
)
def test_request_path(target: str, path: str) -> None:
    assert request_path(target) == path


@pytest.mark.parametrize(
    ("value", "ok"),
    [
        ("notes-tester.mcpwarp.test", True),
        ("h.example:8443", True),
        ("h.example:", True),
        ("127.0.0.1", True),
        ("[::1]:80", True),
        ("hé.example", True),
        ("", False),
        ("a b", False),
        ("u@h", False),
        ("h/x", False),
        ("h:65536", False),
        ("h:x", False),
        ("h%41", False),
        ("[zz]", False),
    ],
)
def test_is_authority(value: str, ok: bool) -> None:
    assert is_authority(value) is ok


def test_forwardable_request_headers() -> None:
    fields = [
        ("Host", "h"),
        ("Connection", "keep-alive, X-Drop"),
        ("Keep-Alive", "timeout=5"),
        ("TE", "trailers"),
        ("Trailer", "x"),
        ("Upgrade", "h2c"),
        ("Proxy-Authorization", "x"),
        ("Proxy-Connection", "x"),
        ("Proxy-Authenticate", "x"),
        ("Transfer-Encoding", "chunked"),
        ("x-drop", "listed"),
        ("Expect", "100-continue"),
        ("Content-Length", "4"),
        ("mcp-session-id", "abc"),
        ("Accept", "a"),
    ]
    assert forwardable_request_headers(fields) == [
        ("Host", "h"),
        ("mcp-session-id", "abc"),
        ("Accept", "a"),
    ]


# --- chunked bodies ------------------------------------------------------------------------


def decode_all(raw: bytes, max_body: int | None = None, step: int = 0) -> tuple[bytes, bytes]:
    dec = ChunkedDecoder(max_body)
    parts: list[bytes] = []
    if step:
        for i in range(0, len(raw), step):
            parts += dec.feed(raw[i : i + step])
    else:
        parts = dec.feed(raw)
    assert dec.done
    return b"".join(parts), dec.leftover()


@pytest.mark.parametrize("step", [0, 1, 2, 3, 7])
def test_chunked_decoding_any_split(step: int) -> None:
    raw = b"5;name=value\r\nhello\r\n6 ; x\r\n world\r\n0\r\nx-trailer: 1\r\n\r\nNEXT"
    assert decode_all(raw, step=step) == (b"hello world", b"NEXT")


def test_chunked_not_done_until_the_trailer_section_ends() -> None:
    dec = ChunkedDecoder()
    assert dec.feed(b"3\r\nabc\r\n0\r\n") == [b"abc"]
    assert not dec.done
    assert dec.feed(b"t: 1\r\n") == []
    assert not dec.done
    dec.feed(b"\r\n")
    assert dec.done


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (b"zz\r\n", "malformed chunk size"),
        (b"\r\n", "malformed chunk size"),
        (b"0x5\r\n", "malformed chunk size"),
        (b"+5\r\n", "malformed chunk size"),
        (b"-1\r\n", "malformed chunk size"),
        (b"3\r\nabcX\r\n", "missing CRLF after chunk data"),
        (b"3\r\nabc\rX", "missing CRLF after chunk data"),
        (b"1" * (MAX_CHUNK_LINE + 2), "line too long"),
        (b"1;" + b"e" * MAX_CHUNK_LINE + b"\r\n", "line too long"),
        (b"0\r\n" + b"t" * (MAX_CHUNK_LINE + 1) + b"\r\n", "line too long"),
    ],
)
def test_chunked_errors(raw: bytes, message: str) -> None:
    with pytest.raises(RequestParseError) as info:
        ChunkedDecoder().feed(raw)
    assert (info.value.status, info.value.message) == (400, message)


def test_chunk_line_of_exactly_4096_bytes_is_fine() -> None:
    line = b"1;" + b"e" * (MAX_CHUNK_LINE - 2)
    assert len(line) == MAX_CHUNK_LINE
    assert decode_all(line + b"\r\na\r\n0\r\n\r\n") == (b"a", b"")


def test_chunked_cap_counts_decoded_bytes_not_framing() -> None:
    """Regression (JS port bug): the cap must count decoded body bytes, never chunk-size lines,
    extensions or CRLFs. Ten 1-byte chunks with 1 KiB extensions are a 10-byte body."""
    ext = b";ext=" + b"x" * 1024
    raw = b"".join(b"1" + ext + b"\r\n" + bytes([65 + i]) + b"\r\n" for i in range(10))
    raw += b"0" + ext + b"\r\n\r\n"
    assert len(raw) > 10_000
    body, _ = decode_all(raw, max_body=10, step=5)
    assert body == b"ABCDEFGHIJ"


def test_chunked_cap_trips_on_the_size_line_before_the_data() -> None:
    dec = ChunkedDecoder(max_body=10)
    assert dec.feed(b"6\r\nabcdef\r\n") == [b"abcdef"]
    with pytest.raises(RequestParseError) as info:
        dec.feed(b"5\r\n")  # no data yet: 6 + 5 > 10 is known from the size line
    assert (info.value.status, info.value.message) == (413, "request body too large")
    assert dec.total == 6


def test_chunked_cap_exactly_at_the_limit() -> None:
    assert decode_all(b"a\r\n0123456789\r\n0\r\n\r\n", max_body=10)[0] == b"0123456789"


# --- replies --------------------------------------------------------------------------------


def test_error_reply_is_byte_exact() -> None:
    assert format_error_reply(413, "request body too large", "POST") == (
        b"HTTP/1.1 413 Content Too Large\r\nContent-Type: application/json\r\n"
        b'Content-Length: 34\r\n\r\n{"error":"request body too large"}'
    )


def test_error_reply_to_head_keeps_content_length_without_body() -> None:
    assert format_error_reply(503, "service disabled", "HEAD") == (
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\n"
        b"Content-Length: 28\r\n\r\n"
    )


@pytest.mark.parametrize(
    ("status", "phrase"),
    [
        (413, "Content Too Large"),
        (431, "Request Header Fields Too Large"),
        (422, "Unprocessable Content"),
        (504, "Gateway Timeout"),
        # Table only: http.HTTPStatus differs across Python versions for these.
        (414, "URI Too Long"),
        (416, "Range Not Satisfiable"),
        (418, "Unknown"),
        (599, "Unknown"),
        (299, "Unknown"),
        (999, "Unknown"),
    ],
)
def test_reason_phrase(status: int, phrase: str) -> None:
    assert reason_phrase(status) == phrase
