from __future__ import annotations

import os
import struct

import pytest

from ws_mixer._rfc6455 import _parse_url, accept_key, apply_mask, encode_frame


def test_mask_matches_bytewise_xor() -> None:
    key = b"\x01\x80\xff\x37"
    for n in (1, 3, 4, 5, 1001, 65_544):
        data = os.urandom(n)
        assert apply_mask(data, key) == bytes(b ^ key[i % 4] for i, b in enumerate(data))
    assert apply_mask(b"\x00\x00\x00", key) == key[:3]
    assert apply_mask(b"\x00" * 8, b"\x00\x00\x00\x01") == b"\x00\x00\x00\x01" * 2
    assert apply_mask(b"", key) == b""


def test_accept_key_rfc_example() -> None:
    assert accept_key("dGhlIHNhbXBsZSBub25jZQ==") == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="


def test_encode_frame_lengths() -> None:
    k = b"abcd"
    assert encode_frame(2, b"", k) == b"\x82\x80abcd"
    assert encode_frame(2, b"x" * 125, k)[:2] == b"\x82\xfd"
    assert encode_frame(2, b"x" * 126, k)[:4] == b"\x82\xfe\x00\x7e"
    assert encode_frame(2, b"x" * 65_535, k)[:4] == b"\x82\xfe\xff\xff"
    assert encode_frame(2, b"x" * 65_536, k)[:10] == b"\x82\xff" + struct.pack("!Q", 65_536)
    assert encode_frame(8, b"\x03\xe8", k)[6:] == apply_mask(b"\x03\xe8", k)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("ws://h/v1/tunnel", (False, "h", 80, "h", "/v1/tunnel")),
        ("wss://H.example/", (True, "h.example", 443, "h.example", "/")),
        ("wss://h:443", (True, "h", 443, "h", "/")),
        ("ws://h:8080/p?q=1", (False, "h", 8080, "h:8080", "/p?q=1")),
        ("ws://[::1]:9/x", (False, "::1", 9, "[::1]:9", "/x")),
        ("ws://h/café x?q=ü", (False, "h", 80, "h", "/caf%C3%A9%20x?q=%C3%BC")),
        ("ws://h/a%20b", (False, "h", 80, "h", "/a%20b")),
        (
            "WSS://bücher.example/",
            (True, "xn--bcher-kva.example", 443, "xn--bcher-kva.example", "/"),
        ),
    ],
)
def test_parse_url(url: str, expected: tuple[bool, str, int, str, str]) -> None:
    assert _parse_url(url) == expected


@pytest.mark.parametrize(
    "url", ["http://h/", "ws://h/#frag", "ws://u:p@h/", "ws:///path", "ws://h:99999/"]
)
def test_parse_url_rejects(url: str) -> None:
    with pytest.raises(ValueError):
        _parse_url(url)
