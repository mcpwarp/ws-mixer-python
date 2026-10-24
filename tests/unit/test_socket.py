from __future__ import annotations

import pytest

from ws_mixer.socket import (
    InvalidHandshake,
    UnexpectedSubprotocol,
    UpgradeRejected,
    WebSocketClosed,
    WebSocketInit,
    build_headers,
    check_close_args,
    parse_retry_after,
)


def test_build_headers_order_and_override() -> None:
    h = build_headers("tok", "ws-mixer-py/0.1.0 (python/3.11.9 linux/x86_64)", {"X-A": "1"})
    assert list(h) == ["Authorization", "User-Agent", "X-A"]
    assert h["Authorization"] == "Bearer tok"
    h = build_headers("tok", "ua", {"user-agent": "mine", "x-b": "2"})
    assert h == {"Authorization": "Bearer tok", "user-agent": "mine", "x-b": "2"}


def test_build_headers_without_token() -> None:
    assert build_headers(None, "ua", None) == {"User-Agent": "ua"}


@pytest.mark.parametrize(
    "extra",
    [
        {"Host": "x"},
        {"sec-websocket-protocol": "x"},
        {"Connection": "close"},
        {"Bad Name": "x"},
        {"X-Ok": "a\r\nInjected: 1"},
    ],
)
def test_build_headers_rejects(extra: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        build_headers(None, "ua", extra)


def test_build_headers_error_never_echoes_token() -> None:
    with pytest.raises(ValueError) as ei:
        build_headers("sekrit\nx", "ua", None)
    assert "sekrit" not in str(ei.value)


@pytest.mark.parametrize(
    ("value", "now", "expected"),
    [
        ("0", None, 0),
        ("120", None, 120_000),
        (" 7 ", None, 7_000),
        ("1.5", None, 1_500),
        (".5", None, 500),
        ("1e3", None, 1_000_000),
        ("0x10", None, 16_000),
        ("-1", None, 0),
        ("-5", None, 0),
        ("", None, 0),
        ("  ", None, 0),
        ("99999", None, 99_999_000),
        ("99999999", None, 2**31 - 1),
        ("Wed, 21 Oct 2015 07:28:00 GMT", 1_445_412_470_000, 10_000),
        ("Wed, 21 Oct 2015 07:28:00 GMT", 1_445_412_490_000, 0),
        ("Wednesday, 21-Oct-15 07:28:00 GMT", 1_445_412_470_000, 10_000),
        ("Wed, 21 Oct 2015 07:28:00 GMT", 0, 2**31 - 1),
        ("1e400", None, None),
        ("Infinity", None, None),
        ("+0x10", None, None),
        ("soon", None, None),
        (None, None, None),
    ],
)
def test_parse_retry_after(value: str | None, now: float | None, expected: float | None) -> None:
    assert parse_retry_after(value, now) == expected


def test_upgrade_rejected_lowercases_and_parses() -> None:
    e = UpgradeRejected(429, {"Retry-After": "3", "X-Thing": "y"})
    assert e.status == 429
    assert dict(e.headers) == {"retry-after": "3", "x-thing": "y"}
    assert e.retry_after_ms() == 3000
    assert UpgradeRejected(500).retry_after_ms() is None
    assert "429" in str(e)


def test_websocket_closed_fields() -> None:
    e = WebSocketClosed(1000, "bye", received=True)
    assert (e.code, e.reason, e.received) == (1000, "bye", True)
    assert (WebSocketClosed(1006).reason, WebSocketClosed(1006).received) == ("", False)


@pytest.mark.parametrize(
    ("code", "reason"), [(1000, ""), (1001, "x"), (3000, ""), (4999, "é" * 61)]
)
def test_check_close_args_accepts(code: int, reason: str) -> None:
    check_close_args(code, reason)


@pytest.mark.parametrize(
    ("code", "reason"),
    [(999, ""), (1002, ""), (1005, ""), (1006, ""), (1009, ""), (2999, ""), (5000, ""),
     (1000, "x" * 124), (4000, "é" * 62)],
)  # fmt: skip
def test_check_close_args_rejects(code: int, reason: str) -> None:
    with pytest.raises(ValueError):
        check_close_args(code, reason)


def test_unexpected_subprotocol_is_not_retryable() -> None:
    e = UnexpectedSubprotocol("evil")
    assert e.subprotocol == "evil"
    assert not isinstance(e, OSError)
    assert issubclass(InvalidHandshake, OSError)


def test_package_exports_the_seam() -> None:
    import ws_mixer

    for name in (
        "StdlibConnector", "WebSocketInit", "WebSocketConnector", "WebSocketConnection",
        "UpgradeRejected", "WebSocketClosed", "UnexpectedSubprotocol", "InvalidHandshake",
        "Clock", "TimerHandle", "LoopClock", "default_ssl_context", "SUBPROTOCOL",
    ):  # fmt: skip
        assert name in ws_mixer.__all__
        assert getattr(ws_mixer, name) is not None


def test_websocket_init_defaults() -> None:
    init = WebSocketInit()
    assert init.protocols == ("ws-mixer.v1",)
    assert init.max_message == 65_544
    assert dict(init.headers) == {}
