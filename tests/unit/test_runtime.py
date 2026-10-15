from __future__ import annotations

import platform
import sys

import pytest

from ws_mixer._runtime import detect_os, detect_runtime, user_agent


def test_detect_runtime() -> None:
    assert detect_runtime() == f"python/{platform.python_version()}"


def test_detect_os_shape() -> None:
    os_ = detect_os()
    assert os_ == f"{sys.platform}/{platform.machine()}".lower()
    assert len(os_) <= 64


def test_detect_os_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platform, "machine", lambda: "")
    assert detect_os() == f"{sys.platform}/unknown"
    monkeypatch.setattr(platform, "machine", lambda: "arm 64")
    assert detect_os() == "unknown"
    monkeypatch.setattr(platform, "machine", lambda: "x" * 100)
    assert len(detect_os()) == 64

    def boom() -> str:
        raise OSError

    monkeypatch.setattr(platform, "machine", boom)
    assert detect_os() == "unknown"


def test_user_agent() -> None:
    assert (
        user_agent("ws-mixer-py", "0.1.0", "python/3.13.5", "darwin/arm64")
        == "ws-mixer-py/0.1.0 (python/3.13.5 darwin/arm64)"
    )
