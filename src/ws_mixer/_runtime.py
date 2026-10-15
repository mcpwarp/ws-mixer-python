"""Runtime and OS identity for `hello.agent` and the User-Agent header, computed at call time."""

from __future__ import annotations

import platform
import sys

_MAX_FIELD = 64


def _clean(value: str) -> str:
    if not value or not all("!" <= c <= "~" for c in value):
        return "unknown"
    return value[:_MAX_FIELD]


def detect_runtime() -> str:
    """e.g. "python/3.13.5"."""
    try:
        return _clean(f"python/{platform.python_version()}")
    except Exception:
        return "unknown"


def detect_os() -> str:
    """e.g. "darwin/arm64", "linux/x86_64", "win32/amd64"; "unknown" when undeterminable."""
    try:
        machine = platform.machine() or "unknown"
        return _clean(f"{sys.platform}/{machine}".lower())
    except Exception:
        return "unknown"


def user_agent(sdk: str, version: str, runtime: str, os: str) -> str:
    """`<sdk>/<version> (<runtime> <os>)`, e.g. "ws-mixer-py/0.1.0 (python/3.13.5 darwin/arm64)"."""
    return f"{sdk}/{version} ({runtime} {os})"
