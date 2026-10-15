from __future__ import annotations

import logging
import ssl
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from ws_mixer._tls import default_ssl_context


def _fake_certifi(monkeypatch: pytest.MonkeyPatch, where: str) -> None:
    mod = types.ModuleType("certifi")
    mod.where = lambda: where  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "certifi", mod)


def test_uses_certifi_when_importable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[dict[str, Any]] = []
    real = ssl.create_default_context

    def spy(*args: Any, **kwargs: Any) -> ssl.SSLContext:
        calls.append(kwargs)
        return real()

    _fake_certifi(monkeypatch, str(tmp_path / "cacert.pem"))
    monkeypatch.setattr(ssl, "create_default_context", spy)
    ctx = default_ssl_context()
    assert calls == [{"cafile": str(tmp_path / "cacert.pem")}]
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname


def test_falls_back_without_certifi(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "certifi", None)
    ctx = default_ssl_context()
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname


def test_warns_once_when_no_ca_certs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setitem(sys.modules, "certifi", None)
    real = ssl.create_default_context

    def empty(*args: Any, **kwargs: Any) -> ssl.SSLContext:
        ctx = real()
        monkeypatch.setattr(ctx.__class__, "get_ca_certs", lambda self, binary_form=False: [])
        return ctx

    monkeypatch.setattr(ssl, "create_default_context", empty)
    monkeypatch.setattr(
        ssl,
        "get_default_verify_paths",
        lambda: ssl.DefaultVerifyPaths("/dev/null/cert.pem", "/dev/null/certs", "", "", "", ""),
    )
    with caplog.at_level(logging.WARNING, logger="ws_mixer"):
        ctx = default_ssl_context()
    records = [r for r in caplog.records if r.name == "ws_mixer"]
    assert len(records) == 1
    assert "ssl_context=" in records[0].getMessage()
    assert "certifi" in records[0].getMessage()
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_no_warning_with_system_store(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    monkeypatch.setitem(sys.modules, "certifi", None)
    bundle = tmp_path / "ca.pem"
    bundle.write_text("")
    monkeypatch.setattr(
        ssl,
        "get_default_verify_paths",
        lambda: ssl.DefaultVerifyPaths(str(bundle), "", "", "", "", ""),
    )
    with caplog.at_level(logging.WARNING, logger="ws_mixer"):
        default_ssl_context()
    assert not [r for r in caplog.records if r.name == "ws_mixer"]
