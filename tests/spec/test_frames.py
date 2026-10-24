"""Every spec/fixtures/frames/*.json case through decode_frame (spec/README.md, "Consuming")."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ws_mixer.errors import ConnError, WsMixerError, code_name, parse_error_code
from ws_mixer.frame import (
    decode_frame,
    frame_type_name,
    reset_code,
    reset_message,
    window_increment,
)

# Fixture count at spec.pin v0.8.0; bump together with the pin.
EXPECTED_FRAME_FIXTURES = 29

# Informational keys in `decoded` that describe the input rather than the decoded frame.
INFORMATIONAL = {"reserved_ignored", "message_sanitized", "policy_violation"}


def test_frame_fixture_count(spec_fixtures: Path) -> None:
    assert len(list((spec_fixtures / "frames").glob("*.json"))) == EXPECTED_FRAME_FIXTURES


def _decoded_view(frame_hex: str, keys: set[str]) -> dict[str, Any]:
    frame = decode_frame(bytes.fromhex(frame_hex))
    view: dict[str, Any] = {
        "type": frame_type_name(frame.type),
        "flags": frame.flags,
        "stream_id": frame.stream_id,
    }
    if "payload_hex" in keys:
        view["payload_hex"] = frame.payload.hex()
    if "payload_length" in keys:
        view["payload_length"] = len(frame.payload)
    if "increment" in keys:
        view["increment"] = window_increment(frame)
    if "code" in keys:
        view["code"] = reset_code(frame)
    if "message" in keys:
        view["message"] = reset_message(frame)
    return view


def test_frame_fixture(spec_dir: Path, frame_case: Path) -> None:
    fx = json.loads(frame_case.read_text(encoding="utf-8"))
    if fx["valid"]:
        expected = {k: v for k, v in fx["decoded"].items() if k not in INFORMATIONAL}
        unknown = set(expected) - {
            "type",
            "flags",
            "stream_id",
            "payload_hex",
            "payload_length",
            "increment",
            "code",
            "message",
        }
        assert not unknown, f"fixture has decoded keys this runner does not know: {unknown}"
        assert _decoded_view(fx["hex"], set(expected)) == expected
    else:
        with pytest.raises(WsMixerError) as info:
            decode_frame(bytes.fromhex(fx["hex"]))
        want = fx["expect"]
        assert parse_error_code(want["error_code"]) is not None
        assert code_name(info.value.code) == want["error_code"]
        assert isinstance(info.value, ConnError) == want["connection_fatal"]
