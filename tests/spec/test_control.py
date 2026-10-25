"""Every spec/fixtures/control/**/*.json case through parse_control and both schemas.

parse_control must agree with `wire_valid`; as a cross-check, the strict control.schema.json must
agree with `schema_valid` and control.relaxed.schema.json with `wire_valid` (spec/README.md).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from ws_mixer.control import parse_control
from ws_mixer.errors import ConnError, code_name

# Fixture count at spec.pin v0.8.2; bump together with the pin.
EXPECTED_CONTROL_FIXTURES = 51
MESSAGE_TYPES = {"hello", "welcome", "ping", "pong", "drain", "error", "app", "envelope"}


def _validator(spec_dir: Path, name: str) -> Draft202012Validator:
    schema = json.loads((spec_dir / name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


@pytest.fixture(scope="module")
def strict(spec_dir: Path) -> Draft202012Validator:
    return _validator(spec_dir, "control.schema.json")


@pytest.fixture(scope="module")
def relaxed(spec_dir: Path) -> Draft202012Validator:
    return _validator(spec_dir, "control.relaxed.schema.json")


def test_control_fixture_count(spec_fixtures: Path) -> None:
    files = list((spec_fixtures / "control").glob("**/*.json"))
    assert len(files) == EXPECTED_CONTROL_FIXTURES
    assert {p.parent.parent.name for p in files} == MESSAGE_TYPES


def test_control_fixture(
    spec_dir: Path,
    control_case: Path,
    strict: Draft202012Validator,
    relaxed: Draft202012Validator,
) -> None:
    fx: dict[str, Any] = json.loads(control_case.read_text(encoding="utf-8"))
    raw: str = fx["raw"] if "raw" in fx else json.dumps(fx["message"], ensure_ascii=False)

    if fx["wire_valid"]:
        parse_control(raw)
        parse_control(raw.encode("utf-8"))
    else:
        expect = fx.get("expect", {})
        hint = expect.get("error_code")
        for payload in (raw, raw.encode("utf-8")):
            with pytest.raises(ConnError) as info:
                parse_control(payload)
            if hint is not None and not hint.startswith("SCHEMA_"):
                assert code_name(info.value.code) == hint, str(info.value)
                if "message_contains" in expect:
                    assert expect["message_contains"] in info.value.message

    if "message" in fx:
        message = fx["message"]
        assert strict.is_valid(message) == fx["schema_valid"]
        assert relaxed.is_valid(message) == fx["wire_valid"]
    else:
        # malformed_json: never parses, so neither schema can see it.
        assert fx["schema_valid"] is False
        assert fx["wire_valid"] is False
