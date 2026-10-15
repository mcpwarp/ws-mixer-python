"""Every spec/fixtures/mcpwarp/envelope/**/*.json case (MCPWARP.md section 10.2).

An edge->agent op decodes (with the section 2.3 defaults), fails with MALFORMED_ENVELOPE, or decodes
as Unknown (UNSUPPORTED_VERSION / UNKNOWN_OP). A valid register/unregister equals the encoder's
output, byte for byte on the wire. Invalid agent->edge cases carry the edge's verdict: the agent
side checks its own name rule and that the decoder ignores a v/op it does not know; SCHEMA_* ones
are schema-checked only. Every case is also run through mcpwarp.schema.json, which must agree
with `schema_valid`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from tests.conftest import _cases
from ws_mixer.control import encode_control
from ws_mixer.mcpwarp import (
    AppError,
    ConflictOp,
    Disable,
    Enable,
    InboundEnvelope,
    McpwarpError,
    Registered,
    RegisteredService,
    ServiceError,
    Unknown,
    Unregistered,
    UnregisteredService,
    decode_envelope,
    encode_register,
    encode_unregister,
    is_valid_service_name,
)

# Floor: the fixture count at spec.pin v0.6.1 (a later spec only adds fixtures).
MIN_ENVELOPE_FIXTURES = 60
AGENT_TO_EDGE = {"register", "unregister"}

_CASES = _cases("mcpwarp/envelope", "**/*.json")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "envelope_case" in metafunc.fixturenames:
        if _CASES:
            metafunc.parametrize(
                "envelope_case", [p for p, _ in _CASES], ids=[i for _, i in _CASES]
            )
        else:
            metafunc.parametrize("envelope_case", [None], ids=["spec-unavailable"])


@pytest.fixture(scope="module")
def schema(spec_dir: Path) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads((spec_dir / "mcpwarp.schema.json").read_text("utf-8"))
    Draft202012Validator.check_schema(doc)
    return doc


def _validator(schema: dict[str, Any], op: str | None = None) -> Draft202012Validator:
    if op is None:
        return Draft202012Validator(schema)
    # The op's own $defs entry, so a valid message is known to match the op the fixture names.
    sub = {k: v for k, v in schema.items() if k != "oneOf"} | {"$ref": f"#/$defs/{op}"}
    return Draft202012Validator(sub)


def _expected_decode(m: dict[str, Any]) -> InboundEnvelope:
    """What section 2.3's defaults make of a well-formed edge->agent message."""
    op = m["op"]
    if op == "registered":
        return Registered(
            services=[
                RegisteredService(s["name"], s["id"], s["url"], s.get("created") or False)
                for s in m.get("services") or []
            ],
            errors=[
                ServiceError(e["name"], e["code"], e.get("message", ""), e.get("id"))
                for e in m.get("errors") or []
            ],
        )
    if op == "unregistered":
        return Unregistered(
            [UnregisteredService(s["name"], s.get("id", "")) for s in m.get("services") or []]
        )
    if op == "disable":
        return Disable(m["id"], m.get("reason", "disabled"))
    if op == "enable":
        return Enable(m["id"], m["name"])
    if op == "conflict":
        return ConflictOp(m["id"], m["name"], m.get("reason", "taken_over"))
    if op == "error":
        return AppError(m["code"], m.get("message", ""))
    raise AssertionError(f"no expected decode for op {op!r}")


def test_envelope_fixture_count(spec_fixtures: Path) -> None:
    files = list((spec_fixtures / "mcpwarp" / "envelope").glob("**/*.json"))
    assert len(files) >= MIN_ENVELOPE_FIXTURES


def test_envelope_fixture(spec_fixtures: Path, envelope_case: Path, schema: dict[str, Any]) -> None:
    fx: dict[str, Any] = json.loads(envelope_case.read_text(encoding="utf-8"))
    op: str = fx["op"]
    m: dict[str, Any] = fx["message"]
    hint: str | None = fx.get("expect", {}).get("error_code")

    assert fx["schema_valid"] == (envelope_case.parent.name == "valid")
    assert _validator(schema).is_valid(m) == fx["schema_valid"]
    if fx["schema_valid"]:
        assert _validator(schema, op).is_valid(m)

    if op in AGENT_TO_EDGE:
        if fx["wire_valid"] and fx["schema_valid"]:
            svc = m["services"][0]
            encoded = (
                encode_register(svc["name"], svc["kind"])
                if op == "register"
                else encode_unregister(svc["name"])
            )
            assert encoded["mcpwarp"] == m
            # Byte-exact on the wire, in the fixture's key order (section 2.2).
            body = json.dumps({"mcpwarp": m}, separators=(",", ":"), ensure_ascii=False)
            wire = encode_control({"t": "app", "body": encoded})
            assert wire == ('{"t":"app","body":' + body + "}").encode("utf-8")
            assert is_valid_service_name(svc["name"])
        elif hint == "INVALID_NAME":
            assert not is_valid_service_name(m["services"][0]["name"])
        elif hint in ("UNSUPPORTED_VERSION", "UNKNOWN_OP"):
            assert decode_envelope({"mcpwarp": m}) == Unknown(raw_op=m.get("op"), v=m.get("v"))
        # Otherwise (SCHEMA_* hints) the verdict is the edge's: the schema check above is all.
        return

    if fx["wire_valid"]:
        assert hint is None or hint.startswith("SCHEMA_"), hint
        assert decode_envelope({"mcpwarp": m}) == _expected_decode(m)
        return
    if hint in ("UNSUPPORTED_VERSION", "UNKNOWN_OP"):
        assert decode_envelope({"mcpwarp": m}) == Unknown(raw_op=m.get("op"), v=m.get("v"))
        return
    assert hint == "MALFORMED_ENVELOPE"
    with pytest.raises(McpwarpError) as info:
        decode_envelope({"mcpwarp": m})
    assert info.value.code == "MALFORMED_ENVELOPE"
    contains = fx["expect"].get("message_contains")
    if contains:
        assert contains in str(info.value)
