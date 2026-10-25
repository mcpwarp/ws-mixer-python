"""Every spec/fixtures/mcpwarp/envelope/**/*.json case (MCPWARP.md section 10.2).

An edge->agent op decodes (with the section 2.3 defaults), fails with MALFORMED_ENVELOPE, or decodes
as Unknown (UNSUPPORTED_VERSION / UNKNOWN_OP). A valid register/unregister equals the encoder's
output, byte for byte on the wire. Invalid agent->edge cases carry the edge's verdict: the agent
side checks its own name rule and that the decoder ignores a v/op it does not know. Every other
case is also run through mcpwarp.schema.json, which must agree with `schema_valid`.

Skipped (MCPWARP.md section 10.6, as clarified for v0.8.1): the encoder comparison of the two
multi-entry `register` fixtures is host guidance, and an agent->edge case whose only verdict is
the edge's schema check (SCHEMA_* and TOO_MANY_SERVICES hints) is checker/CI tooling, not an SDK
assertion. Every multi-service edge->agent case runs through the decoder.
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

# The fixture count at spec.pin v0.8.2.
ENVELOPE_FIXTURES = 68
AGENT_TO_EDGE = {"register", "unregister"}
PROFILE_SKIP = "host guidance, §10.6 Fixture status"
# Section 10.6: multi-entry registers a single-service encoder cannot produce.
MULTI_SERVICE_SKIPPED = ("valid/register_batch_valid", "valid/register_two_services")
# Section 10.6's other multi-service envelope cases, which a single-service SDK still runs.
MULTI_SERVICE_RUN = (
    "valid/registered_partial_errors",
    "valid/unregistered_subset",
    "valid/conflict_per_name",
)
SCHEMA_ONLY_SKIP = "schema verdict only, checker/CI tooling, §10.6"
# Agent->edge cases with no agent-side check: their verdict is the edge's schema check.
SCHEMA_ONLY = (
    "invalid/register_batch_101_invalid",
    "invalid/register_empty_services",
    "invalid/register_missing_kind",
    "invalid/register_unknown_kind",
    "invalid/register_unknown_service_field",
    "invalid/unregister_empty_services",
    "invalid/unregister_missing_services",
    "invalid/unregister_name_wrong_type",
    "invalid/unregister_with_kind",
)

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
    root = spec_fixtures / "mcpwarp" / "envelope"
    names = {p.relative_to(root).with_suffix("").as_posix() for p in root.glob("**/*.json")}
    assert len(names) == ENVELOPE_FIXTURES
    assert len(MULTI_SERVICE_SKIPPED) == 2
    assert set(MULTI_SERVICE_SKIPPED) <= names
    assert set(MULTI_SERVICE_RUN) <= names
    assert len(SCHEMA_ONLY) == 9
    assert set(SCHEMA_ONLY) <= names


def test_envelope_fixture(spec_fixtures: Path, envelope_case: Path, schema: dict[str, Any]) -> None:
    fx: dict[str, Any] = json.loads(envelope_case.read_text(encoding="utf-8"))
    op: str = fx["op"]
    m: dict[str, Any] = fx["message"]
    hint: str | None = fx.get("expect", {}).get("error_code")

    name = f"{envelope_case.parent.name}/{envelope_case.stem}"
    if name in MULTI_SERVICE_SKIPPED:
        pytest.skip(f"{PROFILE_SKIP}: {name}")
    if name in SCHEMA_ONLY:
        assert op in AGENT_TO_EDGE and hint is not None
        assert hint == "TOO_MANY_SERVICES" or hint.startswith("SCHEMA_"), hint
        pytest.skip(f"{SCHEMA_ONLY_SKIP}: {name}")

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
        else:
            assert hint in ("UNSUPPORTED_VERSION", "UNKNOWN_OP"), hint
            assert decode_envelope({"mcpwarp": m}) == Unknown(raw_op=m.get("op"), v=m.get("v"))
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
