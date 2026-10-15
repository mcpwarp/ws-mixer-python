"""spec/fixtures/mcpwarp/status-text (MCPWARP.md sections 7 and 10.5).

The shipped tables must equal the per-locale JSON string for string, every cases.json input must
describe exactly as expected, and rules.json must match the resolution order and colon joiners.
status_text.py keeps its own tables (no file reads at runtime); this is what keeps them in sync.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import SPEC
from ws_mixer._jscompat import JS_WHITESPACE
from ws_mixer.mcpwarp import STATUS_LOCALES, TunnelStatusCode, describe_status
from ws_mixer.mcpwarp.status_text import _JOINERS, _TABLES

# Floors at spec.pin v0.6.1: six locale tables plus cases.json and rules.json; 26 cases.
MIN_STATUS_TEXT_FILES = 8
MIN_CASES = 26


def _collect_cases() -> list[dict[str, Any]]:
    if SPEC.dir is None:
        return []
    path = SPEC.dir / "fixtures" / "mcpwarp" / "status-text" / "cases.json"
    cases: list[dict[str, Any]] = json.loads(path.read_text(encoding="utf-8"))["cases"]
    return cases


_CASES = _collect_cases()


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:60]


def _load(spec_fixtures: Path, name: str) -> Any:
    path = spec_fixtures / "mcpwarp" / "status-text" / name
    return json.loads(path.read_text(encoding="utf-8"))


def _table(spec_fixtures: Path, locale: str) -> dict[str, dict[str, str]]:
    """A locale file without its `_`/`$` meta keys, as check-fixtures.mjs reads it."""
    data: dict[str, dict[str, str]] = _load(spec_fixtures, f"{locale}.json")
    return {k: v for k, v in data.items() if not k.startswith(("_", "$"))}


@pytest.fixture(scope="module")
def rules(spec_fixtures: Path) -> dict[str, Any]:
    out: dict[str, Any] = _load(spec_fixtures, "rules.json")
    return out


def _js_replace(s: str, rewrite: dict[str, str]) -> str:
    """`s.replace(new RegExp(pattern, flags), replacement)` for the one rule rules.json holds."""
    assert rewrite == {"pattern": ":\\s+", "flags": "g", "replacement": "; "}
    return re.sub(":[" + re.escape(JS_WHITESPACE) + "]+", "; ", s)


def test_status_text_file_count(spec_fixtures: Path) -> None:
    files = list((spec_fixtures / "mcpwarp" / "status-text").glob("*.json"))
    assert len(files) >= MIN_STATUS_TEXT_FILES


def test_locales_match_rules(rules: dict[str, Any]) -> None:
    assert list(STATUS_LOCALES) == rules["locales"]
    assert list(STATUS_LOCALES) == rules["localeResolution"]["order"]
    assert describe_status({"state": "off"}, locale="zh") == describe_status(
        {"state": "off"}, locale=rules["zhAlias"]
    )


@pytest.mark.parametrize("locale", STATUS_LOCALES)
def test_locale_table(spec_fixtures: Path, rules: dict[str, Any], locale: str) -> None:
    table = _table(spec_fixtures, locale)
    assert sorted(table) == sorted(_table(spec_fixtures, "en"))
    key_re = re.compile(rules["keyFormat"]["pattern"])
    for key, want in table.items():
        assert key_re.search(key), key
        state, code = key.split(".")
        status = {"state": state} if code == "none" else {"state": state, "code": code}
        assert describe_status(status, locale=locale).to_dict() == want, f"{locale}:{key}"
        if code == "none":
            continue
        # The raw sentence, unhidden by the equal-to-label rule.
        raw = describe_status(status, locale=locale, messages={"states": {state: "\x00"}})
        assert raw.detail == want.get("detail", want["label"]), f"{locale}:{key} sentence"
    codes = {k.split(".")[1] for k in table}
    assert {c.value for c in TunnelStatusCode} <= codes


@pytest.mark.parametrize("locale", STATUS_LOCALES)
def test_derived_tables_equal_shipped(spec_fixtures: Path, locale: str) -> None:
    """rules.json keyFormat: states[state] = the label of every <state>.* key; codes[code] = the
    sentence (detail or label) of every *.<code> key."""
    table = _table(spec_fixtures, locale)
    states: dict[str, str] = {}
    codes: dict[str, str] = {}
    for key, v in table.items():
        state, code = key.split(".")
        assert states.setdefault(state, v["label"]) == v["label"], key
        if code != "none":
            sentence = v.get("detail", v["label"])
            assert codes.setdefault(code, sentence) == sentence, key
    shipped = _TABLES[locale]
    assert dict(shipped.states) == states
    assert dict(shipped.codes) == codes


@pytest.mark.parametrize("locale", STATUS_LOCALES)
def test_invalid_config_composition(rules: dict[str, Any], locale: str) -> None:
    sentence = describe_status({"state": "error", "code": "INVALID_CONFIG"}, locale=locale).detail
    assert sentence is not None
    generic = rules["invalidConfigComposition"]["genericLastError"]
    status = {"state": "error", "code": "INVALID_CONFIG", "lastError": generic}
    assert describe_status(status, locale=locale).detail == sentence
    last_error = "Bad value:\t x: y"
    status = {"state": "error", "code": "INVALID_CONFIG", "lastError": last_error}
    joiner = rules["colonJoiner"][locale]
    want = sentence + joiner + _js_replace(last_error, rules["lastErrorColonRewrite"])
    assert describe_status(status, locale=locale).detail == want
    assert want.endswith("Bad value; x; y")


def test_joiner_code_points(rules: dict[str, Any]) -> None:
    joiners = rules["colonJoiner"]
    for loc in STATUS_LOCALES:
        assert _JOINERS.get(loc, ": ") == joiners[loc], loc
    assert joiners["fr"] == chr(0xA0) + ": "
    assert joiners["ja"] == joiners["zh-CN"] == chr(0xFF1A)
    assert joiners["en"] == joiners["de"] == joiners["es"] == ": "


def test_new_in_spec(spec_fixtures: Path, rules: dict[str, Any]) -> None:
    for entry in rules["newInSpec"]:
        locale, key = entry.split(":")
        state, code = key.split(".")
        got = describe_status({"state": state, "code": code}, locale=locale).to_dict()
        assert got == _load(spec_fixtures, f"{locale}.json")[key], entry


def test_host_off_pattern(rules: dict[str, Any]) -> None:
    ex = rules["hostOffPattern"]["example"]
    assert describe_status(ex["status"], locale=ex["locale"]).to_dict() == ex["expect"]


def test_case_count(spec_fixtures: Path) -> None:
    assert len(_load(spec_fixtures, "cases.json")["cases"]) >= MIN_CASES


@pytest.mark.parametrize(
    "case",
    list[Any](_CASES) or [None],
    ids=[_slug(c["description"]) for c in _CASES] or ["spec-unavailable"],
)
def test_case(spec_fixtures: Path, case: Any) -> None:
    inp = case["input"]
    assert set(inp) <= {"status", "locale"}, case["description"]
    if inp["locale"] is None:
        got = describe_status(inp["status"])
    else:
        got = describe_status(inp["status"], locale=inp["locale"])
    assert got.to_dict() == case["expect"], case["description"]
