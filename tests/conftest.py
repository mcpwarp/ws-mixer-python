"""Locates the ws-mixer-spec checkout the spec fixture tests read from.

Resolution order (same as ws-mixer-js's test/helpers/spec-dir.ts):

1. `WSMIXER_SPEC_DIR`: either the spec's `spec/` directory (has `fixtures/`) or the repo root
   (has `spec/fixtures/`). Neither existing is an error, with no further fallback.
2. `.spec/spec`, as populated by `make fetch-spec`, while `.spec/.pin` equals `spec.pin`.
3. `../ws-mixer-spec/spec`, while that repo's HEAD is exactly the pinned tag and `spec/` has
   no local changes.

With none usable, spec tests are skipped locally and fail when `CI` is set.

Spec tests take a `frame_case` or `control_case` argument (a fixture path, parametrized here at
collection time) and the `spec_dir` fixture.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class SpecLocation:
    dir: Path | None
    reason: str = ""


def _git(repo: Path, *args: str) -> str | None:
    try:
        out = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip()


def _locate() -> SpecLocation:
    env = os.environ.get("WSMIXER_SPEC_DIR", "").strip()
    if env:
        base = Path(env)
        if (base / "fixtures").is_dir():
            return SpecLocation(base)
        if (base / "spec" / "fixtures").is_dir():
            return SpecLocation(base / "spec")
        return SpecLocation(
            None, f"WSMIXER_SPEC_DIR={env!r} has neither fixtures/ nor spec/fixtures/ under it"
        )

    try:
        pin = (REPO_ROOT / "spec.pin").read_text().strip()
    except OSError:
        return SpecLocation(None, "spec.pin is missing")

    notes: list[str] = []
    fetched = REPO_ROOT / ".spec" / "spec"
    if fetched.is_dir():
        try:
            fetched_pin = (REPO_ROOT / ".spec" / ".pin").read_text().strip()
        except OSError:
            fetched_pin = "an unknown tag"
        if fetched_pin == pin:
            return SpecLocation(fetched)
        notes.append(f".spec/ is at {fetched_pin}")

    sibling = REPO_ROOT.parent / "ws-mixer-spec"
    if (sibling / "spec" / "fixtures").is_dir():
        tag = _git(sibling, "describe", "--tags", "--exact-match", "HEAD")
        status = _git(sibling, "status", "--porcelain", "--untracked-files=all", "--", "spec")
        if tag == pin and status == "":
            return SpecLocation(sibling / "spec")
        if tag != pin:
            notes.append(f"../ws-mixer-spec is at {tag or 'no tag'}")
        else:
            notes.append(f"../ws-mixer-spec is at {tag} but has local changes under spec/")

    found = "; ".join(notes) if notes else "no spec checkout found"
    return SpecLocation(
        None,
        f"spec fixtures unavailable ({found}; spec.pin is {pin}): run 'make fetch-spec' "
        "or set WSMIXER_SPEC_DIR",
    )


SPEC = _locate()


def _unavailable() -> None:
    if os.environ.get("CI"):
        pytest.fail(SPEC.reason, pytrace=False)
    pytest.skip(SPEC.reason)


@pytest.fixture(scope="session")
def spec_dir() -> Path:
    """The spec checkout's `spec/` directory (holds `fixtures/` and the schemas)."""
    if SPEC.dir is None:
        _unavailable()
    assert SPEC.dir is not None
    return SPEC.dir


@pytest.fixture(scope="session")
def spec_fixtures(spec_dir: Path) -> Path:
    return spec_dir / "fixtures"


def pytest_report_header() -> str:
    return f"ws-mixer-spec: {SPEC.dir or SPEC.reason}"


def _cases(subdir: str, pattern: str) -> list[tuple[Path, str]]:
    if SPEC.dir is None:
        return []
    root = SPEC.dir / "fixtures" / subdir
    return [(p, p.relative_to(root).with_suffix("").as_posix()) for p in sorted(root.glob(pattern))]


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    for argname, subdir, pattern in (
        ("frame_case", "frames", "*.json"),
        ("control_case", "control", "**/*.json"),
    ):
        if argname not in metafunc.fixturenames:
            continue
        cases = _cases(subdir, pattern)
        if cases:
            metafunc.parametrize(argname, [p for p, _ in cases], ids=[i for _, i in cases])
        else:
            # One placeholder case; the spec_dir fixture then skips it (or fails it in CI).
            metafunc.parametrize(argname, [None], ids=["spec-unavailable"])
