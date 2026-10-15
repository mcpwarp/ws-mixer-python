"""Static and runtime checks on src/ws_mixer that keep it vendorable and deterministic.

1. Only relative imports of ws_mixer inside the package (vendoring copies it under another name).
2. Only stdlib imports outside adapters/ and testing/.
3. No real-time, timer or randomness primitives outside clock.py: every timer goes through a
   Clock and all randomness through an injected random(). `asyncio.sleep(0)` (a bare yield) is
   allowed; `.call_later`/`.call_at`/`.time` are allowed only on a receiver named clock/_clock.
   This applies to testing/ too: a fake clock needs none of these.
4. Importing the package starts no threads and configures no logging.
5. No module-level or class-level mutable state (several vendored copies and instances must
   coexist).
"""

from __future__ import annotations

import ast
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

PKG = Path(__file__).resolve().parent.parent / "src" / "ws_mixer"
STDLIB = sys.stdlib_module_names | {"__future__"}
THIRD_PARTY_OK = ("adapters", "testing")
TIMING_OK = {"clock.py"}
# RFC 6455 wants Sec-WebSocket-Key and frame masking keys from a strong source; they never affect
# protocol-observable behaviour, so there is nothing for a test to inject.
TIMING_EXCEPTIONS = {("_rfc6455.py", "os.urandom")}

BANNED_MODULES = {"time", "random", "secrets", "datetime", "uuid"}
BANNED_MEMBERS = {
    "asyncio": {"sleep", "timeout", "timeout_at", "wait_for"},
    "os": {"urandom"},
    "threading": {"Timer"},
}
CLOCK_ONLY_METHODS = {"call_later", "call_at", "time"}
CLOCK_NAMES = {"clock", "_clock"}
MUTABLE_CALLS = {"list", "dict", "set", "bytearray", "defaultdict", "deque", "OrderedDict"}
MUTABLE_OK_NAMES = {"__all__", "__slots__"}


def _modules() -> list[Path]:
    return sorted(PKG.rglob("*.py"))


def _rel(path: Path) -> str:
    return path.relative_to(PKG).as_posix()


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _imports(tree: ast.Module) -> Iterator[tuple[int, str]]:
    """(line, absolute top-level module) for every non-relative import."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield node.lineno, alias.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            yield node.lineno, node.module.split(".")[0]


def test_package_found() -> None:
    assert (PKG / "__init__.py").is_file()
    assert len(_modules()) >= 5


def test_no_absolute_self_imports() -> None:
    bad = [
        f"{_rel(p)}:{line}"
        for p in _modules()
        for line, mod in _imports(_parse(p))
        if mod == "ws_mixer"
    ]
    assert not bad, f"use relative imports for ws_mixer: {bad}"


def test_stdlib_only_outside_adapters_and_testing() -> None:
    bad = []
    for p in _modules():
        if _rel(p).split("/")[0] in THIRD_PARTY_OK:
            continue
        for line, mod in _imports(_parse(p)):
            if mod not in STDLIB and mod != "ws_mixer":
                bad.append(f"{_rel(p)}:{line} imports {mod}")
    assert not bad, f"non-stdlib imports in the core: {bad}"


def _module_aliases(tree: ast.Module) -> dict[str, str]:
    """Local name -> module for `import x` / `import x as y` of the modules this rule watches."""
    watched = BANNED_MODULES | set(BANNED_MEMBERS)
    out: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".")[0]
                if top in watched:
                    out[alias.asname or top] = top
    return out


def _is_zero(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and node.value == 0 and not isinstance(node.value, bool)


def _receiver_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _timing_violations(tree: ast.Module) -> Iterator[tuple[int, str]]:
    aliases = _module_aliases(tree)
    zero_sleeps: set[int] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and aliases.get(node.func.value.id) == "asyncio"
            and node.func.attr == "sleep"
            and len(node.args) == 1
            and not node.keywords
            and _is_zero(node.args[0])
        ):
            zero_sleeps.add(id(node.func))

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in BANNED_MODULES:
                    yield node.lineno, f"import {alias.name}"
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            top = node.module.split(".")[0]
            if top in BANNED_MODULES:
                yield node.lineno, f"from {node.module} import ..."
            for alias in node.names:
                if alias.name in BANNED_MEMBERS.get(node.module, set()):
                    yield node.lineno, f"from {node.module} import {alias.name}"
        elif isinstance(node, ast.Attribute):
            mod = aliases.get(node.value.id) if isinstance(node.value, ast.Name) else None
            if mod in BANNED_MODULES:
                yield node.lineno, f"{mod}.{node.attr}"
            elif mod in BANNED_MEMBERS and node.attr in BANNED_MEMBERS[mod]:
                if id(node) not in zero_sleeps:
                    yield node.lineno, f"{mod}.{node.attr}"
            elif node.attr in CLOCK_ONLY_METHODS and _receiver_name(node.value) not in CLOCK_NAMES:
                yield node.lineno, f"<non-clock>.{node.attr}"


def test_no_real_time_or_randomness_outside_clock() -> None:
    bad = [
        f"{_rel(p)}:{line} {what}"
        for p in _modules()
        if _rel(p) not in TIMING_OK
        for line, what in _timing_violations(_parse(p))
        if (_rel(p), what) not in TIMING_EXCEPTIONS
    ]
    assert not bad, f"timers and randomness must go through Clock / random(): {bad}"


def test_timing_check_catches_violations() -> None:
    src = (
        "import time\n"  # 1
        "import asyncio as aio\n"  # 2 (fine on its own)
        "from random import random\n"  # 3
        "async def f(loop, self):\n"  # 4
        "    await aio.sleep(1)\n"  # 5
        "    await aio.sleep(0)\n"  # 6 (allowed)
        "    aio.wait_for(x, 1)\n"  # 7
        "    loop.call_later(1, f)\n"  # 8
        "    self._loop.call_at(1, f)\n"  # 9
        "    self._clock.call_later(1, f)\n"  # 10 (allowed)
        "    aio.get_running_loop().time()\n"  # 11
        "    time.monotonic()\n"  # 12
        "import os, threading, uuid\n"  # 13
        "os.urandom(4)\n"  # 14
        "threading.Timer(1, f)\n"  # 15
        "from datetime import datetime\n"  # 16
        "from os import urandom\n"  # 17
        "clock.call_at(1, f)\n"  # 18 (allowed)
    )
    lines = sorted({ln for ln, _ in _timing_violations(ast.parse(src))})
    assert lines == [1, 3, 5, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17]


def _is_mutable(value: ast.expr) -> bool:
    if isinstance(value, ast.List | ast.Dict | ast.Set | ast.ListComp | ast.DictComp | ast.SetComp):
        return True
    if isinstance(value, ast.Call):
        name = _receiver_name(value.func)
        return name in MUTABLE_CALLS
    return False


def _target_names(target: ast.expr) -> list[str]:
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, ast.Tuple | ast.List):
        return [n for elt in target.elts for n in _target_names(elt)]
    return []


def _assignments(body: list[ast.stmt]) -> Iterator[tuple[int, ast.expr, ast.expr]]:
    """(line, target, value) pairs, with `a, b = x, y` split element-wise."""
    for node in body:
        if isinstance(node, ast.Assign):
            pairs = [(t, node.value) for t in node.targets]
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            pairs = [(node.target, node.value)]
        else:
            continue
        for target, value in pairs:
            if (
                isinstance(target, ast.Tuple | ast.List)
                and isinstance(value, ast.Tuple | ast.List)
                and len(target.elts) == len(value.elts)
            ):
                for t, v in zip(target.elts, value.elts, strict=True):
                    yield node.lineno, t, v
            else:
                yield node.lineno, target, value


def _mutable_state(tree: ast.Module) -> Iterator[tuple[int, str]]:
    scopes: list[tuple[str, list[ast.stmt]]] = [("", tree.body)]
    while scopes:
        prefix, body = scopes.pop()
        for node in body:
            if isinstance(node, ast.ClassDef):
                scopes.append((f"{prefix}{node.name}.", node.body))
        for line, target, value in _assignments(body):
            names = _target_names(target)
            if set(names) <= MUTABLE_OK_NAMES and names:
                continue
            if _is_mutable(value):
                yield line, prefix + (", ".join(names) or "<target>")


def test_no_module_or_class_level_mutable_state() -> None:
    bad = [
        f"{_rel(p)}:{line} {name}" for p in _modules() for line, name in _mutable_state(_parse(p))
    ]
    assert not bad, f"use tuples/frozensets/MappingProxyType for shared constants: {bad}"


def test_mutable_state_check_catches_violations() -> None:
    src = (
        "A = []\n"  # 1
        "B, C = (), {}\n"  # 2 (C)
        "__all__ = ['x']\n"  # 3 (allowed)
        "class K:\n"  # 4
        "    cache: dict[str, int] = {}\n"  # 5
        "    ok = frozenset()\n"  # 6 (allowed)
        "    class Inner:\n"  # 7
        "        items = list()\n"  # 8
        "D = E = set()\n"  # 9
    )
    found = sorted(_mutable_state(ast.parse(src)))
    assert [ln for ln, _ in found] == [1, 2, 5, 8, 9, 9]
    assert (2, "C") in found and (5, "K.cache") in found and (8, "K.Inner.items") in found


IMPORT_PROBE = """
import importlib, logging, pkgutil, sys, threading
import ws_mixer
for info in pkgutil.walk_packages(ws_mixer.__path__, "ws_mixer."):
    importlib.import_module(info.name)
assert threading.active_count() == 1, threading.enumerate()
assert logging.root.handlers == [], logging.root.handlers
configured = [
    name for name, lg in logging.Logger.manager.loggerDict.items()
    if isinstance(lg, logging.Logger) and (lg.handlers or lg.level != logging.NOTSET)
]
assert not configured, configured
print("ok")
"""


def test_import_has_no_side_effects() -> None:
    out = subprocess.run(
        [sys.executable, "-I", "-c", IMPORT_PROBE],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if out.returncode != 0:
        pytest.fail(f"import probe failed:\n{out.stdout}\n{out.stderr}")
    assert out.stdout.strip() == "ok"
