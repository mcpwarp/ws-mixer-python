"""The conformance adapter's own logic: per-stream FIFO, queue cap, out-of-band reset, events.

The wire behaviour is the Go runner's job (`make conformance-fixture`/`conformance-pair`); these
tests drive `Adapter` directly against a scripted stream.
"""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from ws_mixer.client import ReconnectOptions
from ws_mixer.errors import ErrorCode, StreamError
from ws_mixer.testing import settle

pytestmark = pytest.mark.asyncio

ADAPTER = Path(__file__).resolve().parents[2] / "conformance" / "adapter" / "adapter.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("ws_mixer_conformance_adapter", ADAPTER)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


adapter_mod = _load()


class ScriptedStream:
    def __init__(self, sid: int) -> None:
        self.id = sid
        self.ops: list[str] = []
        self.gate = asyncio.Event()
        self.gate.set()
        self.reset_with: tuple[int, str] | None = None
        self._pending: list[asyncio.Future[None]] = []

    async def write(self, data: bytes) -> None:
        if self.reset_with is not None:
            raise StreamError(self.reset_with[0], self.id, "reset")
        if not self.gate.is_set():
            fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._pending.append(fut)
            try:
                await fut
            finally:
                self._pending.remove(fut)
        self.ops.append(f"write:{data.decode()}")

    def release(self) -> None:
        self.gate.set()
        for fut in self._pending:
            if not fut.done():
                fut.set_result(None)

    async def close_write(self) -> None:
        self.ops.append("close_write")

    def reset(self, code: int, message: str) -> None:
        self.reset_with = (code, message)
        self.ops.append(f"reset:{code}")
        for fut in self._pending:
            if not fut.done():
                fut.set_exception(StreamError(code, self.id, "reset"))


def make() -> tuple[Any, list[dict[str, Any]], ScriptedStream]:
    events: list[dict[str, Any]] = []
    a = adapter_mod.Adapter(emit=events.append)
    s = ScriptedStream(1)
    a.streams[1] = s
    return a, events, s


def acks(events: list[dict[str, Any]]) -> list[Any]:
    return [e["seq"] for e in events if e["event"] == "ack"]


def errors(events: list[dict[str, Any]]) -> list[tuple[Any, str]]:
    return [(e.get("seq"), e["message"]) for e in events if e["event"] == "error"]


async def test_write_close_write_keep_command_order() -> None:
    a, events, s = make()
    s.gate.clear()
    await a.handle({"seq": 1, "cmd": "write", "id": 1, "data_b64": "YQ=="})
    await a.handle({"seq": 2, "cmd": "write", "id": 1, "data_b64": "Yg=="})
    await a.handle({"seq": 3, "cmd": "close_write", "id": 1})
    await settle()
    assert s.ops == [] and acks(events) == []
    s.release()
    await settle()
    assert s.ops == ["write:a", "write:b", "close_write"]
    assert acks(events) == [1, 2, 3]
    closed = [e["direction"] for e in events if e["event"] == "stream_closed"]
    assert closed == ["write"]


async def test_queue_cap_rejects_with_queue_full() -> None:
    a, events, s = make()
    s.gate.clear()
    cap = adapter_mod.STREAM_QUEUE_CAP
    for seq in range(cap + 2):
        await a.handle({"seq": seq, "cmd": "write", "id": 1, "data_b64": "eA=="})
    await settle()
    # One write is running, `cap` are queued behind it, the next is refused.
    errs = errors(events)
    assert len(errs) == 1 and errs[0][0] == cap + 1 and errs[0][1].startswith("queue_full")
    s.release()
    await settle()
    assert len(acks(events)) == cap + 1


async def test_reset_is_out_of_band_and_aborts_the_queue() -> None:
    a, events, s = make()
    s.gate.clear()
    await a.handle({"seq": 1, "cmd": "write", "id": 1, "data_b64": "YQ=="})
    await a.handle({"seq": 2, "cmd": "write", "id": 1, "data_b64": "Yg=="})
    await a.handle({"seq": 3, "cmd": "close_write", "id": 1})
    await settle()
    await a.handle({"seq": 4, "cmd": "reset", "id": 1, "code": int(ErrorCode.CANCEL)})
    await settle()
    assert s.ops == ["reset:7"]
    assert acks(events) == [4]
    errs = dict(errors(events))
    assert set(errs) == {1, 2, 3}
    assert errs[2].startswith("reset:") and errs[3].startswith("reset:")
    assert 1 not in a.streams and 1 in a.locally_reset


async def test_unknown_stream_and_bad_close_code() -> None:
    a, events, _ = make()
    await a.handle({"seq": 1, "cmd": "write", "id": 9, "data_b64": ""})
    await a.handle({"seq": 2, "cmd": "close", "code": 0})
    await a.handle({"seq": 3, "cmd": "open_stream"})
    await a.handle({"seq": 4, "cmd": "bogus"})
    assert [seq for seq, _ in errors(events)] == [1, 2, 3, 4]
    unsupported = [e for e in events if e.get("unsupported")]
    assert unsupported and unsupported[0]["seq"] == 3 and unsupported[0]["ok"] is False


async def test_both_halves_closed_emits_both() -> None:
    a, events, _ = make()
    a.note_half_closed(1, "read")
    a.note_half_closed(1, "write")
    dirs = [e["direction"] for e in events if e["event"] == "stream_closed"]
    assert dirs == ["read", "write", "both"]
    assert all(isinstance(e["t_ms"], int) for e in events)
    assert 1 not in a.streams


async def test_reconnect_options_mapping() -> None:
    a, _, _ = make()
    assert a.reconnect_options(None) == ReconnectOptions(max_attempts=0)
    assert a.reconnect_options({"enabled": False}) == ReconnectOptions(max_attempts=0)
    got = a.reconnect_options({"enabled": True, "baseMs": 50, "capMs": 300})
    assert got == ReconnectOptions(base_ms=50, cap_ms=300, max_attempts=None)
    assert a.reconnect_options({"enabled": True, "maxAttempts": 3}).max_attempts == 3


async def test_connect_with_bad_options_errors_with_seq() -> None:
    a, events, _ = make()
    cmd = {"url": "ws://127.0.0.1:1/v1/tunnel", "token": "t"}
    await a.handle(
        {"seq": 5, "cmd": "connect", **cmd, "reconnect": {"enabled": True, "maxAttempts": "x"}}
    )
    await settle()
    [(seq, message)] = errors(events)
    assert seq == 5 and message.startswith("connect:")
    assert a.client is None
