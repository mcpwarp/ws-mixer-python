"""Every role:"client" spec/fixtures/sequences/*.json transcript, replayed against a real MixerConn
over a FakeWebSocket on a FakeClock (port of ws-mixer-js test/sequence.test.ts).

- `recv` pushes the encoded frame or control message into the fake socket.
- `send` scans what the SDK wrote, from a cursor, for a matching frame (normalized: type, stream
  id, increment, code, payload length; control messages by `t` plus their identifying fields).
  Sends only application code can cause are commanded first: DATA -> stream.write(), CLOSE ->
  stream.close_write(), RESET other than STREAM_LIMIT/STREAM_CLOSED -> stream.reset(),
  app -> conn.send_app(), drain -> conn.close().
- `wait_ms` advances the fake clock by exactly that much: the keepalive fixtures run at the
  spec's real 30 s / 90 s values, with no scaling.
- `expect` checks the close event (error name, semantic close code), the last touched stream's
  reset code, state and send window. For an expected error it also checks the live invariants:
  an `error` with that code is the last stream-0 message, then the wire close code.

role:"server" transcripts are written from the server's side; this SDK is client-only.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import _cases
from ws_mixer.conn import CloseInfo, MixerConn
from ws_mixer.control import encode_control
from ws_mixer.errors import ConnError, ErrorCode, close_code, code_name, wire_close_code
from ws_mixer.frame import (
    Frame,
    FrameType,
    decode_frame,
    encode_close,
    encode_data,
    encode_open,
    encode_reset,
    encode_window,
    frame_type_name,
    reset_code,
    window_increment,
)
from ws_mixer.socket import WebSocketInit
from ws_mixer.stream import MixerStream
from ws_mixer.testing import FakeClock, FakeConnector, FakeWebSocket, settle

# Fixture counts at spec.pin v0.6.1; bump together with the pin.
EXPECTED_SEQUENCE_FIXTURES = 43
EXPECTED_CLIENT_FIXTURES = 24
# Sends the SDK makes on its own; every other stream RESET is an application command.
AUTONOMOUS_RESETS = {ErrorCode.STREAM_LIMIT, ErrorCode.STREAM_CLOSED}

_CASES = _cases("sequences", "*.json")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "sequence_case" in metafunc.fixturenames:
        if _CASES:
            metafunc.parametrize(
                "sequence_case", [p for p, _ in _CASES], ids=[i for _, i in _CASES]
            )
        else:
            metafunc.parametrize("sequence_case", [None], ids=["spec-unavailable"])


def test_sequence_fixture_count(spec_fixtures: Path) -> None:
    files = sorted((spec_fixtures / "sequences").glob("*.json"))
    roles = [json.loads(p.read_text("utf-8"))["role"] for p in files]
    assert len(files) == EXPECTED_SEQUENCE_FIXTURES
    assert roles.count("client") == EXPECTED_CLIENT_FIXTURES
    assert roles.count("server") == EXPECTED_SEQUENCE_FIXTURES - EXPECTED_CLIENT_FIXTURES


def _is_frame(v: dict[str, Any]) -> bool:
    return "type" in v


def _payload(v: dict[str, Any]) -> bytes:
    if "payload_hex" in v:
        return bytes.fromhex(v["payload_hex"])
    return bytes(v.get("payload_length", 0))


def _encode_step(v: dict[str, Any]) -> bytes:
    if not _is_frame(v):
        return encode_data(0, encode_control(v))
    sid = v["stream_id"]
    kind = v["type"]
    if kind == "OPEN":
        return encode_open(sid)
    if kind == "CLOSE":
        return encode_close(sid)
    if kind == "WINDOW":
        return encode_window(sid, v["increment"])
    if kind == "RESET":
        return encode_reset(sid, v["code"], v.get("message", ""))
    if kind == "DATA":
        return encode_data(sid, _payload(v))
    raise AssertionError(f"unknown frame type in fixture: {kind}")


def _decode(raw: bytes) -> Frame | None:
    try:
        return decode_frame(raw)
    except Exception:
        return None


def _control_of(frame: Frame) -> dict[str, Any] | None:
    if frame.type != FrameType.DATA or frame.stream_id != 0:
        return None
    msg = json.loads(frame.payload)
    return msg if isinstance(msg, dict) else None


def _matches(raw: bytes, want: dict[str, Any]) -> bool:
    frame = _decode(raw)
    if frame is None:
        return False
    if _is_frame(want):
        if frame_type_name(frame.type) != want["type"] or frame.stream_id != want["stream_id"]:
            return False
        if want["type"] == "WINDOW" and "increment" in want:
            return bool(window_increment(frame) == want["increment"])
        if want["type"] == "RESET" and "code" in want:
            return bool(reset_code(frame) == want["code"])
        if want["type"] == "DATA" and ("payload_hex" in want or "payload_length" in want):
            return frame.payload == _payload(want)
        return True
    got = _control_of(frame)
    if got is None or got.get("t") != want["t"]:
        return False
    # Identifying fields only: token/agent/messages/ts of our own pings are ours, not the
    # fixture's (the transcript's are examples).
    keys = {
        "hello": ("v", "window", "max_streams"),
        "error": ("code",),
        "ping": ("id",),
        "pong": ("id", "ts"),
        "drain": ("reason",),
        "app": ("body",),
    }.get(want["t"], ())
    return all(got.get(k) == want[k] for k in keys if k in want)


def _skippable(raw: bytes) -> bool:
    frame = _decode(raw)
    if frame is None:
        return False
    if frame.type == FrameType.WINDOW:
        return True
    msg = _control_of(frame)
    return msg is not None and msg.get("t") in ("ping", "pong")


def _describe(raw: bytes) -> str:
    frame = _decode(raw)
    if frame is None:
        return raw.hex()
    msg = _control_of(frame)
    if msg is not None:
        return json.dumps(msg)
    return f"{frame_type_name(frame.type)} stream {frame.stream_id} ({len(frame.payload)} B)"


class _Runner:
    def __init__(self, fx: dict[str, Any]) -> None:
        self.fx = fx
        self.clock = FakeClock()
        self.ws = FakeWebSocket()
        self.streams: dict[int, MixerStream] = {}
        self.closes: list[CloseInfo] = []
        self.cursor = 0
        self.last_sid: int | None = None
        self.tasks: list[asyncio.Task[Any]] = []

    async def run(self) -> None:
        connector = FakeConnector([self.ws])
        self.ws.open()
        sock = await connector("wss://edge.test/v1/tunnel", WebSocketInit())
        hello = next(s["send"] for s in self.fx["steps"] if s.get("send", {}).get("t") == "hello")
        conn = MixerConn(
            sock,
            token=hello["token"],
            agent=hello["agent"],
            clock=self.clock,
            random=lambda: 0.5,
            window=hello.get("window", 262_144),
            max_streams=hello.get("max_streams", 64),
            on_stream=lambda s: self.streams.__setitem__(s.id, s),
        )
        self.conn = conn
        conn.on("close", self.closes.append)
        handshake = asyncio.create_task(conn.handshake())
        try:
            for step in self.fx["steps"]:
                await settle()
                if "wait_ms" in step:
                    await self.clock.advance(step["wait_ms"])
                elif "recv" in step:
                    self._recv(step["recv"])
                elif "send" in step:
                    await self._send(step["send"])
                elif "expect" in step:
                    self._expect(step["expect"])
                else:
                    raise AssertionError(f"step with no known key: {step}")
        finally:
            if not conn.closed:
                conn.fail(ConnError(ErrorCode.NO_ERROR, "fixture done"))
            await conn.wait_closed()
            await asyncio.gather(handshake, *self.tasks, return_exceptions=True)
            await settle()
        assert self.clock.pending() == 0, "a timer outlived the connection"

    def _recv(self, payload: dict[str, Any]) -> None:
        if _is_frame(payload) and payload.get("stream_id"):
            self.last_sid = payload["stream_id"]
        self.ws.push(_encode_step(payload))

    def _spawn(self, coro: Any) -> None:
        self.tasks.append(asyncio.ensure_future(coro))

    async def _send(self, want: dict[str, Any]) -> None:
        if _is_frame(want):
            sid = want["stream_id"]
            if sid:
                self.last_sid = sid
            stream = self.streams.get(sid)
            if want["type"] == "DATA" and stream is not None:
                self._spawn(stream.write(_payload(want)))
            elif want["type"] == "CLOSE" and stream is not None:
                self._spawn(stream.close_write())
            elif (
                want["type"] == "RESET"
                and want.get("code") not in AUTONOMOUS_RESETS
                and stream is not None
            ):
                stream.reset(want["code"], want.get("message", ""))
        elif want["t"] == "app":
            self._spawn(self.conn.send_app(want["body"]))
        elif want["t"] == "drain":
            self._spawn(self.conn.close())
        await settle()
        sent = self.ws.sent
        for i in range(self.cursor, len(sent)):
            if _matches(sent[i], want):
                self.cursor = i + 1
                return
            # Strict, as the Go runner: only keepalive and credit traffic may come in between.
            assert _skippable(sent[i]), (
                f"while waiting for {want}, the client sent unexpected frame #{i}: "
                f"{_describe(sent[i])}"
            )
        raise AssertionError(f"the client never sent {want} (after frame #{self.cursor})")

    def _sent_reset_code(self, sid: int) -> int | None:
        found = None
        for raw in self.ws.sent:
            f = _decode(raw)
            if f is not None and f.type == FrameType.RESET and f.stream_id == sid:
                found = reset_code(f)
        return found

    def _expect(self, want: dict[str, Any]) -> None:
        if "error_code" in want:
            if want["error_code"] is None:
                assert not self.closes, f"connection closed: {self.closes[-1]}"
                assert not self.ws.close_calls
            else:
                assert self.closes, "expected the connection to fail"
                info = self.closes[-1]
                assert info.error_name == want["error_code"], info
                self._check_error_invariants(info)
        if want.get("close_code") is not None:
            assert self.closes and self.closes[-1].ws_code == want["close_code"]
        sid = self.last_sid
        stream = self.streams.get(sid) if sid is not None else None
        if "stream_reset_code" in want:
            assert sid is not None
            code = self._sent_reset_code(sid)
            if code is None and stream is not None:
                code = stream.reset_code
            assert code is not None and code_name(code) == want["stream_reset_code"]
        if want.get("stream_state") is not None:
            if stream is not None:
                assert stream.state == want["stream_state"]
            else:
                # Refused before a MixerStream existed (STREAM_LIMIT): the RESET is the evidence.
                assert sid is not None and self._sent_reset_code(sid) is not None, (
                    f"stream {sid} was never created and never reset"
                )
                assert want["stream_state"] == "closed"
        if "send_window" in want:
            assert stream is not None
            assert stream.send_window == want["send_window"]

    def _check_error_invariants(self, info: CloseInfo) -> None:
        assert info.error_code is not None
        assert info.ws_code == close_code(info.error_code)
        assert self.ws.close_calls[0][0] == wire_close_code(info.ws_code)
        controls = [
            c
            for c in (_control_of(f) for f in map(_decode, self.ws.sent) if f is not None)
            if c is not None
        ]
        assert controls and controls[-1]["t"] == "error", "error must be the last stream-0 message"
        assert controls[-1]["code"] == info.error_code


@pytest.mark.asyncio
async def test_sequence_fixture(spec_dir: Path, sequence_case: Path) -> None:
    fx = json.loads(sequence_case.read_text(encoding="utf-8"))
    if fx["role"] != "client":
        pytest.skip(
            "role: server -- written from the server's side; this SDK is client-only and does "
            "not replay server transcripts"
        )
    await _Runner(fx).run()
