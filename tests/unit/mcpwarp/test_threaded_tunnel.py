"""ThreadedTunnel: create_tunnel() on a LoopThread, driven from the test thread as a host's main
thread would. Real threads and a real LoopClock; the edge is the spec runner's scripted
`EdgeSocket`, extended to open at once and answer hello and register by itself.
"""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import math
import queue
import socket
import threading
import time
import warnings
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from tests.spec.test_tunnel import WELCOME, EdgeSocket
from tests.unit.mcpwarp.test_tunnel import LEGAL
from ws_mixer._rfc6455 import StdlibConnector, accept_key
from ws_mixer.clock import LoopClock
from ws_mixer.control import encode_control
from ws_mixer.frame import FrameType, decode_frame, encode_data
from ws_mixer.mcpwarp import (
    Request,
    Response,
    ThreadedTunnel,
    TunnelConfig,
    TunnelState,
    TunnelStatus,
)
from ws_mixer.mcpwarp.threaded import _Sockets, _Tracked
from ws_mixer.socket import SUBPROTOCOL, WebSocketConnection, WebSocketInit
from ws_mixer.testing import FakeConnector, FakeWebSocket

UUID = "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10"
URL = "https://notes-tester.mcpwarp.test/mcp"
CONFIG = TunnelConfig(enabled=True, token="tok-6f1e9d", name="notes")
OFF = TunnelStatus(TunnelState.OFF)
ONLINE = TunnelStatus(TunnelState.ONLINE, URL)
T = 2.0  # upper bound for anything that should be near-instant


class AutoEdge(EdgeSocket):
    """Opens at once, answers hello with welcome and register with registered."""

    def __init__(self, *, hold: bool = False) -> None:
        super().__init__()
        self.open()
        if hold:
            self.hold()  # unregister and the SDK's close go unanswered

    async def send(self, data: bytes) -> None:
        await super().send(data)
        frame = decode_frame(data)
        if frame.stream_id != 0 or frame.type != FrameType.DATA:
            return
        msg = json.loads(frame.payload)
        loop = asyncio.get_running_loop()
        if msg["t"] == "hello":
            loop.call_soon(self.control, dict(WELCOME))
        elif msg["t"] == "app":
            env = msg["body"].get("mcpwarp") or {}
            if env.get("op") == "register":
                name = env["services"][0]["name"]
                svc = {"name": name, "url": URL, "id": f"srv_{name}", "created": True}
                loop.call_soon(self.app, {"op": "registered", "services": [svc]})


async def handler(request: Request) -> Response:
    return Response(200, body=b"ok")


def connector(*, hold: bool = False) -> FakeConnector:
    def make(url: str, init: WebSocketInit) -> AutoEdge:
        return AutoEdge(hold=hold)

    return FakeConnector(factory=make)


def make(
    name: str = "mcpwarp-test", *, hold: bool = False, **kw: Any
) -> tuple[ThreadedTunnel, FakeConnector]:
    conn = connector(hold=hold)
    tt = ThreadedTunnel(integration=UUID, handler=handler, web_socket=conn, thread_name=name, **kw)
    return tt, conn


def wait_until(pred: Callable[[], bool], timeout: float = T) -> None:
    end = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > end:
            raise AssertionError("condition not reached in time")
        time.sleep(0.002)


def threads_named(name: str) -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == name]


@pytest.fixture(autouse=True)
def baseline() -> Iterator[None]:
    before = set(threading.enumerate())
    yield
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        gc.collect()  # a leaked coroutine or transport warns here, not in a later test
    assert not caught, [str(w.message) for w in caught]
    leaked = [t for t in threading.enumerate() if t not in before and t.is_alive()]
    assert not leaked, leaked


def test_configure_online_disconnect_reconfigure_stop() -> None:
    tt, conn = make()
    assert tt.status == OFF
    tt.start()
    assert tt.status == OFF and conn.dials == 0  # start() never dials
    tt.configure(CONFIG).result(T)
    wait_until(lambda: tt.status == ONLINE)
    tt.disconnect().result(T)
    assert tt.status == OFF
    tt.configure(CONFIG).result(T)  # the same config starts again after disconnect()
    wait_until(lambda: tt.status == ONLINE)
    assert conn.dials == 2
    t0 = time.monotonic()
    assert tt.stop(timeout=T)
    assert time.monotonic() - t0 < 0.5
    assert tt.status == OFF
    assert not threads_named("mcpwarp-test")
    assert conn.sockets[0].close_calls  # disconnect() closed it gracefully


def test_start_stop_start_gets_a_fresh_thread_and_tunnel() -> None:
    tt, conn = make()
    seen: list[tuple[TunnelStatus, threading.Thread]] = []
    tt.subscribe(lambda s: seen.append((s, threading.current_thread())))
    with tt:
        tt.start()  # idempotent
        tt.configure(CONFIG).result(T)
        wait_until(lambda: tt.status == ONLINE)
    assert tt.status == OFF
    tt.start()
    assert tt.status == OFF  # a fresh Tunnel, not the disposed one
    tt.configure(CONFIG).result(T)
    wait_until(lambda: tt.status == ONLINE)
    assert tt.stop(timeout=T)
    first, second = (t for s, t in seen if s == ONLINE)
    assert first is not second
    assert first.name == second.name == "mcpwarp-test"
    # The restarted Tunnel's opening "off" repeats the last one and is not delivered again.
    connecting = TunnelStatus(TunnelState.CONNECTING)
    assert [s for s, _ in seen] == [OFF, connecting, ONLINE, OFF, connecting, ONLINE, OFF]
    assert conn.dials == 2


def test_stop_never_started_and_twice() -> None:
    tt, _ = make()
    assert tt.stop()
    assert tt.stop(timeout=0)
    tt.start()
    assert tt.stop(timeout=T)
    assert tt.stop(timeout=T)
    assert tt.status == OFF


def test_configure_and_disconnect_need_start() -> None:
    tt, conn = make()
    with pytest.raises(RuntimeError, match="not started"):
        tt.configure(CONFIG)
    with pytest.raises(RuntimeError, match="not started"):
        tt.disconnect()
    tt.start()
    assert tt.stop(timeout=T)
    with pytest.raises(RuntimeError, match="not started"):
        tt.configure(CONFIG)
    assert conn.dials == 0


def test_validation_errors_raise_in_init_before_any_thread() -> None:
    before = threading.active_count()
    with pytest.raises(ValueError, match="RFC 4122 UUID"):
        ThreadedTunnel(integration="nope", handler=handler)
    with pytest.raises(TypeError, match="exactly one"):
        ThreadedTunnel(integration=UUID)
    with pytest.raises(TypeError, match="bogus"):
        ThreadedTunnel(integration=UUID, handler=handler, bogus=1)
    assert threading.active_count() == before


def test_stop_from_a_callback_on_the_loop_thread() -> None:
    tt, _ = make("mcpwarp-inside")
    results: list[bool] = []

    def on_status(s: TunnelStatus) -> None:
        if s.state == TunnelState.ONLINE and not results:
            results.append(tt.stop(timeout=T))

    tt.subscribe(on_status)
    tt.start()
    (thread,) = threads_named("mcpwarp-inside")
    tt.configure(CONFIG).result(T)
    wait_until(lambda: bool(results))
    assert results == [False]
    thread.join(T)
    assert not thread.is_alive()
    assert tt.status == OFF
    assert tt.stop(timeout=T)  # joins what is left: already gone


def test_dispatch_hook_delivers_on_the_host_thread() -> None:
    q: queue.SimpleQueue[Callable[[], None]] = queue.SimpleQueue()
    tt, _ = make(dispatch=q.put)
    seen: list[tuple[TunnelStatus, int]] = []
    tt.subscribe(lambda s: seen.append((s, threading.get_ident())))
    assert seen == []  # queued, not run inline

    def drain() -> None:  # Blender's main-thread timer
        while not q.empty():
            q.get_nowait()()

    drain()
    assert seen == [(OFF, threading.get_ident())]
    tt.start()
    tt.configure(CONFIG).result(T)

    def online() -> bool:
        drain()
        return any(s == ONLINE for s, _ in seen)

    wait_until(online)
    assert tt.stop(timeout=T)
    drain()
    assert {ident for _, ident in seen} == {threading.get_ident()}
    assert [s.state for s, _ in seen] == ["off", "connecting", "online", "off"]


def test_unsubscribe_holds_across_restart() -> None:
    q: queue.SimpleQueue[Callable[[], None]] = queue.SimpleQueue()
    tt, _ = make(dispatch=q.put)
    kept: list[TunnelStatus] = []
    dropped: list[TunnelStatus] = []
    tt.subscribe(kept.append)
    unsubscribe = tt.subscribe(dropped.append)
    tt.start()
    tt.configure(CONFIG).result(T)
    wait_until(lambda: tt.status == ONLINE)
    unsubscribe()  # already queued deliveries are dropped too
    unsubscribe()
    assert tt.stop(timeout=T)
    tt.start()
    tt.configure(CONFIG).result(T)
    wait_until(lambda: tt.status == ONLINE)
    assert tt.stop(timeout=T)
    while not q.empty():
        q.get_nowait()()
    assert dropped == []
    states = ["off", "connecting", "online", "off", "connecting", "online", "off"]
    assert [s.state for s in kept] == states


def test_stop_is_bounded_against_a_hung_edge(caplog: pytest.LogCaptureFixture) -> None:
    tt, conn = make(hold=True)
    tt.start()
    tt.configure(CONFIG).result(T)
    wait_until(lambda: tt.status == ONLINE)
    with caplog.at_level(logging.WARNING):
        t0 = time.monotonic()
        assert tt.stop(timeout=0.3)
        elapsed = time.monotonic() - t0
    assert elapsed < 0.45, elapsed  # the unregister budget alone is 2 s
    assert elapsed > 0.2  # it did wait for the graceful part
    assert tt.status == OFF
    assert conn.sockets and all(ws.aborted for ws in conn.sockets)
    gc.collect()
    assert not [r for r in caplog.records if "Task was destroyed" in r.getMessage()]
    assert any("dispose() gave up" in r.getMessage() for r in caplog.records)


def test_status_pairs_stay_legal_under_churn() -> None:
    tt, _ = make()
    tt.start()
    seen: set[tuple[str, str | None]] = set()
    done = threading.Event()

    def reader() -> None:
        while not done.is_set():
            s = tt.status
            seen.add((str(s.state), None if s.code is None else str(s.code)))
            time.sleep(0)  # a GIL-hogging spin would starve the loop thread

    th = threading.Thread(target=reader)
    th.start()
    try:
        for _ in range(10):
            tt.configure(CONFIG)
            tt.disconnect()
        tt.configure(CONFIG).result(T)
        wait_until(lambda: tt.status == ONLINE)
        tt.disconnect().result(T)
    finally:
        done.set()
        th.join(T)
        assert tt.stop(timeout=T)
    assert ("online", None) in seen or ("connecting", None) in seen
    assert seen <= LEGAL, seen - LEGAL


def test_call_dispatched_runs_on_the_host_thread() -> None:
    q: queue.SimpleQueue[Callable[[], None]] = queue.SimpleQueue()
    tt, _ = make(dispatch=q.put)
    tt.start()
    lt = tt._lt

    async def roundtrip() -> int:
        return await tt.call_dispatched(threading.get_ident)

    fut = lt.submit(roundtrip())
    q.get(timeout=T)()
    assert fut.result(T) == threading.get_ident()
    assert tt.stop(timeout=T)


# --- review follow-ups --------------------------------------------------------------------------


def _recv_exact(conn: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            raise ConnectionError("client went away")
        data += chunk
    return data


def _client_message(conn: socket.socket) -> tuple[int, bytes]:
    """One masked client frame: (opcode, unmasked payload)."""
    b0, b1 = _recv_exact(conn, 2)
    length = b1 & 0x7F
    if length == 126:
        length = int.from_bytes(_recv_exact(conn, 2), "big")
    elif length == 127:
        length = int.from_bytes(_recv_exact(conn, 8), "big")
    mask = _recv_exact(conn, 4)
    payload = _recv_exact(conn, length)
    return b0 & 0x0F, bytes(b ^ mask[i % 4] for i, b in enumerate(payload))


def _server_control(msg: dict[str, Any]) -> bytes:
    """An unmasked binary server frame carrying one stream-0 control message."""
    payload = encode_data(0, encode_control(msg))
    n = len(payload)
    assert n < 0x10000
    head = bytes([0x82, n]) if n < 126 else bytes([0x82, 126]) + n.to_bytes(2, "big")
    return head + payload


@contextmanager
def quiet_edge() -> Iterator[tuple[str, threading.Thread]]:
    """A real TCP WebSocket server: upgrade, welcome, `registered` for the register, and then
    never another word (no `unregistered`, no close echo). Its thread ends when the client's
    side of the connection goes away."""
    srv = socket.create_server(("127.0.0.1", 0))
    srv.settimeout(T)
    accepted: list[socket.socket] = []

    def serve() -> None:
        try:
            conn, _ = srv.accept()
        except OSError:
            return
        accepted.append(conn)
        conn.settimeout(T)
        try:
            head = b""
            while b"\r\n\r\n" not in head:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                head += chunk
            key = next(
                line.split(b":", 1)[1].strip().decode()
                for line in head.split(b"\r\n")
                if line.lower().startswith(b"sec-websocket-key:")
            )
            conn.sendall(
                (
                    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                    f"Connection: Upgrade\r\nSec-WebSocket-Accept: {accept_key(key)}\r\n"
                    f"Sec-WebSocket-Protocol: {SUBPROTOCOL}\r\n\r\n"
                ).encode()
            )
            conn.sendall(_server_control(dict(WELCOME)))
            while True:
                opcode, payload = _client_message(conn)
                if opcode != 0x2:
                    continue  # the client's close goes unanswered
                frame = decode_frame(payload)
                if frame.stream_id != 0 or frame.type != FrameType.DATA:
                    continue
                msg = json.loads(frame.payload)
                env = msg.get("body", {}).get("mcpwarp", {}) if msg["t"] == "app" else {}
                if env.get("op") == "register":
                    name = env["services"][0]["name"]
                    svc = {"name": name, "url": URL, "id": f"srv_{name}", "created": True}
                    body = {"mcpwarp": {"v": 1, "op": "registered", "services": [svc]}}
                    conn.sendall(_server_control({"t": "app", "body": body}))
        except OSError:
            pass

    th = threading.Thread(target=serve, name="quiet-edge")
    th.start()
    try:
        yield f"ws://127.0.0.1:{srv.getsockname()[1]}", th
    finally:
        srv.close()
        for conn in accepted:
            conn.close()
        th.join(T)


def test_stop_zero_with_a_live_fake_socket() -> None:
    tt, conn = make(hold=True)
    seen: list[TunnelStatus] = []
    tt.subscribe(seen.append)
    tt.start()
    tt.configure(CONFIG).result(T)
    wait_until(lambda: tt.status == ONLINE)
    tt.stop(timeout=0)
    assert tt.stop(timeout=T)
    assert conn.sockets and all(ws.aborted for ws in conn.sockets)
    assert [s.state for s in seen] == ["off", "connecting", "online", "off"]


def test_stop_zero_with_a_real_socket_leaks_no_transport() -> None:
    tt = ThreadedTunnel(integration=UUID, handler=handler, thread_name="mcpwarp-real")
    seen: list[TunnelStatus] = []
    tt.subscribe(seen.append)
    with quiet_edge() as (url, server):
        tt.start()
        tt.configure(TunnelConfig(True, "tok-6f1e9d", "notes", url=url)).result(T)
        wait_until(lambda: tt.status == ONLINE)
        tt.stop(timeout=0)
        assert tt.stop(timeout=T)
        server.join(T)
        assert not server.is_alive()  # the client side went away: the transport was aborted
    gc.collect()
    assert seen[-1] == OFF
    assert "error" not in [s.state for s in seen]


@pytest.mark.parametrize("timeout", [math.inf, 1e300, math.nan, -1.0])
def test_stop_takes_any_float(timeout: float) -> None:
    tt, conn = make()
    tt.start()
    tt.configure(CONFIG).result(T)
    wait_until(lambda: tt.status == ONLINE)
    t0 = time.monotonic()
    joined = tt.stop(timeout=timeout)  # no OverflowError; NaN and -1 count as 0
    assert time.monotonic() - t0 < 0.5
    if timeout > 0:
        assert joined  # clamped to an hour, done long before
    assert tt.stop(timeout=T)
    assert all(ws.aborted or ws.close_calls for ws in conn.sockets)


def test_loop_bound_clocks_are_refused() -> None:
    with pytest.raises(ValueError, match="LoopClock"):
        ThreadedTunnel(integration=UUID, handler=handler, clock=LoopClock())
    with pytest.raises(ValueError, match="LoopClock"):
        ThreadedTunnel(
            integration=UUID, handler=handler, web_socket=StdlibConnector(clock=LoopClock())
        )


def test_duck_typed_config_connector_is_tracked() -> None:
    tt, default = make()
    own = connector(hold=True)
    cfg = SimpleNamespace(
        enabled=True, token="tok-6f1e9d", name="notes", url=None, kind=None, web_socket=own
    )
    tt.start()
    tt.configure(cfg).result(T)  # type: ignore[arg-type]
    wait_until(lambda: tt.status == ONLINE)
    assert tt.stop(timeout=0.3)
    assert default.dials == 0
    assert own.sockets and all(ws.aborted for ws in own.sockets)


@pytest.mark.parametrize("delegate", [True, False])
def test_tracked_connectors_compare_by_what_they_wrap(
    delegate: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    if not delegate:
        monkeypatch.setattr(_Tracked, "__eq__", lambda self, other: self is other)
    tt, _ = make()
    own = connector()
    cfg = TunnelConfig(True, "tok-6f1e9d", "notes", web_socket=own)
    with tt:
        tt.configure(cfg).result(T)
        wait_until(lambda: tt.status == ONLINE)
        tt.configure(cfg).result(T)  # the same config: a no-op only if the wrappers compare equal
        wait_until(lambda: tt.status == ONLINE)
    assert own.dials == (1 if delegate else 2)


def test_a_dial_landing_after_the_teardown_is_aborted() -> None:
    sockets = _Sockets()
    sockets.aborted = True

    async def dial() -> FakeWebSocket:
        ws = FakeWebSocket()
        ws.open()

        async def inner(url: str, init: WebSocketInit) -> WebSocketConnection:
            return ws

        await _Tracked(inner, sockets)("ws://edge.test", WebSocketInit())
        return ws

    ws = asyncio.run(dial())
    assert ws.aborted
    assert sockets.all() == [ws]


def test_start_waits_for_a_stop_running_on_another_thread() -> None:
    tt, _ = make(hold=True)
    tt.start()
    tt.configure(CONFIG).result(T)
    wait_until(lambda: tt.status == ONLINE)
    results: list[bool] = []
    stopper = threading.Thread(target=lambda: results.append(tt.stop(timeout=0.4)))
    stopper.start()
    wait_until(lambda: tt.status == OFF)  # stop() has taken the run
    t0 = time.monotonic()
    tt.start()
    waited = time.monotonic() - t0
    stopper.join(T)
    assert results == [True]
    assert waited > 0.2, waited  # the dispose part alone is 0.32 s
    tt.configure(CONFIG).result(T)
    wait_until(lambda: tt.status == ONLINE)
    assert tt.stop(timeout=0.3)


def test_start_on_the_loop_thread_of_a_run_being_stopped_raises() -> None:
    tt, _ = make("mcpwarp-restart")
    errors: list[BaseException] = []

    def on_status(s: TunnelStatus) -> None:
        if s.state == TunnelState.ONLINE and not errors:
            assert tt.stop() is False
            try:
                tt.start()
            except RuntimeError as e:
                errors.append(e)

    tt.subscribe(on_status)
    tt.start()
    (thread,) = threads_named("mcpwarp-restart")
    tt.configure(CONFIG).result(T)
    wait_until(lambda: bool(errors))
    assert "still in progress" in str(errors[0])
    thread.join(T)
    assert not thread.is_alive()
    assert tt.stop(timeout=T)


def test_unsubscribe_during_attach_leaves_no_listener() -> None:
    unsubscribe: list[Callable[[], None]] = []
    ran: list[TunnelStatus] = []
    ready = threading.Event()  # `unsubscribe` is filled in

    def dispatch(fn: Callable[[], None]) -> None:
        ready.wait(T)
        if unsubscribe:
            unsubscribe.pop()()  # inside Tunnel.subscribe(), on the loop thread
        fn()

    tt, _ = make(dispatch=dispatch)
    tt.start()
    wait_until(lambda: tt._gen is not None and tt._gen.tunnel is not None)
    gen = tt._gen
    assert gen is not None and gen.tunnel is not None
    holder: list[Callable[[], None]] = []
    holder.append(tt.subscribe(ran.append))
    unsubscribe.extend(holder)
    ready.set()
    wait_until(lambda: not unsubscribe)
    tt.configure(CONFIG).result(T)
    wait_until(lambda: tt.status == ONLINE)
    assert gen.tunnel._listeners == {}
    assert ran == []
    assert tt.stop(timeout=T)
