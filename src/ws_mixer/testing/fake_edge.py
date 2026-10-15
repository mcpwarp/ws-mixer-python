"""An in-process fake of the mcpwarp edge (ported from ws-mixer-js's src/testing/fake-edge.ts).

The agent dials it through `edge.web_socket`, registers over the mcpwarp control envelope, and
test code sends HTTP requests to the public URL with `edge.request()`: each one becomes a
ws-mixer stream carrying raw HTTP/1.1 in Go `Request.Write` shape, as the real edge writes it.
Each connection is the server half of a `socket_pair()`, so frames, flow control (WINDOW
credit-back both ways), CLOSE and RESET are real; every timer goes through the given Clock.

Known differences from the real edge: it writes the request body while already reading the
response (the edge writes the whole request first); `enable` goes only to the routed (or
latest) connection (the edge sends it to every connection of the owner); and a second
connection registering a held name just takes the route (the edge also sends the old holder
`conflict`; here that is `edge.conflict()`).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, TypeVar
from urllib.parse import urlsplit

from .._http1 import (
    HOP_BY_HOP,
    MAX_HEAD_SIZE,
    ChunkedDecoder,
    HttpParseError,
    connection_tokens,
    is_bodyless,
    parse_response_head,
)
from .._jscompat import truncate_utf8
from ..clock import Clock, LoopClock, TimerHandle
from ..control import DEFAULT_MAX_STREAMS, DEFAULT_WINDOW, Hello, encode_control, parse_control
from ..errors import ConnError, ErrorCode, StreamError, WsMixerError, close_code, wire_close_code
from ..frame import (
    MAX_CHUNK,
    FrameType,
    decode_frame,
    encode_data,
    encode_open,
    encode_reset,
    reset_code,
    reset_message,
    window_increment,
)
from ..socket import (
    MAX_CLOSE_REASON,
    SUBPROTOCOL,
    UpgradeRejected,
    WebSocketConnection,
    WebSocketInit,
)
from ..stream import MixerStream, StreamCloseReason
from .fake_socket import socket_pair

_Close = tuple[int, str]
_T = TypeVar("_T")


@dataclass(frozen=True)
class FakeEdgeUpgrade:
    url: str
    protocols: tuple[str, ...]
    headers: dict[str, str]


@dataclass(frozen=True)
class EdgeResponse:
    """A response read back through the agent. `headers` has lowercase names, every value in
    order; `end` is how the agent ended the stream (Finished, or Reset after a complete
    response it did not close cleanly)."""

    status: int
    reason: str
    headers: dict[str, list[str]]
    body: bytes
    end: StreamCloseReason | None = None

    def header(self, name: str) -> str | None:
        values = self.headers.get(name.lower())
        return ", ".join(values) if values else None

    @property
    def text(self) -> str:
        return self.body.decode("utf-8")


@dataclass
class _Service:
    conn: EdgeConnection | None = None
    disabled: bool = False
    ever_registered: bool = False
    failure: dict[str, str] | None = None
    failure_times: float = 0
    withhold: int = 0
    withhold_unregistered: int = 0
    open: set[MixerStream] = field(default_factory=set)


class _StreamHost:
    __slots__ = ("_conn",)

    def __init__(self, conn: EdgeConnection) -> None:
        self._conn = conn

    def send_data(self, stream_id: int, chunk: bytes) -> asyncio.Future[None]:
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if self._conn._enqueue(encode_data(stream_id, chunk)):
            fut.set_result(None)
        else:
            fut.set_exception(ConnError(ErrorCode.INTERNAL_ERROR, "fake edge: connection closed"))
        return fut

    def send_control_frame(self, frame: bytes) -> None:
        self._conn._enqueue(frame)

    def retire_stream(self, stream_id: int) -> None:
        self._conn._retire(stream_id)


class EdgeConnection:
    """The server side of one fake-edge connection: hello/welcome, ping/pong, `app` routing,
    server-opened streams (odd ids), drain and close."""

    def __init__(
        self,
        edge: FakeEdge,
        socket: WebSocketConnection,
        *,
        session: str,
        window: int,
        max_streams: int,
        ping_interval: int,
        ping_timeout: int,
    ) -> None:
        loop = asyncio.get_running_loop()
        self.session = session
        self.hello: Hello | None = None
        self.controls: list[dict[str, Any]] = []
        """Every control message received from the agent, hello included, in order."""
        self.closed = False
        """Torn down: no more frames in or out."""
        self._edge = edge
        self._socket = socket
        self._window = window
        self._max_streams = max_streams
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._welcomed = False
        self._peer_window = DEFAULT_WINDOW
        self._next_id = 1
        self._highest_opened = 0
        self._draining = False
        self._sealed = False
        self._drain_timer: TimerHandle | None = None
        self._streams: dict[int, MixerStream] = {}
        self._host = _StreamHost(self)
        self._out: asyncio.Queue[bytes | _Close] = asyncio.Queue()
        self._writer = loop.create_task(self._run_writer())
        self._reader = loop.create_task(self._run_reader())

    def __repr__(self) -> str:
        return f"<EdgeConnection {self.session} {'closed' if self.closed else 'open'}>"

    # --- actions -----------------------------------------------------------------------------

    def open_stream(self) -> MixerStream:
        """Opens a stream toward the agent. Raises RuntimeError once closed or draining."""
        if self.closed or not self._welcomed:
            raise RuntimeError("fake edge: connection is not usable")
        if self._draining:
            raise RuntimeError("fake edge: connection is draining")
        sid = self._next_id
        self._next_id += 2
        self._highest_opened = sid
        stream = MixerStream(sid, self._host, self._window, self._peer_window)
        self._streams[sid] = stream
        self._enqueue(encode_open(sid))
        return stream

    def send_control(self, msg: Mapping[str, Any]) -> None:
        if not self.closed:
            self._enqueue(encode_data(0, encode_control(msg)))

    def send_app(self, body: Mapping[str, Any]) -> None:
        self.send_control({"t": "app", "body": dict(body)})

    def drain(self, reason: str, deadline_ms: int) -> None:
        """drain: no new streams; survivors are RESET(CANCEL) at the deadline, then
        error{GOING_AWAY} + close 4012 (earlier once none are left)."""
        if self.closed or self._draining:
            return
        self._draining = True
        self.send_control(
            {
                "t": "drain",
                "reason": reason,
                "last_stream_id": self._highest_opened,
                "deadline_ms": deadline_ms,
            }
        )
        if not self._streams:
            self._close_going_away()
            return
        self._drain_timer = self._edge._clock.call_later(deadline_ms, self._on_drain_deadline)

    def close(self, ws_code: int, error_code: int | None = None, message: str = "") -> None:
        """Closes with `ws_code`, sending error{error_code, message} first when given."""
        if self.closed:
            return
        if error_code is not None:
            self.send_control({"t": "error", "code": error_code, "message": message})
        self._seal(ws_code, truncate_utf8(message, MAX_CLOSE_REASON))
        self._teardown()

    # --- internals ---------------------------------------------------------------------------

    def _enqueue(self, item: bytes) -> bool:
        if self._sealed:
            return False
        self._out.put_nowait(item)
        return True

    def _seal(self, code: int, reason: str = "") -> None:
        if not self._sealed:
            self._sealed = True
            self._out.put_nowait((code, reason))

    async def _run_writer(self) -> None:
        while True:
            item = await self._out.get()
            if isinstance(item, tuple):
                try:
                    await self._socket.close(*item)
                except Exception:
                    pass
                return
            try:
                await self._socket.send(item)
            except Exception:
                return

    async def _run_reader(self) -> None:
        while True:
            try:
                data = await self._socket.recv()
            except Exception:
                break
            if self.closed:
                continue
            if isinstance(data, str):
                self._fail(ConnError(ErrorCode.PROTOCOL_ERROR, "text frame received"))
                continue
            self._on_message(data)
        self._teardown()
        if not self._writer.done():
            self._writer.cancel()

    def _on_message(self, data: bytes) -> None:
        try:
            frame = decode_frame(data)
            if frame.stream_id == 0:
                if frame.type == FrameType.DATA:
                    self._on_control(frame.payload)
                return
            if not self._welcomed:
                raise ConnError(ErrorCode.PROTOCOL_ERROR, "frame before welcome")
            if frame.type == FrameType.OPEN:
                raise ConnError(ErrorCode.PROTOCOL_ERROR, "the agent opened a stream")
            stream = self._streams.get(frame.stream_id)
            if stream is None:
                return
            if frame.type == FrameType.DATA:
                stream.handle_data(frame.payload)
            elif frame.type == FrameType.WINDOW:
                stream.handle_window(window_increment(frame))
            elif frame.type == FrameType.CLOSE:
                stream.handle_close()
            elif frame.type == FrameType.RESET:
                stream.handle_reset(reset_code(frame), reset_message(frame))
        except StreamError as e:
            sid = e.stream_id or 0
            live = self._streams.get(sid)
            if live is not None:
                live.abort(e.code, e.message)
            else:
                self._enqueue(encode_reset(sid, e.code, e.message))
        except WsMixerError as e:
            self._fail(e)
        except Exception as e:
            self._fail(ConnError(ErrorCode.INTERNAL_ERROR, f"fake edge: {e!r}"))

    def _on_control(self, payload: bytes) -> None:
        msg: Any = parse_control(payload)
        self.controls.append(msg)
        t = msg["t"]
        if not self._welcomed:
            if t != "hello":
                raise ConnError(ErrorCode.PROTOCOL_ERROR, f"expected hello, got {t}")
            self.hello = msg
            self._edge.hellos.append(msg)
            if not self._edge._accept_token(msg["token"]):
                self.close(4000 + ErrorCode.UNAUTHORIZED, ErrorCode.UNAUTHORIZED, "invalid token")
                return
            self._peer_window = msg.get("window", DEFAULT_WINDOW)
            self._welcomed = True
            self.send_control(
                {
                    "t": "welcome",
                    "v": 1,
                    "session": self.session,
                    "window": self._window,
                    "max_streams": self._max_streams,
                    "ping_interval": self._ping_interval,
                    "ping_timeout": self._ping_timeout,
                    "server": {"name": "fake-edge"},
                }
            )
            return
        if t == "ping":
            pong: dict[str, Any] = {"t": "pong", "id": msg["id"]}
            if "ts" in msg:
                pong["ts"] = msg["ts"]
            self.send_control(pong)
        elif t == "app":
            self._edge._on_app(self, msg["body"])
        elif t == "drain":
            # The agent is closing (client_requested): open nothing new.
            self._draining = True
        elif t == "error":
            # `error` is the agent's last message; it closes right behind it.
            self._seal(wire_close_code(close_code(msg["code"])))
            self._teardown()
        elif t != "pong":
            raise ConnError(ErrorCode.PROTOCOL_ERROR, f"unexpected {t} from the agent")

    def _fail(self, err: WsMixerError) -> None:
        self.close(wire_close_code(close_code(err.code)), err.code, err.message)

    def _close_going_away(self) -> None:
        self.close(4000 + ErrorCode.GOING_AWAY, ErrorCode.GOING_AWAY, "drained")

    def _on_drain_deadline(self) -> None:
        self._drain_timer = None
        for stream in list(self._streams.values()):
            stream.reset(ErrorCode.CANCEL, "drain deadline")
        self._close_going_away()

    def _retire(self, stream_id: int) -> None:
        self._streams.pop(stream_id, None)
        if self._draining and not self._streams and not self.closed:
            # After the stream's own last frame, already queued.
            asyncio.get_running_loop().call_soon(self._close_going_away)

    def _teardown(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._sealed = True
        if self._drain_timer is not None:
            self._drain_timer.cancel()
            self._drain_timer = None
        err = ConnError(ErrorCode.INTERNAL_ERROR, "fake edge: connection closed")
        streams, self._streams = list(self._streams.values()), {}
        for stream in streams:
            stream.terminate(err)
        self._edge._on_closed(self)


class _Reader:
    def __init__(self, stream: MixerStream) -> None:
        self._stream = stream
        self.buf = bytearray()
        self.eof = False

    async def fill(self) -> bool:
        if self.eof:
            return False
        chunk = await self._stream.read()
        if not chunk:
            self.eof = True
            return False
        self.buf += chunk
        return True


class FakeEdge:
    """The mcpwarp edge, in memory. Call `await start()` before dialing it."""

    def __init__(
        self,
        *,
        username: str = "tester",
        domain: str = "mcpwarp.test",
        window: int = DEFAULT_WINDOW,
        max_streams: int = DEFAULT_MAX_STREAMS,
        ping_interval: int = 30_000,
        ping_timeout: int = 90_000,
        request_timeout_ms: float = 30_000,
        accept_token: Callable[[str], bool] | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.url = f"wss://connect.{domain}"
        self.upgrades: list[FakeEdgeUpgrade] = []
        self.hellos: list[Hello] = []
        self.app_log: list[dict[str, Any]] = []
        """Every `app` body received from agents, in order."""
        self.connections: list[EdgeConnection] = []
        """Live connections, oldest first."""
        self._username = username
        self._domain = domain
        self._window = window
        self._max_streams = max_streams
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._request_timeout_ms = request_timeout_ms
        self._accept: Callable[[str], bool] = accept_token or (lambda token: len(token) > 0)
        self._clock: Clock = clock or LoopClock()
        self._started = False
        self._sessions = 0
        self._pending_reject: tuple[int, Mapping[str, str] | None] | None = None
        self._services: dict[str, _Service] = {}
        self._waiters: list[Callable[[], None]] = []
        self._requests: set[MixerStream] = set()
        self._tasks: set[asyncio.Task[None]] = set()

    # --- lifecycle and dialing ---------------------------------------------------------------

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        """Closes every connection (1001) and cancels pending requests."""
        self._started = False
        for stream in list(self._requests):
            stream.reset(ErrorCode.CANCEL, "fake edge stopped")
        for conn in list(self.connections):
            conn.close(1001)

    async def web_socket(self, url: str, init: WebSocketInit) -> WebSocketConnection:
        """The WebSocketConnector to pass as `web_socket`."""
        self.upgrades.append(FakeEdgeUpgrade(url, tuple(init.protocols), dict(init.headers)))
        if not self._started:
            raise ConnectionRefusedError("fake edge: not started")
        reject, self._pending_reject = self._pending_reject, None
        if reject is not None:
            raise UpgradeRejected(*reject)
        subprotocol = SUBPROTOCOL if SUBPROTOCOL in init.protocols else None
        client, server = socket_pair(subprotocol)
        self._sessions += 1
        conn = EdgeConnection(
            self,
            server,
            session=f"fake-{self._sessions}",
            window=self._window,
            max_streams=self._max_streams,
            ping_interval=self._ping_interval,
            ping_timeout=self._ping_timeout,
        )
        self.connections.append(conn)
        return client

    def reject_next_upgrade(self, status: int, headers: Mapping[str, str] | None = None) -> None:
        """The next dial is refused at the HTTP upgrade (`UpgradeRejected`)."""
        self._pending_reject = (status, headers)

    # --- the registry ------------------------------------------------------------------------

    def public_url(self, name: str) -> str:
        return f"https://{self._public_host(name)}/mcp"

    def server_id(self, name: str) -> str:
        return f"srv_{name}"

    def fail_register(self, name: str, error: Mapping[str, str] | None, times: float = 1) -> None:
        """The next register(s) for `name` fail with `error` ({code, message?, id?}); `times`
        may be math.inf, until cleared with None."""
        s = self._service(name)
        s.failure = None if error is None else dict(error)
        s.failure_times = times if error is not None else 0

    def withhold_registered(self, name: str, times: int = 1) -> None:
        """The next register(s) for `name` route it to the connection but get no `registered`
        reply, as when the edge's `conflict` overtakes it (follow with `conflict(name)`)."""
        self._service(name).withhold = times

    def withhold_unregistered(self, name: str, times: int = 1) -> None:
        """The next unregister(s) naming `name` get no `unregistered` reply (the name is still
        released), so the agent waits out its unregister budget."""
        self._service(name).withhold_unregistered = times

    async def wait_for_app(
        self, op: str, count: int = 1, timeout_ms: float = 5000
    ) -> dict[str, Any]:
        """The `count`-th app body ever received whose `mcpwarp.op` is `op`."""

        def find() -> dict[str, Any] | None:
            seen = [b for b in self.app_log if _op_of(b) == op]
            return seen[count - 1] if len(seen) >= count else None

        return await self._wait_until(find, timeout_ms, f"app op {op} #{count}")

    async def wait_for_registered(self, name: str, timeout_ms: float = 5000) -> tuple[str, str]:
        """(server id, public URL) once `name` is routed to a connection."""

        def find() -> tuple[str, str] | None:
            s = self._services.get(name)
            if s is None or s.conn is None:
                return None
            return self.server_id(name), self.public_url(name)

        return await self._wait_until(find, timeout_ms, f"registration of {name}")

    # --- edge-initiated messages -------------------------------------------------------------

    def send_app(self, body: Mapping[str, Any], conn: EdgeConnection | None = None) -> None:
        self._conn_for(conn).send_app(body)

    def disable(self, name: str, reason: str = "disabled") -> None:
        """RESET(CANCEL)s the service's open streams, marks it disabled, sends disable."""
        s = self._service(name)
        s.disabled = True
        for stream in list(s.open):
            stream.reset(ErrorCode.CANCEL, "service disabled")
        if s.conn is not None:
            s.conn.send_app(_env("disable", id=self.server_id(name), reason=reason))

    def enable(self, name: str) -> None:
        """Marks the service enabled and sends enable{id, name}."""
        s = self._service(name)
        s.disabled = False
        target = s.conn or self._latest()
        if target is not None:
            target.send_app(_env("enable", id=self.server_id(name), name=name))

    def conflict(self, name: str, conn: EdgeConnection | None = None) -> None:
        """What the edge does to the old holder when another connection registers `name`:
        RESET(CANCEL)s its open streams, unroutes it, sends conflict{id, name, "taken_over"}
        to `conn` (default: the holder, else the latest connection)."""
        s = self._service(name)
        holder = conn or s.conn or self._latest()
        for stream in list(s.open):
            stream.reset(ErrorCode.CANCEL, "server taken over")
        if s.conn is holder:
            s.conn = None
        if holder is not None:
            holder.send_app(
                _env("conflict", id=self.server_id(name), name=name, reason="taken_over")
            )

    def app_error(self, code: str, message: str = "", conn: EdgeConnection | None = None) -> None:
        self._conn_for(conn).send_app(_env("error", code=code, message=message))

    def drain(
        self,
        reason: str = "rollout",
        deadline_ms: int = 30_000,
        conn: EdgeConnection | None = None,
    ) -> None:
        """drain on `conn` (default: the latest); at `deadline_ms` survivors are RESET and it
        closes 4012."""
        self._conn_for(conn).drain(reason, deadline_ms)

    def close_connection(
        self,
        ws_code: int,
        error_code: int | None = None,
        message: str = "",
        conn: EdgeConnection | None = None,
    ) -> None:
        self._conn_for(conn).close(ws_code, error_code, message)

    # --- requests ----------------------------------------------------------------------------

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | Iterable[tuple[str, str]] = (),
        body: bytes | str | None = None,
        chunked: bool = False,
        conn: EdgeConnection | None = None,
    ) -> EdgeResponse:
        """Sends one HTTP request to a public URL through the routed agent, as the real edge
        does, and reads the whole response.

        `chunked` frames a body as chunks instead of Content-Length. `conn` sends it on that
        connection, bypassing routing (and the edge's own 503 for a disabled service). A body
        given as str gets `content-type: text/plain;charset=UTF-8` unless one is set. Raises
        StreamError when the agent RESETs the stream, TimeoutError without a response head
        within `request_timeout_ms`.
        """
        parts = urlsplit(url)
        host = parts.netloc
        method = method.upper()
        fields = list(headers.items()) if isinstance(headers, Mapping) else list(headers)
        if conn is None:
            name = self._service_for_host(host)
            if name is None:
                raise ValueError(f"fake edge: {host} is not a service host of {self._domain}")
            svc = self._services.get(name)
            if svc is None or svc.conn is None:
                raise RuntimeError(f"fake edge: no agent registered for {name}")
            if svc.disabled:
                return EdgeResponse(
                    503,
                    "Service Unavailable",
                    {"content-type": ["application/json"]},
                    b'{"error":"service disabled"}',
                )
            target_conn = svc.conn
        else:
            svc = None
            target_conn = conn

        data: bytes | None = None
        if body is not None:
            if isinstance(body, str):
                data = body.encode("utf-8")
                if not any(n.lower() == "content-type" for n, _ in fields):
                    fields.append(("content-type", "text/plain;charset=UTF-8"))
            else:
                data = bytes(body)
        framing: str | int = "none" if data is None else "chunked" if chunked else len(data)
        target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        head = _request_head(method, target, host, fields, framing)

        stream = target_conn.open_stream()
        self._requests.add(stream)
        if svc is not None:
            svc.open.add(stream)
        writer = asyncio.get_running_loop().create_task(_write_request(stream, head, data, chunked))
        self._tasks.add(writer)
        writer.add_done_callback(self._tasks.discard)
        try:
            return await self._read_response(stream, method, writer)
        finally:
            self._requests.discard(stream)
            if svc is not None:
                svc.open.discard(stream)
            if not writer.done():
                writer.cancel()

    async def _read_response(
        self, stream: MixerStream, method: str, writer: asyncio.Task[None]
    ) -> EdgeResponse:
        reader = _Reader(stream)
        timed_out = False

        def on_timeout() -> None:
            nonlocal timed_out
            timed_out = True
            stream.reset(ErrorCode.CANCEL, "response head timeout")

        timer = self._clock.call_later(self._request_timeout_ms, on_timeout)
        try:
            while True:
                try:
                    status, reason, headers, consumed = parse_response_head(bytes(reader.buf))
                    break
                except HttpParseError as e:
                    if str(e) != "incomplete head" or len(reader.buf) >= MAX_HEAD_SIZE:
                        raise
                if not await reader.fill():
                    raise ConnectionError("fake edge: stream ended before the response head")
        except StreamError:
            if timed_out:
                raise TimeoutError(
                    f"fake edge: no response head within {self._request_timeout_ms:g}ms"
                ) from None
            raise
        finally:
            timer.cancel()
        del reader.buf[:consumed]
        if status < 200:
            stream.reset(ErrorCode.CANCEL, "interim response")
            raise ValueError("fake edge: the agent sent an interim 1xx response")
        body = await _read_body(reader, method, status, headers)
        if not writer.done():
            # The agent answered before consuming the whole request body: abandon the rest.
            stream.reset(ErrorCode.CANCEL, "response complete")
        end = await stream.wait_closed()
        return EdgeResponse(status, reason, headers, body, end)

    # --- internals ---------------------------------------------------------------------------

    def _public_host(self, name: str) -> str:
        return f"{name}-{self._username}.{self._domain}"

    def _service_for_host(self, host: str) -> str | None:
        suffix = f"-{self._username}.{self._domain}"
        h = host.lower()
        return h[: -len(suffix)] if h.endswith(suffix) and len(h) > len(suffix) else None

    def _service(self, name: str) -> _Service:
        s = self._services.get(name)
        if s is None:
            s = self._services[name] = _Service()
        return s

    def _latest(self) -> EdgeConnection | None:
        return self.connections[-1] if self.connections else None

    def _conn_for(self, conn: EdgeConnection | None) -> EdgeConnection:
        c = conn or self._latest()
        if c is None:
            raise RuntimeError("fake edge: no live connection")
        return c

    def _accept_token(self, token: str) -> bool:
        return self._accept(token)

    async def _wait_until(self, find: Callable[[], _T | None], timeout_ms: float, what: str) -> _T:
        found = find()
        if found is not None:
            return found
        fut: asyncio.Future[_T] = asyncio.get_running_loop().create_future()

        def check() -> None:
            value = find()
            if value is not None and not fut.done():
                fut.set_result(value)

        def expire() -> None:
            if not fut.done():
                fut.set_exception(TimeoutError(f"fake edge: timed out waiting for {what}"))

        self._waiters.append(check)
        timer = self._clock.call_later(timeout_ms, expire)
        try:
            return await fut
        finally:
            timer.cancel()
            self._waiters.remove(check)

    def _notify(self) -> None:
        for check in list(self._waiters):
            check()

    def _on_closed(self, conn: EdgeConnection) -> None:
        if conn in self.connections:
            self.connections.remove(conn)
        for s in self._services.values():
            if s.conn is conn:
                s.conn = None
        self._notify()

    def _on_app(self, conn: EdgeConnection, body: dict[str, Any]) -> None:
        self.app_log.append(body)
        env = body.get("mcpwarp")
        if isinstance(env, dict):
            v = env.get("v")
            op = env.get("op")
            if v != 1 or isinstance(v, bool):
                conn.send_app(
                    _env("error", code="UNSUPPORTED_VERSION", message=f"unsupported version {v}")
                )
            elif op == "register":
                self._on_register(conn, env.get("services"))
            elif op == "unregister":
                self._on_unregister(conn, env.get("services"))
            else:
                conn.send_app(_env("error", code="UNKNOWN_OP", message=f"unknown op {op}"))
        self._notify()

    def _on_register(self, conn: EdgeConnection, raw: object) -> None:
        services: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        withheld = False
        for item in raw if isinstance(raw, list) else []:
            name = item.get("name") if isinstance(item, dict) else None
            if not isinstance(name, str):
                continue
            s = self._service(name)
            if s.withhold > 0:
                s.withhold -= 1
                s.conn = conn
                s.ever_registered = True
                withheld = True
                continue
            if s.failure is not None and s.failure_times > 0:
                failure = s.failure
                entry = {
                    "name": name,
                    "code": failure["code"],
                    "message": failure.get("message", failure["code"].lower()),
                }
                if "id" in failure:
                    entry["id"] = failure["id"]
                errors.append(entry)
                s.failure_times -= 1
                if s.failure_times <= 0:
                    s.failure = None
                continue
            if s.disabled:
                errors.append(
                    {
                        "name": name,
                        "code": "SERVER_DISABLED",
                        "message": "server disabled",
                        "id": self.server_id(name),
                    }
                )
                continue
            s.conn = conn
            services.append(
                {
                    "name": name,
                    "id": self.server_id(name),
                    "url": self.public_url(name),
                    "created": not s.ever_registered,
                }
            )
            s.ever_registered = True
        if withheld and not services and not errors:
            return
        conn.send_app(_env("registered", services=services, errors=errors))

    def _on_unregister(self, conn: EdgeConnection, raw: object) -> None:
        services: list[dict[str, Any]] = []
        withheld = False
        for item in raw if isinstance(raw, list) else []:
            name = item.get("name") if isinstance(item, dict) else None
            if not isinstance(name, str):
                continue
            s = self._services.get(name)
            if s is not None and s.withhold_unregistered > 0:
                s.withhold_unregistered -= 1
                withheld = True
            # Like the edge: names this connection does not hold are skipped.
            if s is None or s.conn is not conn:
                continue
            s.conn = None
            services.append({"name": name, "id": self.server_id(name)})
        if not withheld:
            conn.send_app(_env("unregistered", services=services))


def _env(op: str, **fields: Any) -> dict[str, Any]:
    return {"mcpwarp": {"v": 1, "op": op, **fields}}


def _op_of(body: Mapping[str, Any]) -> object:
    env = body.get("mcpwarp")
    return env.get("op") if isinstance(env, dict) else None


def _request_head(
    method: str, target: str, host: str, fields: list[tuple[str, str]], framing: str | int
) -> bytes:
    """Go's Request.Write shape: request line, Host, User-Agent, the forwardable fields, then
    the body framing. Hop-by-hop, Connection-listed and Authorization fields are dropped."""
    listed = connection_tokens(fields)
    kept = [
        (n, v)
        for n, v in fields
        if (low := n.lower()) not in HOP_BY_HOP
        and low not in listed
        and low not in ("authorization", "host", "content-length", "transfer-encoding")
    ]
    if not any(n.lower() == "user-agent" for n, _ in kept):
        kept.insert(0, ("User-Agent", "Go-http-client/1.1"))
    lines = [f"{method} {target} HTTP/1.1", f"Host: {host}"]
    lines += [f"{n}: {v}" for n, v in kept]
    if framing == "chunked":
        lines.append("Transfer-Encoding: chunked")
    elif framing != "none":
        lines.append(f"Content-Length: {framing}")
    return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")


async def _write_request(
    stream: MixerStream, head: bytes, data: bytes | None, chunked: bool
) -> None:
    try:
        await stream.write(head)
        if data is not None:
            for i in range(0, len(data), MAX_CHUNK):
                piece = data[i : i + MAX_CHUNK]
                if chunked:
                    await stream.write(b"%x\r\n%b\r\n" % (len(piece), piece))
                else:
                    await stream.write(piece)
            if chunked:
                await stream.write(b"0\r\n\r\n")
        await stream.close_write()
    except WsMixerError:
        pass


async def _read_body(
    reader: _Reader, method: str, status: int, headers: dict[str, list[str]]
) -> bytes:
    if is_bodyless(method, status):
        return b""
    te = headers.get("transfer-encoding")
    lengths = headers.get("content-length")
    if te:
        decoder = ChunkedDecoder()
        parts = decoder.feed(bytes(reader.buf))
        reader.buf.clear()
        while not decoder.done:
            if not await reader.fill():
                raise ConnectionError("fake edge: response ended inside a chunked body")
            parts += decoder.feed(bytes(reader.buf))
            reader.buf.clear()
        return b"".join(parts)
    if lengths:
        length = int(lengths[0])
        while len(reader.buf) < length:
            if not await reader.fill():
                raise ConnectionError("fake edge: response body shorter than Content-Length")
        return bytes(reader.buf[:length])
    while await reader.fill():
        pass
    return bytes(reader.buf)


__all__ = ["EdgeConnection", "EdgeResponse", "FakeEdge", "FakeEdgeUpgrade"]
