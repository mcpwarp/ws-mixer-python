# API reference

Every public name exported by `ws_mixer`, `ws_mixer.mcpwarp`, `ws_mixer.mcpwarp.threaded`,
`ws_mixer.adapters.websockets` and `ws_mixer.testing`, with its signature and purpose. Derived
from the code at 0.1.0; docstrings in `src/` have the full detail.

Conventions:

- `None` means "absent" everywhere (status fields, options, config). Fixture-facing objects
  have `to_dict()`, which drops absent keys and uses the JS spelling (`lastError`).
- Times are milliseconds and named `*_ms`, except the host-facing `stop(timeout=...)` methods,
  which take seconds like `threading`.
- Enum values are the JS strings. `TunnelState`, `TunnelStatusCode`, `ExposeState`,
  `RegisterErrorCode` and `AppErrorCode` are `StrEnum`s, so `st.state == "online"` works.
- Options can be passed as the dataclass or as keyword arguments, not both (`TypeError`).
- Validation errors never echo the value that failed (it could be a token).

## Transport: `ws_mixer`

### Connecting

| Name | Signature | Purpose |
|---|---|---|
| `connect` | `async connect(url, *, token, **options) -> MixerClient` | `MixerClient(url, token=token, **options)`, started; returns on the first `welcome`. |
| `MixerClient` | `MixerClient(url, *, token, meta=None, agent=None, window=262144, max_streams=64, capabilities=None, headers=None, reconnect=None, web_socket=None, on_stream=None, on_app=None, on_drain=None, on_connect=None, on_disconnect=None, on_reconnect=None, clock=None, random=None, _timing=None)` | A reconnecting ws-mixer.v1 client. `agent` is a mapping that overrides `sdk`, `sdk_version`, `runtime`, `os`. |
| `MixerClient.start` | `async start() -> None` | Dials; returns on the first welcome. Raises a fatal `WsMixerError` on a fatal failure or exhausted attempts, a non-fatal `WsMixerError(NO_ERROR)` when `close()` wins the race, and `RuntimeError` on a second call. Cancelling the caller aborts the client. |
| `MixerClient.state` | property, `ClientState` | `"idle"`, `"dialing"`, `"connected"`, `"backoff"` or `"closed"`. |
| `MixerClient.current_conn` | `current_conn() -> MixerConn \| None` | The live, welcomed connection. |
| `MixerClient.stats` | `stats() -> Stats \| None` | The live connection's counters. |
| `MixerClient.send_app` | `async send_app(body: Mapping) -> None` | Sends `app{body}`; `WsMixerError` when not connected. |
| `MixerClient.close` | `async close(message: str \| None = None) -> None` | `None`: graceful (drain, up to 5 s, close 1000). A `str`: application close (`error{APPLICATION_CLOSE}`, 4014). Idempotent. |
| `MixerClient.abort` | `abort() -> None` | Synchronous hard stop: no more dials, sockets dropped. |
| `MixerClient.on` / `once` / `off` | `on(event, cb) -> unsubscribe`, `once(...)`, `off(event, cb)` | Event listeners; see [Events](#transport-events). |
| `ClientState` | `Literal["idle", "dialing", "connected", "backoff", "closed"]` | Client state. |
| `ReconnectOptions` | `ReconnectOptions(base_ms=1000, cap_ms=60000, connect_timeout_ms=10000, max_attempts=None, stable_after_ms=10000)` | Backoff policy. `max_attempts=None`: unbounded; `0`: never reconnect. Validated on construction. |
| `ReconnectInfo` | `ReconnectInfo(attempt, delay_ms, cause)` | Passed to `on_reconnect` before every scheduled redial, zero delays included. `cause`: `"drain"`, `"token-refresh"`, `"going_away"`, `"keepalive_timeout"`, `"enhance_your_calm"`, `"application_close"` or the failure message. |
| `DisconnectReason` | `DisconnectReason(phase, fatal, message, ws_code=None, error_code=None, error_name=None, http_status=None, close_reason=None, protocol_error=False, cause=None)` | One per disconnect. `phase`: `"dial"`, `"handshake"`, `"connected"`. |
| `full_jitter_delay` | `full_jitter_delay(attempt, base_ms, cap_ms, random) -> float` | `random() * min(cap, base * 2**attempt)`. |
| `TokenProvider` | `str \| Callable[[], str \| Awaitable[str]]` | A static token or a provider called on every dial (never cached). |
| `token_refresh_requested` | `token_refresh_requested() -> bool` | True only while the provider is called for the one refresh-retry after a rejected token (HTTP 401 or close 4011 before welcome). |
| `TokenUnavailableError` | `class TokenUnavailableError(Exception)` | Raise from a provider (directly or as `__cause__`) for a temporary failure: the dial is retried. Any other provider exception is fatal. |
| `is_token_unavailable` | `is_token_unavailable(exc) -> bool` | Looks for the marker on `exc` and its `__cause__` chain (10 links, cycle-safe). |

### Connections and streams

| Name | Signature | Purpose |
|---|---|---|
| `MixerConn` | `MixerConn(socket, *, token, agent, clock, random, meta=None, window=262144, max_streams=64, capabilities=None, hello_timeout_ms=10000, on_stream=None, _timing=None)` | One connection on an open WebSocket. Usually created by `MixerClient`. Attributes: `session`, `peer_window`, `our_window`, `max_streams`, `ping_interval_ms`, `ping_timeout_ms`, `welcome`, `welcome_meta`. |
| | `closed`, `close_info` | Properties: torn down yet; the `CloseInfo`. |
| | `stats()`, `is_draining()`, `live_stream_count()` | Counters and state. |
| | `async handshake() -> Welcome` | Sends hello, returns the validated welcome. |
| | `async send_control(msg)`, `async send_app(body)` | Stream-0 messages. |
| | `async close(grace_ms=5000)` | Graceful close: `drain{client_requested}`, wait, close 1000. |
| | `async close_with_message(message)` | Application close, 4014. |
| | `fail(err)`, `retire(deadline_ms)`, `cancel_delivery()` | Local error teardown; hand-over retire; drop queued handler events. |
| | `async wait_closed() -> CloseInfo` | Waits for teardown. |
| `MixerStream` | created by the SDK only | One server-opened stream. Attributes `id`, `reset_code`; properties `state`, `send_window`, `recv_window`. |
| | `async read(n=-1) -> bytes` | Next bytes; `b""` at EOF. Raises `StreamError` after a RESET, `ConnError` when the connection is gone. Credits the window. |
| | `async for chunk in stream` | Iterates `read()` until EOF. |
| | `async write(data: bytes \| bytearray \| memoryview \| str)` | Sends in DATA frames of at most 16 KiB, waiting for credit. Serialized. |
| | `async close_write()` | Sends CLOSE. Idempotent, never raises. |
| | `reset(code=ErrorCode.CANCEL, message="")` | RESET with a stream-scoped code, its name, or an application code `0x1000_0000..0xFFFF_FFFF`. `ValueError` for connection-only codes. |
| | `close()`, `refuse()` | RESET CANCEL unless the peer already closed (else `close_write()`); RESET REFUSED_STREAM. |
| | `async wait_closed() -> StreamCloseReason` | Never raises. |
| `StreamState` | `Literal["open", "half_closed_local", "half_closed_remote", "closed"]` | Stream state. |
| `StreamCloseReason` | `Finished \| Reset \| ConnectionLost` | How a stream ended. |
| `Finished` | `Finished()` | CLOSE sent and received. |
| `Reset` | `Reset(code, name, message, by_peer)` | A RESET in either direction. |
| `ConnectionLost` | `ConnectionLost(error: ConnError)` | The connection ended first. |
| `Stats` | `Stats(unknown_frame_types, stale_frames, duplicate_pongs, refused_opens, drain_violations, unknown_drain_reasons, handler_errors, protocol_violations, bytes_in, bytes_out)` | Per-connection counters (restart with each connection). |
| `DrainInfo` | `DrainInfo(reason, last_stream_id, deadline_ms=None, retry_after_ms=None, message=None)` | A received drain; unknown reasons normalized to `"maintenance"`. |
| `PongInfo` | `PongInfo(id, rtt_ms)` | The `pong` event payload. |
| `HandlerErrorInfo` | `HandlerErrorInfo(event, error)` | A stream/app/drain handler or listener raised. |
| `CloseInfo` | `CloseInfo(ws_code, message, phase, local, received_close, error, error_code=None, error_name=None, close_reason=None, hello_timeout=False)` | Raw facts of one connection's end; `MixerClient` turns it into a `DisconnectReason`. |
| `Timing` | `Timing(hello_timeout_ms=None, allow_subfloor=False, min_ping_interval=None)` | Test and conformance hooks; never set in production. |

### Errors and codes

| Name | Signature | Purpose |
|---|---|---|
| `ErrorCode` | `IntEnum`, 15 members `NO_ERROR` (0) .. `APPLICATION_CLOSE` (0x0E) | WIRE.md §2.8 codes. |
| `WsMixerError` | `WsMixerError(code, message, *, fatal=False, stream_id=None, ws_code=None, close_reason=None, http_status=None)` | Base error; `.code_name` property. |
| `ConnError` | `WsMixerError` + `last_stream_id`, `control_type` | Connection-level error. |
| `StreamError` | `StreamError(code, stream_id, message)` | Stream-scoped error (RESET). |
| `code_name` | `code_name(code) -> str` | Wire name; unknown codes give `"INTERNAL_ERROR"`. |
| `parse_error_code` | `parse_error_code(name) -> int \| None` | Code for a wire name. |
| `close_code` | `close_code(code) -> int` | `4000 + code`, `NO_ERROR` -> 1000. |
| `wire_close_code` | `wire_close_code(ws_code) -> int` | Clamps to a code legal on the wire (anything outside 1000/4000-4999 -> 4002). |

### Wire codec

| Name | Signature | Purpose |
|---|---|---|
| `Frame` | `Frame(type, flags, stream_id, payload)` | One decoded mux frame. Codec functions live in `ws_mixer.frame` (`decode_frame`, `encode_*`). |
| `FrameType` | `IntEnum`: `OPEN`, `DATA`, `WINDOW`, `CLOSE`, `RESET` | Frame types. |
| `MAX_MESSAGE_SIZE` | `65544` | Largest legal WebSocket message (8-byte header + 64 KiB). |
| `MAX_STREAM_ZERO_PAYLOAD` | `16384` | Control-message cap. |
| `MAX_CHUNK` | `16384` | DATA chunk size the SDK sends. |
| `MAX_SEND_WINDOW` | `0x7FFF_FFFF` | Largest send-credit window. |
| `parse_control` | `parse_control(raw, *, allow_subfloor=False) -> ControlMessage` | Validates one stream-0 payload; every failure is a `ConnError`. |
| `encode_control` | `encode_control(msg) -> bytes` | Compact JSON, `t` first. |
| `known_drain_reason` | `known_drain_reason(reason) -> bool` | Whether the reason is documented. |
| `ControlMessage` | `Hello \| Welcome \| Ping \| Pong \| Drain \| ErrorMsg \| App` | `TypedDict`s with the wire's snake_case keys. `Hello`, `Welcome`, `Ping`, `Pong`, `Drain`, `ErrorMsg` and `App` are each exported too. |
| `AgentInfo`, `ServerInfo` | `TypedDict` | `hello.agent`, `welcome.server`. |
| `SUBPROTOCOL` | `"ws-mixer.v1"` | The WebSocket subprotocol. |
| `SDK_VERSION` | `"0.1.0"` | This SDK's version. |

### WebSocket seam

| Name | Signature | Purpose |
|---|---|---|
| `WebSocketConnector` | `Protocol`: `async __call__(url, init: WebSocketInit) -> WebSocketConnection` | Opens one connection per call. |
| `WebSocketConnection` | `Protocol`: `subprotocol`, `async recv() -> bytes \| str`, `async send(data)`, `async close(code=1000, reason="")`, `abort()` | What the SDK needs from a socket. A `str` from `recv()` is a text frame (a protocol error). |
| `WebSocketInit` | `WebSocketInit(protocols=("ws-mixer.v1",), headers={}, max_message=65544)` | What the SDK asks a connector for. |
| `StdlibConnector` | `StdlibConnector(*, ssl_context=None, open_timeout_ms=None, happy_eyeballs_delay=0.25, close_timeout_ms=1000, clock=None)` | Default connector: stdlib asyncio + `ssl`. No proxy, no redirects, no extensions, no RFC pings. |
| `default_ssl_context` | `default_ssl_context() -> ssl.SSLContext` | certifi's CA bundle when importable, else the system store (one warning if that is empty). |
| `UpgradeRejected` | `UpgradeRejected(status, headers=None)`; `.retry_after_ms(now_ms=None)` | Non-101 answer to the upgrade. 401 is fatal (after one refresh-retry when `token` is a provider), 403/404 fatal, 429 honours `Retry-After`, the rest retried. |
| `ProxyRejected` | subclass of `UpgradeRejected` | An HTTP proxy refused CONNECT; always fatal. Raised by the `websockets` adapter. |
| `UnexpectedSubprotocol` | `UnexpectedSubprotocol(subprotocol)` | The 101 echoed an unoffered subprotocol; fatal UNSUPPORTED. |
| `InvalidHandshake` | subclass of `OSError` | A 101 that breaks RFC 6455 otherwise; retried. |
| `WebSocketClosed` | `WebSocketClosed(code, reason="", received=False)` | Raised by `recv()` once closed. `received`: the peer's close frame arrived. |

`ws_mixer.adapters.websockets.websockets_connector(*, ssl_context=None, **connect_kwargs) -> WebSocketConnector`
wraps `websockets.asyncio.client.connect` (`websockets>=13,<18`, imported on call). It pins
`compression=None`, `max_size`, `ping_interval=20`, `ping_timeout=None`, `subprotocols` and
`additional_headers` (passing them raises `TypeError`), refuses redirects, and uses websockets'
proxy support (newer releases; reads `wss_proxy`/`https_proxy` and OS settings).

### Time and randomness

| Name | Signature | Purpose |
|---|---|---|
| `Clock` | `Protocol`: `now_ms() -> float`, `call_later(delay_ms, cb) -> TimerHandle` | Every SDK timer goes through one. |
| `TimerHandle` | `Protocol`: `cancel()` | A scheduled timer. |
| `LoopClock` | `LoopClock(loop=None)` | `Clock` over an asyncio loop; binds lazily on first use. |
| `RandomFn` | `Callable[[], float]` | Returns `[0, 1)`; default `random.random`. |

### Host: `LoopThread`

| Name | Signature | Purpose |
|---|---|---|
| `LoopThread` | `LoopThread(*, name="ws-mixer", dispatch=None)` | A daemon thread with a private asyncio loop. No signal handlers, no logging config. |
| | `start()` | Idempotent; starts again with a fresh loop after `stop()`. |
| | `stop(timeout=3.0) -> bool` | Cancels every task, stops the loop, joins. True once the thread exited. Any thread; from the loop thread it schedules the shutdown and returns False. |
| | `submit(coro) -> concurrent.futures.Future` | Runs a coroutine on the loop from any thread; `RuntimeError` when not running. |
| | `call_soon(fn)` | Runs `fn` on the loop thread. |
| | `call_dispatched(fn) -> asyncio.Future` | On the loop: runs `fn` through `dispatch` (inline without one) and resolves with its result. |
| | `dispatch_callback(fn)` | Hands a host callback to `dispatch`; exceptions logged. |
| | `running`, `loop`, `clock`, `fatal_error` | Properties. |
| | context manager | `with LoopThread() as lt:` starts and stops. |

### Transport events

`MixerClient` (via `on`/`once`):

| Event | Payload | When |
|---|---|---|
| `welcome` | `Welcome` | Each connection's welcome. |
| `stream` | `MixerStream` | A server-opened stream (wire order). |
| `app` | `dict` | An `app` body (wire order). |
| `drain` | `DrainInfo` | A received drain (wire order). |
| `reconnecting` | `ReconnectInfo` | Before a redial with `delay_ms > 0`. |
| `close` | `DisconnectReason` | Once per disconnect. |
| `closed` | `DisconnectReason \| None` | Once, when the client enters `closed`: the fatal reason, else None. |
| `error` | `WsMixerError` or the raised exception | Only when listened to; also when a callback or listener raises. |
| `fatal` | `WsMixerError` | A fatal failure. |
| `pong` | `PongInfo` | A pong to the SDK's ping. |
| `handler_error` | `HandlerErrorInfo` | An `on_stream`/`on_app`/`on_drain` handler or listener raised; delivery goes on. |

`MixerConn` emits `welcome`, `stream`, `app`, `drain`, `drain_received` (`DrainInfo`, on receipt),
`pong`, `error`/`fatal` (`ConnError`), `close` (`CloseInfo`, always once) and `handler_error`.

A listener that raises does not stop the others: its exception is re-emitted as `error`.

## mcpwarp: `ws_mixer.mcpwarp`

### `expose()`

| Name | Signature | Purpose |
|---|---|---|
| `expose` | `async expose(options: ExposeOptions \| None = None, /, **kwargs) -> ExposeHandle` | Registers one service and serves its requests. Validates before any I/O (`TypeError`/`ValueError`, spec §5.2 messages). Returns once registered or disabled; raises a fatal `McpwarpError` otherwise. Cancelling the awaiting task closes the client and re-raises `CancelledError`. |
| `ExposeOptions` | `ExposeOptions(token, name, integration, url=DEFAULT_URL, kind="http", web_socket=None, agent=None, reconnect=None, max_request_body_bytes=4194304, on_retry=None, handler=None, target=None, target_timeout_ms=None, clock=None, random=None, handler_timeout_ms=None)` | Exactly one of `handler` and `target`. `token` may be a `TokenProvider`. `handler_timeout_ms` (SDK extension, `handler` only; see [DEVIATIONS](DEVIATIONS.md)): an async handler with no response by then is cancelled and answered 504 `local server timed out`; None = no deadline. |
| `ExposeHandle` | returned by `expose()` | `url`, `server_id`, `registration`, `path` (`"/mcp"`), `state` (`ExposeState`), `client` (`MixerClient`); `on`/`once` (return an unsubscribe function), `off`; `async close()` (idempotent, never raises); `async with`. |
| `ExposeState` | `StrEnum`: `connecting`, `registering`, `registered`, `disabled`, `conflict`, `closing`, `closed` | Handle state. |
| `Agent` | `Agent(runtime=None, os=None)` | Overrides detected runtime/OS (1-64 printable ASCII each). |
| `RetryInfo` | `RetryInfo(attempt, delay_ms, reason)` | `on_retry` payload. Drain: `(0, retry_after_ms + jitter, "drain")`; token refresh: `(1, 0, "token-refresh")`. |
| `Registration` | `Registration(url, server_id, created)` | One successful register. |
| `Conflict` | `Conflict(server_id, name, reason)` | Take-over; `reason` `"taken_over"` or `"replaced"`. |
| `Disabled` | `Disabled(server_id, reason)` | Disabled from the dashboard. |
| `Enabled` | `Enabled(server_id)` | Re-enabled. |
| `Closed` | `Closed(error=None)` | `None` after `close()`, the fatal `McpwarpError`, or `CancelledError`. |
| `McpwarpError` | `McpwarpError(code, message, *, fatal=False, service_name=None)` | `.code`, `.message`, `.fatal`, `.service_name`; cause in `__cause__`. Codes: a register/app error code, or `MALFORMED_ENVELOPE`, `CLOSED`, `UNAUTHORIZED`, `TAKEN_OVER`, `DISCONNECTED_FATAL`. |

`ExposeHandle` events:

| Event | Payload |
|---|---|
| `registered` | `Registration` |
| `disabled` | `Disabled` |
| `enabled` | `Enabled` |
| `conflict` | `Conflict` |
| `error` | an `Exception` (non-fatal: handler errors, transient register errors, ...) |
| `disconnect` | `DisconnectReason` (every transport disconnect) |
| `close` | `Closed` (once, last) |

When a `disable` for an id the handle did not know yet is held and a later `registered` brings
that id, the handle emits `registered` then `disabled`, back to back: `url` and `registration`
are set by then and `state` ends `disabled` (spec §5.9 Held `disable`). A tunnel shows the pair
as one `disabled` snapshot.

### Handlers

| Name | Signature | Purpose |
|---|---|---|
| `Handler` | `Callable[[Request], Awaitable[Response]]` | A bridged request handler. |
| `Request` | `Request(method, url, target, path, query, headers, body, http_version)` | One request, body fully buffered and size-capped. `url` is `https://<Host><target>`; `headers` is the forwardable set. |
| `Response` | `Response(status, headers=(), body=None, reason=None)` | `body`: bytes, an async iterable of bytes (streamed, chunked) or None. `Content-Length` is sent only when `headers` declares it. Recognised by shape, not by class. |
| `Headers` | `Headers(items=())` | Ordered, case-insensitive, multi-valued, immutable. `get`, `get_all`, `items`, `in`, `len`. |
| `sync_handler` | `sync_handler(fn, *, executor=None) -> Handler` | Runs a blocking `fn(Request) -> Response` in an executor. |
| `asgi_handler` | `asgi_handler(app) -> Handler` | Serves an ASGI 3 HTTP app. Lifespan is never run. |
| `target_handler` | `target_handler(target, *, timeout_ms=30000, ssl_context=None, clock=None) -> Handler` | Forwards to a local HTTP server; 502 on connection failure, 504 when no response head arrives in time. Used by `target=`. |

### `create_tunnel()`

| Name | Signature | Purpose |
|---|---|---|
| `create_tunnel` | `create_tunnel(options: TunnelOptions \| None = None, /, **kwargs) -> Tunnel` | A stopped tunnel. Synchronous and loop-free. Validates `integration`, `agent` and `handler`/`target` at once. |
| `TunnelOptions` | `TunnelOptions(integration, agent=None, web_socket=None, max_request_body_bytes=4194304, reconnect=None, on_retry=None, handler=None, target=None, target_timeout_ms=None, logger=None, clock=None, random=None, handler_timeout_ms=None)` | `logger` defaults to `logging.getLogger("mcpwarp")`. `handler_timeout_ms` as in `ExposeOptions`. |
| `TunnelConfig` | `TunnelConfig(enabled, token, name, url=None, kind=None, web_socket=None)` | What changes at runtime. `token` is a `str` or a `TokenProvider` (called on every dial; compared with `==`). A mapping or any object with these attributes works too. |
| `Tunnel.status` | property, `TunnelStatus` | Immutable; any thread. |
| `Tunnel.subscribe` | `subscribe(cb) -> Callable[[], None]` | Calls `cb` now and on every change. Each call registers separately. |
| `Tunnel.configure` | `async configure(config) -> None` | Applies a config. No-op when unchanged; never raises. Returns once dialing. |
| `Tunnel.stop` | `async stop() -> None` | Goes `off` at once, then waits for the unregister + drain (up to ~7 s). `configure()` starts again. |
| `Tunnel.dispose` | `dispose(timeout_ms=5000) -> Coroutine` | Final stop, bounded by `timeout_ms`. The timeout is checked at the call (`TypeError`/`ValueError`). Memoized. |
| `TunnelStatus` | `TunnelStatus(state, url=None, code=None, last_error=None)`; `to_dict()` | The status. |
| `TunnelState` | `StrEnum`: `off`, `connecting`, `online`, `disabled`, `conflict`, `error` | |
| `TunnelStatusCode` | `StrEnum`, 13 members | See [lastError table](#lasterror-table). |

### `ThreadedTunnel` (`ws_mixer.mcpwarp.threaded`)

Defined in `ws_mixer.mcpwarp.threaded` and re-exported from `ws_mixer.mcpwarp`.

| Member | Signature | Purpose |
|---|---|---|
| `ThreadedTunnel` | `ThreadedTunnel(options=None, /, *, dispatch=None, thread_name="mcpwarp", **kwargs)` | `create_tunnel()` on a `LoopThread`. Validates like `create_tunnel()`. Raises `ValueError` when `clock` is a `LoopClock` or `web_socket` is a `StdlibConnector` with a `LoopClock` (every start makes a new loop). |
| `start` | `start() -> None` | Starts the thread and an `off` tunnel; never dials. Idempotent; restarts after `stop()`, waiting for a `stop()` still running on another thread. `RuntimeError` when called on the loop thread while a stop runs there. |
| `configure` | `configure(config) -> concurrent.futures.Future[None]` | `Tunnel.configure()` on the loop; `RuntimeError` when not started. |
| `disconnect` | `disconnect() -> concurrent.futures.Future[None]` | `Tunnel.stop()`. |
| `status` | property | Lock-free; `off` before `start()` and after `stop()`. |
| `subscribe` | `subscribe(cb) -> Callable[[], None]` | `cb` through `dispatch`; works before `start()`, survives restarts; repeats of the same status are skipped. |
| `call_dispatched` | `async call_dispatched(fn) -> T` | Runs `fn` on the host thread; await it on the loop. |
| `stop` | `stop(timeout=3.0) -> bool` | `dispose()` with 0.8 x timeout, abort open sockets, send a final `off` to subscribers, stop and join. True once the thread exited. Any thread, idempotent, safe before `start()` (True). From the loop thread it schedules the shutdown and returns False. NaN and negative timeouts count as 0. |

### Status text

| Name | Signature | Purpose |
|---|---|---|
| `describe_status` | `describe_status(status, locale=None, messages=None) -> StatusText` | Short localized UI text. `status` may be a `TunnelStatus` or a mapping (`lastError` or `last_error`). Never raises. |
| `StatusText` | `StatusText(label, detail=None)`; `to_dict()` | `label`: the state; `detail`: why, or None. |
| `StatusMessages` | `StatusMessages(states={}, codes={})` | Overrides for single strings of the resolved locale. |
| `STATUS_LOCALES` | `("en", "de", "es", "fr", "ja", "zh-CN")` | Shipped locales, in resolution order. |

### Envelope codec

| Name | Signature | Purpose |
|---|---|---|
| `encode_register` | `encode_register(name, kind="http") -> dict` | `{"mcpwarp": {"v": 1, "op": "register", ...}}`, keys in wire order. |
| `encode_unregister` | `encode_unregister(name) -> dict` | Unregister body. |
| `decode_envelope` | `decode_envelope(body) -> InboundEnvelope \| None` | None when not ours; `Unknown` for another `v` or op; `McpwarpError(MALFORMED_ENVELOPE)` for a bad known op. |
| `InboundEnvelope` | `Registered \| Unregistered \| Disable \| Enable \| ConflictOp \| AppError \| Unknown` | Decoded edge->agent ops. |
| `Registered` | `Registered(services: list[RegisteredService], errors: list[ServiceError])` | |
| `RegisteredService` | `RegisteredService(name, id, url, created=False)` | |
| `ServiceError` | `ServiceError(name, code, message="", id=None)` | |
| `Unregistered` | `Unregistered(services: list[UnregisteredService])` | |
| `UnregisteredService` | `UnregisteredService(name, id="")` | |
| `Disable` | `Disable(id, reason="disabled")` | |
| `Enable` | `Enable(id, name)` | |
| `ConflictOp` | `ConflictOp(id, name, reason="taken_over")` | |
| `AppError` | `AppError(code, message="")` | |
| `Unknown` | `Unknown(raw_op, v)` | Ignored by the SDK. |
| `RegisterErrorCode` | `StrEnum`, 17 members | `registered.errors[].code` values. Decoded codes stay `str`. |
| `AppErrorCode` | `StrEnum`, 6 members | `op:"error"` codes. |
| `TRANSIENT_REGISTER_ERRORS` | `frozenset[str]` | Register errors retried with backoff. |
| `ServiceKind` | `Literal["http", "stdio"]` | |
| `MCPWARP_V` | `1` | Envelope version. |
| `DEFAULT_URL` | `"wss://connect.mcpwarp.io"` | The edge. |
| `SERVICE_NAME_RE`, `SERVICE_NAME_MAX` | pattern, `30` | Service name rule. |
| `is_valid_service_name` | `is_valid_service_name(name) -> bool` | |
| `INTEGRATION_ID_RE` | pattern | RFC 9562 UUID (versions 1-8, plus nil and max), checked lowercased. |

## Testing: `ws_mixer.testing`

Stdlib only; ships in the wheel.

| Name | Signature | Purpose |
|---|---|---|
| `FakeClock` | `FakeClock(start_ms=0)` | A `Clock` that moves only on `await advance(ms)`. Also `pending()`, `next_due_ms()`, `settle()`, `run_all()`. |
| `FakeTimer` | from `FakeClock.call_later` | `cancel()`. |
| `settle` | `async settle()` | Yields until the loop is idle twice in a row (peeks at CPython's `loop._ready`). |
| `FakeWebSocket` | `FakeWebSocket(*, echo_close=True)` | A scripted socket: `open()`, `reject()`, `fail()`, `push()`, `server_close()`, `server_drop()`, `set_echo_close()`, `pause_sends()`, `resume_sends()`, `next_sent()`; records `sent`, `close_calls`, `aborted`, `dial_cancelled`. |
| `FakeConnector` | `FakeConnector(sockets=(), factory=None)` | A connector handing out `FakeWebSocket`s; `calls`, `dials`, `cancelled_dials`, `add()`, `next_socket()`. |
| `socket_pair` | `socket_pair(subprotocol="ws-mixer.v1") -> (a, b)` | Two connected in-memory ends. |
| `FakeEdge` | `FakeEdge(*, username="tester", domain="mcpwarp.test", window=262144, max_streams=64, ping_interval=30000, ping_timeout=90000, request_timeout_ms=30000, accept_token=None, clock=None)` | In-process mcpwarp edge. Call `await edge.start()` before dialing and `await edge.stop()` at the end. Pass `edge.web_socket` as the connector and `edge.url` as the URL. `request()`, `disable()`, `enable()`, `conflict()`, `drain()`, `app_error()`, `fail_register()`, `reject_next_upgrade()`, `wait_for_registered()`, ... |
| `EdgeConnection` | `edge.connections[i]` | Server side of one connection: `open_stream()`, `send_control()`, `send_app()`, `drain()`, `close()`. |
| `EdgeResponse` | `EdgeResponse(status, reason, headers, body, end=None)` | `edge.request()` result; `header(name)`, `text`. |
| `FakeEdgeUpgrade` | `FakeEdgeUpgrade(url, protocols, headers)` | One recorded upgrade. |

`FakeEdge` differs from the real edge in three documented ways: it writes the request body while
reading the response, sends `enable` only to the routed connection, and a second registration of
a held name just takes the route (call `edge.conflict()` to send the old holder `conflict`).

## JS to Python name map

### Transport

Names not listed are the same in both.

| JS | Python |
|---|---|
| `ConnectOptions` | keyword arguments of `MixerClient` / `connect()` |
| `maxStreams` | `max_streams` |
| `onStream` / `onApp` / `onDrain` / `onConnect` / `onDisconnect` | `on_stream` / `on_app` / `on_drain` / `on_connect` / `on_disconnect` |
| `onReconnect` (internal in JS) | public `on_reconnect` |
| `webSocket` | `web_socket` |
| `ReconnectOptions{base, cap, connectTimeout, maxAttempts: Infinity, stableAfter}` | `ReconnectOptions(base_ms, cap_ms, connect_timeout_ms, max_attempts=None, stable_after_ms)` |
| `currentConn()` | `current_conn()` |
| `currentState()` | `.state` |
| `sendApp` | `send_app` |
| `close({message})` | `close(message=None)` |
| `read()` -> `null` at EOF | `read()` -> `b""` |
| `closeWrite` | `close_write` |
| `closed` (a Promise) | `wait_closed()` |
| `getState` / `getSendWindow` / `getRecvWindow` | `.state` / `.send_window` / `.recv_window` |
| `toWebStreams` | n/a: `MixerStream` is the Python stream (`read`/`write`/`async for`) |
| `WebSocketLike` | `WebSocketConnection` |
| `WebSocketFactory` | `WebSocketConnector` |
| `defaultWebSocketFactory` | `StdlibConnector` |
| `webSocketFactory(ctor)` | `ws_mixer.adapters.websockets.websockets_connector()` |
| `upgradeFailure` | `UpgradeRejected` (and `ProxyRejected`) |
| `fullJitterDelay` | `full_jitter_delay` |
| `codeName` / `parseErrorCode` / `closeCode` | `code_name` / `parse_error_code` / `close_code` |
| `DisconnectPayload` | `DisconnectReason` (with `protocol_error` folded in) |
| `stats` keys | snake_case of the same keys |
| `_timing` | `_timing` (`Timing`) |
| (none) | `token_refresh_requested()`, `MixerClient.abort()`, `LoopThread` |

### mcpwarp (MCPWARP.md section 9, Python column)

| JS (canonical) | Python |
|---|---|
| `expose(options)` | `await expose(**options)` or `await expose(ExposeOptions(...))` -> `ExposeHandle` |
| `ExposeOptions` | `ExposeOptions` (dataclass) |
| `url`, `token`, `name`, `kind`, `integration` | same |
| `webSocket` | `web_socket` |
| `agent {runtime, os}` | `agent=Agent(runtime, os)` |
| `reconnect` | `reconnect=ReconnectOptions(...)` |
| `maxRequestBodyBytes` | `max_request_body_bytes` |
| `signal` | task cancellation (`CancelledError`) |
| `onRetry({attempt, delayMs, reason})` | `on_retry(RetryInfo(attempt, delay_ms, reason))` |
| `handler` | `handler`: `async (Request) -> Response`; `asgi_handler(app)` adapts ASGI 3 |
| `target`, `targetTimeoutMs` | `target`, `target_timeout_ms` |
| `ExposeHandle` | `ExposeHandle` |
| `.url`, `.serverId`, `.registration`, `.path`, `.state`, `.client` | `.url`, `.server_id`, `.registration`, `.path`, `.state`, `.client` |
| `.on` / `.once` / `.off` | `on(event, cb)` / `once` / `off` (`on`/`once` return an unsubscribe function) |
| events `registered` ... `close` | `"registered"` ... `"close"` |
| `.close()` | `await close()` |
| `ExposeRegistration {url, serverId, created}` | `Registration(url, server_id, created)` |
| `ExposeConflict {serverId, name, reason}` | `Conflict(server_id, name, reason)` |
| (event payloads) | `Disabled(server_id, reason)`, `Enabled(server_id)`, `Closed(error)` |
| `createTunnel(options)` | `create_tunnel(**options)` -> `Tunnel` |
| `TunnelOptions` (+ `logger`) | `TunnelOptions` (+ `logger: logging.Logger`, default `getLogger("mcpwarp")`) |
| `TunnelConfig {enabled, token, name, url, kind, webSocket}` | `TunnelConfig(enabled, token, name, url, kind, web_socket)` |
| `.configure(config)` | `await configure(config)` |
| `.stop()` | `await stop()` |
| `.dispose({timeoutMs})` | `await dispose(timeout_ms=5000)` |
| `.status` | `.status` |
| `.subscribe(l)` -> unsubscribe | `subscribe(cb) -> Callable[[], None]` |
| `TunnelStatus {state, url, code, lastError}` | `TunnelStatus(state, url, code, last_error)` |
| `TunnelState` | `TunnelState` (`StrEnum`) |
| `TunnelStatusCode` | `TunnelStatusCode` (`StrEnum`) |
| `describeStatus(status, {locale, messages})` | `describe_status(status, locale=None, messages=None)` |
| `StatusText {label, detail}` | `StatusText(label, detail)` |
| `statusLocales` | `STATUS_LOCALES` |
| `McpwarpError {code, fatal, serviceName, cause}` | `McpwarpError` `.code`, `.fatal`, `.service_name`, `__cause__` |
| `RegisterErrorCode`, `AppErrorCode`, `TRANSIENT_REGISTER_ERRORS` | same |
| `DEFAULT_URL` | `DEFAULT_URL` |
| `SERVICE_NAME_RE`, `SERVICE_NAME_MAX`, `isValidServiceName` | `SERVICE_NAME_RE`, `SERVICE_NAME_MAX`, `is_valid_service_name` |
| `encodeRegister`, `encodeUnregister`, `decodeEnvelope` | `encode_register`, `encode_unregister`, `decode_envelope` |
| `webSocketFactory(ctor, {headers})` | n/a |
| `TokenUnavailableError` | `TokenUnavailableError` (in `ws_mixer`) |
| (none) | `ThreadedTunnel`, `sync_handler`, `target_handler`, `INTEGRATION_ID_RE`, `MCPWARP_V`, `handler_timeout_ms` |

## lastError table

`code` and `last_error` always come together. The texts are fixed; they never contain a token,
name, URL or server message (those go to the logger).

| State | Code | `last_error` | When |
|---|---|---|---|
| `connecting` | `RETRYING` | Can't reach mcpwarp; retrying. | A dial failed (a token provider's `TokenUnavailableError` included) and no live connection was lost before it. |
| `connecting` | `RETRYING` | mcpwarp couldn't register the service; retrying. | A transient register error or `OVERLOADED`, before registering. |
| `connecting` | `DISCONNECTED` | Connection lost; retrying. | A live connection ended; failed redials keep this code. |
| `connecting` | `INTERNAL` | Something went wrong; the log has the details. | An `error` op or a malformed envelope while connecting. |
| `disabled` | `DISABLED` | This service is disabled in the mcpwarp dashboard. | `disable`, or `SERVER_DISABLED` at register. |
| `conflict` | `TAKEN_OVER` | Another connection took over this name. | `conflict`, or a drain with reason `replaced`. |
| `error` | `UNAUTHORIZED` | The token was rejected. | Close 4011, HTTP 401, or `UNAUTHENTICATED`. |
| `error` | `UNAUTHORIZED` | The token is not allowed to publish this service. | `FORBIDDEN`. |
| `error` | `INVALID_CONFIG` | The tunnel settings are invalid. | Config checks 1, 2, 6, 7, or an option refused at start (e.g. `kind`) with a valid name. |
| `error` | `INVALID_CONFIG` | The token is not set. | Check 4: `token` neither a `str` nor callable. |
| `error` | `INVALID_CONFIG` | The service name is not set. | Check 5: `name` not a `str`. |
| `error` | `INVALID_NAME` | The service name is invalid: use lowercase letters, digits and dashes (not at the start or end), at most 30 characters. | Invalid name at start, or `INVALID_NAME` from the edge. |
| `error` | `USERNAME_REQUIRED` | Choose a username in your mcpwarp account first. | Register error. |
| `error` | `QUOTA_EXCEEDED` | Your mcpwarp account has no services left in its quota. | Register error. |
| `error` | `CONFLICT` | The name is in use by another server. | Register error. |
| `error` | `UNSUPPORTED_VERSION` | This SDK version is no longer supported; please update. | `error` op `UNSUPPORTED_VERSION`. |
| `error` | `DISCONNECTED_FATAL` | The connection to mcpwarp failed. | Any other fatal transport failure (a token provider raising anything else included), or `max_attempts` ran out. |
| `error` | `INTERNAL` | Something went wrong; the log has the details. | Anything else (other register refusals, internal errors). |

`off`, a plain `connecting` and `online` carry no code. `online` carries `url`.

## Status text

`describe_status(status, locale, messages)`:

- **Locale.** Exact tag (case-insensitive, `_` read as `-`), then the language subtag (`de-AT`
  -> `de`, any `zh-*` -> `zh-CN`), then `en`. A non-`str` locale is `en`.
- **label** is the state's string. An unknown state is labelled `str(state)`.
- **detail** is the code's sentence. For `INVALID_CONFIG` the sentence is followed by a
  per-locale joiner and `last_error` (unless `last_error` is the generic "The tunnel settings
  are invalid."). Joiners: `": "` by default, `" : "` for fr (no-break space U+00A0 before the colon), `"："` (U+FF1A) for ja and
  zh-CN. An unknown code gives `last_error` unchanged. `detail` is None when empty or equal to
  `label`.
- **messages** overrides strings of the resolved locale: only keys the table already has, only
  `str` values. A `StatusMessages` or a mapping `{"states": {...}, "codes": {...}}`.

The six tables equal the spec's `status-text/<locale>.json` string for string
(`tests/spec/test_status_text.py` checks it). State labels:

| state | en | de | es | fr | ja | zh-CN |
|---|---|---|---|---|---|---|
| `off` | Off | Aus | Desactivado | Désactivé | オフ | 已关闭 |
| `connecting` | Connecting… | Verbinden… | Conectando… | Connexion… | 接続中… | 正在连接… |
| `online` | Online | Online | En línea | En ligne | オンライン | 在线 |
| `disabled` | Disabled in the mcpwarp dashboard | Im mcpwarp-Dashboard deaktiviert | Desactivado en el panel de mcpwarp | Désactivé dans le tableau de bord mcpwarp | mcpwarp ダッシュボードで無効化されています | 已在 mcpwarp 控制台中停用 |
| `conflict` | Another instance took over this name | Eine andere Instanz hat diesen Namen übernommen | Otra instancia ha tomado este nombre | Une autre instance a repris ce nom | 別のインスタンスがこの名前を取得しました | 另一个实例已接管该名称 |
| `error` | Error | Fehler | Error | Erreur | エラー | 错误 |

English code sentences:

| Code | en |
|---|---|
| `UNAUTHORIZED` | Access token was rejected |
| `TAKEN_OVER` | Another instance took over this name |
| `DISCONNECTED` | Connection lost, reconnecting |
| `RETRYING` | Connection failed, retrying |
| `DISABLED` | Disabled in the mcpwarp dashboard |
| `INVALID_CONFIG` | Invalid settings |
| `INVALID_NAME` | Invalid service name (lowercase letters, digits and inner dashes, at most 30 characters) |
| `USERNAME_REQUIRED` | Set a username in your mcpwarp account |
| `QUOTA_EXCEEDED` | Plan limit reached |
| `CONFLICT` | Name already in use |
| `UNSUPPORTED_VERSION` | Plugin update required |
| `DISCONNECTED_FATAL` | Connection to mcpwarp failed |
| `INTERNAL` | Unexpected error |
