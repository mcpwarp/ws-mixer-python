# Changelog

All notable changes to `ws-mixer` (the Python client SDK) are documented here.

## 0.1.0 - Unreleased

First release. Client role of ws-mixer-spec v0.8.0 (`spec.pin`; what still differs is in
[docs/DEVIATIONS.md](docs/DEVIATIONS.md)). Single-service, as every SDK is since v0.8.0
(MCPWARP.md section 1): no multi-service controller, and the multi-service host guidance
fixtures are skipped as section 10.6 says. Pair-tested against
ws-mixer-go v0.8.0 (`goserver.pin`). Python 3.11-3.14, zero runtime dependencies.

### Added

- Transport (`ws_mixer`):
  - `connect()`, `MixerClient`: reconnecting client with full-jitter backoff and per-connection
    stability reset, drain hand-over, token provider with one refresh-retry
    (`token_refresh_requested()`), `TokenUnavailableError` marker, `DisconnectReason`,
    `ReconnectOptions`/`ReconnectInfo`, events and `on_*` callbacks. A connection that closes
    in the same delivery burst as its `welcome` delivers none of that burst's frames to the
    application, and its retry is the connected-phase one (spec v0.8.0 CLIENT-SDK.md Handler
    delivery).
  - `MixerConn`, `MixerStream`: handshake, flow control, ordered delivery, keepalive, graceful
    and application close, stream state machine with `read`/`write`/`close_write`/`reset`. The
    graceful close's `drain{client_requested}` carries `last_stream_id` = the highest `OPEN` id
    received and `deadline_ms` 5000 (spec v0.6.3 WIRE.md section 2.9).
  - A drain's `retry_after_ms` and a 429's `Retry-After` are capped at 2^31-1 ms
    (`MAX_RETRY_AFTER_MS`, shared; JS parity), the drain hint and its total with jitter each; a
    huge JSON integer no longer leaves the client connected with no replacement scheduled.
  - Frame codec (`ws_mixer.frame`) and hand-written control-message validators
    (`ws_mixer.control`), in the spec fixtures' rule order.
  - `ErrorCode`, `WsMixerError`, `ConnError`, `StreamError` and the code helpers.
  - WebSocket seam: `WebSocketConnector`/`WebSocketConnection`, `StdlibConnector` (stdlib RFC
    6455 client, binary only, no extensions, no redirects), `UpgradeRejected`, `ProxyRejected`,
    `UnexpectedSubprotocol`, `InvalidHandshake`, `WebSocketClosed`, `default_ssl_context()`
    (certifi when importable).
  - `Clock`, `LoopClock`, injectable `random`: every timer and all randomness are injectable.
  - `LoopThread`: a private asyncio loop on a daemon thread with bounded `stop()`.
- `ws_mixer.adapters.websockets.websockets_connector()`: optional connector over `websockets`
  (`>=13,<18`, extra `websockets`). It uses websockets' proxy support (newer releases; reads
  `wss_proxy`/`https_proxy` and OS settings).
- mcpwarp layer (`ws_mixer.mcpwarp`):
  - `expose()`, `ExposeOptions`, `ExposeHandle` and its events.
  - `create_tunnel()`, `Tunnel`, `TunnelOptions`, `TunnelConfig`, `TunnelStatus`,
    `TunnelState`, `TunnelStatusCode`.
  - `TunnelConfig.token` takes a `TokenProvider` too (spec v0.6.3 section 6.2). The rejected
    dial before the refresh-retry is reported as a non-fatal disconnect, so the tunnel shows
    `connecting`/`RETRYING` (T48).
  - A `disable` with an id the agent does not know is held until a `registered` brings that
    id, never adopted (spec v0.6.3 section 2.7, T49/T50). An `enable` matches by name or by the
    agent's server id (section 2.7). When the held `disable` applies, the handle emits
    `registered` then `disabled`, and the tunnel shows one `disabled` snapshot (spec v0.7.0
    section 5.9 Held `disable`).
  - `OVERLOADED` re-sends the outstanding `register`/`unregister` after 1 s, 2 s, 4 s ...
    (capped at 30 s) for consecutive `OVERLOADED`s on one connection; any other reply to the
    outstanding frame ends that streak (spec v0.7.1 section 2.6, D-2026-10-19-01; v0.8.0).
    One frame in flight (same ruling): an `enable` during a pending register waits for its
    reply; `TOO_MANY_SERVICES`, `UNKNOWN_OP`, `BAD_REQUEST` and `UNSUPPORTED_VERSION` answer
    the outstanding frame (ignored during an `OVERLOADED` pause, whose re-send stays, as is
    an empty `registered`); a
    `registered` that does not list the name is re-sent with the transient backoff. A register
    sent meanwhile cancels a scheduled transient re-send, and a reply to the frame cancels a
    pending `OVERLOADED` re-send, so no duplicate register follows. The transient counter resets
    only on a `registered` with a non-transient entry (a success, `SERVER_DISABLED` or a
    refusal) or any `unregistered`. `close()`'s `unregister` wait ends on
    that frame's own reply by frame order: a reply to a register still outstanding ahead of it
    settles that register and the wait goes on, and a register dropped by `OVERLOADED` while
    closing is abandoned (spec v0.7.1 section 3.2 Unregister). No `unregister` when every
    register of the connection was dropped by `OVERLOADED` and no `registered` arrived (spec
    v0.7.2).
  - `registered`, `disable` and `enable` from a connection that has ended are dropped by
    connection identity, also when they arrive after a new connection's `welcome`.
  - `describe_status()` with the six spec locales (en, de, es, fr, ja, zh-CN).
  - Request bridging (MCPWARP.md section 4) with `Request`, `Response`, `Headers`,
    `sync_handler()`, `asgi_handler()` (ASGI 3) and `target_handler()` (local HTTP server).
  - Envelope codec: `encode_register`, `encode_unregister`, `decode_envelope` and the envelope
    types, `RegisterErrorCode`, `AppErrorCode`, `McpwarpError`.
  - `handler_timeout_ms` on `ExposeOptions`/`TunnelOptions` (SDK extension, handler mode only,
    default None): a handler with no response by then is cancelled and answered 504
    `local server timed out` (docs/DEVIATIONS.md).
  - Forwarded request targets keep `[` and `]` verbatim (spec v0.6.3 section 4.3 allowed set).
  - `[` and `]` in the configured `target` URL's path and query are kept verbatim too.
  - `CONNECT`, `TRACE` and `TRACK` (any case) get `400 request cannot be represented as a web
    Request` in both handler and target mode, never reaching the handler or the local server
    (spec v0.6.3 section 4.5).
  - A handler or target response status outside 200..599 is answered `500 handler error`
    (spec v0.6.3 section 4.5).
  - The 431 and the 400 `incomplete request` for a head that never completes are head-only when
    its first line is a `HEAD` request line.
- `ws_mixer.mcpwarp.ThreadedTunnel`: a tunnel on its own loop thread, with `dispatch`
  for host main-thread callbacks.
- `ws_mixer.testing`: `FakeClock`, `settle`, `FakeWebSocket`, `FakeConnector`, `socket_pair`,
  `FakeEdge`.
- Conformance: `conformance/adapter/` for the spec's Go runner; floors `python-client` 25 and
  `go->python` 10 in `conformance/COUNTS.json`.
- Tests: every client-side spec fixture set (frames 29, control 51, sequences 25 client,
  envelope 66 + 2 skipped, bridging 22 + 5 skipped, tunnel 44 + 11 skipped, status text 6
  locales and 26 cases + 11 skipped; the skips beyond `body_413_streamed_reset` are the
  multi-service fixtures), unit tests, and an import-boundary test that keeps the package
  vendorable.
- Tooling: hatchling build, uv dev group, Makefile (`fetch-spec`, `fetch-goserver`,
  `go-adapter`, `smoke`, `conformance-fixture`, `conformance-pair`, `test`, `lint`,
  `typecheck`), CI on Python 3.11-3.14 and a conformance workflow.
- Docs: README, `docs/API.md`, `docs/HOSTS.md` (Anki, Blender), `docs/CONFORMANCE.md`,
  `docs/DEVIATIONS.md`.
