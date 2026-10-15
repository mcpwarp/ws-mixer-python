# Changelog

All notable changes to `ws-mixer` (the Python client SDK) are documented here.

## 0.1.0 - Unreleased

First release. Client role of ws-mixer-spec v0.6.1 (`spec.pin`), plus the spec session's rulings
that land in v0.6.2/v0.6.3 (see [docs/DEVIATIONS.md](docs/DEVIATIONS.md)). Pair-tested against
ws-mixer-go v0.8.0 (`goserver.pin`). Python 3.11-3.14, zero runtime dependencies.

### Added

- Transport (`ws_mixer`):
  - `connect()`, `MixerClient`: reconnecting client with full-jitter backoff and per-connection
    stability reset, drain hand-over, token provider with one refresh-retry
    (`token_refresh_requested()`), `TokenUnavailableError` marker, `DisconnectReason`,
    `ReconnectOptions`/`ReconnectInfo`, events and `on_*` callbacks.
  - `MixerConn`, `MixerStream`: handshake, flow control, ordered delivery, keepalive, graceful
    and application close, stream state machine with `read`/`write`/`close_write`/`reset`.
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
  - `TunnelConfig.token` takes a `TokenProvider` too (spec-provisional for v0.6.3).
  - `describe_status()` with the six spec locales (en, de, es, fr, ja, zh-CN).
  - Request bridging (MCPWARP.md section 4) with `Request`, `Response`, `Headers`,
    `sync_handler()`, `asgi_handler()` (ASGI 3) and `target_handler()` (local HTTP server).
  - Envelope codec: `encode_register`, `encode_unregister`, `decode_envelope` and the envelope
    types, `RegisterErrorCode`, `AppErrorCode`, `McpwarpError`.
  - `handler_timeout_ms` on `ExposeOptions`/`TunnelOptions` (SDK extension, handler mode only,
    default None): a handler with no response by then is cancelled and answered 504
    `local server timed out` (docs/DEVIATIONS.md).
- `ws_mixer.mcpwarp.ThreadedTunnel`: a tunnel on its own loop thread, with `dispatch`
  for host main-thread callbacks.
- `ws_mixer.testing`: `FakeClock`, `settle`, `FakeWebSocket`, `FakeConnector`, `socket_pair`,
  `FakeEdge`.
- Conformance: `conformance/adapter/` for the spec's Go runner; floors `python-client` 24 and
  `go->python` 10 in `conformance/COUNTS.json`.
- Tests: every client-side spec fixture set (frames 29, control 47, sequences 24 client,
  envelope 60, bridging 13 + 1 skipped, tunnel 34, status text 6 locales and 26 cases), unit
  tests, and an import-boundary test that keeps the package vendorable.
- Tooling: hatchling build, uv dev group, Makefile (`fetch-spec`, `fetch-goserver`,
  `go-adapter`, `smoke`, `conformance-fixture`, `conformance-pair`, `test`, `lint`,
  `typecheck`), CI on Python 3.11-3.14 and a conformance workflow.
- Docs: README, `docs/API.md`, `docs/HOSTS.md` (Anki, Blender), `docs/CONFORMANCE.md`,
  `docs/DEVIATIONS.md`.
