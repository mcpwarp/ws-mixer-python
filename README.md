# ws-mixer (Python)

Python client SDK for [ws-mixer.v1](https://github.com/mcpwarp/ws-mixer-spec/blob/v0.8.2/docs/WIRE.md):
N independent, flow-controlled byte streams multiplexed over one WebSocket. It also ships the
mcpwarp layer ([MCPWARP.md](https://github.com/mcpwarp/ws-mixer-spec/blob/v0.8.2/docs/MCPWARP.md)),
which exposes a local MCP server at its public mcpwarp URL.

asyncio only, zero runtime dependencies, Python 3.11+. Nothing happens at import time: no
threads, no logging config, no signal handlers.

Sibling SDKs: [ws-mixer-js](https://github.com/mcpwarp/ws-mixer-js) (canonical names),
[ws-mixer-go](https://github.com/mcpwarp/ws-mixer-go) (client and server) and
[ws-mixer-rust](https://github.com/mcpwarp/ws-mixer-rust).

| Import | What |
|---|---|
| `ws_mixer` | the transport: `connect()`, `MixerClient`, `MixerStream`, the WebSocket seam, `LoopThread` |
| `ws_mixer.mcpwarp` | `expose()`, `create_tunnel()`, `ThreadedTunnel`, `describe_status()`, envelope codec, handler adapters |
| `ws_mixer.adapters.websockets` | `websockets_connector()`, an optional connector over the `websockets` package |
| `ws_mixer.testing` | `FakeClock`, `FakeConnector`, `FakeEdge` and friends for your own tests |

## Status

0.1.0, not released yet. Client role only. Pinned to spec **v0.8.2** (`spec.pin`) and
ws-mixer-go **v0.8.0** (`goserver.pin`) for the pair tests. The rulings the spec session made
after v0.6.1 (integral floats, code-point string limits, hand-over retire timing,
`drain.retry_after_ms` as a floor, the tunnel's token provider, and more) are in the pinned spec
text; what still differs is listed in [docs/DEVIATIONS.md](docs/DEVIATIONS.md). Like every
SDK since spec v0.8.0 it is single-service (`expose()` and the tunnel, one service per
connection): it has no multi-service controller.

Every client-side fixture set of the spec passes, minus the multi-service host guidance fixtures
an SDK runner skips; see [docs/CONFORMANCE.md](docs/CONFORMANCE.md).

## Install

Not on PyPI yet. From a checkout or git:

```sh
pip install "ws-mixer @ git+https://github.com/mcpwarp/ws-mixer-python"
pip install "ws-mixer[certifi] @ git+https://github.com/mcpwarp/ws-mixer-python"     # CA bundle
pip install "ws-mixer[websockets] @ git+https://github.com/mcpwarp/ws-mixer-python"  # optional connector
```

Extras: `certifi` (CA bundle, used automatically when importable) and `websockets` (`>=13,<18`,
only for `websockets_connector()`). Neither is needed by the core.

### Vendoring

Add-on hosts (Anki, Blender) should vendor rather than install, so two add-ons never fight over
one copy:

1. Copy `src/ws_mixer/` under your own package, e.g. `my_addon/vendor/ws_mixer/`.
2. Import it relatively: `from .vendor.ws_mixer.mcpwarp import create_tunnel`.
3. Optionally drop `testing/` and `adapters/`; the core never imports them.

Every import of `ws_mixer` inside the package is relative, and everything outside `adapters/`
and `testing/` imports only the stdlib (`tests/test_boundary.py` enforces both; `_tls.py` loads
`certifi` through `importlib` when present, and `websockets` is imported only in `adapters/`), so
the copy works under any parent package. Loggers are named `ws_mixer` and `mcpwarp`,
whatever the path. Exceptions are classes of their copy: raise `TokenUnavailableError` from the
copy you use. `certifi` is optional; when it is missing the default TLS context falls back to the
system store and logs one warning if that store is empty.

## Quickstart: `expose()`

One call, one public service. The handler is `async (Request) -> Response`.

```python
import asyncio
from ws_mixer.mcpwarp import Request, Response, expose


async def handle(req: Request) -> Response:
    body = await my_mcp_server(req.body)  # your in-process MCP server
    return Response(200, {"content-type": "application/json"}, body)


async def main() -> None:
    async with await expose(
        token="mcpwarp_pat_...",
        name="notes",
        integration="0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10",
        handler=handle,
    ) as exposed:
        print(exposed.url)  # https://notes-<username>.mcpwarp.io/mcp
        await asyncio.Event().wait()
```

`expose()` returns once the service is registered (or disabled from the dashboard) and raises a
fatal `McpwarpError` (`UNAUTHORIZED`, `TAKEN_OVER`, a register error code, ...) otherwise. To
serve an ASGI app use `handler=asgi_handler(app)`; to proxy a local server use
`target="http://127.0.0.1:8000/mcp"`. `handler_timeout_ms=25_000` (an SDK extension, not in the
spec; default None, no deadline) answers 504 `local server timed out` when a handler has not
returned its response in time, as `target_timeout_ms` does in target mode.

## Quickstart: `create_tunnel()`

For hosts with a settings UI: one tunnel, configured on every settings change, one status.

```python
from ws_mixer.mcpwarp import TunnelConfig, create_tunnel, describe_status

tunnel = create_tunnel(integration="0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10", handler=handle)
unsubscribe = tunnel.subscribe(lambda s: print(describe_status(s, "de").label, s.url or ""))
await tunnel.configure(TunnelConfig(enabled=True, token=token, name="notes"))
# ... settings changed: configure() again; unchanged settings are a no-op
await tunnel.dispose(timeout_ms=2000)  # on shutdown
```

`tunnel.status` is an immutable `TunnelStatus`, safe to read from any thread. Hosts without an
asyncio loop use `ThreadedTunnel` (see [docs/HOSTS.md](docs/HOSTS.md)).

## The WebSocket seam

The SDK dials through a `WebSocketConnector`: `async (url, WebSocketInit) -> WebSocketConnection`.

- `StdlibConnector` is the default: stdlib asyncio + `ssl`, binary frames only, no extensions,
  no redirects, no HTTP proxy, no RFC-level pings (the ws-mixer in-band ping is the liveness
  check). Pass `StdlibConnector(ssl_context=...)` to choose the CA store.
- `ws_mixer.adapters.websockets.websockets_connector(**kwargs)` wraps `websockets` (extra
  `websockets`). It uses websockets' proxy support (newer releases; reads `wss_proxy`/`https_proxy`
  and OS settings), so it is the workaround behind a proxy.
- Your own: any callable with that signature whose result has `subprotocol`, `recv()`,
  `send()`, `close()` and `abort()`. Raise `UpgradeRejected` for a non-101 answer and `OSError`
  for network failures.

Pass it as `web_socket=` to `MixerClient`, `expose()`, `create_tunnel()` or `TunnelConfig`.

## Host notes

[docs/HOSTS.md](docs/HOSTS.md) has full guides for Anki (shared asyncio loop, Qt status polling,
token refresh) and Blender (`ThreadedTunnel`, `bpy.app.timers`, `online_access`, TLS on macOS).

## Why Python 3.11+

The supported Blender LTS releases ship Python 3.11 (4.5) and 3.13 (5.2); Anki 25.07+ ships
3.13. 3.11 also gives `enum.StrEnum`, which the status enums use. Anki 25.02 and earlier
(Python 3.9) are not supported. CI runs 3.11 to 3.14.

## Documentation

- [docs/API.md](docs/API.md): every public name, the JS-to-Python name map, event payloads.
- [docs/HOSTS.md](docs/HOSTS.md): Anki and Blender integration.
- [docs/CONFORMANCE.md](docs/CONFORMANCE.md): fixture suites, the Go runner, CI.
- [docs/DEVIATIONS.md](docs/DEVIATIONS.md): where this SDK differs from the spec text or from ws-mixer-js.
- [CHANGELOG.md](CHANGELOG.md).

## Development

```sh
uv sync                  # .venv with the dev group (pytest, ruff, mypy, websockets, starlette, ...)
make fetch-spec          # shallow-clone ws-mixer-spec at spec.pin into .spec/
make test lint typecheck # pytest, ruff check + format --check, mypy --strict
make smoke               # the conformance adapter prints "ready"
make conformance-fixture # Go runner, fixture mode (needs Go)
make conformance-pair    # Go runner, go->python pair mode (builds ws-mixer-go at goserver.pin)
```

The spec tests look for the spec in `WSMIXER_SPEC_DIR`, then `.spec/` (when `.spec/.pin` matches
`spec.pin`), then `../ws-mixer-spec` (when its HEAD is exactly the pinned tag and its `spec/` tree is clean).
Without one they skip locally and fail
when `CI` is set.

## License

Apache-2.0. See `LICENSE` and `NOTICE`.
