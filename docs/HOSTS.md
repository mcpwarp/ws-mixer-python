# Host integration guides

How to run the mcpwarp tunnel inside two embedded-Python hosts: Anki (which already runs an
asyncio loop on a background thread) and Blender (which has no loop and a strict main-thread
rule). The sketches use the real API of this repo; names you supply are marked `# yours`.

Rules that hold for every host:

- Nothing happens at import. The SDK installs no signal handlers, configures no logging and
  keeps no module-level state, so several instances and several vendored copies can coexist.
- All tunnel state lives on one asyncio loop. Only `tunnel.status` (an immutable
  `TunnelStatus`, replaced in one assignment) is safe to read from another thread.
- With `Tunnel`, `subscribe()` callbacks, `on_retry` and handlers run on the loop thread.
  `ThreadedTunnel` delivers `subscribe()` callbacks through `dispatch` instead (on the loop
  thread only when no `dispatch` is given). Callbacks must be plain functions; a coroutine they
  return is closed and logged.
- Logs go to `logging.getLogger("mcpwarp")` (or `TunnelOptions.logger`). They never contain the
  token. Add your own handler; the SDK adds none.
- Vendor the package (see [README](../README.md#vendoring)) and import it relatively.

UI mapping that works for both hosts:

| `status.state` | Colour | Show |
|---|---|---|
| `off` | grey | `describe_status(st, locale).label` |
| `connecting` | amber | label, plus `detail` when set (retrying, connection lost) |
| `online` | green | `st.url` |
| `disabled`, `conflict`, `error` | red | label and `detail` |

## Anki

Anki 25.07+ (Python 3.13). The add-on already runs one daemon thread with
`asyncio.run(...)`, shared by uvicorn and the tunnel, and drives it from the Qt thread with
`asyncio.run_coroutine_threadsafe`. Use `Tunnel` directly on that loop; `ThreadedTunnel` would
add a second loop for nothing.

### Vendoring

Copy `src/ws_mixer/` to `anki_mcp_server/vendor/ws_mixer/`, next to `vendor/shared/`, not inside
it. `vendor/shared` is put on `sys.path`; `ws_mixer` must not be, so import it relatively:

```python
from .vendor.ws_mixer.mcpwarp import (
    Request,
    Response,
    TunnelConfig,
    create_tunnel,
    describe_status,
)
```

The `websockets` collision does not apply: the default `StdlibConnector` never imports
`websockets`, so it does not matter which add-on loaded which version first. Only if you opt into
`ws_mixer.adapters.websockets.websockets_connector()` does it import whatever `websockets` the
process already has; it needs `>=13,<18` and raises `RuntimeError` if `connect.process_redirect`
is missing.

### Handler

The add-on feeds requests to an in-process MCP server, not to a local HTTP port, so pass an async
handler, and keep the add-on's 25 s limit (before the edge's 30 s) with
`create_tunnel(..., handler_timeout_ms=25_000)` (see below):

```python
async def handle(req: Request) -> Response:
    body = await in_memory_mcp(req.body)  # yours: JSON-RPC bytes in, bytes out
    return Response(200, {"content-type": "application/json"}, body)
```

A handler that has not returned its response by then is cancelled and answered with a plain
`504 local server timed out` (a streaming body already under way is not cut). If you want a
JSON-RPC error body instead, leave `handler_timeout_ms` unset and wrap the handler yourself:

```python
TIMEOUT_BODY = b'{"jsonrpc":"2.0","id":null,"error":{"code":-32004,"message":"request timed out"}}'


async def handle(req: Request) -> Response:
    try:
        async with asyncio.timeout(25):
            body = await in_memory_mcp(req.body)
    except TimeoutError:
        return Response(504, {"content-type": "application/json"}, TIMEOUT_BODY)
    return Response(200, {"content-type": "application/json"}, body)
```

Only requests for exactly `/mcp` reach the handler; the SDK answers everything else
(404, 413, 503, ...) itself. An exception from the handler becomes `500 handler error`.

If you would rather serve the Starlette app uvicorn already runs, use
`handler=asgi_handler(app)`. The adapter never runs lifespan; that is fine here because uvicorn
runs it on the same loop. An MCP `StreamableHTTPSessionManager` must be running before the first
request.

### Creating, configuring, stopping

`create_tunnel()` is synchronous and loop-free, so it can run on the Qt thread. The tunnel binds
to the loop the first time one of its coroutines runs there. `configure()`, `stop()` and
`dispose()` do all their work inside the coroutine they return, so hand them to
`run_coroutine_threadsafe`:

```python
class TunnelController:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop  # McpServer's background loop
        self.tunnel = create_tunnel(
            integration=ANKI_INTEGRATION_UUID, handler=handle, handler_timeout_ms=25_000
        )
        self.name = "anki"

    def connect(self) -> None:  # Qt thread, on user request
        # self._token: the provider, see "Token refresh" below.
        cfg = TunnelConfig(enabled=True, token=self._token, name=self.name)
        asyncio.run_coroutine_threadsafe(self.tunnel.configure(cfg), self.loop)

    def disconnect(self) -> None:  # Qt thread
        asyncio.run_coroutine_threadsafe(self.tunnel.stop(), self.loop)
```

`configure()` returns once the new connection is dialing, not when it is online. It never
raises: a bad config shows up as `error`/`INVALID_CONFIG`. Unchanged settings are a no-op, so it
is safe to call on every settings-dialog save. After `conflict` or `error` the tunnel stays down
until the next `configure()`, even with the same settings.

### Status polling

Keep the existing 1 s `QTimer`. Read `status` on the Qt thread; no lock, no signal needed:

```python
import anki.lang


def refresh_status(self) -> None:  # Qt thread, every 1 s
    st = self.controller.tunnel.status
    text = describe_status(st, anki.lang.current_lang)  # "de_DE" works: "_" reads as "-"
    self.label.setText(text.label)
    self.label.setToolTip(text.detail or st.url or "")
```

Read `anki.lang.current_lang` at call time (not `from anki.lang import current_lang`), since Anki
reassigns it when the user changes language. Locales other than en, de, es, fr, ja and zh-CN fall
back to the language subtag, then to English.

Polling also hides short transitions: a reconnect shows as `connecting`, never as `off`.

### Token refresh

Pass a token provider instead of a token: a function, sync or async, returning the current
access token. The tunnel calls it on every dial (never caching the result), on the loop
thread, so do slow work in an async provider. After a rejected token it calls it once more with
`token_refresh_requested()` returning True: refresh then.

```python
from .vendor.ws_mixer import TokenUnavailableError, token_refresh_requested


async def _token(self) -> str:  # loop thread, on every dial
    try:
        return await get_oauth_token(refresh=token_refresh_requested())  # yours
    except OfflineError as e:  # yours: try again later
        raise TokenUnavailableError("can't reach the login server") from e
    except NeedsLogin:  # yours: stops the tunnel
        show_login_needed()  # yours (marshal to the Qt thread)
        raise
```

`TokenUnavailableError` (directly or as `__cause__`) shows `connecting`/`RETRYING` and the dial
is retried with the usual backoff. Any other exception ends in `error`/`DISCONNECTED_FATAL`
until the next `configure()`. A fresh token that is rejected too ends in `error`/`UNAUTHORIZED`.
`self._token` compares equal on every read, so saving unchanged settings does not redial; a new
`lambda` each time would. Neither the token nor the provider's result is logged, but an
exception's message is.

### Shutdown on profile close

`McpServer.stop()` runs on the Qt thread during `profile_will_close`: it flips uvicorn's
`should_exit` (or sets `_async_shutdown` in tunnel-only mode) and joins the loop thread with a
3 s timeout. Dispose the tunnel inside `McpServer`'s own async shutdown, at the end of
`_async_main()`, so it shares that 3 s budget and the Qt thread never waits on a separate
future:

```python
async def _async_main(self) -> None:
    ...
    try:
        if self._config.http_enabled:
            await self._run_http_mode(mcp)
        else:
            await self._async_shutdown.wait()
    finally:
        await self.tunnel_controller.tunnel.dispose(timeout_ms=2000)
```

Uvicorn unwinds within about 100 ms of `should_exit` (longer while it waits for open
connections), which leaves the 2 s dispose inside the 3 s join. If you dispose from the Qt thread
instead, with `run_coroutine_threadsafe(tunnel.dispose(...), loop).result(timeout=2.5)` before
`mcp_server.stop()`, the Qt thread can block for 2.5 s plus the 3 s join.

`dispose()` sends `unregister` (waits up to 2 s for `unregistered`), then closes gracefully
(drain up to 5 s). With `timeout_ms=2000` it returns after at most 2 s and leaves the rest to the
background; stopping the loop then cancels it. The edge sees an abrupt close at worst, and the
next register takes the name back. `dispose()` is final: create a new tunnel for the next
profile. `dispose(timeout_ms=...)` raises `TypeError`/`ValueError` at the call for a timeout that
is not a positive finite number.

## Blender

Blender 4.5 LTS (Python 3.11) and 5.2 LTS (Python 3.13). `bpy` is not thread-safe, there is no
asyncio loop on the main thread, and add-ons are unloaded and reloaded at any time. Use
`ThreadedTunnel` (`ws_mixer.mcpwarp.threaded`): `create_tunnel()` on a private daemon thread
with its own loop.

### What ThreadedTunnel gives you

```text
ThreadedTunnel(options=None, /, *, dispatch=None, thread_name="mcpwarp", **tunnel_options)
    .start() -> None                    # starts the thread and an "off" Tunnel; never dials
    .configure(config) -> concurrent.futures.Future[None]
    .disconnect() -> concurrent.futures.Future[None]    # Tunnel.stop(): off, thread keeps running
    .status -> TunnelStatus             # lock-free; "off" before start() and after stop()
    .subscribe(cb) -> Callable[[], None]                # cb runs through `dispatch`
    .call_dispatched(fn) -> awaitable                   # run fn on the host thread, await it on the loop
    .stop(timeout=3.0) -> bool          # bounded, idempotent, any thread, safe before start()
```

The constructor validates the options like `create_tunnel()` and raises at once. It also raises
`ValueError` for a `LoopClock` in `clock` (or inside a `StdlibConnector(clock=...)`): every
`start()` makes a new loop, so leave `clock` unset. Every method is safe from any thread.
`subscribe()` works before `start()` and survives `stop()`/`start()`. `stop()` runs `dispose()`
with 0.8 x `timeout`, aborts any socket still open, sends subscribers a final `off`, cancels what
is left and joins the thread. It returns True once the thread has exited, and False when called
from the loop thread (the shutdown is then scheduled). Below ~2.5 s the graceful unregister is
cut short; that is harmless.

### Main-thread dispatch

`dispatch` receives zero-argument callables. Put them on a queue and drain it from a
`bpy.app.timers` function, which runs on the main thread:

```python
import queue
import bpy
from .vendor.ws_mixer.mcpwarp import (
    Request,
    Response,
    ThreadedTunnel,
    TunnelConfig,
    describe_status,
)

_q: queue.SimpleQueue = queue.SimpleQueue()
_tt: ThreadedTunnel | None = None
_unsubscribe = None


def _drain() -> float:
    while True:
        try:
            fn = _q.get_nowait()
        except queue.Empty:
            return 0.2  # seconds until the next run
        fn()  # SDK-wrapped: never raises an Exception


async def _handle(req: Request) -> Response:  # loop thread
    body = await _tt.call_dispatched(lambda: run_mcp_on_main_thread(req.body))  # yours
    return Response(200, {"content-type": "application/json"}, body)


def _on_status(st) -> None:  # main thread, via _drain
    store_status_text(describe_status(st, bpy.app.translations.locale))  # yours
    redraw_panels()  # yours


def register() -> None:
    global _tt, _unsubscribe
    _tt = ThreadedTunnel(integration=BLENDER_INTEGRATION_UUID, handler=_handle, dispatch=_q.put)
    _unsubscribe = _tt.subscribe(_on_status)
    _tt.start()  # thread only; never dials
    bpy.app.timers.register(_drain, first_interval=0.2, persistent=True)
```

Every `bpy` call goes through `call_dispatched`; never touch `bpy` from the loop thread.
`call_dispatched` is awaited on the loop, so other requests keep flowing while the main thread
works. Timers only run while the window manager's event loop runs: under `blender -b` nothing
drains the queue, so handlers that need `bpy` hang. A non-persistent timer is dropped on file
load; keep `persistent=True`.

### Network access

Never dial at import or in `register()`. `ThreadedTunnel()` and `start()` do not dial; only
`configure()` with `enabled=True` and a token does. Gate it on the user's choice and on
Blender's online-access preference:

```python
def enable_tunnel(token: str, name: str) -> None:  # from an operator, on a click
    if not bpy.app.online_access:
        report_error("Allow online access in Preferences > System")  # yours
        return
    _tt.configure(TunnelConfig(enabled=True, token=token, name=name))


def disable_tunnel() -> None:
    _tt.disconnect()  # started in register(), so never raises
```

### Unregister

`unregister()` runs on disable, uninstall, Reload Scripts and extension updates. A thread left
running keeps its socket open, and the reloaded add-on's new connection then takes the name over
from it. Stop synchronously and remove the timer:

```python
def unregister() -> None:
    global _tt, _unsubscribe
    if _unsubscribe is not None:
        _unsubscribe()
    if _tt is not None:
        _tt.stop(timeout=3.0)  # blocks the UI for at most 3 s
        _tt = None
    if bpy.app.timers.is_registered(_drain):
        bpy.app.timers.unregister(_drain)
```

`stop()` is safe twice and safe if `start()` never ran. The thread is a daemon, so interpreter
exit never waits for it; an `atexit` hook calling `stop()` is still a good backstop. Blocking
`run_in_executor` jobs are the exception: executor workers are not daemon threads, so keep any
`sync_handler` work bounded.

### TLS on macOS

Blender's bundled Python on macOS has an empty default CA store (`/dev/null/cert.pem`), so a
default `ssl` context fails every `wss://` handshake with `CERTIFICATE_VERIFY_FAILED`. The SDK's
default context uses `certifi` when it can import it, and Blender bundles `certifi`, so the
default works. To be explicit, or to use another bundle:

```python
import ssl, certifi
from .vendor.ws_mixer import StdlibConnector

ctx = ssl.create_default_context(cafile=certifi.where())
_tt = ThreadedTunnel(
    integration=..., handler=_handle, dispatch=_q.put, web_socket=StdlibConnector(ssl_context=ctx)
)
```

When neither `certifi` nor a system store is available, the SDK logs one warning on the
`ws_mixer` logger naming both fixes. It never turns verification off.

### Packaging: extension or legacy add-on

- **Vendoring (recommended for both).** Copy `src/ws_mixer/` under your add-on package and import
  it relatively. Nothing is shared with other add-ons, so no version collision.
- **Extension wheels (4.2+).** ws-mixer is a pure-Python `py3-none-any` wheel with no
  dependencies, so `blender_manifest.toml` can list it:

  ```toml
  wheels = ["./wheels/ws_mixer-0.1.0-py3-none-any.whl"]

  [permissions]
  network = "Publish this scene's MCP server at its mcpwarp URL"
  ```

  Each extension gets its own site-packages for its wheels, but `sys.modules` is process-wide:
  the first `ws_mixer` imported wins, and every other extension importing `ws_mixer` gets that
  copy, whatever version it shipped. Vendoring under your own package name avoids that.
- **Legacy add-ons.** No wheel support: vendor.

Either way, declare the network permission for an extension, and do not `pip install` at runtime.

### No signals, no loop on the main thread

Do not call `asyncio.run()` on Blender's main thread and do not install signal handlers.
`ThreadedTunnel` runs its loop with `loop.run_forever()` on its own thread, which installs none.
