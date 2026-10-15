# Deviations

Every known place where this SDK differs from the spec text at `spec.pin` (v0.6.1) or from
ws-mixer-js 0.11.0, and why. Each entry has one mark:

- **spec-ruled**: the spec session ruled on it after v0.6.1; the ruling lands in v0.6.2/v0.6.3
  and the SDK already follows it.
- **JS deviation**: the behaviour differs from ws-mixer-js; the spec allows both, or does not
  say.
- **Python choice**: a decision of this SDK where the spec is silent, stale or still pending.

Rulings are quoted from the spec session's answers of 2026-10-14 (item numbers are theirs) and
the later v0.6.3 rulings.

## Wire and transport

### Integers accept integral floats, never `bool` (spec-ruled)

Every integer field accepts any integral JSON number (`1.0 == 1`), as JS's `Number.isInteger`
does; `true`/`false` are never integers. A `bool` mcpwarp `v` decodes as an unknown version
(`Unknown`, ignored), not `MALFORMED_ENVELOPE`. Senders always emit plain ints.
Ruling: items 1-7, item 4. Go rejects `1.0`; JS and Python accept it.

### String limits count code points (spec-ruled)

`token` 4096, `error.message` 1024, `drain.message` 256, `session` 64 are counted in code points
(`len()`, JSON Schema `maxLength`), not UTF-8 bytes or UTF-16 units. Senders also stay within the
same byte count. Ruling: item 5.

### Client drain values (spec-ruled)

`close()` sends `drain{reason: "client_requested", last_stream_id: 0, deadline_ms: 5000}`:
`last_stream_id` is the highest stream this side opened, and the client opens none. An `OPEN`
that crosses our drain on the wire gets `RESET REFUSED_STREAM`, never a connection error.
Ruling: item 1.

### Drain hand-over retire timing (spec-ruled)

After a received drain the old connection is retired at `deadline_ms + 1000` (30000 when absent),
timed from the replacement's `welcome`, or earlier once no stream is open. Survivors get a local
`RESET CANCEL`, then `error{NO_ERROR}` and close 1000. Matches JS (D-2026-10-12-04); Go cancels at
the replacement's welcome. Ruling: item 2.

### STREAM_LIMIT is counted, not escalated (spec-ruled)

An `OPEN` above the declared `max_streams` gets `RESET STREAM_LIMIT` and `refused_opens` goes up.
Repeat offences never escalate to `ENHANCE_YOUR_CALM`. Escalation is a MAY; counting without it
conforms. Ruling: item 3.

### `drain.retry_after_ms` is a floor (spec-ruled)

The replacement dial waits `retry_after_ms` plus 0-2 s of jitter, not `retry_after_ms` alone.
Ruling (v0.6.3, binding): wait at least `retry_after_ms` plus up to 2000 ms jitter.

### Stale zero-increment WINDOW (spec-ruled)

A `WINDOW` with increment 0 for a stream that was opened and has since ended is a stale frame:
discarded and counted in `stale_frames`. For a never-opened id it is a connection
`PROTOCOL_ERROR`; for a live stream it resets that stream. Normative in v0.6.3.

### Application close takes a message only (spec-ruled)

`close(message)` always sends `error{APPLICATION_CLOSE, message}` and close 4014. There is no way
to pick a close code. WIRE.md section 2.10 step 14 ("close path with an arbitrary code") is
stale; CLIENT-SDK.md and D-2026-09-25-01 win. Ruling: peer answer 3(a).

### `TokenUnavailableError` (spec-ruled)

The temporary-failure marker for token providers is `TokenUnavailableError`, found with
`isinstance` on the exception and its `__cause__` chain (10 links, cycle-safe), never
`__context__`. Ruling: (c), accepted for the CLIENT-SDK Provider failure row. With vendored
copies, raise the class from the copy you call.

### `hello.meta` identity (spec-ruled)

`hello.meta.client = "sdk-python"` (corrected from the proposal's `"sdk-py"`),
`hello.agent.sdk = "ws-mixer-py"`, `runtime` like `python/3.13.5`, `os` like `darwin/arm64`.
`hello.meta.version` is cut to 100 bytes. Ruling: item 8 and its correction.

### Empty `error.message` accepted (Python choice)

The schema says `error.message` is 1..1024 characters. On receive an empty message is accepted,
because ws-mixer-go sends `message: ""` from `Conn.Close(code, "")` and JS and Go both accept it.
Rejecting it would mean answering a terminal `error` with another one. No ruling yet.

### `reset()` rejects connection-only codes (spec-ruled, SHOULD; JS is the deviation)

`MixerStream.reset(code)` accepts only the stream-scoped codes (`PROTOCOL_ERROR`,
`INTERNAL_ERROR`, `STREAM_CLOSED`, `REFUSED_STREAM`, `CANCEL`, `STREAM_LIMIT`,
`ENHANCE_YOUR_CALM`), their names, or application codes `0x1000_0000..0xFFFF_FFFF`; anything else
raises `ValueError`. WIRE.md section 2.8 defines which codes are stream-scoped. JS sends whatever
code it is given.

### Ping without `ts` (JS deviation)

Keepalive pings are `{"t": "ping", "id": n}`. JS adds `ts: Date.now()`. `ts` is optional and its
unit is epoch milliseconds, which only a wall clock gives; the SDK's timers run on the injected
monotonic `Clock`. A peer's `ts` is still echoed in the pong.

### No RFC 6455 pings from `StdlibConnector` (Python choice)

The default connector answers pings but never sends them. The in-band ws-mixer ping is the
liveness authority, and JS's default socket does not ping either. The `websockets` adapter sends
library pings every 20 s with no death timer.

### No HTTP CONNECT proxy in `StdlibConnector` (Python choice)

The default connector dials directly. Behind a proxy, use
`ws_mixer.adapters.websockets.websockets_connector()`, which uses websockets' proxy support
(newer releases; reads `wss_proxy`/`https_proxy` and OS settings). A proxy
refusing `CONNECT` is `ProxyRejected`, always fatal (a 407 would otherwise retry forever, and a
proxy's 403/404 says nothing about the edge). The proposal left CONNECT support to the owner
(about 80 more lines); 0.1.0 ships without it.

### TLS certificate errors are retried (Python choice)

`ssl.SSLCertVerificationError` is an `OSError`, so a failed certificate check is an ordinary
failed dial with backoff, not a fatal error. A clock skew or a captive portal can fix itself. The
default context never disables verification; it uses certifi when importable and warns once when
no CA store exists.

### Only 429 honours `Retry-After` (Python choice)

An upgrade rejected with 429 waits `Retry-After` (seconds or HTTP date, clamped to 0-1 h). A 503
and other 5xx use normal backoff. This follows the WIRE.md section 2.9 table and JS; the proposal
draft said 429 and 503.

### Our own graceful close has no `error_code` (spec-ruled, normative)

When this side closes gracefully, retires a drained connection or closes after a take-over
(`error{NO_ERROR}`, close 1000), the `DisconnectReason` has `error_code=None`. Our application
close (14) and local violations do carry their code. This is the CLIENT-SDK.md four-sources rule
read literally: NO_ERROR from ourselves is none of the four sources.

### Listener exceptions are isolated (JS deviation)

In `MixerClient`, `MixerConn` and `ExposeHandle` a listener that raises does not stop the other
listeners; its exception is re-emitted as `error` (dropped when an `error` listener raised it).
Node's emitter stops at the first throw. Only `Exception` is isolated; `CancelledError` and
`KeyboardInterrupt` propagate.

## mcpwarp: expose and bridging

### Handler is `async (Request) -> Response`; ASGI through `asgi_handler` (spec-ruled)

MCPWARP.md section 9 says `handler (ASGI app)`. The SDK takes a plain async handler and adapts
ASGI 3 apps with `asgi_handler(app)`. Ruling: (f), accepted; the section 9 row becomes
"async Request -> Response; asgi_handler(app) adapts ASGI 3", with the ASGI mapping folded in as
informative.

### Cancellation is the abort path (spec-ruled)

`signal` maps to task cancellation. Cancelling the task awaiting `expose()` before it settles
closes the client in the background and re-raises `CancelledError`; the handle's `close` event
carries `Closed(error=CancelledError())`. There is no status change and no `AbortError` class.
Ruling: (d), accepted.

### Content-Encoding passes through in target mode (spec-ruled)

The target client never decodes a response body, so `Content-Encoding` and the bytes go back
untouched. MCPWARP.md section 4.6's "decoded response re-served without content-encoding" applies
only to clients that decode. Ruling: items 1-7, item 7.

### Requests during a reconnect after the first registration are served (spec-ruled)

Before the first `registered`, a request in state `connecting`/`registering` gets
`503 service unavailable`. After it, requests that arrive while the SDK reconnects or
re-registers (a drain hand-over, a lost connection) are served: the edge keeps routing to the
connection that still holds the name until the new register lands. `disabled` is 503
`service disabled`, `closing`/`closed` 503 `service closing`, `conflict` a `RESET CANCEL`. The
proposal had 503 for every `connecting`/`registering`.

### `registered`/`disable`/`enable` from an ended connection are ignored (spec-ruled)

The delivery backlog can hand over messages after their connection ended. Such a connection's
registration is gone, so `registered`, `disable` and `enable` from it do not move the state;
`conflict` and `error` still apply.

### A target without a leading `/` is 400 (spec-ruled; JS and Go deviate slightly)

A request target that is neither origin-form (`/...`) nor absolute-form (`http(s)://...`), such
as `\mcp`, is answered `400 unsupported request target form`, before the section 4.4 path check
that would give 404. It is a malformed request, not a wrong path.

### SDK replies to HEAD are head-only, also for parse errors (spec-ruled)

Once the request line has parsed, every SDK-generated reply (400, 404, 413, 431, 503) to a `HEAD`
request carries the head and `Content-Length` but no body, including errors found later in the
head. Before the request line parses, the method is unknown and the full reply is sent.

### 3xx from the target passes through (spec-ruled, informative, accepted)

Target mode does not follow redirects; a 3xx from the local server goes back to the edge like any
other response. JS still answers `502 ... cannot pass through` where its runtime's `fetch` hides
the redirect (`opaqueredirect`). The proposal draft also said 502.

### `handler_timeout_ms`: an opt-in handler-mode deadline (Python choice, owner-approved 2026-10-15)

The spec (section 4) has no per-request deadline in handler mode; target mode has
`target_timeout_ms` (default 30000) for the response head. `ExposeOptions.handler_timeout_ms` and
`TunnelOptions.handler_timeout_ms` add one, as an SDK extension: when set, an async handler that
has not returned its response within that many ms is cancelled and the edge gets
`504 {"error":"local server timed out"}` (byte-exact as target mode's, head-only to `HEAD`), then
CLOSE. Like `target_timeout_ms` it bounds the response head only: a returned body may stream
past it. A synchronous handler that returns a response directly is never cut short (it cannot
be), and `sync_handler`'s thread runs to completion with its result dropped. Allowed only with
`handler` (`TypeError` with `target`); a positive number of at most 2147483647 ms. The default,
None, applies no deadline, as the spec does. Anki sets 25 s so its 504 lands before the edge's own
30 s timeout.

### The request body is always buffered (Python choice)

`Request.body` is the whole body, read and size-checked before the handler runs. The
`*_reset` bridging fixtures, which only apply to adapters that stream the body to the app, are
skipped (MCPWARP.md section 10.3 allows it).

### ASGI mapping details (spec-ruled)

Where `asgi_handler` differs from the mapping table the proposal sent to the spec session:

- **No `root_path` argument.** `asgi_handler(app)` takes only the app; the scope's `root_path`
  is always `""`. The proposal had `asgi_handler(app, *, root_path="")`.
- **CLOSE before background tasks.** Once the response is complete, the bridge CLOSEs the stream
  and then waits for the app to return. Work the app does after its last body message (Starlette
  background tasks) does not delay the client. `receive()` returns `http.disconnect` once the
  response is complete.
- **Completion by Content-Length.** A response is complete when the app sends its last body
  message or its declared `Content-Length` is reached. Bytes past the declared length, and
  further body messages, are dropped without an error.
- After the edge goes away, the app gets `http.disconnect`, one loop turn, and then
  cancellation; `send()` from then on raises `OSError` (ASGI HTTP spec 2.4).

## mcpwarp: tunnel controller

### `TunnelConfig.token` takes a `TokenProvider` (spec-ruled, provisional)

Item 9, owner-approved; provisional for v0.6.3 as a profile change for all SDKs. Until
ws-mixer-js follows, it is also a JS deviation. `TunnelConfig.token` is a `str` or a
`TokenProvider` (sync or async callable returning a `str`), passed through to `expose()`:

- Check 4 is "neither a `str` nor callable": `error`/`INVALID_CONFIG`, "The token is not set.",
  logged as `` INVALID_CONFIG: `token` must be a string or a callable `` (wording spec-ruled for
  v0.6.3). A blank `str` never reaches it:
  it is "off" (check 3). `configure()` never calls, trims or inspects a provider.
- Idempotence compares a provider with `==` (identity for a plain function; receiver and
  function for a bound method), the connector rule; a `str` token still compares trimmed.
- At runtime the transport calls the provider on every dial, never caches it, and calls it once
  more with `token_refresh_requested()` True after HTTP 401 or close 4011 before welcome
  (`on_retry` gets `(1, 0, "token-refresh")`). A `TokenUnavailableError` is a failed dial:
  `connecting`/`RETRYING`, normal backoff (T27). Any other provider exception is fatal:
  `error`/`DISCONNECTED_FATAL` (the `McpwarpError`'s `__cause__` is the transport's
  `WsMixerError`, whose `__cause__` is the provider's exception) (T33). A token rejected again
  after the refresh is `error`/`UNAUTHORIZED` (T32).

Neither the token nor a provider's result is ever logged; a provider's exception message is
(as the `DISCONNECTED_FATAL` log line, or the retry reason), so keep tokens out of it.

### Connector idempotence compares with `==` (spec-ruled)

`configure()` treats two configs as equal when their effective connectors compare equal with
`==`. That is identity for plain objects, and also equal for two lookups of the same bound
method (same receiver, same function), where `is` would differ. Ruling: items 1-7, item 6.

### `configure()` and `stop()` are coroutines (JS deviation)

In JS the synchronous part of `configure()` (the checks, the status change to `connecting`) runs
when it is called. In Python `configure()` and `stop()` are `async def`: nothing happens until
the coroutine runs on the tunnel's loop. That is what lets a host thread hand them to
`asyncio.run_coroutine_threadsafe`. `dispose(timeout_ms)` is a plain method that checks
`timeout_ms` at the call (so a bad value raises synchronously, as section 6.3 requires) and
returns the coroutine.

### `subscribe()` does not deduplicate listeners (JS deviation)

JS keeps listeners in a `Set`, so subscribing the same function twice registers it once. Python
registers each `subscribe()` call separately, and each returned unsubscribe function removes only
its own registration. Status deduplication (no call for an identical status) is the same in
both.

### Check 7 log wording (spec-ruled: option-field wording is SDK-local)

Section 6.2 check 7 logs ``INVALID_CONFIG: `webSocket` must be a function``. Python logs
``INVALID_CONFIG: `web_socket` must be callable``, using the Python field name. Checks 1-6 log
the spec text verbatim.

### No separate "disabled in the dashboard" warn line (Python choice)

Section 6.9 lists two `warn` lines for a disabled service: `the service is disabled in the
mcpwarp dashboard` and `the service was disabled (<reason>)`. The tunnel logs only the second,
`the service was disabled (<reason>)`, once per `disabled` event.

### ThreadedTunnel rejects a bound `LoopClock` (Python choice)

Every `ThreadedTunnel.start()` creates a new loop, and a `LoopClock` binds to the first loop it
is used on, so it would break the next run. `ThreadedTunnel(...)` raises `ValueError` when
`clock` is a `LoopClock` or `web_socket` is a `StdlibConnector` built with a `LoopClock`. Leave
`clock` unset.

### Callbacks must be plain functions (Python choice)

`on_retry` and `subscribe` callbacks are called synchronously. If one returns a coroutine, it is
closed (never awaited) and a warning is logged.

### `stop(timeout=3.0)` for threaded hosts (Python choice)

`LoopThread.stop()` and `ThreadedTunnel.stop()` default to 3 s, and `ThreadedTunnel` gives 0.8 x
the timeout to `dispose()`. A graceful close is unregister (up to 2 s) plus drain (up to 5 s); a
2 s default (the proposal's first draft) would regularly cut the unregister short.

## Python versus JS behaviour

### Unknown state labels use `str()` (JS deviation)

`describe_status` labels an unknown state with `str(state)`, the Python reading of JS
`String()`. A missing state is therefore `"None"`, not `"undefined"`.

### Whitespace and integers follow JS (Python choice)

`str.strip()` and `re`'s `\s` differ from JS (`U+001C..U+001F`, `U+0085`). Token trimming and the
status-text colon rule use the ECMAScript whitespace set (`_jscompat.py`), so results match JS
byte for byte.

### Responses and messages are recognised by shape (Python choice)

A handler's response is accepted when it has an int `status` plus `headers` and `body`, never by
`isinstance`, so a `Response` from another vendored copy works. The same holds for
`StatusMessages` in `describe_status`. Exceptions cannot work this way: the spec forbids
duck-typing the token-unavailable marker.

### Python additions

Not in JS, documented here so they are not mistaken for spec features: `token_refresh_requested()`,
public `on_reconnect`, `MixerClient.abort()`, `on`/`once` returning an unsubscribe function,
`LoopThread`, `ThreadedTunnel`, `sync_handler`, `target_handler` as a public name, `Timing`
as a public dataclass, and `handler_timeout_ms`.
