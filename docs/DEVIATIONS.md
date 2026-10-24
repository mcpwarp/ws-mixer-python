# Deviations

Every known place where this SDK differs from the spec text at `spec.pin` (v0.8.1) or from
ws-mixer-js 0.11.2, and why. Each entry has one mark:

- **spec-ruled**: the spec session ruled on it, the ruling is not in the pinned text yet, and
  the SDK already follows it.
- **in spec at v0.6.2** / **in spec at v0.6.3** / **in spec at v0.7.1** / **in spec at
  v0.8.1**: was spec-ruled; the pinned spec text now says it, so it is no longer a deviation.
  Kept for the ruling history. An entry may also name the later tag that clarified it (v0.7.2,
  v0.8.0, v0.8.1).
- **open**: the pinned spec text says otherwise and the SDK does not follow it yet.
- **JS deviation**: the behaviour differs from ws-mixer-js; the spec allows both, or does not
  say.
- **Python choice**: a decision of this SDK where the spec is silent, stale or still pending.
- **profile**: a part of the pinned spec no SDK ships (MCPWARP.md section 1); not a deviation.

No entry is spec-ruled and none is open.

Rulings are quoted from the spec session's answers of 2026-10-14 (item numbers are theirs) and
the later v0.6.3 rulings.

## Profile

### Single-service profile (profile)

Since v0.8.0 (D-2026-10-24-01) no SDK ships a multi-service controller: section 6A is
informative guidance for multi-service hosts, and every SDK is single-service (`expose()` and
the tunnel). Up to v0.7.2 this SDK was the single-service subset of a multi-service profile; the
scope is unchanged. It does not implement:

- batch `register`/`unregister` (1..100 entries per frame, section 2.1 Batch): it always sends
  one entry, and `encode_register`/`encode_unregister` take one service;
- per-name outcomes of a multi-service connection: per-name `conflict`/take-over, partial
  `registered` replies, subset `unregister` (sections 2.3, 2.7, 3.3 Per name); a `conflict` or a
  reply entry for another name is ignored, as section 2.3 has a single-service agent do;
- routing a stream by `X-Mcpwarp-Server-Id` or by the `Host` label, and the
  `trust_server_id_header` option (section 4.8); every stream goes to the one service, and both
  headers reach the application like any other header;
- the `No service could be registered.` fatal case and its `lastError.NO_SERVICE_REGISTERED`
  text, the `service.*` status labels and `describe_service_status` (sections 6A.6, 6A.9).

The fixture runners skip the matching host guidance fixtures by a declared allow-list, as
section 10.6 Fixture status says (docs/CONFORMANCE.md, Profile).

### Envelope schema verdicts are not asserted (in spec at v0.8.1)

v0.8.0 (MCPWARP.md sections 10.1 and 10.6, Fixture status) still had an SDK run the envelope
schema-verdict cases, `register_batch_valid`, `register_two_services` and
`register_batch_101_invalid` included. The spec session's binding clarification of 2026-10-24
said schema verdicts are checker/CI tooling and an SDK runner asserts wire verdicts on decode
cases only. v0.8.1 says so (D-2026-10-24-05): sections 10.1 and 10.6 mark those three
`checker (CI)`, and `spec/README.md` keeps its general rule (SDK runtime validators assert
`wire_valid`, schema/CI tooling asserts `schema_valid`), which covers the six single-service
`SCHEMA_*` rows as well. `tests/spec/test_envelope.py` therefore skips the schema
check of those two encode-only cases (with the host guidance skip) and skips the nine
agent->edge cases whose only verdict is the edge's schema check (`SCHEMA_*` hints and
`register_batch_101_invalid`), with the reason `schema verdict only, checker/CI tooling, §10.6`.
Every case that still runs is also checked against `mcpwarp.schema.json`.

## Wire and transport

### Integers accept integral floats, never `bool` (in spec at v0.6.3)

Every integer field accepts any integral JSON number (`1.0 == 1`), as JS's `Number.isInteger`
does; `true`/`false` are never integers. A `bool` mcpwarp `v` decodes as an unknown version
(`Unknown`, ignored), not `MALFORMED_ENVELOPE`. Senders always emit plain ints.
Ruling: items 1-7, item 4. Go rejects `1.0`; JS and Python accept it. v0.6.3: WIRE.md section
2.7 Integer fields row, control fixtures `version_integral_float` and `version_boolean`.

### String limits count code points (in spec at v0.6.3)

`token` 4096, `error.message` 1024, `drain.message` 256, `session` 64 are counted in code points
(`len()`, JSON Schema `maxLength`), not UTF-8 bytes or UTF-16 units. Senders also stay within the
same byte count. Ruling: item 5. v0.6.3: WIRE.md section 2.7 String limits row.

### Client drain values (in spec at v0.6.3)

`close()` sends `drain{reason: "client_requested", last_stream_id: <highest_opened>,
deadline_ms: 5000}`, where `highest_opened` is the highest `OPEN` id received so far (0 if
none). An `OPEN` that crosses our drain on the wire gets `RESET REFUSED_STREAM`, never a
connection error. Ruling: item 1, which had `last_stream_id` as the highest stream this side
opened (always 0 for a client), and earlier 0.1.0 builds sent 0. v0.6.3's text (WIRE.md section
2.9 Client side of a drain, section 2.10 item 14) defines it as `highest_opened`, and the spec
session then ruled that text normative; JS and Go send `highest_opened` too.

### Drain hand-over retire timing (in spec at v0.6.3)

After a received drain the old connection is retired at `deadline_ms + 1000` (30000 when absent),
timed from the replacement's `welcome`, or earlier once no stream is open. Survivors get a local
`RESET CANCEL`, then `error{NO_ERROR}` and close 1000. Matches JS (D-2026-10-12-04); Go cancels at
the replacement's welcome. Ruling: item 2. v0.6.3: WIRE.md section 2.9 Client side of a drain.

### STREAM_LIMIT is counted, not escalated (in spec at v0.6.3)

An `OPEN` above the declared `max_streams` gets `RESET STREAM_LIMIT` and `refused_opens` goes up.
Repeat offences never escalate to `ENHANCE_YOUR_CALM`. Escalation is a MAY; counting without it
conforms. Ruling: item 3. v0.6.3: WIRE.md section 2.8, "What kills the connection vs. the
stream".

### `drain.retry_after_ms` is a floor (in spec at v0.6.3)

The replacement dial waits `retry_after_ms` plus 0-2 s of jitter, not `retry_after_ms` alone.
Ruling (v0.6.3, binding): wait at least `retry_after_ms` plus up to 2000 ms jitter. Tunnel
fixture `41_drain_retry_after_honoured`. MCPWARP.md section 11 row 45 ("ignored") describes the
proposal, not this SDK. The hint and the total (hint plus jitter) are each capped at 2^31-1 ms,
the largest JS timer delay; JS 0.11.2 caps only the total, with the same result. Any JSON
integer is accepted, a 400-digit one included, as WIRE.md section 2.7's integer rule allows;
JS's `JSON.parse` turns an integer above about 1e308 into `Infinity`, which fails as a
`PROTOCOL_ERROR` there.

### Server retry hints are capped at 2^31-1 ms (Python choice, JS parity)

Both server hints share one cap, `ws_mixer.socket.MAX_RETRY_AFTER_MS` = 2^31-1 ms (about 24.8
days): `drain.retry_after_ms` (above) and a 429's `Retry-After`. Earlier 0.1.0 builds capped
`Retry-After` at 1 h and left `retry_after_ms` uncapped. v0.7.1 allows such a clamp
(informative; CLIENT-SDK.md Reconnect / backoff, MCPWARP.md section 5.9 Timer clamp). v0.8.0
adds that a server SHOULD NOT send `retry_after_ms` above 60 000 ms (WIRE.md section 2.9); the
client side is unchanged and still honours a larger value up to the cap.

### Stale zero-increment WINDOW (in spec at v0.6.3)

A `WINDOW` with increment 0 for a stream that was opened and has since ended is a stale frame:
discarded and counted in `stale_frames`. For a never-opened id it is a connection
`PROTOCOL_ERROR`; for a live stream it resets that stream. Normative in v0.6.3 (WIRE.md section
2.5), sequence fixture `window_stale_retired_stream`.

### Application close takes a message only (in spec at v0.6.2)

`close(message)` always sends `error{APPLICATION_CLOSE, message}` and close 4014. There is no way
to pick a close code. Up to v0.6.1, WIRE.md section 2.10 step 14 said "close path with an
arbitrary code"; v0.6.2 reworded it to match CLIENT-SDK.md and D-2026-09-25-01. Ruling: peer
answer 3(a).

### `TokenUnavailableError` (in spec at v0.6.2)

The temporary-failure marker for token providers is `TokenUnavailableError`, found with
`isinstance` on the exception and its `__cause__` chain (10 links, cycle-safe), never
`__context__`. Ruling: (c), accepted for the CLIENT-SDK Provider failure row. At v0.6.2 that row
leaves the marker's form to each SDK and its docs; this is ours. v0.6.3 names it in that row
(`ws_mixer.TokenUnavailableError`, `__cause__` chain, 10 links, cycle-safe). With vendored copies,
raise the class from the copy you call.

### `hello.meta` identity (in spec at v0.6.2)

`hello.meta.client = "sdk-python"` (corrected from the proposal's `"sdk-py"`),
`hello.agent.sdk = "ws-mixer-py"`, `runtime` like `python/3.13.5`, `os` like `darwin/arm64`.
`hello.meta.version` is cut to 100 bytes. Ruling: item 8 and its correction. MCPWARP.md section
3.1 names `sdk-python` at v0.6.2; the 100-byte cut was this SDK's own until v0.6.3 capped
`meta.version` at 100 bytes in section 3.1.

### Empty `error.message` accepted (Python choice)

The schema says `error.message` is 1..1024 characters. On receive an empty message is accepted,
because ws-mixer-go sends `message: ""` from `Conn.Close(code, "")` and JS and Go both accept it.
Rejecting it would mean answering a terminal `error` with another one. No ruling yet.

### `reset()` rejects connection-only codes (in spec at v0.6.3, SHOULD; JS is the deviation)

`MixerStream.reset(code)` accepts only the stream-scoped codes (`PROTOCOL_ERROR`,
`INTERNAL_ERROR`, `STREAM_CLOSED`, `REFUSED_STREAM`, `CANCEL`, `STREAM_LIMIT`,
`ENHANCE_YOUR_CALM`), their names, or application codes `0x1000_0000..0xFFFF_FFFF`; anything else
raises `ValueError`. WIRE.md section 2.8 defines which codes are stream-scoped, and at v0.6.3
says a reset API SHOULD reject the others locally. JS sends whatever code it is given.

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

An upgrade rejected with 429 waits `Retry-After` (seconds or HTTP date, clamped to 0 to 2^31-1
ms). A 503 and other 5xx use normal backoff. This follows the WIRE.md section 2.9 table and JS;
the proposal draft said 429 and 503.

### `Retry-After` date forms and a repeated header (JS deviation, kept)

HTTP dates parse in IMF-fixdate, RFC 850 and `+0000` forms, not asctime, ISO 8601 or `-0000`,
which JS `Date.parse` accepts. A repeated header arrives joined (`5, 10`) and gives no hint,
where Node takes the first value. Low impact: the edge sends delta-seconds.

### `welcome` and a close in one burst (in spec at v0.8.1)

A connection whose close arrives in the same delivery burst as its `welcome` "never becomes
current" (CLIENT-SDK.md Handler delivery, D-2026-10-24-03; v0.8.1, D-2026-10-24-05, words it
as below): no frame of that burst behind the `welcome` (an `app`, a `drain`, a
stream `OPEN`) is delivered to the application, and the retry after it is the connected-phase
disconnect's (`phase` `"connected"`, that close code's reconnect policy), never the handshake
failure path. A connected report before that disconnect is allowed, and Python makes one: it
promotes the connection synchronously inside the `welcome`'s dispatch (`welcome` event,
`on_connect`, so `expose()` sends a `register` that the dead socket drops), then on the close
drops the delivery queue that burst filled and forgets a `drain` read in it, so the retry is not
the drain's hand-over delay. Frames right behind a `welcome` of a connection that stays open are
delivered on the current connection, never dropped as stale. Earlier 0.1.0 builds also
delivered that burst's frames after the close (a `conflict` among them still applied, MCPWARP.md
section 2.7 Ended connection), and a `drain` in it set the retry delay. JS 0.11.2 reported the close as `"handshake"`
(MCPWARP.md section 11 row 59, fixed in 0.11.3).

### A drain is decided at ordered delivery (JS deviation, kept)

The reconnect decision for a `drain` runs when its ordered delivery turn comes (so an
`on_drain` take-over can still stop it), not on receipt as in JS 0.11.2. A raw `MixerClient`
whose `on_app` awaits slowly therefore delays the replacement dial; a close of the connection
before then still acts on the recorded drain. `expose()` handlers never await there.

### Our own graceful close has no `error_code` (in spec at v0.6.3)

When this side closes gracefully, retires a drained connection or closes after a take-over
(`error{NO_ERROR}`, close 1000), the `DisconnectReason` has `error_code=None`. Our application
close (14) and local violations do carry their code. This is the CLIENT-SDK.md four-sources rule
read literally: NO_ERROR from ourselves is none of the four sources. v0.6.3 says so in the
Disconnect reason shape row.

### Listener exceptions are isolated (JS deviation)

In `MixerClient`, `MixerConn` and `ExposeHandle` a listener that raises does not stop the other
listeners; its exception is re-emitted as `error` (dropped when an `error` listener raised it).
Node's emitter stops at the first throw. Only `Exception` is isolated; `CancelledError` and
`KeyboardInterrupt` propagate.

## mcpwarp: expose and bridging

### Handler is `async (Request) -> Response`; ASGI through `asgi_handler` (in spec at v0.6.3)

MCPWARP.md section 9 said `handler (ASGI app)` up to v0.6.2. The SDK takes a plain async handler
and adapts ASGI 3 apps with `asgi_handler(app)`. Ruling: (f), accepted; at v0.6.3 the section 9
row is "async Request -> Response; asgi_handler(app) adapts ASGI 3", with the ASGI mapping as
informative section 9.1.

### Cancellation is the abort path (in spec at v0.6.3)

`signal` maps to task cancellation. Cancelling the task awaiting `expose()` before it settles
closes the client in the background and re-raises `CancelledError`; the handle's `close` event
carries `Closed(error=CancelledError())`. There is no status change and no `AbortError` class.
Ruling: (d), accepted; the section 9 `signal` row maps it to task cancellation, and section 9's
Python column names `Closed(error)`.

### Content-Encoding passes through in target mode (in spec at v0.6.3)

The target client never decodes a response body, so `Content-Encoding` and the bytes go back
untouched. MCPWARP.md section 4.6's "decoded response re-served without content-encoding" applies
only to clients that decode, as v0.6.3 says in that row. Ruling: items 1-7, item 7.

### Requests during a reconnect after the first registration are served (in spec at v0.6.3)

Before the first `registered`, a request in state `connecting`/`registering` gets
`503 service unavailable`. After it, requests that arrive while the SDK reconnects or
re-registers (a drain hand-over, a lost connection) are served: the edge keeps routing to the
connection that still holds the name until the new register lands. `disabled` is 503
`service disabled`, `closing`/`closed` 503 `service closing`, `conflict` a `RESET CANCEL`. The
proposal had 503 for every `connecting`/`registering`. v0.6.3: section 4.4's fallback row.

### `registered`/`disable`/`enable` from an ended connection are ignored (in spec at v0.6.3)

The delivery backlog can hand over messages after their connection ended. Such a connection's
registration is gone, so `registered`, `disable` and `enable` from it do not move the state;
`conflict` and `error` still apply. v0.6.3: section 2.7 Ended connection, T51, tunnel fixture
`44_stale_disable_after_disconnect_ignored`.

### A `disable` with an unknown id is held (in spec at v0.6.3)

A `disable` whose id the handle does not know, including one before any `registered`, is held,
never adopted: the edge fans `disable` out to every connection of the owner. A later
`registered` bringing that id applies it: the handle emits `registered` then `disabled`, back to
back, state ending `disabled`, and the tunnel collapses the pair into one `disabled` snapshot
(the session marks that `registered` with the detail `held_disable`); an `enable` for the id
drops it; a held id that never arrives changes nothing. Held ids live only as long as their
connection: a disconnect, a take-over and a new welcome (a drain hand-over included) clear them,
since the next connection's register answer is authoritative (this also bounds the set). Earlier
0.1.0 builds adopted the id when none was known, as JS 0.11.1 did (MCPWARP.md section 11 row 28),
and emitted `disabled` alone, as JS 0.11.1 also did (section 11 row 52); JS 0.11.2 holds the id
and emits `registered` then `disabled`, as this SDK does.
v0.6.3: section 2.7 Unknown `disable` id, T49/T50, tunnel fixture `42_disable_unknown_id_held`.
v0.7.0: section 2.7 Held `disable` lifetime, section 5.9 Held `disable` (`registered` then
`disabled`).

### `enable` is keyed by name or server id (in spec at v0.6.3; JS deviation)

An `enable` applies when its `name` equals the agent's or its `id` equals the agent's server id;
the id is then adopted (v0.6.3 section 2.7). Earlier 0.1.0 builds matched the name only, as JS
0.11.1 did (`session.ts`). Any `enable` also drops a held `disable` for its id, whatever
its name; JS 0.11.2 drops it only when the name matches. The spec is silent
on that. The deployed edge always sends both fields for the same service; not fixture-covered.

### One frame in flight and the `OVERLOADED` backoff (in spec at v0.7.1)

Ruled by the spec session on 2026-10-18; in spec at v0.7.1 (D-2026-10-19-01: MCPWARP.md
sections 2.3 Empty reply, 2.6, 2.7, 3.2, 6.7 T13/T25/T26). v0.7.2 (D-2026-10-19-05) adds the
counter examples, the pause rule and the close skip below; v0.8.0 (D-2026-10-24-03) says the
`OVERLOADED` counter resets only on a reply that answers the outstanding frame (earlier 0.1.0
builds also reset it on a `registered` or `unregistered` answering none, such as a stray one
while close()'s dropped `unregister` waited for its re-send). v0.8.1 (D-2026-10-24-05) spells
out section 2.6's split between an outstanding and a re-queued frame, the stray `registered`
that changes no counter, and the `close()` case where the `unregistered` of an `unregister`
behind a dropped `register` does reset it; a stray `unregistered` is covered by the general
rule only. The counter counts on one
connection: a new `welcome` starts it over (earlier 0.1.0 builds carried it across
reconnects).

v0.7.0 section 2.6 has every agent re-send the dropped frame after `min(1000 * 2^(n-1), 30000)`
ms for every consecutive `OVERLOADED`, and this SDK does (earlier 0.1.0 builds waited a fixed
1000 ms). The v0.7.0 text makes `n` one counter shared with the transient register retry
("consecutive `OVERLOADED` (or all-transient) replies"); tunnel fixture
`26_t16_transient_refusal_on_replacement_unchanged` contradicts that. The spec session ruled the
shared-counter sentence an error: the `OVERLOADED` `n` counts consecutive
`OVERLOADED`s only and any other reply ends the streak; transient refusals keep section 3.2's
own counter, reset only by a successful reply (a `registered` with a non-transient entry: a
`services[]` entry, `SERVER_DISABLED` or a refusal) or by any `unregistered`; a whole-frame
`error` op does not reset it (v0.7.2 section 3.2 spells out these examples). Earlier 0.1.0
builds reset it on a `services[]` entry only. The same ruling, which this SDK follows:

- a `registered` that answers the outstanding register without listing the name is re-sent
  with the transient backoff (`on_retry` reason `register failed: INTERNAL: unlisted in reply`),
  no state change; the deployed edge never sends one;
- an `enable` arriving while a register is outstanding is queued and its register goes out once
  that one is answered (a scheduled re-send carries it instead); `close()` may still send
  `unregister` while a register is outstanding. Its wait ends on the `unregister`'s own reply
  by frame order (v0.7.1 section 3.2 Unregister): a `registered` or `error` op answering that
  register settles it and the wait goes on, and an `OVERLOADED` answering it abandons the
  register (never re-sent while closing). Earlier 0.1.0 builds blamed such an `OVERLOADED` on
  the `unregister`, sending it twice, and ended the wait on an `error` op meant for the
  register. `close()` sends no `unregister` when every register of the connection was dropped
  by `OVERLOADED` and no `registered` arrived on it (v0.7.2 section 3.2 Unregister); earlier
  0.1.0 builds sent one anyway;
- every `error` code other than `OVERLOADED` and `CONNECTION_LIMIT` (MCPWARP.md section 2.6
  names `TOO_MANY_SERVICES`, `UNKNOWN_OP`, `BAD_REQUEST` and unknown codes) answers the
  outstanding frame: it frees the in-flight slot and ends the `OVERLOADED` streak;
  `UNSUPPORTED_VERSION` is also fatal, and `CONNECTION_LIMIT` is a plain non-fatal error.
  `OVERLOADED` keeps the slot reserved for its own re-send (re-queued, not released), so
  nothing else goes out before that; a whole-frame `error` or an empty `registered` during
  that pause answers nothing and leaves the re-send and the `OVERLOADED` counter in place,
  while a `registered` naming the service cancels it (v0.7.2 section 2.6). Earlier 0.1.0
  builds treated an empty `registered` there as the register's answer. Ending `close()`'s
  `unregister` wait on any reply to that frame (`unregistered` or a whole-frame `error`) was
  ruled correct.

### A target without a leading `/` is 400 (in spec at v0.6.3; JS and Go deviate slightly)

A request target that is neither origin-form (`/...`) nor absolute-form (`http(s)://...`), such
as `\mcp`, is answered `400 unsupported request target form`, before the section 4.4 path check
that would give 404. It is a malformed request, not a wrong path. v0.6.3: sections 4.2 and 4.3,
bridging fixture `head_mcp_400_headonly`.

### `[` and `]` are forwarded verbatim (in spec at v0.6.3)

The request target's allowed set is RFC 3986 unreserved, sub-delims, `:` `@` `/` `?`, `[` `]`
and `%` (existing escapes and a lone `%` kept as-is). Every other byte, raw non-ASCII, space,
controls and `` {}|^\"`<> `` included, is percent-encoded exactly once; `#` and what follows
are cut. So `?a[]=1` reaches the local server as `?a[]=1`, as JS's WHATWG URL sends it; earlier
0.1.0 builds sent `%5B%5D`. The same holds for `[` `]` in the configured target URL and in
handler mode's `Request.url`. JS also encodes `'` in a query as `%27`, as WHATWG URL does for
every http(s) URL (the special-query set), so it does so in both of its modes (the forwarded
target and the handler's `Request.url`); Python keeps `'`. Ruled; in spec at v0.6.3 (section 4.3,
bridging fixtures `target_allowed_set`, `target_utf8_query`, `target_lone_percent`,
`target_fragment_dropped`, `target_fragment_dropped_absolute_form`).

### CONNECT, TRACE, TRACK and NUL in a header are 400 (in spec at v0.6.3)

A request with method `CONNECT`, `TRACE` or `TRACK` (any case) is answered
`400 request cannot be represented as a web Request` in every mode (handler, ASGI, target); the
handler is not called and nothing is forwarded. Same as JS in its handler and target modes, whose
WHATWG `Request`/`fetch` refuse those methods (its node server mode is unverified). Only the
methods are new: earlier 0.1.0 builds passed them on, while a forwarded
header value holding NUL, CR or LF was already this 400. The 400 comes after the 404, 503 and 413
checks and after the body is read. The spec sets this as the minimum; an SDK may refuse more.
Ruled; in spec at v0.6.3 (section 4.5, bridging fixtures `target_unrepresentable_400` and
`handler_connect_400`). MCPWARP.md section 11 row 48 still lists this as a Python deviation; it
describes the pre-release tree.

### A response status outside 200..599 is 500 (in spec at v0.6.3)

A handler, ASGI or target response with a status below 200 or above 599 is answered
`500 handler error` with an `error` event. Earlier 0.1.0 builds sent 600..999 through. v0.6.3:
section 4.5, bridging fixture `mcp_handler_status_600_500`.

### SDK replies to HEAD are head-only, also for parse errors (in spec at v0.6.3)

Once the request line has parsed, every SDK-generated reply (400, 404, 413, 431, 503) to a `HEAD`
request carries the head and `Content-Length` but no body, including errors found later in the
head. Before the request line parses, the method is unknown and the full reply is sent.

### 3xx from the target passes through (in spec at v0.6.3, informative)

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

### ASGI mapping details (in spec at v0.6.3, informative)

Where `asgi_handler` differs from the mapping table the proposal sent to the spec session (v0.6.3
section 9.1 now describes this behaviour):

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

### `TunnelConfig.token` takes a `TokenProvider` (in spec at v0.6.3)

Item 9, owner-approved; in spec at v0.6.3 as a profile change for all SDKs (section 6.2 Token
provider, T46-T48, D-2026-10-15-03). JS 0.11.2's `TunnelConfig.token` accepts a provider too
(section 11 row 31 was against 0.11.1). `TunnelConfig.token` is a `str` or a `TokenProvider`
(sync or async callable returning a `str`), passed through to `expose()`:

- Check 4 is "neither a `str` nor callable": `error`/`INVALID_CONFIG`, "The token is not set.",
  logged as `` INVALID_CONFIG: `token` must be a string or a callable `` (the v0.6.3 wording).
  A blank `str` never reaches it:
  it is "off" (check 3). `configure()` never calls, trims or inspects a provider.
- Idempotence compares a provider with `==` (identity for a plain function; receiver and
  function for a bound method), the connector rule; a `str` token still compares trimmed.
- At runtime the transport calls the provider on every dial, never caches it, and calls it once
  more with `token_refresh_requested()` True after HTTP 401 or close 4011 before welcome
  (`on_retry` gets `(1, 0, "token-refresh")`). The rejected dial is reported first as a
  non-fatal disconnect (phase `dial` or `handshake`) on `MixerClient` and `ExposeHandle`, so the
  status is `connecting`/`RETRYING` (T48); earlier 0.1.0 builds reported nothing and stayed
  `connecting`. That report is in spec at v0.6.3 (CLIENT-SDK.md: every disconnect is reported;
  the spec session ruled it required). JS 0.11.1 omitted this disconnect (v0.7.0 section 11
  row 53); JS 0.11.2 reports it. A `TokenUnavailableError` is a failed dial:
  `connecting`/`RETRYING`, normal backoff (T27, T46). Any other provider exception is fatal:
  `error`/`DISCONNECTED_FATAL` (the `McpwarpError`'s `__cause__` is the transport's
  `WsMixerError`, whose `__cause__` is the provider's exception) (T33). A token rejected again
  after the refresh is `error`/`UNAUTHORIZED` (T32). Tunnel fixtures
  `38_token_provider_refresh_retry`, `39_token_provider_unavailable_retrying`,
  `40_token_provider_error_fatal`.

Neither the token nor a provider's result is ever logged; a provider's exception message is
(as the `DISCONNECTED_FATAL` log line, or the retry reason), so keep tokens out of it.

### Connector idempotence compares with `==` (in spec at v0.6.3)

`configure()` treats two configs as equal when their effective connectors compare equal with
`==`. That is identity for plain objects, and also equal for two lookups of the same bound
method (same receiver, same function), where `is` would differ. Ruling: items 1-7, item 6.
v0.6.3: section 6.2 step 3.

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

### Check 7 log wording (in spec at v0.6.3: option-field wording is SDK-local)

Section 6.2 check 7 logs ``INVALID_CONFIG: `webSocket` must be a function``. Python logs
``INVALID_CONFIG: `web_socket` must be callable``, using the Python field name. Checks 1-6 log
the spec text verbatim. v0.6.3 section 6.9: only the `CODE: ` prefix is fixed, with this line as
its example.

### No separate "disabled in the dashboard" warn line (in spec at v0.6.3)

Up to v0.6.2, section 6.9 listed two `warn` lines for a disabled service: `the service is
disabled in the mcpwarp dashboard` and `the service was disabled (<reason>)`. The tunnel logs
only the second, `the service was disabled (<reason>)`, once per `disabled` event. v0.6.3 dropped
the first line from section 6.9.

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
