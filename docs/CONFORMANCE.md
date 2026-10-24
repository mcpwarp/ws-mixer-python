# Conformance

Two layers check this SDK against ws-mixer-spec at `spec.pin` (v0.8.1):

1. **pytest fixture suites** (`tests/spec/`): every fixture file the spec ships for a client
   SDK, run in-process on a fake clock.
2. **The Go conformance runner** from the spec repo, which drives `conformance/adapter/` over a
   real WebSocket, in fixture mode and paired with ws-mixer-go.

The unit tests in `tests/unit/` cover what neither reaches (rejected tokens, provider failures,
reconnect timing, handler backlog, stream teardown, CONFORMANCE.md section 1.3).

## Finding the spec

`tests/conftest.py` resolves the spec checkout in this order (same as ws-mixer-js):

1. `WSMIXER_SPEC_DIR`: the spec repo root or its `spec/` directory. If set and neither has
   fixtures, that is an error with no fallback.
2. `.spec/spec`, populated by `make fetch-spec`, while `.spec/.pin` equals `spec.pin`.
3. `../ws-mixer-spec/spec`, while that repo's HEAD is exactly the pinned tag and `spec/` has no
   local changes.

With none usable, spec tests are skipped locally and fail when `CI` is set.

`make fetch-spec` shallow-clones the tag in `spec.pin` into `.spec/` and stamps `.spec/.pin`; it
is a no-op when the stamp already matches. `make fetch-goserver` does the same for ws-mixer-go
at `goserver.pin` (v0.8.0) into `.goserver/`. Pins are tags, never SHAs.

## Fixture suites (pytest)

| Suite | File | Cases at v0.8.1 | What is checked |
|---|---|---|---|
| frames | `tests/spec/test_frames.py` | 29 | `decode_frame` gives `.decoded` or fails with `.expect`. |
| control | `tests/spec/test_control.py` | 51 | `parse_control` agrees with `wire_valid`; `control.schema.json` agrees with `schema_valid` and `control.relaxed.schema.json` with `wire_valid`. |
| sequences | `tests/spec/test_sequences.py` | 44: 25 client run, 19 server skipped | Client transcripts replayed against a real `MixerConn` over `FakeWebSocket` on `FakeClock`, at the spec's real timer values. Server transcripts are skipped: the SDK is client-only. |
| envelope | `tests/spec/test_envelope.py` | 68: 57 run, 11 skipped | Decode, `MALFORMED_ENVELOPE` or `Unknown`; register/unregister byte-exact against the encoder; every case that runs also through `mcpwarp.schema.json`. |
| bridging | `tests/spec/test_bridging.py` | 27: 22 run, 5 skipped | Raw HTTP/1.1 (`in` written as UTF-8) through `serve_stream`: byte-exact SDK replies, the forwarded target byte for byte, handler call, CLOSE/RESET. Target mode uses a local stub server that echoes or hangs. |
| tunnel | `tests/spec/test_tunnel.py` | 58: 44 run, 14 skipped | `create_tunnel()` over a scripted edge: `to_dict()` snapshots, `expectDials`, nothing left running after `dispose()`. Each `disconnect` event also checks the SDK's own `fatal` verdict against the fixture. `token_provider` events drive a token provider; `disconnect.flush_after` ops are handed to the layer right after the SDK reports the close (the `post-disconnect-delivery` runner requirement). |
| status-text | `tests/spec/test_status_text.py` | 8 files: 6 locales, `cases.json` (37 cases: 26 run, 11 skipped), `rules.json` | The `<state>.<CODE>` keys string-identical to the spec JSON (the seven multi-service keys must be present, not rendered); every case; resolution order and joiners. |

The counts are pinned in the test files, exact at the pin, together with each multi-service
allow-list and its size; a listed fixture missing from the tag fails the count test. Bump them
together with `spec.pin`.

At the pin the suites give 284 passed, 60 skipped: sequences 19, envelope 11, bridging 5,
tunnel 14, status-text 11.

### Profile

Since v0.8.0 no SDK ships a multi-service controller (MCPWARP.md section 1; section 6A is
informative host guidance), and section 10.6 Fixture status classifies the multi-service
fixtures as host guidance, which an SDK runner skips by default. Each runner skips them by a
declared allow-list, with the reason `host guidance, §10.6 Fixture status: <fixture>`:

- **envelope, 2**: `valid/register_batch_valid` and `valid/register_two_services`, multi-entry
  registers a single-service encoder cannot produce; their schema verdicts are skipped too
  (docs/DEVIATIONS.md, schema verdicts). The other new envelope cases run:
  `registered_partial_errors`, `unregistered_subset` and `conflict_per_name` through the decoder.
- **bridging, 4**: `multi_service_route_by_header`, `multi_service_route_by_host`,
  `multi_service_unknown_service_503`, `multi_service_unknown_service_non_mcp_404` (section 4.8
  routing).
- **tunnel, 14**: `ms_01` to `ms_14`, the group fixtures of the multi-service controller.
- **status-text, 11 cases**: the nine `describeServiceStatus` cases and the two
  `NO_SERVICE_REGISTERED` cases (`rules.json` `hostOnlyCases`). The `service.*` and
  `lastError.NO_SERVICE_REGISTERED` keys (`rules.json` `hostOnly`, optional) are not in the
  shipped tables.

Skip notes:

- **sequences, 19 skipped.** `role: "server"` transcripts are written from the server's side;
  this SDK has no server role.
- **multi-service fixtures**: see Profile above.
- **envelope, 9 schema-only skipped** (reason `schema verdict only, checker/CI tooling, §10.6`).
  Agent->edge cases whose only verdict is the edge's schema check (`SCHEMA_*` hints and
  `register_batch_101_invalid`'s `TOO_MANY_SERVICES`): schema verdicts are checker/CI tooling,
  and an SDK runner asserts wire verdicts only (section 10.6 and `spec/README.md`, v0.8.1).
- **bridging, 1 skipped** (`body_413_streamed_reset`). The `*_reset` fixtures apply only to an
  adapter that streams the request body to the app. This SDK always buffers the body before the
  handler runs, so a body over the cap is a 413 before any response byte; `body_413` covers that
  path (MCPWARP.md section 10.3 allows the skip).

Run them with:

```sh
make fetch-spec
make test                                   # everything
uv run pytest tests/spec -q                 # only the fixture suites
WSMIXER_SPEC_DIR=../ws-mixer-spec uv run pytest tests/spec -q   # against another checkout
```

## The Go runner

The runner lives in the spec repo (`.spec/conformance/runner`, its own Go module). It spawns
`conformance/adapter/run` once per fixture or scenario and talks JSON Lines to it
(CONFORMANCE.md sections 1.1 and 1.2).

### The adapter

- `conformance/adapter/run` execs `"${WSMIXER_PYTHON:-python3}" -I "$dir/adapter.py"`, where
  `$dir` is the script's own directory.
  No venv and no `uv run`: the SDK has no dependencies, and `-I` keeps `PYTHON*` variables and
  the script directory off `sys.path`. `adapter.py` puts `src/` on `sys.path` itself.
- `WSMIXER_PYTHON` picks the interpreter (any Python >= 3.11). CI uses 3.11, the floor.
- Client role only: `listen` and `open_stream` answer "unsupported", which the runner reports as
  SKIP for the `python-server` column.
- It drives the public `MixerClient` API plus the test-only `Timing` hook. The runner scales
  `hello_timeout_ms` and the welcome's ping values itself; the adapter stores `time_scale` and
  `floor_ms` but owns no other duration that needs scaling.

### Make targets

| Target | What |
|---|---|
| `make smoke` | Pipes a `shutdown` command into the adapter and checks the first line is `ready`. The runner reports every row as SKIP and exits 0 when the adapter never comes up, so this guards against a silent pass. |
| `make conformance-fixture` | `smoke` + `fetch-spec`, then the runner in fixture mode: `--sdk python --mode fixture`, report in `build/conformance-fixture.xml`. |
| `make go-adapter` | Builds ws-mixer-go's `cmd/conformance-adapter` at `goserver.pin` with `-tags conformance` into `build/go-adapter`. |
| `make conformance-pair` | `smoke` + `go-adapter` + `fetch-spec`, then pair mode: `--sdk go,python --mode pair` (runs go->go and go->python), report in `build/conformance-pair.xml`. |

Both runner targets pass `--counts conformance/COUNTS.json`. They need Go (CI uses 1.24 with
`GOTOOLCHAIN=auto`).

To run the runner by hand, from inside `.spec` with absolute paths back to this repo:

```sh
repo="$PWD"
mkdir -p build
(cd .spec && go run ./conformance/runner \
    --spec-root "$repo/.spec" \
    --adapter "python=$repo/conformance/adapter/run" \
    --sdk python --mode fixture \
    --counts "$repo/conformance/COUNTS.json" \
    --report "$repo/build/conformance.xml")
```

Add `--fixture <name>` to run one fixture (the floors are then not enforced).

### Floors

`conformance/COUNTS.json`:

| Key | Floor | Meaning |
|---|---|---|
| `python-client` | 25 | Fixture mode, client role: all 25 client fixtures pass. |
| `go->python` | 10 | Pair mode with ws-mixer-go v0.8.0 as server: all 10 scenarios pass. |

`python-server` is all SKIP and has no floor. Each floor is the count actually observed passing;
raise it as rows land, never lower it. The runner enforces a key only when the run produced rows
for it and `--fixture` is `*`.

### Time scale

Leave `--time-scale` at the runner's default (1/50); the floors were measured there. At
`--time-scale 1` the keepalive fixtures wait their real 30 s / 90 s, and in one review run
`client_dead_peer_timeout` failed because the runner's raw peer closed before the SDK's
`KEEPALIVE_TIMEOUT` error went out (23 passed, 1 failed). The pytest sequence suite covers those
fixtures at real timer values on the fake clock.

## CI

`.github/workflows/ci.yml`:

| Job | Runs |
|---|---|
| `lint` | `make lint` (ruff check + ruff format --check) on 3.11. |
| `typecheck` | `make typecheck` (mypy --strict on `src` and `tests`) on 3.11. |
| `test` | `make fetch-spec` + `make test` on 3.11, 3.12, 3.13 and 3.14, with `CI=1` (missing spec fails). |

`.github/workflows/conformance.yml`:

| Job | Runs |
|---|---|
| `fixture-matrix` | Python 3.11 + Go 1.24: `make fetch-spec`, `make smoke`, `make conformance-fixture`. A guard step fails the job if the runner skipped the `python-client` floor check because every row was SKIP. Uploads `build/*.xml`. |
| `pair-go-python` | Same setup plus `make fetch-goserver` and `make go-adapter`, then `make conformance-pair`. A go->go failure also fails the job. Same guard for `go->python`. |

Both workflows run on pushes to `main` and on pull requests.
