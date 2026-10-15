"""Every spec/fixtures/mcpwarp/bridging/*.json case (MCPWARP.md section 10.3), buffering mode.

Each fixture's raw HTTP/1.1 `in` is fed into a standalone MixerStream whose host records the
DATA/WINDOW/CLOSE/RESET the agent sends, and served by `serve_stream` in the fixture's
`sdkState` (online -> "available", disabled, closing). The handler is the fixture's: `echo`
answers 200 with the request's content-type and body and no length, `throw` raises. Target
mode forwards to a local stub server that accepts the request and never answers (`hang`); the
fake clock is moved by `targetTimeoutMs` once the stub has the whole request.

Checks, per spec/README.md: the application call equals `upstream` (or never happens when it
is null); an SDK-generated reply is byte-exact; an application response is compared by status
line, header set (lowercased names, any order) and de-chunked body; no 1xx precedes the
response; the stream ends with CLOSE (or RESET `outReset.code`).

The request body is always buffered before the app runs, so the `*_reset` fixtures (which
apply only to an adapter that streams the request body to the app) are skipped with the
section 10.3 note; `body_413` covers the buffering path.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import _cases
from ws_mixer.frame import FrameType, decode_frame, reset_code, window_increment
from ws_mixer.mcpwarp import Handler, Request, Response, target_handler
from ws_mixer.mcpwarp.serve import Availability, serve_stream
from ws_mixer.stream import Finished, MixerStream
from ws_mixer.testing import FakeClock, settle

# Floor: the fixture count at spec.pin v0.6.1 (a later spec only adds fixtures).
MIN_BRIDGING_FIXTURES = 14
BIG_WINDOW = 64 * 1024 * 1024
FEED_CHUNK = 16_384
STATES: dict[str, Availability] = {
    "online": "available",
    "disabled": "disabled",
    "closing": "closing",
}

_CASES = _cases("mcpwarp/bridging", "*.json")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "bridging_case" in metafunc.fixturenames:
        if _CASES:
            metafunc.parametrize(
                "bridging_case", [p for p, _ in _CASES], ids=[i for _, i in _CASES]
            )
        else:
            metafunc.parametrize("bridging_case", [None], ids=["spec-unavailable"])


class RecordingHost:
    """A StreamHost recording what the agent sends; the test plays the edge."""

    def __init__(self) -> None:
        self.events: list[tuple[str, Any]] = []

    def send_data(self, stream_id: int, chunk: bytes) -> asyncio.Future[None]:
        self.events.append(("DATA", bytes(chunk)))
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        return fut

    def send_control_frame(self, frame: bytes) -> None:
        f = decode_frame(frame)
        if f.type == FrameType.CLOSE:
            self.events.append(("CLOSE", None))
        elif f.type == FrameType.RESET:
            self.events.append(("RESET", reset_code(f)))
        elif f.type == FrameType.WINDOW:
            self.events.append(("WINDOW", window_increment(f)))

    def retire_stream(self, stream_id: int) -> None:
        pass

    def written(self) -> bytes:
        return b"".join(v for k, v in self.events if k == "DATA")

    def ends(self) -> list[tuple[str, Any]]:
        return [e for e in self.events if e[0] in ("CLOSE", "RESET")]


def feed(stream: MixerStream, data: bytes) -> None:
    for i in range(0, len(data), FEED_CHUNK):
        stream.handle_data(data[i : i + FEED_CHUNK])


async def wait_until(cond: Callable[[], bool], what: str, timeout_s: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while not cond():
        if loop.time() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await settle()
        if not cond():
            await asyncio.sleep(0.002)


def joined_headers(pairs: list[tuple[str, str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for name, value in pairs:
        low = name.lower()
        out[low] = f"{out[low]}, {value}" if low in out else value
    return out


class StubTarget:
    """A local server that records one request and never answers it."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.port = 0
        self._server: asyncio.Server | None = None

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
            lines = head[:-4].split("\r\n")
            method, target, _ = lines[0].split(" ")
            pairs = [(n, v.strip()) for n, _, v in (line.partition(":") for line in lines[1:])]
            length = int(joined_headers(pairs).get("content-length", "0"))
            body = await reader.readexactly(length)
            # Transport-level fields the client sets itself are not part of the forwarded call.
            transport = {"host", "content-length", "connection"}
            headers = {k: v for k, v in joined_headers(pairs).items() if k not in transport}
            self.calls.append(
                {
                    "method": method,
                    "url": self.origin + target,
                    "headers": headers,
                    "body": body.decode("latin-1"),
                }
            )
            await reader.read()  # hang until the client gives up
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.transport.abort()


def expand_in(fx: dict[str, Any]) -> str:
    raw: str = fx["in"]
    for marker, spec in (fx.get("inExpand") or {}).items():
        raw = raw.replace(marker, spec["repeat"] * spec["count"])
    return raw


def parse_head(raw: str) -> tuple[str, list[tuple[str, str]], str]:
    end = raw.find("\r\n\r\n")
    assert end > 0, f"no end of head in {raw!r}"
    status_line, *lines = raw[:end].split("\r\n")
    headers = [(n.strip().lower(), v.strip()) for n, _, v in (ln.partition(":") for ln in lines)]
    return status_line, sorted(headers), raw[end + 4 :]


def parse_response(raw: str) -> tuple[str, list[tuple[str, str]], str]:
    """Status line, sorted header set, and the body (de-chunked when chunked)."""
    status_line, headers, rest = parse_head(raw)
    if ("transfer-encoding", "chunked") not in headers:
        return status_line, headers, rest
    body = ""
    while True:
        line_end = rest.index("\r\n")
        size = int(rest[:line_end].split(";")[0], 16)
        rest = rest[line_end + 2 :]
        if size == 0:
            assert rest == "\r\n"
            return status_line, headers, body
        body += rest[:size]
        assert rest[size : size + 2] == "\r\n"
        rest = rest[size + 2 :]


def test_bridging_fixture_count(spec_fixtures: Path) -> None:
    files = sorted((spec_fixtures / "mcpwarp" / "bridging").glob("*.json"))
    assert len(files) >= MIN_BRIDGING_FIXTURES
    names = {f.stem for f in files}
    for required in (
        "mcp_post_ok",
        "non_mcp_404",
        "disabled_503",
        "body_413",
        "target_504",
        "handler_throw_500",
        "head_431",
        "notification_passthrough",
        "expect_continue_200",
        "precedence_disabled_unknown_path_404",
        "precedence_closing_over_cap_503",
        "disabled_chunked_body_503",
        "head_mcp_503",
        "body_413_streamed_reset",
    ):
        assert required in names


@pytest.mark.asyncio
async def test_bridging_fixture(spec_fixtures: Path, bridging_case: Path) -> None:
    fx: dict[str, Any] = json.loads(bridging_case.read_text(encoding="utf-8"))
    if "outReset" in fx:
        pytest.skip(
            "a *_reset fixture applies only to an adapter that streams the request body; "
            "this runner drives the buffering path, which body_413 covers (MCPWARP.md 10.3)"
        )
    clock = FakeClock()
    state = STATES[fx["sdkState"]]
    behaviour = fx["handler"]["behaviour"]
    limits = fx["limits"]
    calls: list[dict[str, Any]] = []
    errors: list[BaseException] = []
    stub: StubTarget | None = None
    upstream = fx["upstream"]
    app: Handler

    if fx["mode"] == "handler":

        async def handler(request: Request) -> Response:
            calls.append(
                {
                    "method": request.method,
                    "url": request.url,
                    "headers": joined_headers(request.headers.items()),
                    "body": request.body.decode("latin-1"),
                }
            )
            if behaviour == "throw":
                raise RuntimeError("fixture handler throws")
            ct = request.headers.get("content-type")
            return Response(200, [("content-type", ct)] if ct is not None else [], request.body)

        app = handler
    else:
        assert behaviour == "hang"
        stub = StubTarget()
        await stub.start()
        fixture_origin = re.match(r"https?://[^/]+", fx["target"])
        assert fixture_origin is not None
        app = target_handler(
            stub.origin + fx["target"][fixture_origin.end() :],
            timeout_ms=limits["targetTimeoutMs"],
            clock=clock,
        )
        if upstream is not None:
            upstream = {**upstream, "url": upstream["url"].replace(fixture_origin[0], stub.origin)}

    try:
        host = RecordingHost()
        stream = MixerStream(1, host, BIG_WINDOW, BIG_WINDOW)
        task = asyncio.create_task(
            serve_stream(
                stream,
                handler=app,
                availability=lambda: state,
                max_request_body_bytes=limits["maxRequestBodyBytes"],
                clock=clock,
                on_error=errors.append,
            )
        )
        data = expand_in(fx).encode("latin-1")
        head_end = data.find(b"\r\n\r\n")
        if "inRest" in fx:
            feed(stream, data)
            await wait_until(lambda: b"\r\n\r\n" in host.written(), "the response head")
            feed(stream, fx["inRest"].encode("latin-1"))
        elif state != "available" and head_end >= 0:
            # 404/503 are answered right after the head: they must not wait for the body.
            feed(stream, data[: head_end + 4])
            await wait_until(lambda: len(host.written()) >= len(fx["out"]), "the refusal")
            feed(stream, data[head_end + 4 :])
        else:
            feed(stream, data)
        stream.handle_close()
        if stub is not None:
            await wait_until(lambda: bool(stub.calls), "the upstream request")
            await clock.advance(limits["targetTimeoutMs"])
        await asyncio.wait_for(task, 5)
    finally:
        if stub is not None:
            await stub.stop()

    raw = host.written().decode("latin-1")
    assert not raw.startswith("HTTP/1.1 1"), "a 1xx preceded the final response"
    assert len(re.findall(r"HTTP/1\.[01] \d{3}", raw)) == 1
    assert host.ends() == [("CLOSE", None)]
    assert isinstance(await stream.wait_closed(), Finished)

    if upstream is None:
        assert calls == [] and (stub is None or stub.calls == [])
    else:
        assert (stub.calls if stub is not None else calls) == [upstream]

    from_app = upstream is not None and behaviour == "echo"
    if from_app:
        assert parse_response(raw) == parse_response(fx["out"])
    else:
        assert raw == fx["out"]
    assert len(errors) == (1 if behaviour == "throw" else 0)
