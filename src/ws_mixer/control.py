"""ws-mixer.v1 control channel (stream 0): message types, validators and encoder (WIRE.md 2.7).

The validators are hand-written (dispatch on `t`, then explicit per-field checks), never a JSON
Schema validator. They accept exactly what `control.relaxed.schema.json` accepts: unknown fields
on a known `t` are ignored. Integers follow `Number.isInteger` (`1.0` is accepted, `true` is not)
and string length limits count code points.

Deliberate receive-side leniency: `error.message` may be empty (the schema says 1..1024), because
ws-mixer-go sends `message:""` from `Conn.Close(code, "")` and JS and Go both accept it;
rejecting it would mean answering a terminal `error` with another one.

Python-specific edges:
- Very deep JSON nesting raises RecursionError (reported as PROTOCOL_ERROR); the depth at which
  that happens depends on the Python version and its recursion limit.
- `encode_control` writes lone surrogates in strings as U+FFFD (as JS's TextEncoder does).
- An app body holding a non-finite float (e.g. `1e400` parsed as inf) makes `encode_control`
  raise ValueError, since JSON has no Infinity.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any, Literal, NoReturn, NotRequired, TypedDict

from ._jscompat import is_js_integer, utf8_encode
from .errors import ConnError, ErrorCode
from .frame import MAX_STREAM_ZERO_PAYLOAD


class AgentInfo(TypedDict):
    sdk: str
    sdk_version: str
    runtime: NotRequired[str]
    os: NotRequired[str]


class ServerInfo(TypedDict, total=False):
    name: str
    version: str
    node: str


class Hello(TypedDict):
    t: Literal["hello"]
    v: int
    token: str
    agent: AgentInfo
    window: NotRequired[int]
    max_streams: NotRequired[int]
    capabilities: NotRequired[list[str]]
    meta: NotRequired[dict[str, Any]]


class Welcome(TypedDict):
    t: Literal["welcome"]
    v: int
    session: str
    window: int
    max_streams: int
    ping_interval: int
    ping_timeout: int
    server: NotRequired[ServerInfo]
    capabilities: NotRequired[list[str]]
    meta: NotRequired[dict[str, Any]]


class Ping(TypedDict):
    t: Literal["ping"]
    id: int
    ts: NotRequired[int]


class Pong(TypedDict):
    t: Literal["pong"]
    id: int
    ts: NotRequired[int]


class Drain(TypedDict):
    t: Literal["drain"]
    reason: str
    last_stream_id: int
    deadline_ms: NotRequired[int]
    retry_after_ms: NotRequired[int]
    message: NotRequired[str]


class ErrorMsg(TypedDict):
    t: Literal["error"]
    code: int
    message: str
    stream_id: NotRequired[int]
    last_stream_id: NotRequired[int]


class App(TypedDict):
    t: Literal["app"]
    body: dict[str, Any]


ControlMessage = Hello | Welcome | Ping | Pong | Drain | ErrorMsg | App

PROTOCOL_VERSION = 1
DEFAULT_WINDOW = 262_144
DEFAULT_MAX_STREAMS = 64

_MAX_TOKEN_LEN = 4096
_MAX_AGENT_FIELD_LEN = 128
_WINDOW_MIN = 16_384
_WINDOW_MAX = 0x7FFF_FFFF
_MAX_STREAMS_MIN = 1
_MAX_STREAMS_MAX = 100_000
_MAX_SESSION_LEN = 64
_PING_INTERVAL_MIN = 5000
_MAX_DRAIN_REASON = 64
_MAX_DRAIN_MESSAGE = 256
_MAX_ERROR_MESSAGE = 1024
_MAX_ID = 2**53 - 1
_MAX_STREAM_ID = 0x7FFF_FFFF
_MAX_ERROR_CODE = 0xFFFF_FFFF

_MAX_ECHO = 64

_CAPABILITY_RE = re.compile(r"[a-z0-9_.-]{1,64}")

_KNOWN_DRAIN_REASONS = frozenset(
    {"rollout", "overload", "id_exhausted", "replaced", "maintenance", "client_requested"}
)


def known_drain_reason(reason: str) -> bool:
    """Whether `reason` is a documented drain reason. Unknown ones are tolerated at runtime."""
    return reason in _KNOWN_DRAIN_REASONS


def _fail(message: str, code: int = ErrorCode.PROTOCOL_ERROR) -> NoReturn:
    raise ConnError(code, message)


def _echo(s: str) -> str:
    """A peer string for an error message: JSON-quoted, cut to its first 64 characters."""
    quoted = json.dumps(s[:_MAX_ECHO])
    return quoted + "..." if len(s) > _MAX_ECHO else quoted


def _json_kind(v: object) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int | float):
        return "number"
    if isinstance(v, str):
        return "string"
    if isinstance(v, list):
        return "array"
    if isinstance(v, dict):
        return "object"
    return type(v).__name__


def _require(top: Mapping[str, Any], name: str) -> object:
    if name not in top:
        _fail(f'missing required field "{name}"')
    return top[name]


def _str(raw: object, name: str, *, min_len: int = 0, max_len: int | None = None) -> str:
    if not isinstance(raw, str):
        _fail(f'field "{name}" must be a string')
    n = len(raw)
    if n < min_len or (max_len is not None and n > max_len):
        bound = f"{min_len}..{max_len}" if max_len is not None else f">= {min_len}"
        _fail(f"{name} length {n} out of range {bound}")
    return raw


def _int(raw: object, name: str, lo: int | None = None, hi: int | None = None) -> int:
    if not is_js_integer(raw):
        _fail(f'field "{name}" must be an integer')
    v = int(raw)
    if (lo is not None and v < lo) or (hi is not None and v > hi):
        lo_s = "" if lo is None else str(lo)
        hi_s = "" if hi is None else str(hi)
        _fail(f"{name} {v} out of range {lo_s}..{hi_s}")
    return v


def _object(raw: object, name: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        _fail(f'field "{name}" must be an object, got {_json_kind(raw)}')
    return raw


def _capabilities(raw: object) -> list[str]:
    if not isinstance(raw, list) or not all(isinstance(c, str) for c in raw):
        _fail('field "capabilities" must be an array of strings')
    for c in raw:
        if _CAPABILITY_RE.fullmatch(c) is None:
            _fail(f"capability {_echo(c)} does not match ^[a-z0-9_.-]{{1,64}}$")
    return list(raw)


def _version(top: Mapping[str, Any], t: str) -> int:
    v = _int(_require(top, "v"), "v")
    if v != PROTOCOL_VERSION:
        _fail(f"{t}.v={v} does not match ws-mixer.v1", ErrorCode.UNSUPPORTED)
    return v


def _agent(raw: object) -> AgentInfo:
    obj = _object(raw, "agent")
    lim = _MAX_AGENT_FIELD_LEN
    agent: AgentInfo = {
        "sdk": _str(_require(obj, "sdk"), "agent.sdk", max_len=lim),
        "sdk_version": _str(_require(obj, "sdk_version"), "agent.sdk_version", max_len=lim),
    }
    if "runtime" in obj:
        agent["runtime"] = _str(obj["runtime"], "agent.runtime", max_len=lim)
    if "os" in obj:
        agent["os"] = _str(obj["os"], "agent.os", max_len=lim)
    return agent


def _server(raw: object) -> ServerInfo:
    obj = _object(raw, "server")
    lim = _MAX_AGENT_FIELD_LEN
    server: ServerInfo = {}
    if "name" in obj:
        server["name"] = _str(obj["name"], "server.name", max_len=lim)
    if "version" in obj:
        server["version"] = _str(obj["version"], "server.version", max_len=lim)
    if "node" in obj:
        server["node"] = _str(obj["node"], "server.node", max_len=lim)
    return server


def _parse_hello(top: dict[str, Any]) -> Hello:
    v = _version(top, "hello")
    token = _str(_require(top, "token"), "hello.token", min_len=1, max_len=_MAX_TOKEN_LEN)
    agent = _agent(_require(top, "agent"))
    window = DEFAULT_WINDOW
    if "window" in top:
        window = _int(top["window"], "hello.window", _WINDOW_MIN, _WINDOW_MAX)
    max_streams = DEFAULT_MAX_STREAMS
    if "max_streams" in top:
        max_streams = _int(
            top["max_streams"], "hello.max_streams", _MAX_STREAMS_MIN, _MAX_STREAMS_MAX
        )
    m: Hello = {
        "t": "hello",
        "v": v,
        "token": token,
        "agent": agent,
        "window": window,
        "max_streams": max_streams,
    }
    if "capabilities" in top:
        m["capabilities"] = _capabilities(top["capabilities"])
    if "meta" in top:
        m["meta"] = _object(top["meta"], "meta")
    return m


def _parse_welcome(top: dict[str, Any], allow_subfloor: bool) -> Welcome:
    v = _version(top, "welcome")
    # Presence and types of every required field first, then ranges and floors.
    session = _str(_require(top, "session"), "welcome.session")
    _int(_require(top, "window"), "welcome.window")
    _int(_require(top, "max_streams"), "welcome.max_streams")
    _int(_require(top, "ping_interval"), "welcome.ping_interval")
    _int(_require(top, "ping_timeout"), "welcome.ping_timeout")

    _str(session, "welcome.session", min_len=1, max_len=_MAX_SESSION_LEN)
    window = _int(top["window"], "welcome.window", _WINDOW_MIN, _WINDOW_MAX)
    max_streams = _int(
        top["max_streams"], "welcome.max_streams", _MAX_STREAMS_MIN, _MAX_STREAMS_MAX
    )
    if allow_subfloor:
        ping_interval = _int(top["ping_interval"], "welcome.ping_interval", 1)
        ping_timeout = _int(top["ping_timeout"], "welcome.ping_timeout", 1)
    else:
        ping_interval = _int(top["ping_interval"], "welcome.ping_interval")
        if ping_interval < _PING_INTERVAL_MIN:
            _fail(
                f"welcome.ping_interval {ping_interval} is below the {_PING_INTERVAL_MIN}ms floor"
            )
        ping_timeout = _int(top["ping_timeout"], "welcome.ping_timeout")
        if ping_timeout < 2 * ping_interval:
            _fail(
                f"welcome.ping_timeout ({ping_timeout}) must be at least 2x "
                f"ping_interval ({ping_interval})"
            )

    m: Welcome = {
        "t": "welcome",
        "v": v,
        "session": session,
        "window": window,
        "max_streams": max_streams,
        "ping_interval": ping_interval,
        "ping_timeout": ping_timeout,
    }
    if "server" in top:
        m["server"] = _server(top["server"])
    if "capabilities" in top:
        m["capabilities"] = _capabilities(top["capabilities"])
    if "meta" in top:
        m["meta"] = _object(top["meta"], "meta")
    return m


def _parse_ping(top: dict[str, Any]) -> Ping:
    m: Ping = {"t": "ping", "id": _int(_require(top, "id"), "ping.id", 0, _MAX_ID)}
    if "ts" in top:
        m["ts"] = _int(top["ts"], "ping.ts", 0, _MAX_ID)
    return m


def _parse_pong(top: dict[str, Any]) -> Pong:
    m: Pong = {"t": "pong", "id": _int(_require(top, "id"), "pong.id", 0, _MAX_ID)}
    if "ts" in top:
        m["ts"] = _int(top["ts"], "pong.ts", 0, _MAX_ID)
    return m


def _parse_drain(top: dict[str, Any]) -> Drain:
    reason = _str(_require(top, "reason"), "drain.reason", min_len=1, max_len=_MAX_DRAIN_REASON)
    last = _int(_require(top, "last_stream_id"), "drain.last_stream_id", 0, _MAX_STREAM_ID)
    m: Drain = {"t": "drain", "reason": reason, "last_stream_id": last}
    if "deadline_ms" in top:
        m["deadline_ms"] = _int(top["deadline_ms"], "drain.deadline_ms", 0)
    if "retry_after_ms" in top:
        m["retry_after_ms"] = _int(top["retry_after_ms"], "drain.retry_after_ms", 0)
    if "message" in top:
        m["message"] = _str(top["message"], "drain.message", max_len=_MAX_DRAIN_MESSAGE)
    return m


def _parse_error(top: dict[str, Any]) -> ErrorMsg:
    code = _int(_require(top, "code"), "error.code", 0, _MAX_ERROR_CODE)
    # min 0, not the schema's 1: see the module docstring.
    message = _str(_require(top, "message"), "error.message", max_len=_MAX_ERROR_MESSAGE)
    m: ErrorMsg = {"t": "error", "code": code, "message": message}
    if "stream_id" in top:
        m["stream_id"] = _int(top["stream_id"], "error.stream_id", 0, _MAX_STREAM_ID)
    if "last_stream_id" in top:
        m["last_stream_id"] = _int(top["last_stream_id"], "error.last_stream_id", 0, _MAX_STREAM_ID)
    return m


def _parse_app(top: dict[str, Any]) -> App:
    body = _require(top, "body")
    if not isinstance(body, dict):
        _fail(f"app.body must be a JSON object, got {_json_kind(body)}")
    return {"t": "app", "body": body}


def _reject_constant(name: str) -> NoReturn:
    # JSON has no NaN/Infinity; Python's json module would otherwise accept them.
    raise ValueError(f"{name} is not valid JSON")


def parse_control(
    raw: bytes | bytearray | memoryview | str, *, allow_subfloor: bool = False
) -> ControlMessage:
    """Validates and decodes one stream-0 DATA payload.

    Every failure is a ConnError: ENHANCE_YOUR_CALM for a payload over 16 KiB, UNSUPPORTED for
    a `v` other than 1, PROTOCOL_ERROR otherwise. Its `control_type` is the message's `t` when
    one was decoded, so a caller can tell a malformed peer `error` (which it must not answer
    with another `error`) from other violations. `allow_subfloor` skips the welcome ping floors
    (ping_interval >= 5000, ping_timeout >= 2x ping_interval); it exists for test timing only.
    """
    data = utf8_encode(raw) if isinstance(raw, str) else bytes(raw)
    if len(data) > MAX_STREAM_ZERO_PAYLOAD:
        _fail(
            f"stream 0 payload of {len(data)} bytes exceeds the "
            f"{MAX_STREAM_ZERO_PAYLOAD} byte control-channel limit",
            ErrorCode.ENHANCE_YOUR_CALM,
        )
    if isinstance(raw, str):
        text = raw
    else:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as e:
            raise ConnError(ErrorCode.PROTOCOL_ERROR, f"malformed UTF-8 on stream 0: {e}") from e

    try:
        top = json.loads(text, parse_constant=_reject_constant)
    except (ValueError, RecursionError) as e:
        raise ConnError(ErrorCode.PROTOCOL_ERROR, f"malformed JSON on stream 0: {e}") from e
    if not isinstance(top, dict):
        _fail(f"control message must be a JSON object, got {_json_kind(top)}")

    if "t" not in top:
        _fail('control message missing required field "t"')
    t = _str(top["t"], "t")

    try:
        if t == "hello":
            return _parse_hello(top)
        if t == "welcome":
            return _parse_welcome(top, allow_subfloor)
        if t == "ping":
            return _parse_ping(top)
        if t == "pong":
            return _parse_pong(top)
        if t == "drain":
            return _parse_drain(top)
        if t == "error":
            return _parse_error(top)
        if t == "app":
            return _parse_app(top)
        _fail(f"unknown control message type {_echo(t)}")
    except ConnError as e:
        e.control_type = t
        raise


def encode_control(msg: ControlMessage | Mapping[str, Any]) -> bytes:
    """Serializes a control message to its stream-0 payload: compact JSON, `t` first."""
    ordered: dict[str, Any] = {"t": msg["t"]}
    ordered.update((k, v) for k, v in msg.items() if k != "t")
    text = json.dumps(ordered, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return utf8_encode(text)
