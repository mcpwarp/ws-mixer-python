"""The mcpwarp control envelope (MCPWARP.md section 2), carried in ws-mixer `app` message bodies
as `{"mcpwarp": {...}}`: what the agent sends (register/unregister) and what the edge sends back
(registered/unregistered/disable/enable/conflict/error).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, TypeAlias

from .._jscompat import is_js_integer
from .errors import MALFORMED_ENVELOPE, McpwarpError

MCPWARP_V = 1
DEFAULT_URL = "wss://connect.mcpwarp.io"
SERVICE_NAME_RE = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?\Z")
SERVICE_NAME_MAX = 30
# What the edge accepts as an integration id, checked after lowercasing: an RFC 9562 text-form
# UUID of version 1-8 and variant 8/9/a/b, or the nil or max UUID.
INTEGRATION_ID_RE = re.compile(
    r"^(?:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
    r"|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)\Z"
)

ServiceKind = Literal["http", "stdio"]


def is_valid_service_name(name: object) -> bool:
    """Whether `name` is a str of at most SERVICE_NAME_MAX characters matching SERVICE_NAME_RE."""
    return (
        isinstance(name, str)
        and len(name) <= SERVICE_NAME_MAX
        and SERVICE_NAME_RE.match(name) is not None
    )


class RegisterErrorCode(StrEnum):
    """`registered.errors[].code` values. Decoded codes stay plain `str`, so unknown ones pass."""

    INVALID_NAME = "INVALID_NAME"
    USERNAME_REQUIRED = "USERNAME_REQUIRED"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    CONFLICT = "CONFLICT"
    SERVER_DISABLED = "SERVER_DISABLED"
    INTERNAL = "INTERNAL"
    NO_RESPONDERS = "NO_RESPONDERS"
    TIMEOUT = "TIMEOUT"
    ABORTED = "ABORTED"
    CONNECTION_LOST = "CONNECTION_LOST"
    MALFORMED_REPLY = "MALFORMED_REPLY"
    BAD_REQUEST = "BAD_REQUEST"
    UNAUTHENTICATED = "UNAUTHENTICATED"
    FORBIDDEN = "FORBIDDEN"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
    UNAVAILABLE = "UNAVAILABLE"
    SERVICE_ERROR = "SERVICE_ERROR"


# Retried on the same connection with backoff; never fatal.
TRANSIENT_REGISTER_ERRORS: frozenset[str] = frozenset(
    {
        RegisterErrorCode.INTERNAL.value,
        RegisterErrorCode.NO_RESPONDERS.value,
        RegisterErrorCode.TIMEOUT.value,
        RegisterErrorCode.ABORTED.value,
        RegisterErrorCode.CONNECTION_LOST.value,
        RegisterErrorCode.MALFORMED_REPLY.value,
        RegisterErrorCode.UNAVAILABLE.value,
        RegisterErrorCode.SERVICE_ERROR.value,
    }
)


class AppErrorCode(StrEnum):
    """`op:"error"` codes."""

    BAD_REQUEST = "BAD_REQUEST"
    UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION"
    UNKNOWN_OP = "UNKNOWN_OP"
    TOO_MANY_SERVICES = "TOO_MANY_SERVICES"
    OVERLOADED = "OVERLOADED"
    CONNECTION_LIMIT = "CONNECTION_LIMIT"


@dataclass(frozen=True)
class RegisteredService:
    name: str
    id: str
    url: str
    created: bool = False


@dataclass(frozen=True)
class ServiceError:
    name: str
    code: str
    message: str = ""
    id: str | None = None


@dataclass(frozen=True)
class UnregisteredService:
    name: str
    id: str = ""


@dataclass(frozen=True)
class Registered:
    """Holds lists, so it is not hashable (by design: the decoded wire shape)."""

    services: list[RegisteredService]
    errors: list[ServiceError]


@dataclass(frozen=True)
class Unregistered:
    """Holds a list, so it is not hashable (by design)."""

    services: list[UnregisteredService]


@dataclass(frozen=True)
class Disable:
    id: str
    reason: str = "disabled"


@dataclass(frozen=True)
class Enable:
    id: str
    name: str


@dataclass(frozen=True)
class ConflictOp:
    """Unsolicited: another connection of the same account registered `name` and took it over."""

    id: str
    name: str
    reason: str = "taken_over"


@dataclass(frozen=True)
class AppError:
    code: str
    message: str = ""


@dataclass(frozen=True)
class Unknown:
    """A `v` other than 1 or an op this SDK does not know: ignored. `raw_op` and `v` are the raw
    JSON values, so it is hashable only when they are."""

    raw_op: object
    v: object


InboundEnvelope: TypeAlias = (
    Registered | Unregistered | Disable | Enable | ConflictOp | AppError | Unknown
)


def encode_register(name: str, kind: ServiceKind = "http") -> dict[str, Any]:
    """An `app` body `{"mcpwarp": {...}}` holding a register, keys in wire order (v, op,
    services; name, kind). `encode_control({"t": "app", "body": ...})` gives the wire bytes."""
    return {
        "mcpwarp": {"v": MCPWARP_V, "op": "register", "services": [{"name": name, "kind": kind}]}
    }


def encode_unregister(name: str) -> dict[str, Any]:
    """An `app` body `{"mcpwarp": {...}}` holding an unregister."""
    return {"mcpwarp": {"v": MCPWARP_V, "op": "unregister", "services": [{"name": name}]}}


def _malformed(message: str) -> McpwarpError:
    return McpwarpError(MALFORMED_ENVELOPE, message)


def _req_str(o: Mapping[str, Any], key: str, where: str) -> str:
    v = o.get(key)
    if not isinstance(v, str):
        raise _malformed(f"{where}.{key} must be a string")
    return v


def _opt_str(o: Mapping[str, Any], key: str, where: str, default: str) -> str:
    if key not in o:
        return default
    v = o[key]
    if not isinstance(v, str):
        raise _malformed(f"{where}.{key} must be a string")
    return v


def _list(o: Mapping[str, Any], key: str, where: str) -> list[dict[str, Any]]:
    v = o.get(key)
    if v is None:
        return []
    if not isinstance(v, list) or not all(isinstance(e, dict) for e in v):
        raise _malformed(f"{where}.{key} must be an array of objects")
    return v


def _registered_service(s: Mapping[str, Any]) -> RegisteredService:
    where = "registered.services[]"
    name = _req_str(s, "name", where)
    id_ = _req_str(s, "id", where)
    url = s.get("url")
    if not isinstance(url, str) or url == "":
        raise _malformed(f"{where}.url must be a non-empty string")
    created = s.get("created")
    if created is None:
        created = False
    elif not isinstance(created, bool):
        raise _malformed(f"{where}.created must be a boolean")
    return RegisteredService(name=name, id=id_, url=url, created=created)


def _service_error(e: Mapping[str, Any]) -> ServiceError:
    where = "registered.errors[]"
    name = _req_str(e, "name", where)
    code = _req_str(e, "code", where)
    message = _opt_str(e, "message", where, "")
    id_ = _req_str(e, "id", where) if "id" in e else None
    return ServiceError(name=name, code=code, message=message, id=id_)


def decode_envelope(body: object) -> InboundEnvelope | None:
    """Decodes an `app` body per MCPWARP.md section 2.8.

    None when it has no object-valued `mcpwarp` key (not ours: ignore). A `v` other than the
    integer 1 (a bool is not one) or an unknown op gives Unknown. A known op with a wrong field
    type, or a `registered` service without a non-empty `url`, raises McpwarpError
    MALFORMED_ENVELOPE naming the first problem in the normative check order.
    """
    if not isinstance(body, dict):
        return None
    env = body.get("mcpwarp")
    if not isinstance(env, dict):
        return None
    v = env.get("v")
    op = env.get("op")
    if not (is_js_integer(v) and v == MCPWARP_V):
        return Unknown(raw_op=op, v=v)
    if op == "registered":
        services = [_registered_service(s) for s in _list(env, "services", "registered")]
        errors = [_service_error(e) for e in _list(env, "errors", "registered")]
        return Registered(services=services, errors=errors)
    if op == "unregistered":
        where = "unregistered.services[]"
        return Unregistered(
            services=[
                UnregisteredService(
                    name=_req_str(s, "name", where), id=_opt_str(s, "id", where, "")
                )
                for s in _list(env, "services", "unregistered")
            ]
        )
    if op == "disable":
        return Disable(
            id=_req_str(env, "id", "disable"),
            reason=_opt_str(env, "reason", "disable", "disabled"),
        )
    if op == "enable":
        return Enable(id=_req_str(env, "id", "enable"), name=_req_str(env, "name", "enable"))
    if op == "conflict":
        return ConflictOp(
            id=_req_str(env, "id", "conflict"),
            name=_req_str(env, "name", "conflict"),
            reason=_opt_str(env, "reason", "conflict", "taken_over"),
        )
    if op == "error":
        return AppError(
            code=_req_str(env, "code", "error"),
            message=_opt_str(env, "message", "error", ""),
        )
    return Unknown(raw_op=op, v=v)
