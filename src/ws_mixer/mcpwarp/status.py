"""The tunnel controller's status (MCPWARP.md sections 6.5 and 6.6): states, codes, and the
fixed `last_error` texts paired with each code.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .envelope import SERVICE_NAME_MAX


class TunnelState(StrEnum):
    OFF = "off"
    CONNECTING = "connecting"
    ONLINE = "online"
    DISABLED = "disabled"
    CONFLICT = "conflict"
    ERROR = "error"


class TunnelStatusCode(StrEnum):
    UNAUTHORIZED = "UNAUTHORIZED"
    TAKEN_OVER = "TAKEN_OVER"
    DISCONNECTED = "DISCONNECTED"
    RETRYING = "RETRYING"
    DISABLED = "DISABLED"
    INVALID_CONFIG = "INVALID_CONFIG"
    INVALID_NAME = "INVALID_NAME"
    USERNAME_REQUIRED = "USERNAME_REQUIRED"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    CONFLICT = "CONFLICT"
    UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION"
    DISCONNECTED_FATAL = "DISCONNECTED_FATAL"
    INTERNAL = "INTERNAL"


@dataclass(frozen=True)
class TunnelStatus:
    """None means absent. `code` and `last_error` come together; `url` is set only when online."""

    state: TunnelState
    url: str | None = None
    code: TunnelStatusCode | None = None
    last_error: str | None = None

    def to_dict(self) -> dict[str, str]:
        """The JS shape (`lastError`) with absent keys left out: what the fixtures compare."""
        out = {"state": str(self.state)}
        if self.url is not None:
            out["url"] = self.url
        if self.code is not None:
            out["code"] = str(self.code)
        if self.last_error is not None:
            out["lastError"] = self.last_error
        return out


@dataclass(frozen=True)
class Problem:
    """A status code with its user-facing `last_error` text (never a token, name, URL or server
    message)."""

    code: TunnelStatusCode
    last_error: str


RETRY_DIAL = Problem(TunnelStatusCode.RETRYING, "Can't reach mcpwarp; retrying.")
RETRY_REGISTER = Problem(
    TunnelStatusCode.RETRYING, "mcpwarp couldn't register the service; retrying."
)
LOST = Problem(TunnelStatusCode.DISCONNECTED, "Connection lost; retrying.")
TAKEN_OVER = Problem(TunnelStatusCode.TAKEN_OVER, "Another connection took over this name.")
DISABLED = Problem(TunnelStatusCode.DISABLED, "This service is disabled in the mcpwarp dashboard.")
REJECTED = Problem(TunnelStatusCode.UNAUTHORIZED, "The token was rejected.")
FORBIDDEN = Problem(
    TunnelStatusCode.UNAUTHORIZED, "The token is not allowed to publish this service."
)
INVALID_NAME = Problem(
    TunnelStatusCode.INVALID_NAME,
    "The service name is invalid: use lowercase letters, digits and dashes "
    f"(not at the start or end), at most {SERVICE_NAME_MAX} characters.",
)
INVALID_SETTINGS = Problem(TunnelStatusCode.INVALID_CONFIG, "The tunnel settings are invalid.")
TOKEN_NOT_SET = Problem(TunnelStatusCode.INVALID_CONFIG, "The token is not set.")
NAME_NOT_SET = Problem(TunnelStatusCode.INVALID_CONFIG, "The service name is not set.")
USERNAME_REQUIRED = Problem(
    TunnelStatusCode.USERNAME_REQUIRED, "Choose a username in your mcpwarp account first."
)
QUOTA_EXCEEDED = Problem(
    TunnelStatusCode.QUOTA_EXCEEDED, "Your mcpwarp account has no services left in its quota."
)
CONFLICT = Problem(TunnelStatusCode.CONFLICT, "The name is in use by another server.")
UNSUPPORTED_VERSION = Problem(
    TunnelStatusCode.UNSUPPORTED_VERSION,
    "This SDK version is no longer supported; please update.",
)
DISCONNECTED_FATAL = Problem(
    TunnelStatusCode.DISCONNECTED_FATAL, "The connection to mcpwarp failed."
)
INTERNAL = Problem(TunnelStatusCode.INTERNAL, "Something went wrong; the log has the details.")
