"""The mcpwarp helper's exception type and the codes it raises besides the wire ones."""

from __future__ import annotations

from typing import Any

# Besides a RegisterErrorCode / AppErrorCode value, McpwarpError.code is one of these.
MALFORMED_ENVELOPE = "MALFORMED_ENVELOPE"
CLOSED = "CLOSED"
# The edge rejected the token: ws-mixer close 4011, or a fatal HTTP 401 at the upgrade.
UNAUTHORIZED = "UNAUTHORIZED"
# A `conflict` (or drain "replaced") arrived before the first `registered`.
TAKEN_OVER = "TAKEN_OVER"
# Any other fatal transport failure, or reconnect.max_attempts ran out.
DISCONNECTED_FATAL = "DISCONNECTED_FATAL"


class McpwarpError(Exception):
    """An mcpwarp failure. The underlying exception, if any, is `__cause__` (`raise ... from`)."""

    code: str
    fatal: bool
    service_name: str | None

    def __init__(
        self,
        code: str,
        message: str,
        *,
        fatal: bool = False,
        service_name: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        # True when it ended the exposure (the handle closes with it).
        self.fatal = fatal
        self.service_name = service_name

    @property
    def message(self) -> str:
        return str(self.args[0])

    def __str__(self) -> str:
        return self.message

    def __reduce__(self) -> tuple[Any, ...]:
        # The keyword-only fields travel in the state dict, so pickle and copy keep them.
        return (type(self), (self.code, self.message), self.__dict__)

    def __repr__(self) -> str:
        return (
            f"McpwarpError({self.code!r}, {self.message!r}, fatal={self.fatal!r}, "
            f"service_name={self.service_name!r})"
        )
