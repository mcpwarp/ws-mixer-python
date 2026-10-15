"""A `WebSocketConnector` over the `websockets` package (asyncio API, websockets >= 13, < 18).

`websockets` is imported by `websockets_connector()`, so importing this module never needs it.
Settings are pinned to WIRE.md section 2.10: no compression, `max_size` = the init's
`max_message`, library pings every 20 s with no death timer (ws-mixer's in-band ping is the
liveness authority). Redirects are refused, as in the default connector. Unlike the default
connector, `websockets` honours HTTP(S)_PROXY, so this is the workaround behind a proxy.
"""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import Callable
from typing import Any

from .._tls import default_ssl_context
from ..socket import (
    InvalidHandshake,
    ProxyRejected,
    UnexpectedSubprotocol,
    UpgradeRejected,
    WebSocketClosed,
    WebSocketConnection,
    WebSocketConnector,
    WebSocketInit,
    check_close_args,
)

_FORCED = frozenset(
    {
        "compression",
        "max_size",
        "ping_interval",
        "ping_timeout",
        "subprotocols",
        "additional_headers",
    }
)
# Codes `websockets` sends when it fails the connection itself; reported like the default
# connector does, with received=False.
_FAILURE_CODES = frozenset({1002, 1007, 1009, 1011})


def websockets_connector(
    *, ssl_context: ssl.SSLContext | None = None, **connect_kwargs: Any
) -> WebSocketConnector:
    """Returns a connector that dials with `websockets.asyncio.client.connect`.

    `connect_kwargs` go to `connect()` (e.g. `open_timeout`, `close_timeout`, `proxy`), except
    the settings pinned above, which raise TypeError when passed.
    """
    forced = _FORCED & connect_kwargs.keys()
    if forced:
        raise TypeError(f"websockets_connector pins {sorted(forced)}; do not pass them")
    kwargs = dict(connect_kwargs)
    kwargs.setdefault("open_timeout", None)
    holder: dict[str, ssl.SSLContext] = {}

    def tls() -> ssl.SSLContext:
        if ssl_context is not None:
            return ssl_context
        if "ctx" not in holder:
            holder["ctx"] = default_ssl_context()
        return holder["ctx"]

    try:
        from websockets import exceptions as wsx
        from websockets.asyncio.client import connect
    except ImportError as e:
        raise ImportError("websockets_connector needs `websockets>=13,<18` installed") from e
    if not callable(getattr(connect, "process_redirect", None)):
        raise RuntimeError(
            "this websockets version has no connect.process_redirect, so the adapter cannot "
            "refuse redirects; use websockets>=13,<18 or the default StdlibConnector"
        )

    class _NoRedirect(connect):
        def process_redirect(self, exc: Exception) -> Exception | str:
            return exc

    proxy_status: Any = getattr(wsx, "InvalidProxyStatus", ())
    proxy_config: Any = getattr(wsx, "InvalidProxy", ())

    async def connector(url: str, init: WebSocketInit) -> WebSocketConnection:
        headers = dict(init.headers)
        call_kwargs = dict(kwargs)
        if url.lower().startswith("wss:") and "ssl" not in call_kwargs:
            call_kwargs["ssl"] = tls()
        if any(k.lower() == "user-agent" for k in headers):
            call_kwargs.setdefault("user_agent_header", None)
        try:
            ws = await _NoRedirect(
                url,
                subprotocols=list(init.protocols),  # type: ignore[arg-type]
                additional_headers=headers,
                compression=None,
                max_size=init.max_message,
                ping_interval=20,
                ping_timeout=None,
                **call_kwargs,
            )
        except proxy_status as e:
            raise _rejected(e.response, ProxyRejected) from e
        except wsx.InvalidStatus as e:
            raise _rejected(e.response) from e
        except wsx.NegotiationError as e:
            prefix = "unsupported subprotocol: "
            if str(e).startswith(prefix):
                raise UnexpectedSubprotocol(str(e)[len(prefix) :]) from e
            raise InvalidHandshake(str(e)) from e
        except wsx.InvalidHeader as e:
            if e.name.lower() == "sec-websocket-protocol":
                raise UnexpectedSubprotocol(_echoed(e)) from e
            raise InvalidHandshake(str(e)) from e
        except (wsx.InvalidURI, proxy_config) as e:
            raise ValueError(str(e)) from e
        except wsx.InvalidHandshake as e:
            raise InvalidHandshake(str(e)) from e
        return _Connection(ws, wsx.ConnectionClosed)

    return connector


def _rejected(response: Any, cls: type[UpgradeRejected] = UpgradeRejected) -> UpgradeRejected:
    joined: dict[str, str] = {}
    for name, value in response.headers.raw_items():
        key = name.lower()
        joined[key] = f"{joined[key]}, {value}" if key in joined else value
    return cls(response.status_code, joined)


def _echoed(e: Any) -> str | None:
    """The Sec-WebSocket-Protocol value an InvalidHeader(Format) complained about."""
    header = getattr(e, "header", None)
    if isinstance(header, str):
        return header
    value = getattr(e, "value", None)
    prefix = "multiple values: "
    if isinstance(value, str) and value.startswith(prefix):
        return value[len(prefix) :]
    return value if isinstance(value, str) else None


class _Connection:
    """Adapts a `websockets` ClientConnection to `WebSocketConnection`."""

    def __init__(self, ws: Any, closed_type: type[Exception]) -> None:
        self._ws = ws
        self._closed_type = closed_type
        self._close_task: asyncio.Task[None] | None = None
        self.subprotocol: str | None = ws.subprotocol

    def _map(self, e: Exception) -> WebSocketClosed:
        rcvd = getattr(e, "rcvd", None)
        sent = getattr(e, "sent", None)
        if (
            sent is not None
            and int(sent.code) in _FAILURE_CODES
            and (rcvd is None or getattr(e, "rcvd_then_sent", None) is False)
        ):
            return WebSocketClosed(int(sent.code), str(sent.reason))
        if rcvd is not None:
            return WebSocketClosed(int(rcvd.code), str(rcvd.reason), received=True)
        return WebSocketClosed(1006)

    async def _guard(self, op: Callable[[], Any]) -> Any:
        try:
            return await op()
        except Exception as e:
            if isinstance(e, self._closed_type):
                raise self._map(e) from e
            raise

    async def recv(self) -> bytes | str:
        data = await self._guard(self._ws.recv)
        assert isinstance(data, bytes | str)
        return data

    async def send(self, data: bytes) -> None:
        await self._guard(lambda: self._ws.send(bytes(data)))

    async def close(self, code: int = 1000, reason: str = "") -> None:
        check_close_args(code, reason)
        await self._ws.close(code, reason)

    def abort(self) -> None:
        transport = getattr(self._ws, "transport", None)
        if transport is not None:
            transport.abort()
        elif self._close_task is None:
            self._close_task = asyncio.get_running_loop().create_task(self._ws.close())
