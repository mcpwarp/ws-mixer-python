"""The request and response types a bridged handler sees (MCPWARP.md section 4).

A handler is `async (Request) -> Response`. The SDK recognises a response by its shape (an int
`status`, plus `headers` and `body` attributes), never by isinstance, so a Response from another
vendored copy of this package, or any object shaped like one, works.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable, Iterator, Mapping
from concurrent.futures import Executor
from dataclasses import dataclass
from typing import Literal, cast


class Headers:
    """Ordered, case-insensitive, multi-valued HTTP header fields. Immutable.

    Built from (name, value) pairs, a mapping, or anything with an `items()` method that
    yields pairs (another Headers included). Names keep their spelling; lookups ignore case.
    """

    __slots__ = ("_items",)

    _items: tuple[tuple[str, str], ...]

    def __init__(self, items: Iterable[tuple[str, str]] | Mapping[str, str] | Headers = ()) -> None:
        pairs = items_of(items)
        for pair in pairs:
            if (
                not isinstance(pair, tuple)
                or len(pair) != 2
                or not isinstance(pair[0], str)
                or not isinstance(pair[1], str)
            ):
                raise TypeError("header fields must be (str, str) pairs")
        object.__setattr__(self, "_items", tuple(pairs))

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("Headers is immutable")

    def get(self, name: str, default: str | None = None) -> str | None:
        """Every value of `name` joined with ", ", or `default` when there is none."""
        values = self.get_all(name)
        return ", ".join(values) if values else default

    def get_all(self, name: str) -> list[str]:
        low = name.lower()
        return [v for n, v in self._items if n.lower() == low]

    def items(self) -> list[tuple[str, str]]:
        return list(self._items)

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):
            return False
        low = name.lower()
        return any(n.lower() == low for n, _ in self._items)

    def __iter__(self) -> Iterator[str]:
        return (n for n, _ in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Headers):
            return NotImplemented
        return [(n.lower(), v) for n, v in self._items] == [(n.lower(), v) for n, v in other._items]

    def __hash__(self) -> int:
        return hash(tuple((n.lower(), v) for n, v in self._items))

    def __repr__(self) -> str:
        return f"Headers({list(self._items)!r})"


def items_of(headers: object) -> list[tuple[str, str]]:
    """(name, value) pairs from a Headers-like object, a mapping, or an iterable of pairs."""
    if headers is None:
        return []
    items = getattr(headers, "items", None)
    if callable(items):
        return list(cast("Iterable[tuple[str, str]]", items()))
    if isinstance(headers, str | bytes) or not isinstance(headers, Iterable):
        raise TypeError("headers must be pairs, a mapping or a Headers")
    return list(cast("Iterable[tuple[str, str]]", headers))


@dataclass(frozen=True)
class Request:
    """One bridged request, its body fully read and within the size cap."""

    method: str
    url: str
    """`https://<Host><target>`: the public URL."""
    target: str
    """The request target as received, origin-form (path and query); bytes outside the RFC 3986
    set percent-encoded."""
    path: str
    """`target` up to the first "?" or "#"; not decoded."""
    query: str
    """`target` after the first "?" up to "#", without the "?"; "" when there is none."""
    headers: Headers
    """The forwardable fields (section 4.6): hop-by-hop, Connection-listed, `expect` and
    `content-length` removed; `host` kept."""
    body: bytes
    """The whole body; b"" for GET and HEAD, and when the request had no body framing."""
    http_version: Literal["1.0", "1.1"]


@dataclass
class Response:
    """What a handler returns.

    `Content-Length` is sent only when `headers` declares one; otherwise a body is sent chunked,
    one chunk per item of an async iterable body, each written as soon as it is produced. A
    `None` body is an empty one. `reason` defaults to the RFC 9110 phrase. A status outside
    200..599 (a 1xx is never sent) is answered 500 like any other non-response (section 4.5).
    """

    status: int
    headers: Headers | Mapping[str, str] | Iterable[tuple[str, str]] = ()
    body: bytes | bytearray | memoryview | AsyncIterable[bytes] | None = None
    reason: str | None = None


@dataclass
class ErrorReply(Response):
    """A reply the SDK itself generates (section 4.5), such as target mode's 502/504.

    Shaped like any Response (a JSON `{"error": ...}` body with its Content-Length), so code
    wrapping a handler can inspect it. The bridge recognises the `mcpwarp_error_message`
    attribute and writes the byte-exact section 4.5 reply instead, which to HEAD keeps the
    Content-Length.
    """

    mcpwarp_error_message: str = ""


def error_reply(status: int, message: str) -> ErrorReply:
    body = json.dumps({"error": message}, ensure_ascii=False, separators=(",", ":")).encode()
    headers = Headers([("Content-Type", "application/json"), ("Content-Length", str(len(body)))])
    return ErrorReply(status, headers, body, None, message)


Handler = Callable[[Request], Awaitable[Response]]


def sync_handler(fn: Callable[[Request], Response], *, executor: Executor | None = None) -> Handler:
    """A Handler that runs the blocking `fn` in `executor` (the loop's default when None).

    The edge cancelling the request cancels the await, not the thread: `fn` runs to completion
    and its result is dropped.
    """

    async def handler(request: Request) -> Response:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, fn, request)

    return handler
