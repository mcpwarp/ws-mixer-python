"""Helpers that reproduce JavaScript semantics the spec's fixtures were written against.

`str.strip()` and `re`'s `\\s` use Python's notion of whitespace, which differs from
ECMAScript's (Python also strips U+001C..U+001F and U+0085; JS does not). Integers follow
`Number.isInteger` (so `1.0` is an integer) but `bool` is never one.
"""

from __future__ import annotations

import math
from typing import TypeGuard

# ECMAScript WhiteSpace (TAB, VT, FF, SP, NBSP, ZWNBSP and the Zs category) plus
# LineTerminator (LF, CR, LS, PS): the set `String.prototype.trim()` and `\s` use.
JS_WHITESPACE = "".join(
    map(
        chr,
        (
            *(0x0009, 0x000B, 0x000C, 0x0020, 0x00A0, 0xFEFF),
            *(0x1680, *range(0x2000, 0x200B), 0x202F, 0x205F, 0x3000),
            *(0x000A, 0x000D, 0x2028, 0x2029),
        ),
    )
)


def js_trim(s: str) -> str:
    """`String.prototype.trim()`."""
    return s.strip(JS_WHITESPACE)


def is_js_integer(x: object) -> TypeGuard[int | float]:
    """`Number.isInteger(x)` for a JSON-decoded value, with `bool` rejected."""
    if isinstance(x, bool):
        return False
    if isinstance(x, int):
        return True
    if isinstance(x, float):
        return math.isfinite(x) and x.is_integer()
    return False


def js_number(x: float) -> str:
    """`String(x)` as JavaScript writes a number."""
    if math.isnan(x):
        return "NaN"
    if math.isinf(x):
        return "Infinity" if x > 0 else "-Infinity"
    if float(x).is_integer() and abs(x) < 1e21:
        return str(int(x))
    return repr(float(x))


def utf8_encode(s: str) -> bytes:
    """`TextEncoder.encode(s)`: lone surrogates become U+FFFD instead of raising."""
    try:
        return s.encode("utf-8")
    except UnicodeEncodeError:
        # Pairs up any split surrogate pair and replaces the lone ones with U+FFFD.
        well_formed = s.encode("utf-16", "surrogatepass").decode("utf-16", "replace")
        return well_formed.encode("utf-8")


def truncate_utf8(s: str, max_bytes: int) -> str:
    """Truncates `s` to at most `max_bytes` UTF-8 bytes, backing off to a character boundary.

    Lone surrogates come back as U+FFFD whether or not anything was cut.
    """
    b = utf8_encode(s)
    end = max(min(max_bytes, len(b)), 0)
    while 0 < end < len(b) and (b[end] & 0xC0) == 0x80:
        end -= 1
    return b[:end].decode("utf-8")
