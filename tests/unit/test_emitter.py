from __future__ import annotations

import pytest

from ws_mixer._emitter import Emitter


def test_on_emit_in_registration_order() -> None:
    em = Emitter()
    got: list[tuple[str, object]] = []
    em.on("x", lambda a: got.append(("a", a)))
    em.on("x", lambda a: got.append(("b", a)))
    assert em.emit("x", 1) is True
    assert got == [("a", 1), ("b", 1)]
    assert em.emit("nobody") is False


def test_once_and_unsubscribe_and_off() -> None:
    em = Emitter()
    got: list[str] = []
    em.once("x", lambda _: got.append("once"))
    unsub = em.on("x", lambda _: got.append("on"))

    def third(_: object) -> None:
        got.append("off")

    em.on("x", third)
    em.emit("x")
    em.emit("x")
    assert got == ["once", "on", "off", "on", "off"]
    unsub()
    unsub()  # idempotent
    em.off("x", third)
    em.off("x", third)
    em.off("missing", third)
    assert em.listener_count("x") == 0
    assert em.emit("x") is False


def test_off_removes_latest_registration_only() -> None:
    em = Emitter()
    got: list[int] = []

    def cb(_: object) -> None:
        got.append(1)

    em.on("x", cb)
    em.on("x", cb)
    em.off("x", cb)
    em.emit("x")
    assert got == [1]


def test_emit_iterates_a_snapshot() -> None:
    em = Emitter()
    got: list[str] = []

    def first(_: object) -> None:
        got.append("first")
        em.on("x", lambda _: got.append("late"))
        em.off("x", second)

    def second(_: object) -> None:
        got.append("second")

    em.on("x", first)
    em.on("x", second)
    em.emit("x")
    assert got == ["first", "second"]
    got.clear()
    em.emit("x")
    assert got == ["first", "late"]


def test_listener_exception_is_isolated_and_reemitted_as_error() -> None:
    em = Emitter()
    got: list[object] = []
    boom = RuntimeError("boom")

    def bad(_: object) -> None:
        raise boom

    em.on("x", bad)
    em.on("x", lambda _: got.append("next"))
    em.emit("x")  # no error listener: dropped, no raise
    assert got == ["next"]
    em.on("error", got.append)
    em.emit("x")
    assert got == ["next", boom, "next"]


def test_error_without_listener_and_failing_error_listener() -> None:
    em = Emitter()
    assert em.emit("error", RuntimeError("x")) is False

    def bad(_: object) -> None:
        raise RuntimeError("inside error listener")

    em.on("error", bad)
    em.emit("error", RuntimeError("x"))  # dropped, no recursion


def test_base_exceptions_propagate() -> None:
    em = Emitter()

    def interrupt(_: object) -> None:
        raise KeyboardInterrupt

    em.on("x", interrupt)
    with pytest.raises(KeyboardInterrupt):
        em.emit("x")
