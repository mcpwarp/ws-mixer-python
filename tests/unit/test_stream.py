"""MixerStream on its own, against a recording host: the receive table (WIRE.md 2.5), credit
accounting (2.6), write chunking and backpressure, reads, and teardown (CLIENT-SDK.md)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from ws_mixer.errors import ConnError, ErrorCode, StreamError
from ws_mixer.frame import (
    MAX_SEND_WINDOW,
    Frame,
    FrameType,
    decode_frame,
    reset_code,
    reset_message,
    window_increment,
)
from ws_mixer.stream import ConnectionLost, Finished, MixerStream, Reset
from ws_mixer.testing import settle

pytestmark = pytest.mark.asyncio

WINDOW = 16_384


class Host:
    def __init__(self) -> None:
        self.control: list[Frame] = []
        self.data: list[bytes] = []
        self.retired: list[int] = []
        self.hold = False
        self.held: list[asyncio.Future[None]] = []

    def send_data(self, stream_id: int, chunk: bytes) -> asyncio.Future[None]:
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.data.append(chunk)
        if self.hold:
            self.held.append(fut)
        else:
            fut.set_result(None)
        return fut

    def send_control_frame(self, frame: bytes) -> None:
        self.control.append(decode_frame(frame))

    def retire_stream(self, stream_id: int) -> None:
        self.retired.append(stream_id)

    def kinds(self) -> list[str]:
        return [FrameType(f.type).name for f in self.control]


def make(recv: int = WINDOW, send: int = WINDOW) -> tuple[MixerStream, Host]:
    host = Host()
    return MixerStream(1, host, recv, send), host


async def to_state(s: MixerStream, state: str) -> None:
    if state == "half_closed_local":
        await s.close_write()
    elif state == "half_closed_remote":
        s.handle_close()
    elif state == "closed":
        s.handle_reset(ErrorCode.CANCEL, "x")
    assert s.state == state


# --- the receive table, stream-level rows (the conn-level rows are in test_conn.py) ---------


@pytest.mark.parametrize("state", ["open", "half_closed_local"])
async def test_data_is_buffered_and_debits_credit(state: str) -> None:
    s, _ = make()
    await to_state(s, state)
    s.handle_data(b"abc")
    assert s.recv_window == WINDOW - 3
    assert s.state == state
    assert await s.read() == b"abc"


async def test_data_after_peer_close_is_stream_closed() -> None:
    s, _ = make()
    s.handle_close()
    with pytest.raises(StreamError) as ei:
        s.handle_data(b"x")
    assert ei.value.code == ErrorCode.STREAM_CLOSED and ei.value.stream_id == 1


async def test_data_on_closed_stream_is_stream_closed() -> None:
    s, _ = make()
    s.handle_reset(ErrorCode.CANCEL, "")
    with pytest.raises(StreamError) as ei:
        s.handle_data(b"x")
    assert ei.value.code == ErrorCode.STREAM_CLOSED


@pytest.mark.parametrize("state", ["open", "half_closed_local", "half_closed_remote"])
async def test_window_credits_or_is_tolerated(state: str) -> None:
    s, _ = make(send=100)
    await to_state(s, state)
    s.handle_window(50)
    assert s.send_window == 150


async def test_window_on_closed_stream_is_ignored() -> None:
    s, _ = make(send=100)
    s.handle_reset(ErrorCode.CANCEL, "")
    s.handle_window(50)
    assert s.send_window == 100


async def test_close_transitions() -> None:
    s, host = make()
    s.handle_close()
    assert s.state == "half_closed_remote"
    with pytest.raises(StreamError) as ei:
        s.handle_close()
    assert ei.value.code == ErrorCode.STREAM_CLOSED

    s2, host2 = make()
    await s2.close_write()
    s2.handle_close()
    assert s2.state == "closed"
    assert host2.retired == [1]
    assert await s2.wait_closed() == Finished()

    s3, _ = make()
    s3.handle_reset(ErrorCode.CANCEL, "")
    s3.handle_close()  # no-op on a closed stream
    assert s3.state == "closed"


@pytest.mark.parametrize("state", ["open", "half_closed_local", "half_closed_remote"])
async def test_reset_closes_drops_buffer_and_sends_nothing(state: str) -> None:
    s, host = make()
    if state != "half_closed_remote":
        s.handle_data(b"buffered")
    await to_state(s, state)
    sent_before = list(host.control)
    s.handle_reset(ErrorCode.CANCEL, "bye")
    assert s.state == "closed" and s.reset_code == ErrorCode.CANCEL
    assert host.control == sent_before
    assert host.retired == [1]
    with pytest.raises(StreamError) as ei:
        await s.read()
    assert ei.value.code == ErrorCode.CANCEL
    assert await s.wait_closed() == Reset(ErrorCode.CANCEL, "CANCEL", "bye", True)
    s.handle_reset(ErrorCode.INTERNAL_ERROR, "again")  # ignored once closed
    assert s.reset_code == ErrorCode.CANCEL


# --- credit ---------------------------------------------------------------------------------


async def test_window_returned_at_half_window_on_consumption() -> None:
    s, host = make()
    s.handle_data(bytes(8192))
    assert host.control == []  # never on receipt
    assert await s.read(8191) == bytes(8191)
    assert host.control == []
    assert await s.read(1) == b"\0"
    assert host.kinds() == ["WINDOW"] and window_increment(host.control[0]) == 8192
    assert s.recv_window == WINDOW


async def test_no_window_after_peer_close_or_on_closed_stream() -> None:
    s, host = make()
    s.handle_data(bytes(WINDOW))
    s.handle_close()
    assert await s.read() == bytes(WINDOW)
    assert await s.read() == b""
    assert host.control == []

    s2, host2 = make()
    s2.handle_data(bytes(WINDOW))
    s2.terminate(ConnError(ErrorCode.INTERNAL_ERROR, "gone"))
    assert await s2.read() == bytes(WINDOW)
    assert host2.control == []


async def test_credit_violation_is_connection_fatal() -> None:
    s, _ = make()
    s.handle_data(bytes(WINDOW))
    with pytest.raises(ConnError) as ei:
        s.handle_data(b"x")
    assert ei.value.code == ErrorCode.FLOW_CONTROL_ERROR


async def test_empty_data_frame_is_not_eof() -> None:
    s, _ = make()
    s.handle_data(b"")
    s.handle_data(b"x")
    assert await s.read() == b"x"


@pytest.mark.parametrize("closed", [False, True])
async def test_window_overflow_rejected_even_when_closed(closed: bool) -> None:
    s, _ = make(send=MAX_SEND_WINDOW - 10)
    if closed:
        s.handle_reset(ErrorCode.CANCEL, "")
    s.handle_window(10)
    with pytest.raises(ConnError) as ei:
        s.handle_window(11 if closed else 1)
    assert ei.value.code == ErrorCode.FLOW_CONTROL_ERROR


# --- writing --------------------------------------------------------------------------------


async def test_write_chunks_at_16k_and_debits() -> None:
    s, host = make(send=100_000)
    await s.write(bytes(40_000))
    assert [len(c) for c in host.data] == [16_384, 16_384, 7232]
    assert s.send_window == 60_000
    await s.write("é")
    assert host.data[-1] == "é".encode()
    await s.write(b"")
    assert len(host.data) == 4


async def test_write_waits_for_credit_and_resumes() -> None:
    s, host = make(send=10)
    task = asyncio.create_task(s.write(bytes(25)))
    await settle()
    assert [len(c) for c in host.data] == [10] and not task.done()
    s.handle_window(5)
    await settle()
    assert [len(c) for c in host.data] == [10, 5] and not task.done()
    s.handle_window(100)
    await task
    assert [len(c) for c in host.data] == [10, 5, 10]
    assert s.send_window == 90


async def test_writes_are_serialized() -> None:
    s, host = make(send=4)
    a = asyncio.create_task(s.write(b"aaaa"))
    b = asyncio.create_task(s.write(b"bb"))
    await settle()
    assert host.data == [b"aaaa"]
    s.handle_window(2)
    await asyncio.gather(a, b)
    assert host.data == [b"aaaa", b"bb"]


async def test_write_blocked_on_credit_fails_on_reset_and_terminate() -> None:
    for end in ("reset", "terminate"):
        s, host = make(send=0)
        task = asyncio.create_task(s.write(b"x"))
        await settle()
        if end == "reset":
            s.handle_reset(ErrorCode.CANCEL, "")
            with pytest.raises(StreamError):
                await task
        else:
            s.terminate(ConnError(ErrorCode.KEEPALIVE_TIMEOUT, "dead"))
            with pytest.raises(ConnError):
                await task
        assert host.data == []


async def test_write_rechecks_after_send_wait() -> None:
    s, host = make(send=100)
    host.hold = True
    task = asyncio.create_task(s.write(bytes(20_000)))
    await settle()
    assert len(host.data) == 1
    s.handle_reset(ErrorCode.CANCEL, "")
    host.held[0].set_result(None)
    with pytest.raises(StreamError):
        await task
    assert len(host.data) == 1  # nothing after the reset


async def test_write_after_close_write_or_reset_raises() -> None:
    s, _ = make()
    await s.close_write()
    with pytest.raises(StreamError) as ei:
        await s.write(b"x")
    assert ei.value.code == ErrorCode.STREAM_CLOSED
    s2, _ = make()
    s2.reset("REFUSED_STREAM")
    with pytest.raises(StreamError) as ei:
        await s2.write(b"x")
    assert ei.value.code == ErrorCode.REFUSED_STREAM


async def test_close_write_flushes_queued_writes_first_and_is_idempotent() -> None:
    s, host = make(send=0)
    w = asyncio.create_task(s.write(b"data"))
    await settle()
    c1 = asyncio.create_task(s.close_write())
    c2 = asyncio.create_task(s.close_write())
    await settle()
    assert host.control == [] and not c1.done()
    s.handle_window(10)
    await asyncio.gather(w, c1, c2)
    assert host.data == [b"data"] and host.kinds() == ["CLOSE"]
    assert s.state == "half_closed_local"
    await s.close_write()
    assert host.kinds() == ["CLOSE"]


async def test_close_write_after_reset_or_loss_never_raises_or_sends() -> None:
    s, host = make()
    s.handle_reset(ErrorCode.CANCEL, "")
    await s.close_write()
    s2, host2 = make()
    s2.terminate(ConnError(ErrorCode.INTERNAL_ERROR, "gone"))
    await s2.close_write()
    assert host.control == [] and host2.control == []


async def test_close_write_after_peer_close_finishes() -> None:
    s, host = make()
    s.handle_close()
    await s.close_write()
    assert s.state == "closed" and host.retired == [1]
    assert await s.wait_closed() == Finished()


# --- reset / close / refuse -----------------------------------------------------------------


@pytest.mark.parametrize(
    "code",
    [
        ErrorCode.NO_ERROR,
        ErrorCode.FLOW_CONTROL_ERROR,
        ErrorCode.FRAME_SIZE_ERROR,
        ErrorCode.UNSUPPORTED,
        ErrorCode.UNAUTHORIZED,
        ErrorCode.GOING_AWAY,
        ErrorCode.KEEPALIVE_TIMEOUT,
        "APPLICATION_CLOSE",
        0x0F,
        0x0FFF_FFFF,
    ],
)
async def test_reset_rejects_non_stream_codes(code: int | str) -> None:
    s, host = make()
    with pytest.raises(ValueError):
        s.reset(code)
    assert s.state == "open" and host.control == []


@pytest.mark.parametrize("code", [1, 2, 5, 6, 7, 8, 9, 0x1000_0000, 0xFFFF_FFFF])
async def test_reset_accepts_stream_and_application_codes(code: int) -> None:
    s, host = make()
    s.reset(code)
    assert reset_code(host.control[0]) == code


async def test_reset_by_name_truncates_and_rejects_unknown() -> None:
    s, host = make()
    with pytest.raises(ValueError):
        s.reset("NOPE")
    s.reset("CANCEL", "é" * 200)
    frame = host.control[0]
    assert reset_code(frame) == ErrorCode.CANCEL
    assert len(frame.payload) - 4 == 256 and reset_message(frame) == "é" * 128
    assert host.retired == [1]
    s.reset(ErrorCode.INTERNAL_ERROR)  # no-op once closed
    assert len(host.control) == 1
    reason = await s.wait_closed()
    assert reason == Reset(ErrorCode.CANCEL, "CANCEL", "é" * 128, False)


async def test_close_and_refuse() -> None:
    s, host = make()
    s.close()
    assert reset_code(host.control[0]) == ErrorCode.CANCEL
    s2, host2 = make()
    s2.handle_close()
    s2.close()
    await settle()
    assert host2.kinds() == ["CLOSE"] and s2.state == "closed"
    s3, host3 = make()
    s3.refuse()
    assert reset_code(host3.control[0]) == ErrorCode.REFUSED_STREAM


async def test_abort_sends_reset() -> None:
    s, host = make()
    s.abort(ErrorCode.STREAM_CLOSED, "dup")
    assert reset_code(host.control[0]) == ErrorCode.STREAM_CLOSED
    assert (await s.wait_closed()) == Reset(ErrorCode.STREAM_CLOSED, "STREAM_CLOSED", "dup", False)


# --- reading --------------------------------------------------------------------------------


async def test_reads_are_fifo_and_split() -> None:
    s, _ = make()
    r1 = asyncio.create_task(s.read())
    r2 = asyncio.create_task(s.read(2))
    r3 = asyncio.create_task(s.read())
    await settle()
    s.handle_data(b"first")
    s.handle_data(b"second")
    assert await r1 == b"first"
    assert await r2 == b"se"
    assert await r3 == b"cond"
    assert await s.read(0) == b""


async def test_cancelled_reader_does_not_eat_data() -> None:
    s, _ = make()
    r1 = asyncio.create_task(s.read())
    r2 = asyncio.create_task(s.read())
    await settle()
    r1.cancel()
    await settle()
    s.handle_data(b"x")
    assert await r2 == b"x"


async def test_async_iteration_until_eof() -> None:
    s, _ = make()
    s.handle_data(b"a")
    s.handle_data(b"b")
    s.handle_close()
    assert [c async for c in s] == [b"a", b"b"]


# --- teardown and wait_closed ---------------------------------------------------------------


async def test_terminate_delivers_buffer_and_close_then_eof() -> None:
    s, _ = make()
    s.handle_data(b"tail")
    s.handle_close()
    err = ConnError(ErrorCode.GOING_AWAY, "drained")
    s.terminate(err)
    assert await s.read() == b"tail"
    assert await s.read() == b""
    assert await s.wait_closed() == ConnectionLost(err)


async def test_terminate_delivers_buffer_then_errors() -> None:
    s, _ = make(send=0)
    s.handle_data(b"tail")
    pending_write = asyncio.create_task(s.write(b"x"))
    await settle()
    err = ConnError(ErrorCode.KEEPALIVE_TIMEOUT, "dead")
    s.terminate(err)
    with pytest.raises(ConnError) as ei:
        await pending_write
    assert ei.value is err
    assert await s.read() == b"tail"
    with pytest.raises(ConnError):
        await s.read()
    with pytest.raises(ConnError):
        await s.write(b"later")
    assert s.state == "closed"


async def test_pending_read_fails_promptly_on_terminate() -> None:
    s, _ = make()
    r = asyncio.create_task(s.read())
    await settle()
    s.terminate(ConnError(ErrorCode.INTERNAL_ERROR, "1006"))
    with pytest.raises(ConnError):
        await r


async def test_terminate_with_stream_error_is_a_local_reset() -> None:
    s, host = make()
    s.terminate(StreamError(ErrorCode.CANCEL, 1, "connection drained"))
    assert s.reset_code == ErrorCode.CANCEL and host.control == []
    assert await s.wait_closed() == Reset(ErrorCode.CANCEL, "CANCEL", "connection drained", False)
    s.terminate(ConnError(ErrorCode.INTERNAL_ERROR, "ignored"))  # no-op once closed


@pytest.mark.parametrize(
    ("end", "want"),
    [
        (lambda s: (s.handle_close(), s.close()), Finished()),
        (
            lambda s: s.handle_reset(ErrorCode.STREAM_LIMIT, "m"),
            Reset(8, "STREAM_LIMIT", "m", True),
        ),
        (lambda s: s.reset(ErrorCode.CANCEL, "m"), Reset(7, "CANCEL", "m", False)),
    ],
)
async def test_wait_closed_variants(end: Callable[[MixerStream], object], want: object) -> None:
    s, _ = make()
    waiter = asyncio.create_task(s.wait_closed())
    await settle()
    end(s)
    assert await waiter == want


async def test_wait_closed_survives_caller_cancellation() -> None:
    s, _ = make()
    waiter = asyncio.create_task(s.wait_closed())
    await settle()
    waiter.cancel()
    await settle()
    s.reset()
    assert isinstance(await s.wait_closed(), Reset)
