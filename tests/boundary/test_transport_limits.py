"""Capacity policy and pre-write refusals must not become link failures."""

import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from kajenn.channel.frame import Frame, FrameCodec, FrameStream, HEADER, CHANNEL_MAGIC, CHANNEL_VERSION
from kajenn.channel.local import LocalFrameStream
from kajenn.http_record import HttpRecord
from kajenn.asgi_endpoint import BufferedAsgiEndpoint
from kajenn.transport_limits import FrameTooLarge
from kajenn_orchestra.orchestration import SpaWorker


def test_environment_policy_and_explicit_override(monkeypatch):
    monkeypatch.setenv("KAJENN_FRAME_MAX_BYTES", "2097152")
    monkeypatch.setenv("KAJENN_FRAME_WARN_BYTES", "12345")
    monkeypatch.setenv("KAJENN_FRAME_WARN_INTERVAL_SECONDS", "7")
    codec = FrameCodec()
    assert (codec.max_size, codec.warn_size, codec.warning_interval) == (2097152, 12345, 7)
    assert HttpRecord().max_body_size == 2097152
    assert BufferedAsgiEndpoint(AsyncMock()).max_body_size == 2097152
    assert FrameCodec(max_size=3000).max_size == 3000
    monkeypatch.setenv("KAJENN_HTTP_MAX_BODY_BYTES", "1000")
    assert HttpRecord().max_body_size == 1000
    assert BufferedAsgiEndpoint(AsyncMock()).max_body_size == 1000


@pytest.mark.parametrize("name,value", [
    ("KAJENN_FRAME_MAX_BYTES", "0"), ("KAJENN_FRAME_MAX_BYTES", "many"),
    ("KAJENN_FRAME_MAX_BYTES", str(2**32)), ("KAJENN_FRAME_WARN_BYTES", "-1"),
    ("KAJENN_FRAME_WARN_INTERVAL_SECONDS", "-1"),
])
def test_invalid_configuration_rejected(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        FrameCodec()


def test_warning_threshold_throttling_and_no_payload(caplog, monkeypatch):
    codec = FrameCodec(warn_size=200, warning_interval=60)
    frame = Frame(method="CALL", path="/large", info={"worker_snapshot": {"name": "worker-1"}},
                  payload=b"PRIVATE-PAYLOAD" * 50)
    caplog.set_level(logging.WARNING)
    monkeypatch.setattr("kajenn.channel.frame.time.monotonic", lambda: 100)
    wire = codec.encode(frame)
    codec.encode(frame)
    assert len(caplog.records) == 1
    assert "worker-1" in caplog.text and "path=/large" in caplog.text
    assert "PRIVATE-PAYLOAD" not in caplog.text
    monkeypatch.setattr("kajenn.channel.frame.time.monotonic", lambda: 160)
    codec.get_frame(wire)
    assert len(caplog.records) == 2
    assert "direction=receive" in caplog.text
    FrameCodec(warn_size=0).encode(frame)
    assert len(caplog.records) == 2


def test_exact_boundary_and_larger_limit_does_not_change_wire():
    frame = Frame(method="CALL", path="/data", payload=b"x" * 1024)
    wire = FrameCodec().encode(frame)
    size = len(wire) - HEADER.size
    assert FrameCodec(max_size=size).encode(frame) == wire
    assert FrameCodec(max_size=256 * 1024 * 1024).encode(frame) == wire
    with pytest.raises(FrameTooLarge):
        FrameCodec(max_size=size - 1).encode(frame)
    with pytest.raises(FrameTooLarge):
        FrameCodec(max_size=size - 1).get_frame(wire)


async def test_oversized_header_rejected_before_body_read():
    reader = AsyncMock()
    reader.readexactly.return_value = HEADER.pack(CHANNEL_MAGIC, CHANNEL_VERSION, 10, 10000)
    stream = FrameStream(reader, AsyncMock(), max_size=1000)
    with pytest.raises(FrameTooLarge):
        await stream.read()
    reader.readexactly.assert_awaited_once_with(HEADER.size)


async def test_local_rejection_does_not_enqueue_or_close():
    inbound, outbound = asyncio.Queue(), asyncio.Queue()
    stream = LocalFrameStream(inbound, outbound, max_size=1024)
    with pytest.raises(FrameTooLarge):
        await stream.write(Frame(method="CALL", path="/large", payload=b"x" * 1024))
    assert outbound.empty() and not stream.closed
    await stream.write(Frame(method="CALL", path="/small"))
    assert FrameCodec().get_frame(await outbound.get()).path == "/small"


def test_actual_worker_snapshot_with_400_users_roundtrips():
    worker = SpaWorker("capacity-probe", freeze_handler=None)
    try:
        worker.open_request_slot()
        for index in range(400):
            worker.add_connection(f"connection-{index:06d}", f"user-{index:06d}")
        photo = worker.worker_snapshot
        frame = Frame(method="REPLY", path="/ping", info={"worker_snapshot": photo})
        wire = frame.encode()
        assert len(wire) > 64 * 1024
        assert FrameCodec().get_frame(wire).info["worker_snapshot"] == photo
    finally:
        worker.traffic_pool.shutdown()
        worker.service_pool.shutdown()


async def test_worker_oversized_response_preserves_events_snapshot_and_channel():
    worker = SpaWorker("capacity-probe", freeze_handler=None)
    outgoing = asyncio.Queue()
    worker.stream = LocalFrameStream(asyncio.Queue(), outgoing, max_size=4096)
    calls = []

    async def answer(frame):
        calls.append(frame.id)
        if frame.path == "/large":
            worker.add_connection("connection-1", "user-1")
            await worker.send_reply(frame, result="x" * 8192)
        else:
            await worker.send_reply(frame, result="ok")

    worker.answer_call = answer
    try:
        request = Frame(method="CALL", path="/large")
        await worker._guarded_call(request)
        reply = FrameCodec().get_frame(await asyncio.wait_for(outgoing.get(), 1))
        assert reply.id == request.id
        assert "FrameTooLarge" in reply.info["error"]
        assert reply.info["worker_events"]
        assert reply.info["worker_snapshot"]["user_count"] == 1
        assert not worker.stream.closed
        assert worker._request_slot_var.get() is None
        next_request = Frame(method="CALL", path="/small")
        await worker._guarded_call(next_request)
        success = FrameCodec().get_frame(await asyncio.wait_for(outgoing.get(), 1))
        assert success.id == next_request.id and "error" not in success.info
        assert success.info["worker_events"] == []
        assert calls == [request.id, next_request.id]
    finally:
        worker.traffic_pool.shutdown()
        worker.service_pool.shutdown()


def test_body_above_previous_8mib_limit_roundtrips():
    body = b"x" * (9 * 1024 * 1024)
    payload = HttpRecord().encode_response({"status": 200, "headers": [], "body": body})
    frame = Frame(method="REPLY", path="/http", payload=payload)
    reply = FrameCodec().get_frame(frame.encode())
    assert HttpRecord().decode_response(reply.payload)["body"] == body


async def test_spa_oversized_http_request_returns_413_before_dispatch(monkeypatch):
    from kajenn_orchestra.spa_app import SpaApplication

    monkeypatch.setenv("KAJENN_HTTP_MAX_BODY_BYTES", "1024")
    app = SpaApplication(code="demo")
    receive = AsyncMock(return_value={"type": "http.request", "body": b"x" * 2048})
    send = AsyncMock()
    await app.forward_request({"type": "http", "method": "POST", "path": "/echo", "headers": []}, receive, send)
    assert send.await_args_list[0].args[0]["status"] == 413
