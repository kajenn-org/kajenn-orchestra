"""Capacity policy and pre-write refusals must not become link failures."""

import asyncio
from unittest.mock import AsyncMock

import kbus

from tests.orchestration.message_helpers import incoming, read_control
from kajenn_orchestra.orchestration import SpaWorker


async def test_worker_oversized_response_preserves_events_snapshot_and_channel():
    worker = SpaWorker("capacity-probe", freeze_handler=None)
    parent, child = kbus.core.pipe(kbus.Limits(), kbus.Limits(max_frame=4096))
    child.handler = worker.serve_call
    calls = []

    async def answer(message, reply):
        calls.append(message.meta["route"])
        if message.meta["route"] == "/large":
            worker.add_connection("connection-1", "user-1")
            await worker.send_reply(message, reply, result="x" * 8192)
        else:
            await worker.send_reply(message, reply, result="ok")

    worker.answer_call = answer
    try:
        reply = read_control(await asyncio.wait_for(parent.call(incoming("/large")), 1))
        assert "FrameTooLarge" in reply["error"]
        assert reply["worker_events"]
        assert reply["worker_snapshot"]["user_count"] == 1
        assert not child.closed
        success = read_control(await asyncio.wait_for(parent.call(incoming("/small")), 1))
        assert "error" not in success
        assert success["worker_events"] == []
        assert calls == ["/large", "/small"]
    finally:
        await parent.close()
        worker.traffic_pool.shutdown()
        worker.service_pool.shutdown()


async def test_spa_oversized_http_request_returns_413_before_dispatch(monkeypatch):
    from kajenn_orchestra.spa_app import SpaApplication

    monkeypatch.setenv("KAJENN_HTTP_MAX_BODY_BYTES", "1024")
    app = SpaApplication(code="demo")
    receive = AsyncMock(return_value={"type": "http.request", "body": b"x" * 2048})
    send = AsyncMock()
    await app.forward_request({"type": "http", "method": "POST", "path": "/echo", "headers": []}, receive, send)
    assert send.await_args_list[0].args[0]["status"] == 413
