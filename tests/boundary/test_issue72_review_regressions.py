"""Regression contracts for the four core findings of the independent #72 review."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from kajenn.kbus.control import ControlPayload
from kajenn.kbus.frame import Frame
from kajenn.exceptions import HTTPException
from kajenn_orchestra.orchestration import SpaWorker, WorkerConnector


@pytest.mark.parametrize("wire_state", ["lost", "write_failure", "close_failure"])
async def test_child_call_dead_wire_task_finishes_without_unretrieved_exception(wire_state, caplog):
    entered, finish = asyncio.Event(), asyncio.Event()

    async def service(path, payload):
        entered.set()
        await finish.wait()
        return "done"

    connector = WorkerConnector(SimpleNamespace(serve_child_call=service), "/tmp/review-unused.sock")
    stream = SimpleNamespace(write=AsyncMock(side_effect=OSError("write failed")),
                             close=AsyncMock(side_effect=OSError("close failed") if wire_state == "close_failure" else None))
    connector._stream = stream
    connector._dispatch(Frame(method="CALL", path="/test", payload=ControlPayload().encode({})))
    task, = connector._service_tasks
    await entered.wait()
    if wire_state == "lost":
        connector._stream = None
    finish.set()
    await task
    await asyncio.sleep(0)
    assert not connector._service_tasks
    assert "found no wire" in caplog.text


@pytest.mark.parametrize("sequential", [False, True])
async def test_page_request_accepts_site_connection_name_different_from_inbound_cookie(monkeypatch, sequential):
    worker = SpaWorker("review", freeze_handler=None)
    worker.open_request_slot()
    page = worker.add_page("page", "site-connection", wsx={"sequential": sequential})
    served = []

    @asynccontextmanager
    async def serving(payload, cid):
        assert cid == "handshake-cookie"
        yield

    async def serve(http, identity):
        served.append(http["cid"])
        assert page["call_lock"].locked() == sequential
        return {"status": 200, "headers": [], "body": b"ok"}

    monkeypatch.setattr(worker, "_serving", serving)
    monkeypatch.setattr(SpaWorker, "hosted_app_seam", property(lambda self: SimpleNamespace(serve=serve)))
    try:
        result = await worker._serve_request({"http": {"page_id": "page", "cid": "handshake-cookie"}})
        assert result["status"] == 200 and served == ["handshake-cookie"]
        assert not page["call_lock"].locked()
        page["wsx"] = None
        with pytest.raises(HTTPException) as refused:
            await worker._serve_request({"http": {"page_id": "page", "cid": "handshake-cookie"}})
        assert refused.value.status == 409
    finally:
        worker.traffic_pool.shutdown()
        worker.service_pool.shutdown()
