"""Regression contracts for the four core findings of the independent #72 review."""

import asyncio
import shutil
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.orchestration.group_stub import GroupStub
from tests.orchestration.message_helpers import join_bus, present
from kajenn.exceptions import HTTPException
from kajenn_orchestra.orchestration import SpaWorker, WorkerConnector
from kajenn_orchestra.orchestration.lane_payload import LanePayload
from kajenn_orchestra.orchestration.worker_connector import GROUP_MEMBER


@pytest.mark.parametrize("wire_state", ["lost", "stopped"])
async def test_child_call_dead_wire_task_finishes_without_unretrieved_exception(wire_state, caplog):
    entered, finish = asyncio.Event(), asyncio.Event()

    async def service(path, payload):
        entered.set()
        await finish.wait()
        return "done"

    root = Path(tempfile.mkdtemp(prefix="gnr72_"))
    group = GroupStub(root / "frozen_users", instance_dir=root / "i")
    handler = SimpleNamespace(name="review", group_handler=group, serve_child_call=service,
                              read_envelope=lambda envelope: {}, on_child_lost=lambda: None)
    connector = handler.connector = WorkerConnector(handler)
    group.worker_handler = handler
    await connector.start()
    member = await join_bus(connector.url, None)
    await present(member, {"pid": 1})
    placed = asyncio.create_task(
        member.call(f"{GROUP_MEMBER}./test", LanePayload().control_message({})))
    await entered.wait()
    task, = connector._service_tasks
    if wire_state == "lost":
        await member.close()
    else:
        connector.stop()
    finish.set()
    placed.cancel()
    await asyncio.gather(task, placed, return_exceptions=True)
    await asyncio.sleep(0)
    assert not connector._service_tasks
    assert task.cancelled()
    assert "never retrieved" not in caplog.text
    await group.bus.close()
    shutil.rmtree(root, ignore_errors=True)


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
