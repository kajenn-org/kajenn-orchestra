# Copyright 2025 Softwell S.r.l.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The two channels of the worker events (owner, 2026-09-04; kajenn #60).

An event born while a CALL is being served has an effect on the commander, and it
returns WITH the REPLY of that CALL — never with the REPLY of another CALL that
happened to finish first, never through a queue shared by the whole worker. An
event that no CALL produced — the transfer cycle of a quit — goes up with a CALL
of the worker's own. The login is the first case this contract protects: a ping
ending while ``doLogin`` still runs must evict nobody.

The worker here speaks on a stub wire that keeps every frame it writes and answers
the worker's own CALLs, so a test reads exactly what each REPLY and each CALL
carried. The deposit is a real ``FreezeHandler`` on a real directory.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from tests.orchestration.frame_helpers import control_frame, read_control

from kajenn.channel.frame import Frame
from kajenn_orchestra.orchestration import FreezeHandler, SpaWorker
from kajenn_orchestra.orchestration.worker_connector import (
    CALL_METHOD,
    ENVELOPE_SLOT_WORKER_EVENTS,
    ENVELOPE_SLOT_WORKER_SNAPSHOT,
)
from kajenn_orchestra.orchestration.worker_handler import ANNOUNCE_OP_PATH, PING_OP_PATH

from .conftest import XT_Wire, wait_for

WORKER_NAME = "standard_0001"
CID = "a1b2"


class XT_SiteWorker(SpaWorker):
    """A worker hosting a two-path site: ``/visit`` registers, ``/login`` logs in.

    Both paths block on ``gate`` after acting on the registers, so a test can let
    another CALL finish while the request is still being served.
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        self.wsgi_app = self.site
        self.gate = threading.Event()
        self.acted = threading.Event()

    def site(self, environ: dict[str, Any], start_response: Any) -> list[bytes]:
        if self.connection_register.get(CID) is None:
            self.add_connection(CID)
        if environ["PATH_INFO"] == "/login":
            self.change_connection_user(CID, "mario")
        self.acted.set()
        self.gate.wait(timeout=10)
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"served"]


def http_call(path: str) -> dict[str, Any]:
    """The http CALL form as the front packs an anonymous visit."""
    return {
        "http": {
            "method": "GET",
            "path": path,
            "query_string": "",
            "headers": [["host", "site.example:8080"]],
            "body": "",
            "cid": CID,
        },
        "identity": None,
    }


def events_of(frame: Frame) -> list[str]:
    return [event["op"] for event in (read_control(frame) or {}).get(ENVELOPE_SLOT_WORKER_EVENTS) or ()]


@pytest.fixture
def deposit(tmp_path):
    return FreezeHandler(tmp_path / "frozen_users")


@pytest.fixture
async def worker(deposit):
    worker = XT_SiteWorker(WORKER_NAME, freeze_handler=deposit, deposit_lock_retry_interval=0.01)
    wire = XT_Wire(worker)
    worker.attach_stream(wire)
    yield worker
    worker.gate.set()


async def serve(worker: SpaWorker, path: str, data: Any) -> Frame:
    """Hand the worker one CALL the way the wire does; return the frame, not the answer."""
    frame = control_frame(method=CALL_METHOD, path=path, data=data)
    worker.handle_frame(frame)
    return frame


# -- channel one: the REPLY of the CALL that caused the event --


async def test_a_reply_carries_only_the_events_its_own_call_caused(worker):
    wire = worker.stream
    visit = await serve(worker, "/http", http_call("/visit"))
    await asyncio.get_running_loop().run_in_executor(None, worker.acted.wait)

    ping = await serve(worker, PING_OP_PATH, {})
    await wait_for(lambda: wire.reply_to(ping.id) is not None)
    assert events_of(wire.reply_to(ping.id)) == []

    worker.gate.set()
    await wait_for(lambda: wire.reply_to(visit.id) is not None)
    assert events_of(wire.reply_to(visit.id)) == ["new_user", "new_connection"]


def test_an_event_outside_any_call_is_refused(deposit):
    worker = SpaWorker(WORKER_NAME, freeze_handler=deposit)

    with pytest.raises(RuntimeError):
        worker.add_connection(CID)


# -- the login: its tail belongs to its own request --


async def test_a_ping_ending_during_a_login_evicts_nobody(worker, deposit):
    wire = worker.stream
    login = await serve(worker, "/http", http_call("/login"))
    await asyncio.get_running_loop().run_in_executor(None, worker.acted.wait)

    ping = await serve(worker, PING_OP_PATH, {})
    await wait_for(lambda: wire.reply_to(ping.id) is not None)
    assert events_of(wire.reply_to(ping.id)) == []
    assert worker.connection_register.get(CID)["user"] == "mario"
    assert deposit.read_connection_register_item("mario", CID) is None

    worker.gate.set()
    await wait_for(lambda: wire.reply_to(login.id) is not None)
    assert events_of(wire.reply_to(login.id)) == [
        "new_user",
        "new_connection",
        "connection_user_changed",
        "user_rows_released",
    ]
    assert worker.connection_register.get(CID) is None
    assert deposit.read_connection_register_item("mario", CID) is not None


# -- channel two: a CALL of the worker's own for what no CALL produced --


async def test_the_transfer_cycle_announces_each_freeze_with_a_call_of_its_own(worker, deposit):
    wire = worker.stream
    worker.open_request_slot()
    worker.add_connection(CID)
    worker.change_connection_user(CID, "mario")
    worker.worker_events.clear()

    worker.plan_transfers(transfer_users=["mario"])
    worker._transfers_start_ts = 0.0
    await worker.execute_transfers()

    announced = wire.calls(ANNOUNCE_OP_PATH)
    assert [events_of(frame) for frame in announced] == [["user_frozen"]]
    assert ENVELOPE_SLOT_WORKER_SNAPSHOT in read_control(announced[0])
    assert wire.replies() == []
    assert deposit.read_user_register_item("mario") is not None


async def test_the_vertex_folds_an_announcement_like_a_reply(worker_commander_lane):
    lane = worker_commander_lane
    vertex = lane.commander
    await lane.open_request()
    await lane.verb("add_connection", CID)
    await lane.verb("change_connection_user", CID, "mario")
    await lane.announce()
    await wait_for(lambda: vertex.resolve_user(CID) == "mario")
    assert "mario" in lane.worker_handler.hosted_users

    await lane.verb("plan_transfers", transfer_users=["mario"])
    lane.worker._transfers_start_ts = 0.0
    await lane.worker.execute_transfers()

    await wait_for(lambda: vertex.user_map["mario"]["frozen"] is True)
    assert lane.worker_handler.group_handler.user_worker_map["mario"] is None
    assert "mario" not in lane.worker_handler.hosted_users


async def test_reply_encoding_failure_answers_and_keeps_its_events(worker):
    """An invalid endpoint result must not consume the slot and strand the caller."""
    async def answer(frame):
        worker.add_connection(CID)
        await worker.send_reply(frame, result=object())

    worker.answer_call = answer
    frame = control_frame(method=CALL_METHOD, path="/invalid-result", data={})
    await worker._guarded_call(frame)
    reply = worker.stream.reply_to(frame.id)
    assert "invalid control payload" in reply.info["error"]
    assert events_of(reply) == ["new_user", "new_connection"]
    assert worker._request_slot_var.get() is None
