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

The worker here speaks on a stub member that keeps every message it sends and
answers the worker's own calls, so a test reads exactly what each reply and each
call carried. The deposit is a real ``FreezeHandler`` on a real directory.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from tests.orchestration.message_helpers import read_control

from kajenn_orchestra.orchestration import FreezeHandler, SpaWorker
from kajenn_orchestra.orchestration.worker_connector import (
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


def events_of(message: Any) -> list[str]:
    return [
        event["op"]
        for event in (read_control(message) or {}).get(ENVELOPE_SLOT_WORKER_EVENTS) or ()
    ]


@pytest.fixture
def deposit(tmp_path):
    return FreezeHandler(tmp_path / "frozen_users")


@pytest.fixture
async def worker(deposit):
    worker = XT_SiteWorker(WORKER_NAME, freeze_handler=deposit, deposit_lock_retry_interval=0.01)
    worker.member = XT_Wire(worker)
    worker.loop = asyncio.get_running_loop()
    yield worker
    worker.gate.set()


async def serve(worker: SpaWorker, path: str, data: Any) -> XT_Wire.XT_Reply:
    """Hand the worker one call the way kbus does; return where its answer lands."""
    return worker.member.serve(path, data)


# -- channel one: the REPLY of the CALL that caused the event --


async def test_a_reply_carries_only_the_events_its_own_call_caused(worker):
    visit = await serve(worker, "/http", http_call("/visit"))
    await asyncio.get_running_loop().run_in_executor(None, worker.acted.wait)

    ping = await serve(worker, PING_OP_PATH, {})
    await wait_for(lambda: ping.message is not None)
    assert events_of(ping.message) == []

    worker.gate.set()
    await wait_for(lambda: visit.message is not None)
    assert events_of(visit.message) == ["new_user", "new_connection"]


def test_an_event_outside_any_call_is_refused(deposit):
    worker = SpaWorker(WORKER_NAME, freeze_handler=deposit)

    with pytest.raises(RuntimeError):
        worker.add_connection(CID)


# -- the login: its tail belongs to its own request --


async def test_a_ping_ending_during_a_login_evicts_nobody(worker, deposit):
    login = await serve(worker, "/http", http_call("/login"))
    await asyncio.get_running_loop().run_in_executor(None, worker.acted.wait)

    ping = await serve(worker, PING_OP_PATH, {})
    await wait_for(lambda: ping.message is not None)
    assert events_of(ping.message) == []
    assert worker.connection_register.get(CID)["user"] == "mario"
    assert deposit.read_connection_register_item("mario", CID) is None

    worker.gate.set()
    await wait_for(lambda: login.message is not None)
    assert events_of(login.message) == [
        "new_user",
        "new_connection",
        "connection_user_changed",
        "user_rows_released",
    ]
    assert worker.connection_register.get(CID) is None
    assert deposit.read_connection_register_item("mario", CID) is not None


# -- channel two: a CALL of the worker's own for what no CALL produced --


async def test_the_transfer_cycle_announces_each_freeze_with_a_call_of_its_own(worker, deposit):
    wire = worker.member
    worker.open_request_slot()
    worker.add_connection(CID)
    worker.change_connection_user(CID, "mario")
    worker.worker_events.clear()

    worker.plan_transfers(transfer_users=["mario"])
    worker._transfers_start_ts = 0.0
    await worker.execute_transfers()

    announced = wire.calls(ANNOUNCE_OP_PATH)
    assert [events_of(message) for message in announced] == [["user_frozen"]]
    assert ENVELOPE_SLOT_WORKER_SNAPSHOT in read_control(announced[0])
    assert wire.replies == []
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
    async def answer(message, reply):
        worker.add_connection(CID)
        await worker.send_reply(message, reply, result=object())

    worker.answer_call = answer
    reply = worker.member.serve("/invalid-result", {})
    await wait_for(lambda: reply.message is not None)
    assert "invalid control payload" in read_control(reply.message)["error"]
    assert events_of(reply.message) == ["new_user", "new_connection"]
    assert worker._request_slot_var.get() is None
