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

"""Contract of the group bus: a group is a kbus Dispatcher, its workers are members.

Skeletons: the behaviour is decided, the bindings are the phase's. Each body is
replaced by a real test of exactly what its ``wf:contract:`` lines state, with
real child processes where the line names a process.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any

import kbus
import pytest
from kajenn.http_record import HttpRecord

from tests.orchestration.message_helpers import incoming, join_bus

from kajenn_orchestra.orchestration import GroupHandler, SpaCommander, SpaWorker, WorkerHandler
from kajenn_orchestra.orchestration.lane_payload import LanePayload
from kajenn_orchestra.orchestration.worker_connector import (
    ENVELOPE_SLOT_PRESENTATION,
    ENVELOPE_SLOT_WORKER_EVENTS,
    ENVELOPE_SLOT_WORKER_SNAPSHOT,
)
from kajenn_orchestra.orchestration.worker_handler import PING_OP_PATH, WORKER_ENV_VAR

from .conftest import kill_process, wait_for

#: The route a child of this file takes and never answers.
MUTE_PATH = "/mute"

#: The meta a reply of the bus may carry: routing scalars only.
ROUTING_FIELDS = {"format", "cid", "identity", "user_frozen", "error", "status", "connection_id"}

CHILD_SCRIPT = '''
"""A member of a group bus: presents, reports the answer, serves a beat with its photo."""

import asyncio
import json
import os
from pathlib import Path

from tests.orchestration.message_helpers import join_bus, present

from kajenn_orchestra.orchestration.lane_payload import LanePayload


async def live() -> None:
    payload = json.loads(os.environ["{env_var}"])
    photo = {{"pid": os.getpid(), "name": payload["name"]}}

    async def serve(message, reply):
        if message.meta["route"] == "{mute_path}":
            await asyncio.Event().wait()
        await reply.send(LanePayload().control_message({{"result": {{}}, "{snapshot_key}": photo}}))

    member = await join_bus(payload["kbus_url"], serve)
    answer = await present(member, {{"pid": os.getpid(), "config": payload, "{snapshot_key}": photo}})
    report = Path(payload["kwargs"]["report_dir"]) / (payload["name"] + ".json")
    report.write_text(json.dumps({{"pid": os.getpid(), "answer": answer}}))
    await member.connected.wait_closed()


asyncio.run(live())
'''.format(env_var=WORKER_ENV_VAR, snapshot_key=ENVELOPE_SLOT_WORKER_SNAPSHOT, mute_path=MUTE_PATH)

CHILD_MODULE = "bus_child"


class X_StillPhotoWorker(SpaWorker):
    """A worker whose photo is taken once: two readings of its memory never differ."""

    still_photo: dict[str, Any] | None = None

    @property
    def worker_snapshot(self) -> dict[str, Any]:
        if self.still_photo is None:
            self.still_photo = super().worker_snapshot
        return self.still_photo


class X_Reply:
    """Where one answer lands, as kbus hands it to a worker."""

    message: kbus.Message | None = None

    async def send(self, message: kbus.Message) -> None:
        self.message = message


@pytest.fixture
def instance_root(monkeypatch):
    """A short root holding the bus socket, the deposit, the reports and the child."""
    root = Path(tempfile.mkdtemp(prefix="gnrbus_"))
    (root / f"{CHILD_MODULE}.py").write_text(CHILD_SCRIPT)
    (root / "reports").mkdir()
    inherited = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(root), inherited]).rstrip(os.pathsep))
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
async def make_group(instance_root):
    """Build real groups over the child of this file; stop them all at the end."""
    groups: list[GroupHandler] = []

    def build() -> GroupHandler:
        group = GroupHandler(
            SpaCommander(instance_root / "frozen_users"),
            "standard",
            memory_concession_bytes=8 * 1024**3,
            instance_dir=instance_root / "i",
            frozen_users_path=instance_root / "frozen_users",
            entry_module=CHILD_MODULE,
            worker_kwargs={"report_dir": str(instance_root / "reports")},
            process_ping_timeout=10.0,
        )
        groups.append(group)
        return group

    yield build
    for group in groups:
        await group.stop()


async def test_a_group_listens_on_one_socket_and_admits_each_worker_by_name(make_group, instance_root):
    # wf:contract: a started group owns one kbus.Dispatcher listening on one unix:// address
    # wf:contract: two workers launched in that group are both admitted, each under its own name
    # wf:contract: the group's instance directory holds one socket file for the group, none per worker
    group = make_group()
    first = await group.start_worker()
    second = await group.start_worker()

    assert isinstance(group.bus, kbus.Dispatcher)
    assert group.bus_path == instance_root / "i" / "standard.sock"
    for worker_handler in (first, second):
        assert worker_handler.connector.url.startswith(f"unix://{worker_handler.name}:")
        assert worker_handler.connector.url.endswith(f"@{group.bus_path}")
        answered = await worker_handler.connector.call(PING_OP_PATH, timeout=5.0)
        assert answered[ENVELOPE_SLOT_WORKER_SNAPSHOT] == {
            "pid": worker_handler.process.pid, "name": worker_handler.name,
        }
    assert [path.name for path in (instance_root / "i").iterdir()] == ["standard.sock"]


async def test_the_spawn_payload_carries_a_bus_url_with_the_worker_name_and_a_fresh_token(make_group):
    # wf:contract: spawn_payload carries a URL of the form unix://<worker name>:<token>@<socket path>
    # wf:contract: two launches of the same worker name carry two different tokens
    group = make_group()
    worker_handler = await group.start_worker()
    pattern = rf"unix://{worker_handler.name}:([0-9a-f]+)@{re.escape(str(group.bus_path))}"
    first = re.fullmatch(pattern, worker_handler.spawn_payload["kbus_url"])

    await worker_handler.terminate_process()
    await worker_handler.launch_process()
    second = re.fullmatch(pattern, worker_handler.spawn_payload["kbus_url"])

    assert first is not None and second is not None
    assert first.group(1) != second.group(1)


async def test_a_predecessor_token_no_longer_admits_once_the_successor_is_minted(make_group):
    # wf:contract: after a new URL is minted for a worker name, connecting with the previous URL raises kbus.Rejected
    # wf:contract: the successor connecting with the new URL is admitted
    group = make_group()
    worker_handler = WorkerHandler(group, "standard_0009", **group.worker_settings)
    await worker_handler.connector.start()
    predecessor = worker_handler.connector.url
    await worker_handler.connector.start()
    successor = worker_handler.connector.url

    with pytest.raises(kbus.Rejected):
        await join_bus(predecessor, None)
    member = await join_bus(successor, None)
    await member.close()


async def test_the_presentation_answers_with_what_the_chain_sends_down(make_group, instance_root, monkeypatch):
    # wf:contract: a real child that joins presents its pid, its config and its worker snapshot in one call to the group
    # wf:contract: the reply to that call carries what WorkerHandler.read_envelope composes for the presentation
    # wf:contract: after the presentation the handler state is "running" and worker_snapshot holds the child's photo
    presented: list[dict[str, Any]] = []
    composed: list[dict[str, Any]] = []
    chain = WorkerHandler.read_envelope

    def reading(worker_handler, envelope):
        answer = chain(worker_handler, envelope)
        if ENVELOPE_SLOT_PRESENTATION in envelope:
            answer = {**answer, "composed_for": worker_handler.name}
            presented.append(envelope)
            composed.append(answer)
        return answer

    monkeypatch.setattr(WorkerHandler, "read_envelope", reading)
    group = make_group()
    worker_handler = await group.start_worker()
    report = instance_root / "reports" / f"{worker_handler.name}.json"
    await wait_for(report.exists)

    pid = worker_handler.process.pid
    assert len(presented) == 1
    assert presented[0]["pid"] == pid
    assert presented[0]["config"] == worker_handler.spawn_payload
    assert presented[0][ENVELOPE_SLOT_WORKER_SNAPSHOT] == {"pid": pid, "name": worker_handler.name}
    assert json.loads(report.read_text()) == {"pid": pid, "answer": composed[0]}
    assert worker_handler.state == "running"
    assert worker_handler.worker_snapshot == {"pid": pid, "name": worker_handler.name}


async def test_a_call_in_flight_raises_connection_error_when_the_worker_dies(make_group):
    # wf:contract: a call from the group to a real child that is SIGKILLed before answering raises ConnectionError
    # wf:contract: a call to a worker name that has not joined raises ConnectionError
    group = make_group()
    worker_handler = await group.start_worker()
    in_flight = asyncio.create_task(worker_handler.connector.call(MUTE_PATH))
    await asyncio.sleep(0.1)
    assert not in_flight.done()

    kill_process(worker_handler.process)
    with pytest.raises(ConnectionError):
        await asyncio.wait_for(in_flight, 10.0)

    stranger = WorkerHandler(group, "standard_0099", **group.worker_settings)
    with pytest.raises(ConnectionError):
        await stranger.connector.call(PING_OP_PATH, timeout=5.0)


async def test_a_wild_death_reaches_on_child_lost_through_the_dispatcher(make_group, monkeypatch, caplog):
    # wf:contract: a real child SIGKILLed after its presentation makes the group wake once with state "aborted"
    # wf:contract: the dead worker's name is no longer a member of the group's dispatcher
    # wf:contract: a group stopped on purpose closes its dispatcher and announces no death
    group = make_group()
    worker_handler = await group.start_worker()
    wakes: list[str] = []
    monkeypatch.setattr(group, "ping_now", lambda: wakes.append(worker_handler.state))

    kill_process(worker_handler.process)
    await wait_for(lambda: wakes)
    await asyncio.sleep(0.1)

    assert wakes == ["aborted"]
    with pytest.raises(kbus.NoSuchMember):
        await group.bus_member.call(f"{worker_handler.name}.{PING_OP_PATH}", kbus.Message())

    survivor = await group.start_worker()
    with caplog.at_level("WARNING"):
        await group.stop()

    assert not group.bus_path.exists()
    with pytest.raises(kbus.LinkLost):
        await group.bus_member.call(f"{survivor.name}.{PING_OP_PATH}", kbus.Message())
    assert survivor.state != "aborted"
    assert f"Worker {survivor.name}: WILD death" not in caplog.text


async def test_a_400_user_snapshot_travels_in_the_payload_and_meta_stays_small():
    # wf:contract: a reply of a SpaWorker hosting 400 connections carries worker_snapshot and worker_events in the payload
    # wf:contract: the meta of that reply holds only routing fields and stays under kbus.Limits().max_meta
    # wf:contract: decoded on the group side, the snapshot equals SpaWorker.worker_snapshot
    worker = X_StillPhotoWorker("standard_0001", freeze_handler=None, worker_snapshot_ttl=0)
    parent, child = kbus.core.pipe()
    child.handler = worker.serve_call

    async def populate(message, reply):
        for n in range(400):
            worker.add_connection(f"cid-{n:03d}", f"user-{n:03d}")
        await worker.send_reply(message, reply, result={})

    worker.answer_call = populate
    try:
        reply = await asyncio.wait_for(parent.call(incoming("/populate")), 5.0)
        envelope = LanePayload().envelope(reply)

        assert envelope[ENVELOPE_SLOT_WORKER_SNAPSHOT]["connection_count"] == 400
        assert envelope[ENVELOPE_SLOT_WORKER_EVENTS]
        assert set(reply.meta) <= ROUTING_FIELDS
        assert len(json.dumps(reply.meta)) < kbus.Limits().max_meta
        assert len(reply.payload) > kbus.Limits().max_meta
        assert envelope[ENVELOPE_SLOT_WORKER_SNAPSHOT] == worker.worker_snapshot
    finally:
        await parent.close()
        worker.traffic_pool.shutdown()
        worker.service_pool.shutdown()


async def test_an_http_reply_carries_its_envelope_and_its_record_unchanged():
    # wf:contract: an http reply payload is a 4-byte length, the envelope JSON, then the HttpRecord bytes
    # wf:contract: decoding it gives back the same envelope and the same HttpRecord bytes
    worker = SpaWorker("standard_0001", freeze_handler=None, worker_snapshot_ttl=0)
    worker.open_request_slot()
    reply = X_Reply()
    response = {"status": 201, "headers": [("content-type", "text/plain")], "body": b"made"}
    try:
        await worker.send_reply(
            incoming("/site/orders", {"http": {"method": "POST", "path": "/orders"}}),
            reply,
            result={**response, "connection_id": "cid-a"},
        )
    finally:
        worker.traffic_pool.shutdown()
        worker.service_pool.shutdown()

    payload = reply.message.payload
    size = int.from_bytes(payload[:4], "big")
    envelope = json.loads(payload[4:4 + size])
    record = payload[4 + size:]

    assert reply.message.meta == {"format": "http", "connection_id": "cid-a"}
    assert envelope[ENVELOPE_SLOT_WORKER_EVENTS] == []
    assert ENVELOPE_SLOT_WORKER_SNAPSHOT in envelope
    assert record == HttpRecord().encode_response(response)
    assert LanePayload().decode_http(payload) == (envelope, record)
