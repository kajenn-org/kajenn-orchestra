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

"""WorkerConnector tests: one worker on its group's bus, the presentation and the end.

Everything runs on a real UDS and a real ``kbus.Dispatcher``: the bus is the one
place that has to behave like the kernel behaves, so a fake transport would
assert nothing. The child is a ``ChildPeer``, a ``kbus.Member`` joined with the
URL the connector minted — it presents itself, answers calls, and dies by
closing its member, which is exactly what a worker process does.

The socket lives under a short ``mkdtemp`` root and not under ``tmp_path``: the
system caps a UDS path at about a hundred characters, and pytest's own
directory is already past it.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import kbus
import pytest

from tests.orchestration.message_helpers import join_bus, join_worker, present

from kajenn_orchestra.orchestration import SpaWorker, WorkerConnector
from kajenn_orchestra.orchestration.lane_payload import LanePayload
from kajenn_orchestra.orchestration.worker_connector import GROUP_MEMBER, CommanderCallFailed

from .conftest import wait_for
from .group_stub import GroupStub


class ChildPeer:
    """The worker process, seen from the other end of the bus."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.received: list[tuple[str, Any]] = []
        self.reply_result: Any = None
        self.answer_calls = True
        self.member: kbus.Member | None = None

    async def present(self, config: Any = None) -> Any:
        """Join, send the presentation, return the answer that came back."""
        await self.connect_without_presenting()
        return await present(self.member, {"pid": os.getpid(), "config": config})

    async def connect_without_presenting(self) -> None:
        self.member = await join_bus(self.url, self.serve)

    async def close(self) -> None:
        await self.member.close()

    async def call(self, path: str, data: Any = None) -> Any:
        reply = await self.member.call(f"{GROUP_MEMBER}.{path}", LanePayload().control_message(data))
        return LanePayload().decode_control(reply.payload)

    async def serve(self, message: kbus.Message, reply: kbus.Reply) -> None:
        self.received.append((message.meta["route"], LanePayload().decode_control(message.payload)))
        if not self.answer_calls:
            await asyncio.Event().wait()
        await reply.send(LanePayload().control_message(
            {"result": self.reply_result, "worker_events": []}
        ))


class HandlerStub:
    """The WorkerHandler seen by its connector: the envelopes it takes, what it is told.

    The connector reads nothing of what arrives: it hands the envelope over whole
    and writes back down whatever comes out. So the handler of these tests is the
    fold — it keeps the envelopes it was given, and answers with a payload of its
    own, whatever the real chain happens to compose today.
    """

    def __init__(self, group_handler: GroupStub, name: str = "standard_0001") -> None:
        self.group_handler = group_handler
        self.name = name
        self.descent: dict[str, Any] = {"descending": "what the chain composed"}
        self.envelopes: list[dict[str, Any]] = []
        self.losses = 0
        self.connector = WorkerConnector(self)
        group_handler.worker_handler = self

    def read_envelope(self, envelope: dict[str, Any]) -> dict[str, Any]:
        """Keep what arrived, and answer with what goes down."""
        self.envelopes.append(envelope)
        return dict(self.descent)

    def on_child_lost(self) -> None:
        self.losses += 1


@pytest.fixture
async def group(short_root):
    group = GroupStub(short_root / "frozen_users", instance_dir=short_root / "i")
    yield group
    await group.bus.close()


@pytest.fixture
def handler(group):
    return HandlerStub(group)


@pytest.fixture
async def connector(handler):
    wire = handler.connector
    await wire.start()
    yield wire
    wire.stop()


async def test_the_socket_is_bound_in_a_private_directory(connector, group):
    token = group.bus.secrets["standard_0001"]
    assert group.bus_path.exists()
    assert os.stat(group.bus_path.parent).st_mode & 0o777 == 0o700
    assert connector.url == f"unix://standard_0001:{token}@{group.bus_path}"
    assert connector.connected is False


async def test_a_stale_socket_is_unlinked_before_the_bind(short_root, handler):
    socket_path = short_root / "i" / "standard.sock"
    socket_path.parent.mkdir(mode=0o700, parents=True)
    socket_path.write_bytes(b"what the crash left behind")

    await handler.connector.start()
    try:
        child = ChildPeer(handler.connector.url)
        await child.present()
        await child.close()
    finally:
        handler.connector.stop()


async def test_the_presentation_is_answered_with_what_the_chain_composed(connector, handler):
    child = ChildPeer(connector.url)
    reply = await child.present(config={"pool_size": 4})

    assert reply == handler.descent
    assert handler.envelopes[0]["config"] == {"pool_size": 4}
    await connector.wait_connected()
    assert connector.connected is True

    await child.close()


async def test_a_call_travels_and_its_reply_comes_back(connector):
    child = ChildPeer(connector.url)
    await child.present()
    child.reply_result = {"alive": True}

    payload = await connector.call("/probe", {"kwargs": {}})

    assert payload == {"result": {"alive": True}, "worker_events": []}
    assert child.received == [("/probe", {"kwargs": {}})]

    await child.close()


async def test_a_reply_after_the_deadline_is_folded_and_the_order_is_not_cancelled(
    connector, handler
):
    """The caller gives up; the order goes on on the worker and its events still climb."""
    child = ChildPeer(connector.url)
    await child.present()
    release = asyncio.Event()
    finished: list[str] = []

    async def slow(message: kbus.Message, reply: kbus.Reply) -> None:
        await release.wait()
        finished.append(message.meta["route"])
        await reply.send(LanePayload().control_message(
            {"result": None, "worker_events": [{"op": "user_frozen", "user": "mario"}]}
        ))

    child.member.connection.handler = slow
    with pytest.raises(TimeoutError):
        await connector.call("/group/freeze_user", {"user": "mario"}, timeout=0.05)
    release.set()

    await wait_for(lambda: len(handler.envelopes) == 2)
    assert finished == ["/group/freeze_user"]
    assert handler.envelopes[1]["worker_events"] == [{"op": "user_frozen", "user": "mario"}]

    await child.close()


async def test_a_worker_the_group_no_longer_has_is_refused_and_fails_its_call(
    connector, group
):
    """The group refuses a caller it dropped; the worker reads that as a failed call."""
    worker = SpaWorker("standard_0001", freeze_handler=None)
    try:
        await join_worker(worker, connector.url)
        group.worker_handler = None

        with pytest.raises(CommanderCallFailed, match="not a worker of group standard"):
            await worker.call("/group/announce", {})

        await worker.member.close()
    finally:
        worker.traffic_pool.shutdown()
        worker.service_pool.shutdown()


async def test_a_call_the_handler_has_no_hook_for_comes_back_as_an_error(connector, handler):
    """This handler serves no calls at all — the child is answered all the same."""
    child = ChildPeer(connector.url)
    await child.present()

    answer = await child.call("/op/ask", {"user": "mario"})

    assert "AttributeError" in answer["error"]
    assert handler.losses == 0

    await child.close()


async def test_an_unencodable_child_call_result_comes_back_as_an_error(connector, handler):
    child = ChildPeer(connector.url)
    await child.present()
    handler.serve_child_call = lambda path, payload: object()

    answer = await child.call("/op/unencodable", {"value": 1})

    assert "not encodable" in answer["error"]
    assert connector.connected is True
    await child.close()


async def test_the_death_of_the_child_is_a_local_event(connector, handler):
    child = ChildPeer(connector.url)
    await child.present()
    await connector.wait_connected()

    await child.close()
    await wait_for(lambda: handler.losses == 1)

    assert connector.connected is False


async def test_a_call_in_flight_dies_with_the_child(connector):
    child = ChildPeer(connector.url)
    await child.present()
    await connector.wait_connected()
    child.answer_calls = False

    async def kill_the_child() -> None:
        await asyncio.sleep(0.05)
        await child.close()

    asyncio.create_task(kill_the_child())
    with pytest.raises(ConnectionError):
        await connector.call("/freeze_everybody")


async def test_a_deliberate_stop_announces_no_death(handler, group):
    wire = handler.connector
    await wire.start()
    child = ChildPeer(wire.url)
    await child.present()
    await wire.wait_connected()

    wire.stop()
    await child.close()

    assert handler.losses == 0
    assert wire.connected is False
    assert "standard_0001" not in group.bus.secrets


async def test_the_successor_finds_the_same_socket(connector, handler):
    first = ChildPeer(connector.url)
    await first.present()
    await connector.wait_connected()
    await first.close()
    await wait_for(lambda: connector.connected is False)

    await connector.start()
    handler.descent = {"descending": "what the chain composes now"}
    successor = ChildPeer(connector.url)
    reply = await successor.present()
    await connector.wait_connected()

    assert reply == {"descending": "what the chain composes now"}

    await successor.close()


async def test_a_second_child_on_a_taken_wire_is_refused(connector):
    resident = ChildPeer(connector.url)
    await resident.present()
    await connector.wait_connected()

    intruder = ChildPeer(connector.url)
    with pytest.raises(kbus.Rejected):
        await intruder.connect_without_presenting()

    resident.reply_result = "still here"
    assert await connector.call("/probe") == {"result": "still here", "worker_events": []}

    await resident.close()


async def test_a_child_that_does_not_present_itself_is_refused(connector):
    intruder = ChildPeer(connector.url)
    await intruder.connect_without_presenting()

    with pytest.raises(kbus.Refused):
        await intruder.call("/whatever")
    assert connector.connected is False
    await intruder.close()


async def test_calling_a_wire_with_no_child_is_an_error(connector):
    with pytest.raises(ConnectionError):
        await connector.call("/probe")
