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

"""Contract: what a worker may call up the worker_commander_lane is a routing tree (#59, D59-9..11).

The CALLs a worker places on the worker_commander_lane are resolved by name on genro-routes
trees — not by a chain of ``if`` nor by a hand-written ``getattr``. The first
segment of the path names the LEVEL that serves: ``group`` for the group's own
operations (the announcement), ``commander`` for the vertex's (the global
store, the observation). The
worker talks to its group: the group's dispatcher serves ``group/…`` itself and
forwards ``commander/…`` to the commander's dispatcher, so the boundary between
group and vertex is drawn today and the paths survive the group moving to a
process of its own. The core attaches its own operation classes; a consumer
attaches its own under a name of its own, on the commander, once, and the wire
reaches them the same way. A path nobody serves is refused BY NAME
(``NotFound``), a call that does not fit the operation's signature is refused
before the body runs, and each tree says what it offers without being called.

The tests place raw CALLs on a real worker_commander_lane: a ``SpaWorker`` on one end, a real
``WorkerHandler`` under a real ``SpaCommander`` on the other.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from genro_routes import RoutingClass, route
from genro_tytx import from_tytx, to_tytx

from kajenn_orchestra.orchestration.worker_connector import CommanderCallFailed
from kajenn_orchestra.orchestration.worker_handler import ANNOUNCE_OP_PATH


STORE_SET = "/commander/store/set"
STORE_GET = "/commander/store/get"
STORE_DEL = "/commander/store/del"
STORE_LOCK = "/commander/store/lock"
STORE_UNLOCK = "/commander/store/unlock"
OBSERVATION = "/commander/observation"


class XT_ExtraOperations(RoutingClass):
    """What a consumer attaches under the commander: one operation of its own."""

    def __init__(self, spa_commander: Any) -> None:
        self.spa_commander = spa_commander

    @route()
    def echo(self, **payload: Any) -> dict[str, Any]:
        return {"echo": payload, "commander": type(self.spa_commander).__name__}


async def test_store_set_then_get_round_trip_over_the_lane(worker_commander_lane):
    """set and get are leaves of the store branch; the get reply says whether the key exists."""
    assert await worker_commander_lane.worker.call(
        STORE_SET, {"key": "a.b", "value": to_tytx(42, "json")}
    ) == {"key": "a.b"}
    reply = await worker_commander_lane.worker.call(STORE_GET, {"key": "a.b"})
    assert reply["key"] == "a.b" and reply["exists"] is True
    assert from_tytx(reply["value"], "json") == 42


async def test_store_del_removes_the_key(worker_commander_lane):
    await worker_commander_lane.worker.call(STORE_SET, {"key": "a.b", "value": to_tytx(1, "json")})
    assert await worker_commander_lane.worker.call(STORE_DEL, {"key": "a.b"}) == {"key": "a.b"}
    reply = await worker_commander_lane.worker.call(STORE_GET, {"key": "a.b"})
    assert reply["exists"] is False
    assert from_tytx(reply["value"], "json") is None


async def test_store_lock_grants_and_unlock_publishes_the_complete_value(worker_commander_lane):
    """The two halves of a turn are leaves of the same branch: the release carries the value whole."""
    grant = await worker_commander_lane.worker.call(
        STORE_LOCK, {"worker": worker_commander_lane.worker_name, "request_id": "r1", "key": "k"}
    )
    assert grant["request_id"] == "r1" and grant["exists"] is False
    assert worker_commander_lane.commander.global_lock.holds("r1")
    reply = await worker_commander_lane.worker.call(
        STORE_UNLOCK, {"request_id": "r1", "apply": True, "value": to_tytx(7, "json")}
    )
    assert reply == {"applied": True}
    assert not worker_commander_lane.commander.global_lock.holds("r1")
    assert worker_commander_lane.commander.global_register["k"] == 7


async def test_an_observation_call_reaches_the_watchers(worker_commander_lane):
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    await worker_commander_lane.commander.subscribe_observation(queue)
    try:
        await worker_commander_lane.worker.call(
            OBSERVATION,
            {"kind": "xt_kind", "source": worker_commander_lane.worker_name, "data": {"k": 1}},
        )
        observed = await asyncio.wait_for(queue.get(), 5.0)
    finally:
        await worker_commander_lane.commander.unsubscribe_observation(queue)
    assert observed["kind"] == "xt_kind"
    assert observed["source"] == worker_commander_lane.worker_name


async def test_an_unknown_path_is_refused_by_name(worker_commander_lane):
    with pytest.raises(CommanderCallFailed) as refusal:
        await worker_commander_lane.worker.call("/commander/nothing_here", {})
    assert refusal.value.path == "/commander/nothing_here"
    assert "NotFound" in str(refusal.value)


async def test_a_call_that_does_not_fit_the_signature_is_refused_before_the_body_runs(
    worker_commander_lane,
):
    with pytest.raises(CommanderCallFailed) as refusal:
        await worker_commander_lane.worker.call(STORE_GET, {"nope": 1})
    assert "TypeError" in str(refusal.value)
    assert "key" in str(refusal.value)


async def test_the_announcement_is_the_groups_own_operation(worker_commander_lane):
    """The worker's second channel climbs to ITS group: ``group/announce`` folds the envelope."""
    assert ANNOUNCE_OP_PATH == "/group/announce"
    await worker_commander_lane.verb("add_connection", "c1", "mario")
    await worker_commander_lane.announce()
    assert worker_commander_lane.commander.connection_user_map["c1"] == "mario"
    assert worker_commander_lane.group.user_worker_map["mario"] == worker_commander_lane.worker_name


async def test_each_dispatcher_says_what_it_serves(worker_commander_lane):
    """Introspection, not documentation: the trees list their operations and branches."""
    group = worker_commander_lane.group.group_dispatcher.route.nodes()
    assert set(group["routers"]) == {"group", "commander"}
    assert "announce" in group["routers"]["group"]["entries"]
    commander = worker_commander_lane.commander.commander_dispatcher.route.nodes()
    assert "observation" in commander["entries"]
    assert set(commander["routers"]) == {"store"}
    assert set(commander["routers"]["store"]["entries"]) == {"set", "get", "del", "lock", "unlock"}


async def test_a_consumer_attaches_its_own_operations_and_the_wire_reaches_them(
    worker_commander_lane,
):
    """Composition: the consumer's class hangs under the commander's dispatcher, by a name of its own."""
    worker_commander_lane.commander.commander_dispatcher.add_branches(
        {"name": "xt_extra", "instance": XT_ExtraOperations(worker_commander_lane.commander)}
    )
    reply = await worker_commander_lane.worker.call("/commander/xt_extra/echo", {"x": 1})
    assert reply == {"echo": {"x": 1}, "commander": "SpaCommander"}
    assert (
        "xt_extra" in worker_commander_lane.commander.commander_dispatcher.route.nodes()["routers"]
    )
