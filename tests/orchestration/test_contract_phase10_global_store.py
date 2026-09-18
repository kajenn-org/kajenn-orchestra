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

"""Phase 10 contract: the global store lives only on the commander.

The ratified digression (registro 2026-08-20 §7-bis): no replicas on the
workers. Every access is a CALL on the phase-7 lane through the worker's
``global_store`` client, with an immediate REPLY. Since issue #74 (2026-09-07)
the store is one dictionary behind one FIFO lock: the grant of a turn brings
the selected value, the release brings the COMPLETE value back and the
commander replaces it in one assignment — the change batch of the earlier
protocol is gone, and so is the replica machinery of the executed phase 5.

Derived from ``tests/test_spa_global_store.py``, the original contract of that
protocol.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from genro_bag import Bag

from kajenn_orchestra.orchestration import FreezeHandler, SpaCommander, SpaWorker
from kajenn_orchestra.orchestration import spa_commander as spa_commander_module
from kajenn_orchestra.orchestration import spa_worker as spa_worker_module
from kajenn_orchestra.orchestration import worker_connector as worker_connector_module

from .conftest import WORKER_NAME, XT_WorkerCommanderLane, wait_for

SECOND_WORKER_NAME = "standard_0002"
STORE_LOCK = "/commander/store/lock"


@pytest.fixture
async def second_worker_commander_lane(worker_commander_lane, tmp_path):
    """A second worker under the same commander: what a FIFO grant needs two of."""
    lane = XT_WorkerCommanderLane(
        worker_commander_lane.commander,
        worker_commander_lane.worker_handler.group_handler,
        FreezeHandler(tmp_path / "frozen_users_second"),
        worker_name=SECOND_WORKER_NAME,
    )
    await lane.open()
    yield lane
    await lane.close()


async def test_a_set_lands_on_the_master_before_it_answers(worker_commander_lane):
    # wf:contract: global_store.set(key, value) is a CALL on the lane; when it
    # wf:contract: returns, the commander's master already holds the value — no
    # wf:contract: replica anywhere, no waiting for any later push.
    await asyncio.to_thread(worker_commander_lane.worker.global_store.set, "gnr.a", 1)

    assert worker_commander_lane.commander.global_register["gnr.a"] == 1


async def test_a_delete_removes_the_key_rather_than_nulling_it(worker_commander_lane):
    # wf:contract: after delete returns, the master's key is GONE — not None.
    store = worker_commander_lane.worker.global_store
    await asyncio.to_thread(store.set, "gnr.a", 1)
    await asyncio.to_thread(store.set, "gnr.b", 2)

    await asyncio.to_thread(store.delete, "gnr.a")

    assert worker_commander_lane.commander.global_register == {"gnr.b": 2}


async def test_the_grant_carries_the_true_master_state(worker_commander_lane):
    # wf:contract: for_update's grant answers with the master's own value at
    # wf:contract: grant time — a worker that never saw any state reads the
    # wf:contract: current truth, no staleness question.
    worker_commander_lane.commander.global_register["gnr.a"] = 12

    async with worker_commander_lane.worker.global_store.for_update("gnr.a") as turn:
        assert turn.value == 12


async def test_the_release_publishes_the_complete_value(worker_commander_lane):
    # wf:contract: the value the body left on the lease lands on the master only
    # wf:contract: at the release, attributes of a Bag included; while the turn
    # wf:contract: is in force the master shows nothing of it.
    master = worker_commander_lane.commander.global_register
    master["gnr"] = Bag({"a": 12})

    async with worker_commander_lane.worker.global_store.for_update("gnr") as turn:
        turn.value.set_item("a", turn.value["a"] * 2, _attributes={"tag": "recount"})
        assert master["gnr"]["a"] == 12

    assert master["gnr"]["a"] == 24
    assert master["gnr"].get_attr("a") == {"tag": "recount"}


async def test_a_body_that_raises_applies_nothing(worker_commander_lane):
    # wf:contract: a turn body that raises releases with nothing applied — the
    # wf:contract: all-or-nothing of the lease.
    master = worker_commander_lane.commander.global_register
    master["gnr.a"] = 1

    with pytest.raises(RuntimeError, match="the site fell over"):
        async with worker_commander_lane.worker.global_store.for_update("gnr.a") as turn:
            turn.value = 99
            raise RuntimeError("the site fell over")

    assert master["gnr.a"] == 1
    # And the lock is back: the next turn is served.
    async with worker_commander_lane.worker.global_store.for_update("gnr.a") as turn:
        assert turn.value == 1


async def test_the_waiters_are_served_in_order_and_see_the_previous_release(
    worker_commander_lane, second_worker_commander_lane
):
    # wf:contract: a second holder's grant is taken from the master AFTER the
    # wf:contract: first holder's release published: FIFO, read-modify-write safe.
    master = worker_commander_lane.commander.global_register
    master["gnr.a"] = 1
    granted = asyncio.Event()
    release_now = asyncio.Event()
    second_read: list[int] = []

    async def first_holder() -> None:
        async with worker_commander_lane.worker.global_store.for_update("gnr.a") as turn:
            granted.set()
            turn.value += 10
            await release_now.wait()

    async def second_holder() -> None:
        async with second_worker_commander_lane.worker.global_store.for_update("gnr.a") as turn:
            second_read.append(turn.value)

    first = asyncio.create_task(first_holder())
    await granted.wait()
    second = asyncio.create_task(second_holder())

    # The second's call is on the wire and unanswered: it is parked on the lock,
    # which the first still holds.
    await wait_for(lambda: bool(second_worker_commander_lane.worker._parent_calls))
    assert second_read == []

    release_now.set()
    await asyncio.gather(first, second)

    assert second_read == [11]
    assert master["gnr.a"] == 11


async def test_a_dead_holders_lock_is_released_with_the_master_untouched(
    worker_commander_lane, second_worker_commander_lane
):
    # wf:contract: the holder's channel ending releases the lock without
    # wf:contract: publishing anything; the next waiter gets a clean grant.
    commander = worker_commander_lane.commander
    commander.global_register["gnr.a"] = 1
    await worker_commander_lane.worker.call(
        STORE_LOCK, {"worker": WORKER_NAME, "request_id": "hold-1", "key": "gnr.a"}
    )

    assert commander.global_lock.held_by(WORKER_NAME) is True

    worker_commander_lane.worker_handler.on_child_lost()

    assert commander.global_lock.holder is None
    assert commander.global_register["gnr.a"] == 1
    async with second_worker_commander_lane.worker.global_store.for_update("gnr.a") as turn:
        assert turn.value == 1


async def test_the_replica_machinery_is_gone(worker_commander_lane):
    # wf:contract: SpaWorker holds no global replica and no queued writes; no
    # wf:contract: envelope slot carries the global store in either direction;
    # wf:contract: old_value exists nowhere. ``global_store`` is the CLIENT now
    # wf:contract: (#74), no longer the name of the phase-5 replica.
    for gone in ("GLOBAL_STORE_KEY", "GLOBAL_WRITES_KEY", "ENVELOPE_SLOT_GLOBAL_STORE"):
        assert not hasattr(worker_connector_module, gone)
    for gone in (
        "global_replica",
        "global_register_item_tytx",
        "record_global_write",
        "_global_writes",
        "_take_global_store",
    ):
        assert not hasattr(worker_commander_lane.worker, gone)
    assert isinstance(SpaWorker.global_store, property)
    assert not hasattr(SpaCommander, "apply_global_writes")

    written = "".join(
        Path(module.__file__).read_text()
        for module in (spa_worker_module, spa_commander_module, worker_connector_module)
    )
    assert "old_value" not in written
    assert "global_replica" not in written
