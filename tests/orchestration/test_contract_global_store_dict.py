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

"""The global store is one dictionary on the commander, behind one FIFO lock.

Contract of issue #74 as fixed by the owner on 2026-09-07
(``temp/GLOBAL_STORE_DICT_PLAN_2026-09-07.md``, v1.1):

- the commander owns ``dict[str, Any]``; values are opaque, keys are literal;
- ``get``, ``set``, ``delete`` and a ``for_update`` turn share ONE FIFO lock,
  so every operation waits while a turn is in force, reads included;
- the ``get`` reply carries ``exists`` and ``value``: the client answers the
  caller's default when the key is absent, the stored value — ``None``
  included — when it is there;
- a keyed turn transfers one value and replaces only that key at the release;
  a turn with no key transfers the whole dictionary and replaces it whole;
- the release carries the complete replacement (``apply=True``) or aborts
  (``apply=False``); a release for a turn no longer in force does nothing;
- a holder that dies releases only its own turn, master untouched.

Written BEFORE the implementation (Step 1 of the plan): red until Step 2.
"""

from __future__ import annotations

import asyncio
from datetime import datetime

import pytest
from genro_bag import Bag
from genro_tytx import to_tytx

from kajenn_orchestra.global_store import GlobalStoreCommitUnconfirmed
from kajenn_orchestra.orchestration import FreezeHandler
from kajenn_orchestra.orchestration.worker_connector import CommanderCallFailed

from .conftest import WORKER_NAME, XT_WorkerCommanderLane, wait_for

SECOND_WORKER_NAME = "standard_0002"
STORE_GET = "/commander/store/get"
STORE_LOCK = "/commander/store/lock"
STORE_UNLOCK = "/commander/store/unlock"


@pytest.fixture
async def second_worker_commander_lane(worker_commander_lane, tmp_path):
    lane = XT_WorkerCommanderLane(
        worker_commander_lane.commander,
        worker_commander_lane.worker_handler.group_handler,
        FreezeHandler(tmp_path / "frozen_users_second"),
        worker_name=SECOND_WORKER_NAME,
    )
    await lane.open()
    yield lane
    await lane.close()


# --- the dictionary and the three simple operations ---------------------------


async def test_the_master_is_a_dictionary_with_literal_keys(worker_commander_lane):
    # wf:contract: the commander's global_register is a dict; a dotted key is
    # wf:contract: one literal key, never a path.
    store = worker_commander_lane.worker.global_store
    master = worker_commander_lane.commander.global_register

    await asyncio.to_thread(store.set, "a.b", 1)

    assert isinstance(master, dict)
    assert master == {"a.b": 1}


async def test_get_of_an_absent_key_answers_the_callers_default(worker_commander_lane):
    # wf:contract: exists=False on the wire → the client returns the default,
    # wf:contract: which never travels to the commander.
    store = worker_commander_lane.worker.global_store

    reply = await worker_commander_lane.worker.call(STORE_GET, {"key": "missing"})
    assert reply["exists"] is False

    assert await asyncio.to_thread(store.get, "missing", default="fallback") == "fallback"


async def test_get_of_a_key_holding_none_answers_none_not_the_default(worker_commander_lane):
    # wf:contract: exists=True on the wire → the client returns the stored
    # wf:contract: value even when it is None; the default is not consulted.
    store = worker_commander_lane.worker.global_store
    await asyncio.to_thread(store.set, "empty", None)

    reply = await worker_commander_lane.worker.call(STORE_GET, {"key": "empty"})
    assert reply["exists"] is True

    assert await asyncio.to_thread(store.get, "empty", default="fallback") is None


async def test_delete_removes_the_key_and_is_idempotent(worker_commander_lane):
    store = worker_commander_lane.worker.global_store
    master = worker_commander_lane.commander.global_register
    await asyncio.to_thread(store.set, "a", 1)

    await asyncio.to_thread(store.delete, "a")
    await asyncio.to_thread(store.delete, "a")

    assert "a" not in master


async def test_values_keep_their_types_across_the_wire(worker_commander_lane):
    # wf:contract: a Bag with attributes and a datetime travel as values of the
    # wf:contract: dictionary under the existing TYTX conventions; the reader
    # wf:contract: gets a copy, never the master's own object.
    store = worker_commander_lane.worker.global_store
    master = worker_commander_lane.commander.global_register
    bag = Bag()
    bag.set_item("x", datetime(2026, 1, 1, 12), _attributes={"tag": "t"})
    bag.set_item("empty", Bag())

    await asyncio.to_thread(store.set, "CACHE_TS", bag)
    read = await asyncio.to_thread(store.get, "CACHE_TS")

    assert isinstance(read, Bag)
    assert read["x"].replace(tzinfo=None) == datetime(2026, 1, 1, 12)
    assert read.get_attr("x") == {"tag": "t"}
    assert isinstance(read["empty"], Bag) and len(read["empty"]) == 0
    read.set_item("x", 0)
    assert master["CACHE_TS"]["x"] != 0


# --- the turn ------------------------------------------------------------------


async def test_a_keyed_turn_replaces_only_its_key(worker_commander_lane):
    # wf:contract: for_update(key) yields a lease whose value is a private copy;
    # wf:contract: the master shows nothing until the exit, then only that key
    # wf:contract: changes.
    store = worker_commander_lane.worker.global_store
    master = worker_commander_lane.commander.global_register
    master["config"] = Bag({"enabled": False})
    master["other"] = 1

    async with store.for_update("config") as turn:
        assert turn.exists is True
        turn.value["enabled"] = True
        assert master["config"]["enabled"] is False

    assert master["config"]["enabled"] is True
    assert master["other"] == 1


async def test_an_absent_key_starts_the_turn_with_exists_false_and_value_none(
    worker_commander_lane,
):
    # wf:contract: no Bag is invented by the commander; the consumer decides.
    store = worker_commander_lane.worker.global_store
    master = worker_commander_lane.commander.global_register

    async with store.for_update("counter") as turn:
        assert turn.exists is False and turn.value is None
        turn.value = (turn.value if turn.exists else 0) + 1

    assert master["counter"] == 1


async def test_a_turn_may_publish_none(worker_commander_lane):
    store = worker_commander_lane.worker.global_store
    master = worker_commander_lane.commander.global_register
    master["k"] = 1

    async with store.for_update("k") as turn:
        turn.value = None

    assert "k" in master and master["k"] is None


async def test_a_turn_with_no_key_replaces_the_whole_dictionary(worker_commander_lane):
    # wf:contract: the legacy `with globalStore()` shape: the value is the
    # wf:contract: dictionary snapshot, edits and deletions land atomically.
    store = worker_commander_lane.worker.global_store
    master = worker_commander_lane.commander.global_register
    master.update({"a": 1, "b": 2})

    async with store.for_update() as turn:
        turn.value["a"] = 10
        del turn.value["b"]
        turn.value["c"] = 3
        assert master == {"a": 1, "b": 2}

    assert master == {"a": 10, "c": 3}


async def test_a_body_that_raises_aborts_and_creates_nothing(worker_commander_lane):
    store = worker_commander_lane.worker.global_store
    master = worker_commander_lane.commander.global_register

    with pytest.raises(RuntimeError, match="fell over"):
        async with store.for_update("fresh") as turn:
            turn.value = 99
            raise RuntimeError("the site fell over")

    assert "fresh" not in master
    assert worker_commander_lane.commander.global_lock.holder is None


async def test_an_aborted_turn_publishes_nothing_and_frees_the_lock_at_exit(
    worker_commander_lane,
):
    # wf:contract: turn.abort() marks the turn; the lock is still held until the
    # wf:contract: with exits, then the release carries apply=False — whatever
    # wf:contract: the body did to value after the call. An absent key stays absent.
    store = worker_commander_lane.worker.global_store
    commander = worker_commander_lane.commander

    async with store.for_update("CACHE_TS") as turn:
        assert turn.exists is False
        turn.abort()
        assert commander.global_lock.holder == turn.request_id
        turn.value = {"foo": 1}

    assert "CACHE_TS" not in commander.global_register
    assert commander.global_lock.holder is None
    assert not commander.global_lock.lock.locked()


async def test_the_sync_lease_works_from_a_pool_thread(worker_commander_lane):
    store = worker_commander_lane.worker.global_store
    master = worker_commander_lane.commander.global_register

    def body() -> None:
        with store.for_update("n") as turn:
            turn.value = 7

    await asyncio.to_thread(body)
    assert master["n"] == 7


# --- one FIFO lock for everything ---------------------------------------------


async def test_a_read_of_any_key_waits_while_a_turn_is_in_force(
    worker_commander_lane, second_worker_commander_lane
):
    # wf:contract: a get during a turn is parked on the same FIFO lock, even on
    # wf:contract: an unrelated key, and answers after the release.
    master = worker_commander_lane.commander.global_register
    master.update({"held": 1, "unrelated": 1})
    granted = asyncio.Event()
    release_now = asyncio.Event()
    seen: list[dict] = []

    async def holder() -> None:
        async with worker_commander_lane.worker.global_store.for_update("held") as turn:
            granted.set()
            turn.value = 2
            await release_now.wait()

    async def reader() -> None:
        seen.append(await second_worker_commander_lane.worker.call(STORE_GET, {"key": "unrelated"}))

    first = asyncio.create_task(holder())
    await granted.wait()
    second = asyncio.create_task(reader())
    await wait_for(lambda: bool(second_worker_commander_lane.worker._parent_calls))
    assert seen == []

    release_now.set()
    await asyncio.gather(first, second)
    assert seen[0]["exists"] is True and master["held"] == 2


async def test_waiting_turns_are_served_in_order_and_see_the_previous_release(
    worker_commander_lane, second_worker_commander_lane
):
    master = worker_commander_lane.commander.global_register
    master["n"] = 1
    granted = asyncio.Event()
    release_now = asyncio.Event()
    second_saw: list[int] = []

    async def first_holder() -> None:
        async with worker_commander_lane.worker.global_store.for_update("n") as turn:
            granted.set()
            turn.value += 10
            await release_now.wait()

    async def second_holder() -> None:
        async with second_worker_commander_lane.worker.global_store.for_update("n") as turn:
            second_saw.append(turn.value)

    first = asyncio.create_task(first_holder())
    await granted.wait()
    second = asyncio.create_task(second_holder())
    await wait_for(lambda: bool(second_worker_commander_lane.worker._parent_calls))
    release_now.set()
    await asyncio.gather(first, second)

    assert second_saw == [11]


# --- releases out of turn, deaths ----------------------------------------------


async def test_a_stale_release_neither_writes_nor_frees_a_newer_turn(worker_commander_lane):
    # wf:contract: an unlock quoting a request id that is not the holder's does
    # wf:contract: nothing: the newer turn stays in force, the master is untouched.
    worker = worker_commander_lane.worker
    commander = worker_commander_lane.commander
    commander.global_register["k"] = 1
    await worker.call(STORE_LOCK, {"worker": WORKER_NAME, "request_id": "current", "key": "k"})

    reply = await worker.call(
        STORE_UNLOCK, {"request_id": "stale", "apply": True, "value": to_tytx(99, "json")}
    )

    assert reply == {"applied": False}
    assert commander.global_lock.holds("current") is True
    assert commander.global_register["k"] == 1
    await worker.call(STORE_UNLOCK, {"request_id": "current", "apply": False})
    assert commander.global_lock.holder is None


async def test_a_dead_holder_frees_only_its_turn_with_the_master_untouched(
    worker_commander_lane, second_worker_commander_lane
):
    commander = worker_commander_lane.commander
    commander.global_register["k"] = 1
    await worker_commander_lane.worker.call(
        STORE_LOCK, {"worker": WORKER_NAME, "request_id": "r1", "key": "k"}
    )
    assert commander.global_lock.held_by(WORKER_NAME) is True

    worker_commander_lane.worker_handler.on_child_lost()

    assert commander.global_lock.holder is None
    assert commander.global_register["k"] == 1
    async with second_worker_commander_lane.worker.global_store.for_update("k") as turn:
        assert turn.value == 1


async def test_only_string_keys_are_accepted(worker_commander_lane):
    # wf:contract: key=None on the wire means the whole dictionary in a turn and
    # wf:contract: nothing else; any other non-string key is refused by the commander.
    worker = worker_commander_lane.worker
    with pytest.raises(CommanderCallFailed, match="TypeError"):
        await worker.call(STORE_GET, {"key": 3})
    with pytest.raises(CommanderCallFailed, match="TypeError"):
        await worker.call(STORE_LOCK, {"worker": WORKER_NAME, "request_id": "r", "key": 3})
    assert worker_commander_lane.commander.global_lock.holder is None


async def test_a_commit_whose_answer_is_lost_is_reported_uncertain(worker_commander_lane):
    # wf:contract: the commander publishes the value, the wire ends before the
    # wf:contract: REPLY arrives: the lease raises GlobalStoreCommitUnconfirmed,
    # wf:contract: retries nothing, and the local turn state is cleared.
    worker = worker_commander_lane.worker
    commander = worker_commander_lane.commander
    commander.global_register["k"] = 1
    real_release = commander.global_lock.release

    def release_then_lose_the_wire() -> None:
        # The value is already published when the commander releases; the REPLY
        # has not left yet. Closing the worker's socket here is the lost answer.
        real_release()
        worker.stream.writer.close()

    commander.global_lock.release = release_then_lose_the_wire
    try:
        with pytest.raises(GlobalStoreCommitUnconfirmed) as report:
            async with worker.global_store.for_update("k") as turn:
                turn.value = 2
    finally:
        commander.global_lock.release = real_release

    assert commander.global_register["k"] == 2
    assert report.value.key == "k" and isinstance(report.value.cause, ConnectionError)
    assert worker.global_store.active_turn.get() is None
    assert commander.global_lock.holder is None


async def test_the_change_batch_machinery_is_gone():
    # wf:contract: no capturing store, no applied changes, no datachange import.
    from pathlib import Path

    from kajenn_orchestra import global_store as global_store_module
    from kajenn_orchestra.orchestration import SpaCommander, spa_commander

    assert not hasattr(global_store_module, "CapturingGlobalStore")
    assert not hasattr(SpaCommander, "apply_global_store_changes")
    written = Path(global_store_module.__file__).read_text() + Path(spa_commander.__file__).read_text()
    assert "datachange" not in written
