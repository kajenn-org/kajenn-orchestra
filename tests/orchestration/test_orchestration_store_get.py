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

"""global_store.get: one value per read, a CALL on the lane, never a stale copy.

The read side of the global store. The store lives on the commander alone, so
a read pays its round trip — the owner's own call (2026-08-21): a sub-commander
topology guarantees no shared disk, and the lane works on any of them. Since
#74 the store is a dictionary: a key is literal, and a Bag stored under it
comes back whole.
"""

from __future__ import annotations

import asyncio
import datetime

from genro_bag import Bag


async def test_a_read_answers_the_masters_current_value(worker_commander_lane):
    store = worker_commander_lane.worker.global_store
    await asyncio.to_thread(store.set, "gnr.a", 1)

    assert await asyncio.to_thread(store.get, "gnr.a") == 1

    worker_commander_lane.commander.global_register["gnr.a"] = 2
    assert await asyncio.to_thread(store.get, "gnr.a") == 2


async def test_a_key_the_store_does_not_hold_answers_the_default(worker_commander_lane):
    store = worker_commander_lane.worker.global_store
    assert await asyncio.to_thread(store.get, "gnr.missing") is None
    assert await asyncio.to_thread(store.get, "gnr.missing", 0) == 0


async def test_typed_values_travel_whole(worker_commander_lane):
    stamp = datetime.datetime(2026, 8, 21, 10, 0, tzinfo=datetime.timezone.utc)
    worker_commander_lane.commander.global_register["RESTART_TS"] = stamp

    assert await asyncio.to_thread(worker_commander_lane.worker.global_store.get, "RESTART_TS") == stamp


async def test_a_bag_value_comes_back_as_a_bag(worker_commander_lane):
    worker_commander_lane.commander.global_register["counters"] = Bag({"a": 1, "b": 2})

    value = await asyncio.to_thread(worker_commander_lane.worker.global_store.get, "counters")

    assert isinstance(value, Bag)
    assert value["a"] == 1 and value["b"] == 2
