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

"""The observation stream: register mutations pushed worker → commander → watcher."""

from __future__ import annotations

import asyncio


from .conftest import wait_for


async def test_nobody_watching_leaves_the_worker_silent(worker_commander_lane):
    assert worker_commander_lane.worker.observation_on is False
    assert worker_commander_lane.commander.observation_watched is False

    await worker_commander_lane.verb("add_connection", "a1b2")

    assert worker_commander_lane.worker.observation_on is False


async def test_a_watcher_switches_the_worker_on_and_hears_a_birth(worker_commander_lane):
    queue: asyncio.Queue = asyncio.Queue()
    await worker_commander_lane.commander.subscribe_observation(queue)
    assert worker_commander_lane.worker.observation_on is True

    await worker_commander_lane.verb("add_connection", "a1b2")
    await wait_for(lambda: queue.qsize() >= 2)
    await asyncio.sleep(0)

    # The vertex reports the same births when it folds the announcement; what
    # this test listens for is the worker's own report.
    heard = [queue.get_nowait() for _ in range(queue.qsize())]
    events = {event["kind"]: event for event in heard if event["source"] == "standard_0001"}
    assert set(events) == {"new_user", "new_connection"}
    assert events["new_connection"]["data"]["user"].startswith("guest_")


async def test_the_last_watcher_leaving_switches_the_worker_off(worker_commander_lane):
    queue: asyncio.Queue = asyncio.Queue()
    other: asyncio.Queue = asyncio.Queue()
    await worker_commander_lane.commander.subscribe_observation(queue)
    await worker_commander_lane.commander.subscribe_observation(other)

    await worker_commander_lane.commander.unsubscribe_observation(queue)
    assert worker_commander_lane.worker.observation_on is True

    await worker_commander_lane.commander.unsubscribe_observation(other)
    assert worker_commander_lane.worker.observation_on is False
    assert worker_commander_lane.commander.observation_watched is False


async def test_the_fold_at_the_vertex_is_published_too(worker_commander_lane):
    queue: asyncio.Queue = asyncio.Queue()
    await worker_commander_lane.commander.subscribe_observation(queue)

    worker_commander_lane.worker_handler.read_envelope(
        {"worker_events": [{"op": "new_user", "worker": "standard_0001", "user": "u1"}]}
    )

    event = await queue.get()
    assert event["kind"] == "new_user"
    assert event["source"] == "commander"
