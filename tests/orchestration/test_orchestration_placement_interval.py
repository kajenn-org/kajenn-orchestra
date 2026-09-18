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

"""The placement walks the open workers hottest-first, spaced by the admission interval.

Implementation tests. A worker that just admitted somebody is skipped until
``worker_admission_interval_seconds`` elapsed; when every open worker is in
its window the hottest that admits takes the user anyway, and the journal says
which rule decided.
"""

from __future__ import annotations

import json

from .test_orchestration_cpu_growth import DECISIONS_LOGGER, arrival, declare_cpu
from .test_orchestration_group_handler import WORKER_CEILING
from .test_orchestration_group_handler import commander  # noqa: F401
from .test_orchestration_group_handler import group_settings  # noqa: F401
from .test_orchestration_group_handler import instance_root  # noqa: F401
from .test_orchestration_group_handler import make_group  # noqa: F401


def placements(caplog) -> list[dict]:
    return [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == DECISIONS_LOGGER
        and json.loads(record.getMessage())["decision"] == "placement"
    ]


async def two_open_workers(make_group, **policies):
    group = make_group(cpu_admission_close_percent=50.0, **policies)
    first = await group.start_worker()
    second = await group.start_worker()
    declare_cpu(first, 20.0)
    declare_cpu(second, 40.0)
    return group, first, second


async def test_the_hottest_open_worker_takes_the_newcomer(make_group, commander):
    group, first, second = await two_open_workers(make_group)
    assert await arrival(commander, group, "a") == second.name


async def test_a_worker_that_just_admitted_is_skipped_for_the_interval(
    make_group, commander, caplog
):
    group, first, second = await two_open_workers(make_group)
    assert await arrival(commander, group, "a") == second.name
    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        assert await arrival(commander, group, "b") == first.name
    row = placements(caplog)[-1]
    assert row["reason"] == "hottest_cpu_open_candidate"
    skipped = [c for c in row["candidates"] if c["name"] == second.name][0]
    assert skipped["recently_admitted"] is True
    assert skipped["skipped"] == "worker_recently_admitted"


async def test_when_every_open_worker_is_in_its_window_the_hottest_admits(
    make_group, commander, caplog
):
    group, first, second = await two_open_workers(make_group)
    await arrival(commander, group, "a")
    await arrival(commander, group, "b")
    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        assert await arrival(commander, group, "c") == second.name
    row = placements(caplog)[-1]
    assert row["reason"] == "admission_interval_waived"
    winner = [c for c in row["candidates"] if c["name"] == second.name][0]
    assert "skipped" not in winner  # the one that took him is not marked skipped
    assert len(group.worker_handler_map) == 2  # the interval never births a worker


async def test_an_interval_of_zero_switches_the_rule_off(make_group, commander):
    group, first, second = await two_open_workers(
        make_group, worker_admission_interval_seconds=0.0
    )
    homes = [await arrival(commander, group, f"u{i}") for i in range(3)]
    assert homes == [second.name] * 3


async def test_a_memory_full_worker_is_journaled_as_such(make_group, commander, caplog):
    group, first, second = await two_open_workers(make_group)
    second.worker_snapshot = {"rss_bytes": int(0.9 * WORKER_CEILING)}
    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        assert await arrival(commander, group, "a") == first.name
    row = placements(caplog)[-1]
    full = [c for c in row["candidates"] if c["name"] == second.name][0]
    assert full["skipped"] == "worker_memory_full"
    assert "of memory" in full["refusal"]


async def test_a_worker_at_its_head_count_is_journaled_as_such(make_group, commander, caplog):
    # The interval is off, so the refusal the journal sees is the head count alone.
    group, first, second = await two_open_workers(
        make_group, worker_max_users=1, worker_admission_interval_seconds=0.0
    )
    await arrival(commander, group, "a")  # on the hotter one, now full by heads
    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        assert await arrival(commander, group, "b") == first.name
    row = placements(caplog)[-1]
    full = [c for c in row["candidates"] if c["name"] == second.name][0]
    assert full["skipped"] == "worker_max_users_reached"


async def test_one_worker_in_its_window_and_one_full_waives_the_interval(
    make_group, commander, caplog
):
    # The interval never births a worker: with the cold one full by memory and
    # the hot one inside its window, the hot one takes him and the journal says
    # the interval was waived, not that everybody was recently admitted.
    group, first, second = await two_open_workers(make_group)
    first.worker_snapshot = {"rss_bytes": int(0.9 * WORKER_CEILING)}
    await arrival(commander, group, "a")
    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        assert await arrival(commander, group, "b") == second.name
    assert placements(caplog)[-1]["reason"] == "admission_interval_waived"
    assert len(group.worker_handler_map) == 2
