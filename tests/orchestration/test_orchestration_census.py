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

"""The census: the whole pool read out as JSON, for a human to look at."""

from __future__ import annotations

import json

import pytest


class XT_MuteWorkerHandler:
    """A worker handler whose lane answers nothing: the census must survive it."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.state = "running"
        self.worker_snapshot = None
        self.cpu_temperature_percent = None
        self.cpu_temperature_sample_percent = None
        self.cpu_temperature_interval_seconds = None
        self.cpu_temperature_sampled_at = None
        self.connector = self

    def get_cpu_temperature_percent(self):
        return None

    async def call(self, path: str, data: dict) -> dict:
        raise ConnectionError("the wire is gone")


@pytest.fixture
async def populated_worker_commander_lane(worker_commander_lane):
    """One user with one connection and one page, both sides knowing about him."""
    worker_commander_lane.worker.add_connection("a1b2")
    worker_commander_lane.worker.add_page("page-0", "a1b2")
    worker_commander_lane.commander.record_connection_user("a1b2", "guest_a1b2")
    return worker_commander_lane


async def test_the_worker_census_holds_its_three_registers(populated_worker_commander_lane):
    census = populated_worker_commander_lane.worker.census()

    assert census["name"] == "standard_0001"
    assert "guest_a1b2" in census["user_register"]
    assert "a1b2" in census["connection_register"]
    assert "page-0" in census["page_register"]


async def test_the_pool_census_carries_the_user_on_both_sides(populated_worker_commander_lane):
    census = await populated_worker_commander_lane.commander.get_pool_census()

    assert "guest_a1b2" in census["user_map"]
    assert census["connection_user_map"] == {"a1b2": "guest_a1b2"}
    assert census["default_group"] == "standard"
    worker_census = census["workers"]["standard_0001"]
    assert "guest_a1b2" in worker_census["user_register"]


async def test_the_pool_census_declares_how_worker_memory_was_accounted(
    populated_worker_commander_lane,
):
    group = populated_worker_commander_lane.commander.group_map["standard"]
    worker = populated_worker_commander_lane.worker_handler
    worker.worker_snapshot = {"rss_bytes": 900, "pss_bytes": 300}

    census = await populated_worker_commander_lane.commander.get_pool_census()

    summary = census["groups"]["standard"]
    assert summary["memory_accounting"] == "pss"
    worker_summary = summary["workers"][worker.name]
    assert worker_summary["rss_bytes"] == 900
    assert worker_summary["pss_bytes"] == 300
    assert worker_summary["accounted_memory_bytes"] == 300.0
    assert worker_summary["memory_accounting"] == "pss"
    assert worker_summary["memory_occupancy_percent"] == pytest.approx(
        100.0 * 300 / group.worker_memory_ceiling_bytes
    )
    assert "occupancy_percent" not in worker_summary
    assert "worker_cap" not in worker_summary
    assert "cpu_temperature_sample_percent" in worker_summary


async def test_the_whole_census_is_json(populated_worker_commander_lane):
    census = await populated_worker_commander_lane.commander.get_pool_census()

    assert json.loads(json.dumps(census))["workers"]["standard_0001"]["name"] == "standard_0001"


async def test_a_worker_that_does_not_answer_is_an_error_entry(populated_worker_commander_lane):
    group = populated_worker_commander_lane.commander.group_map["standard"]
    group.worker_handler_map["standard_0002"] = XT_MuteWorkerHandler("standard_0002")

    census = await populated_worker_commander_lane.commander.get_pool_census()

    assert "the wire is gone" in census["workers"]["standard_0002"]["error"]
    assert "guest_a1b2" in census["workers"]["standard_0001"]["user_register"]
