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

"""The debug door: one eval, any process of the pool, over the lane there is."""

from __future__ import annotations

import pytest

from kajenn_orchestra.spa_console import SpaConsole, SpaConsoleMcpApplication
from kajenn_orchestra.orchestration.spa_worker import GUEST_PREFIX


async def test_the_commander_answers_about_itself(worker_commander_lane):
    worker_commander_lane.commander.record_connection_user("cid-a", "guest_legacy1")

    assert (
        await worker_commander_lane.commander.eval_in_target("commander", "len(commander.user_map)")
        == "1"
    )


async def test_a_worker_answers_over_the_lane(worker_commander_lane):
    worker_commander_lane.worker.add_connection("a1b2")
    worker_commander_lane.worker.add_page("page-0", "a1b2")

    connections = await worker_commander_lane.commander.eval_in_target(
        "standard_0001", "len(worker.connection_register)"
    )
    pages = await worker_commander_lane.commander.eval_in_target(
        "standard_0001", "sorted(worker.page_register.keys())"
    )

    assert connections == "1"
    assert pages == "['page-0']"


async def test_an_expression_that_fails_in_the_child_travels_back_as_the_error(
    worker_commander_lane,
):
    with pytest.raises(RuntimeError, match="NameError"):
        await worker_commander_lane.commander.eval_in_target("standard_0001", "no_such_name")


async def test_an_unknown_target_names_the_ones_there_are(worker_commander_lane):
    with pytest.raises(KeyError, match="commander, standard_0001"):
        await worker_commander_lane.commander.eval_in_target("standard_9999", "1")


class XT_Server:
    """The one attribute of a server the console reads: its applications."""

    def __init__(self, applications):
        self.applications = applications


async def test_the_tools_reach_the_front_and_list_the_targets(worker_commander_lane):
    from kajenn_orchestra.spa_app import SpaApplication

    spa_front = SpaApplication.__new__(SpaApplication)
    spa_front._commander = worker_commander_lane.commander
    console_app = SpaConsoleMcpApplication(code="console")
    console_app.server = XT_Server({"shop": spa_front, "other": object()})
    console = SpaConsole(console_app)

    worker_commander_lane.worker.add_connection("a1b2")
    guest = f"{GUEST_PREFIX}a1b2"

    assert await console.targets() == {"shop": ["commander", "standard_0001"]}

    answer = await console.eval("sorted(worker.user_register.keys())", "standard_0001")
    assert answer == {"target": "standard_0001", "repr": f"['{guest}']"}
