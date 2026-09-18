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

"""GroupHandler holds a policy: the delegation, the swap, the snapshot, the checkpoint.

The stage is the one ``test_orchestration_group_handler`` builds — real child
processes under a real group and a real vertex. Implementation tests: they
photograph how the setpoints reach the decisions, and go with that.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from kajenn_orchestra.orchestration import AssignmentRefused
from kajenn_orchestra.orchestration.group_handler import GroupHandler
from kajenn_orchestra.orchestration.group_policy import GroupPolicy

from .test_orchestration_group_handler import WORKER_CEILING, known_at_the_vertex
from .test_orchestration_group_handler import commander  # noqa: F401
from .test_orchestration_group_handler import group_settings  # noqa: F401
from .test_orchestration_group_handler import instance_root  # noqa: F401
from .test_orchestration_group_handler import make_group  # noqa: F401

ORDERS_LOGGER = "kajenn.orchestration.orders"

#: The names the delegation must keep: every setpoint a decision reads.
SETPOINT_NAMES = tuple(GroupPolicy.SETPOINTS)

#: The decisions that cross an await and must therefore work off a snapshot.
SNAPSHOT_DECISIONS = (
    "assign_user",
    "check_occupancy",
    "check_user_activity",
    "_spare_worker",
)


def decision_source(name: str) -> str:
    """The body of one decision, past the decorator and the docstring."""
    method = getattr(GroupHandler, name)
    return inspect.getsource(inspect.unwrap(method))


async def test_setpoint_attributes_delegate_to_policy(make_group):
    group = make_group()
    assert group.worker_memory_admission_percent == 80.0
    assert group.worker_max_users == float("inf")

    new_policy = GroupPolicy.from_settings(
        {
            "worker_memory_admission_percent": 70.0,
            "restart_occupancy_max_percent": 90.0,
            "cpu_close_percent": 25.0,
            "cpu_admission_close_percent": 55.0,
            "cpu_admission_reopen_percent": 30.0,
            "worker_min_life_seconds": 12.0,
            "worker_max_users": 9,
            "user_idle_freeze_minutes": 45.0,
            "memory_max_percent": 60.0,
            "worker_max_number": 4,
            "worker_memory_max_percent": 33.0,
        }
    )
    group.apply_policy(new_policy, [])

    assert group.policy is new_policy
    for name in SETPOINT_NAMES:
        assert getattr(group, name) == getattr(new_policy, name), name
    # And nothing but the setpoints moved: the group is where it was.
    assert group.worker_handler_map == {}
    assert group.state == "running"


async def test_the_derived_worker_share_follows_the_swap(make_group):
    group = make_group()
    group.apply_policy(GroupPolicy.from_settings({"worker_max_number": 5}), [])

    assert group.worker_memory_max_percent == 100.0 / 5


async def test_apply_policy_is_synchronous_assignments_only(make_group, caplog):
    group = make_group()
    worker_handler = await group.start_worker()
    worker_handler.cpu_admission_open = False
    new_policy = GroupPolicy.from_settings({"cpu_admission_close_percent": 70.0})

    assert not inspect.iscoroutinefunction(GroupHandler.apply_policy)
    body = decision_source("apply_policy")
    assert "await " not in body
    assert "log_order" not in body
    assert "start_worker" not in body

    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        caplog.clear()
        group.apply_policy(new_policy, [(worker_handler.name, True)])

    assert group.policy is new_policy
    assert worker_handler.cpu_admission_open is True
    assert [record for record in caplog.records if record.name == ORDERS_LOGGER] == []
    assert list(group.worker_handler_map) == [worker_handler.name]


async def test_decision_binds_policy_snapshot(make_group):
    # Structural: every decision that crosses an await reads its setpoints off a
    # local bound at the top, so one swap can never split a single decision.
    for name in SNAPSHOT_DECISIONS:
        body = decision_source(name)
        if name != "_spare_worker":
            assert "policy = self.policy" in body, name
        for setpoint in SETPOINT_NAMES:
            assert f"self.{setpoint}" not in body, f"{name} reads self.{setpoint}"

    # Behavioural: the one decision handed its policy judges by THAT policy and
    # not by the group's current one.
    group = make_group()
    first = await group.start_worker()
    second = await group.start_worker()
    first.get_cpu_temperature_percent = lambda: 30.0
    second.get_cpu_temperature_percent = lambda: 30.0
    group.apply_policy(
        GroupPolicy.from_settings({"worker_min_life_seconds": 0.0}),
        [],
    )
    cold = GroupPolicy.from_settings(
        {
            "worker_min_life_seconds": 0.0,
            "cpu_close_percent": 79.0,
        }
    )
    hot = GroupPolicy.from_settings(
        {
            "worker_min_life_seconds": 0.0,
            "cpu_close_percent": 10.0,
        }
    )

    assert group._spare_worker(cold) is second
    assert group._spare_worker(hot) is None


async def test_checkpoint_suppresses_effect_after_swap(make_group, commander, caplog):
    # A user nobody admits: his placement would father a worker. The lock is
    # held, so the birth waits — and the policy is swapped while it waits.
    group = make_group(rss_bytes=int(0.79 * WORKER_CEILING))
    await group.start_worker()
    known_at_the_vertex(commander, "cid-a", "mario")
    await group._placement_lock.acquire()
    placement = asyncio.create_task(group.assign_user("mario"))
    for _ in range(5):
        await asyncio.sleep(0)

    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        group.apply_policy(GroupPolicy.from_settings({}), [])
        group._placement_lock.release()
        with pytest.raises(AssignmentRefused):
            await placement

    assert len(group.worker_handler_map) == 1  # no birth
    assert "suppressed: policy changed while deciding" in caplog.text
