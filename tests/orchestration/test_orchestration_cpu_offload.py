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

"""The CPU offload judge: one hot worker slims by one user per beat.

The stage is the one ``test_orchestration_group_handler`` builds — real child
processes under a real group and a real vertex — and both the CPU and the
per-user recent work are DECLARED, written straight into the photo the judge
reads. Implementation tests: they photograph the experimental policy and go
with it. A CPU-closed worker past ``cpu_offload_percent`` cedes its least
busy active user through the ordered freeze; a single active user is never
transferred; the standing conditions reach the journal once, not every beat.
"""

from __future__ import annotations

import json
import time

import pytest

from kajenn_orchestra.orchestration.group_policy import GroupPolicy, GroupPolicyError

from .test_orchestration_group_handler import known_at_the_vertex
from .test_orchestration_group_handler import commander  # noqa: F401
from .test_orchestration_group_handler import group_settings  # noqa: F401
from .test_orchestration_group_handler import instance_root  # noqa: F401
from .test_orchestration_group_handler import make_group  # noqa: F401

DECISIONS_LOGGER = "kajenn.orchestration.decisions"


def declare_cpu(worker_handler, cpu_temperature_percent: float) -> None:
    """Declare the CPU channel directly; policy tests do not test its clock."""
    worker_handler.cpu_temperature_percent = cpu_temperature_percent
    worker_handler.cpu_temperature_sampled_at = time.monotonic()
    worker_handler.cpu_temperature_interval_seconds = 0.1
    worker_handler.get_cpu_temperature_percent = lambda: cpu_temperature_percent


async def offload_group(make_group, commander, users, **policies):
    """One worker past the offload threshold, its users placed and declared.

    The scripted child carries the users as RESIDENTS (no transfer flag: a
    flag is read at the vertex as a hold), and every identity is known at the
    vertex before the child registers.
    """
    group = make_group(
        users=list(users),
        transfer_flag=None,
        cpu_admission_close_percent=50.0,
        cpu_offload_percent=75.0,
        **policies,
    )
    for user in users:
        known_at_the_vertex(commander, f"c_{user}", user)
    worker_handler = await group.start_worker()
    for user in users:
        assert await group.assign_user(user) == worker_handler.name
        worker_handler.hosted_users.add(user)
    worker_handler.cpu_admission_open = False
    declare_cpu(worker_handler, 80.0)
    return group, worker_handler


def declare_service(worker_handler, user, *, seconds=0.0, calls=0, pending=0):
    """Write one user's recent work into the photo the judge reads."""
    item = worker_handler.worker_snapshot["users"][user]["item"]
    item["recent_service_seconds"] = seconds
    item["recent_call_count"] = calls
    item["pending_call_count"] = pending


def offload_decisions(caplog):
    return [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == DECISIONS_LOGGER
        and json.loads(record.getMessage())["decision"] == "cpu_offload"
    ]


# --- who is ceded, and when nobody is ---------------------------------------


async def test_below_the_threshold_nobody_is_ceded(make_group, commander):
    group, worker_handler = await offload_group(make_group, commander, ["mario", "lucia"])
    declare_cpu(worker_handler, 60.0)
    declare_service(worker_handler, "mario", seconds=5.0, calls=3)
    declare_service(worker_handler, "lucia", seconds=3.0, calls=1)

    await group.check_cpu_offload()

    assert group.user_worker_map["mario"] == worker_handler.name
    assert group.user_worker_map["lucia"] == worker_handler.name


async def test_with_the_policy_off_the_judge_is_inert(make_group, commander, caplog):
    group, worker_handler = await offload_group(make_group, commander, ["mario", "lucia"])
    declare_service(worker_handler, "mario", seconds=5.0, calls=3)
    declare_service(worker_handler, "lucia", seconds=3.0, calls=1)
    group.apply_policy(
        GroupPolicy.from_settings(group.policy.to_settings() | {"cpu_offload_percent": None}),
        [],
    )

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_cpu_offload()

    assert group.user_worker_map["mario"] == worker_handler.name
    assert group.user_worker_map["lucia"] == worker_handler.name
    assert offload_decisions(caplog) == []


async def test_one_beat_cedes_the_least_busy_material_alone(make_group, commander, caplog):
    group, worker_handler = await offload_group(
        make_group, commander, ["mario", "lucia", "pia"]
    )
    declare_service(worker_handler, "mario", seconds=5.0, calls=9)
    declare_service(worker_handler, "lucia", seconds=1.5, calls=2)
    declare_service(worker_handler, "pia", seconds=2.0, calls=4)

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_cpu_offload()

    # S=8.5, N=3, threshold ~1.417: all three material, lucia the least busy.
    assert commander.user_is_frozen("lucia") is True
    assert group.user_worker_map["lucia"] is None
    assert group.user_worker_map["mario"] == worker_handler.name
    assert group.user_worker_map["pia"] == worker_handler.name
    rows = offload_decisions(caplog)
    reasons = [row["reason"] for row in rows]
    assert reasons == ["cpu_offload_threshold", "cpu_offload_user_selected", "cpu_offload_completed"]
    # Every row carries the numbers that rebuild the judgment.
    threshold_row = rows[0]
    assert threshold_row["numbers"]["cpu_temperature_percent"] == 80.0
    assert threshold_row["numbers"]["window_service_seconds"] == pytest.approx(8.5)
    assert threshold_row["numbers"]["active_users"] == 3
    assert threshold_row["numbers"]["material_threshold"] == pytest.approx(8.5 / 6)
    assert threshold_row["numbers"]["material_contributors"] == 3
    assert threshold_row["numbers"]["cedible_contributors"] == 3
    selected_row = rows[1]
    assert selected_row["numbers"]["recent_service_seconds"] == 1.5
    assert selected_row["numbers"]["recent_call_count"] == 2
    assert selected_row["numbers"]["pending_call_count"] == 0


async def test_a_call_in_flight_keeps_a_user_from_the_head(make_group, commander):
    """recent seconds 0 with a pending call is a call just begun, not idleness."""
    group, worker_handler = await offload_group(
        make_group, commander, ["mario", "lucia", "pia"]
    )
    declare_service(worker_handler, "mario", seconds=10.0, calls=9)
    declare_service(worker_handler, "lucia", seconds=0.0, calls=0, pending=1)
    declare_service(worker_handler, "pia", seconds=3.0, calls=4)

    await group.check_cpu_offload()

    assert commander.user_is_frozen("pia") is True
    assert group.user_worker_map["lucia"] == worker_handler.name


async def test_all_material_busy_defers_and_nobody_is_frozen(make_group, commander, caplog):
    group, worker_handler = await offload_group(make_group, commander, ["mario", "lucia"])
    declare_service(worker_handler, "mario", seconds=6.0, calls=5, pending=1)
    declare_service(worker_handler, "lucia", seconds=4.0, calls=3, pending=1)

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_cpu_offload()
        await group.check_cpu_offload()

    # Both material, both mid-call: no freeze at all, one deferred row (dedup).
    assert commander.user_is_frozen("mario") is False
    assert commander.user_is_frozen("lucia") is False
    assert group.user_worker_map["lucia"] == worker_handler.name
    rows = offload_decisions(caplog)
    assert [row["reason"] for row in rows] == ["cpu_offload_deferred_pending_calls"]
    assert rows[0]["numbers"]["material_contributors"] == 2
    assert rows[0]["numbers"]["cedible_contributors"] == 0


async def test_the_next_beat_cedes_the_contributor_that_came_free(make_group, commander):
    group, worker_handler = await offload_group(make_group, commander, ["mario", "lucia"])
    declare_service(worker_handler, "mario", seconds=6.0, calls=5, pending=1)
    declare_service(worker_handler, "lucia", seconds=4.0, calls=3, pending=1)
    await group.check_cpu_offload()
    assert commander.user_is_frozen("lucia") is False

    declare_service(worker_handler, "lucia", seconds=4.0, calls=3, pending=0)
    await group.check_cpu_offload()

    assert commander.user_is_frozen("lucia") is True
    assert group.user_worker_map["mario"] == worker_handler.name


async def test_the_inactive_user_is_no_candidate(make_group, commander, caplog):
    """Nothing recent and nothing in flight belongs to the idle freeze, not here."""
    group, worker_handler = await offload_group(make_group, commander, ["mario", "lucia"])
    declare_service(worker_handler, "mario", seconds=5.0, calls=3)
    declare_service(worker_handler, "lucia")

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_cpu_offload()

    assert commander.user_is_frozen("lucia") is False
    assert commander.user_is_frozen("mario") is False
    assert [row["reason"] for row in offload_decisions(caplog)] == ["single_user_overload"]


async def test_negligible_activity_never_makes_a_candidate(make_group, commander, caplog):
    """Noise-level users are alive but immaterial: the dominant one stays alone."""
    group, worker_handler = await offload_group(
        make_group, commander, ["mario", "lucia", "pia"]
    )
    declare_service(worker_handler, "mario", seconds=10.0, calls=9)
    declare_service(worker_handler, "lucia", seconds=0.05, calls=1)
    declare_service(worker_handler, "pia", seconds=0.05, calls=1)

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_cpu_offload()

    # S=10.1, N=3, threshold ~1.68: only mario is material.
    assert commander.user_is_frozen("lucia") is False
    assert commander.user_is_frozen("pia") is False
    rows = offload_decisions(caplog)
    assert [row["reason"] for row in rows] == ["single_user_overload"]
    assert rows[0]["subject"] == "mario"
    assert rows[0]["numbers"]["active_users"] == 3
    assert rows[0]["numbers"]["material_contributors"] == 1


async def test_exact_equality_with_the_threshold_is_material(make_group, commander):
    """s == S/(2N) is material: a strict comparison would leave mario alone."""
    group, worker_handler = await offload_group(make_group, commander, ["mario", "lucia"])
    declare_service(worker_handler, "mario", seconds=3.0, calls=3)
    declare_service(worker_handler, "lucia", seconds=1.0, calls=1)

    await group.check_cpu_offload()

    # S=4, N=2, threshold exactly 1.0: lucia is material and the least busy.
    assert commander.user_is_frozen("lucia") is True


# --- the black sheep and the journal's silence -------------------------------


async def test_a_single_active_user_is_said_once_and_never_moved(
    make_group, commander, caplog
):
    group, worker_handler = await offload_group(make_group, commander, ["mario"])
    declare_service(worker_handler, "mario", seconds=9.0, calls=9)

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_cpu_offload()
        await group.check_cpu_offload()
        await group.check_cpu_offload()

    assert group.user_worker_map["mario"] == worker_handler.name
    rows = offload_decisions(caplog)
    assert [row["reason"] for row in rows] == ["single_user_overload"]
    assert rows[0]["subject"] == "mario"


async def test_the_condition_speaks_again_after_it_fell(make_group, commander, caplog):
    group, worker_handler = await offload_group(make_group, commander, ["mario"])
    declare_service(worker_handler, "mario", seconds=9.0, calls=9)

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_cpu_offload()
        declare_cpu(worker_handler, 40.0)
        await group.check_cpu_offload()
        declare_cpu(worker_handler, 80.0)
        await group.check_cpu_offload()

    assert [row["reason"] for row in offload_decisions(caplog)] == [
        "single_user_overload",
        "single_user_overload",
    ]


async def test_nobody_active_is_said_once(make_group, commander, caplog):
    group, worker_handler = await offload_group(make_group, commander, ["mario", "lucia"])
    declare_service(worker_handler, "mario")
    declare_service(worker_handler, "lucia")

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_cpu_offload()
        await group.check_cpu_offload()

    assert [row["reason"] for row in offload_decisions(caplog)] == [
        "cpu_offload_no_active_candidate"
    ]


# --- the departure's roads ----------------------------------------------------


async def test_a_refused_freeze_releases_the_hold_and_says_so(
    make_group, commander, caplog
):
    group, worker_handler = await offload_group(
        make_group, commander, ["mario", "lucia"], freeze_refused=True
    )
    declare_service(worker_handler, "mario", seconds=5.0, calls=3)
    declare_service(worker_handler, "lucia", seconds=3.0, calls=1)

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_cpu_offload()

    assert commander.user_is_frozen("lucia") is False
    assert commander.user_map["lucia"]["on_hold"] is None
    assert group.user_worker_map["lucia"] == worker_handler.name
    assert offload_decisions(caplog)[-1]["reason"] == "cpu_offload_refused"


async def test_the_offloaded_user_cannot_return_to_the_closed_worker(
    make_group, commander
):
    group, worker_handler = await offload_group(make_group, commander, ["mario", "lucia"])
    declare_service(worker_handler, "mario", seconds=5.0, calls=3)
    declare_service(worker_handler, "lucia", seconds=3.0, calls=1)
    await group.check_cpu_offload()
    assert group.user_worker_map["lucia"] is None

    placed = await group.assign_user("lucia")

    assert placed != worker_handler.name
    assert len(group.living_workers) == 2


async def test_the_hottest_of_the_closed_workers_is_the_target(make_group, commander):
    group, first = await offload_group(make_group, commander, ["mario", "lucia"])
    declare_service(first, "mario", seconds=5.0, calls=3)
    declare_service(first, "lucia", seconds=3.0, calls=1)
    second = await group.start_worker()
    for user in ("carla", "nino"):
        known_at_the_vertex(commander, f"c_{user}", user)
        group.user_worker_map[user] = second.name
        second.hosted_users.add(user)
    second.cpu_admission_open = False
    declare_cpu(second, 90.0)
    # The second child's photo carries no users of its own: hand it the rows.
    second.worker_snapshot["users"] = {
        user: {"transfer_flag": None, "item": {"state": "active"}} for user in ("carla", "nino")
    }
    declare_service(second, "carla", seconds=4.0, calls=2)
    declare_service(second, "nino", seconds=2.0, calls=1)

    await group.check_cpu_offload()

    # The 90% worker cedes nino; the 80% one keeps everybody this beat.
    assert group.user_worker_map["nino"] is None
    assert group.user_worker_map["lucia"] == first.name


async def test_a_cession_stamps_the_cpu_pressure_clock(make_group, commander):
    """Coherence of the pressure history: the retirement is already suspended
    while a worker is CPU-closed; the stamp keeps the quiet after it true."""
    group, worker_handler = await offload_group(make_group, commander, ["mario", "lucia"])
    declare_service(worker_handler, "mario", seconds=5.0, calls=3)
    declare_service(worker_handler, "lucia", seconds=3.0, calls=1)
    group._cpu_pressure_monotonic = None

    await group.check_cpu_offload()

    assert group._cpu_pressure_monotonic is not None


# --- the policy road ------------------------------------------------------------


async def test_the_offload_threshold_applies_live(make_group, commander):
    group, worker_handler = await offload_group(make_group, commander, ["mario", "lucia"])
    declare_service(worker_handler, "mario", seconds=5.0, calls=3)
    declare_service(worker_handler, "lucia", seconds=3.0, calls=1)
    group.apply_policy(
        GroupPolicy.from_settings(group.policy.to_settings() | {"cpu_offload_percent": 90.0}),
        [],
    )

    await group.check_cpu_offload()
    assert commander.user_is_frozen("lucia") is False

    declare_cpu(worker_handler, 95.0)
    await group.check_cpu_offload()
    assert commander.user_is_frozen("lucia") is True
    assert worker_handler is group.worker_handler_map[worker_handler.name]


def test_a_group_built_with_offload_but_no_admission_does_not_exist(make_group):
    with pytest.raises(GroupPolicyError) as caught:
        make_group(cpu_offload_percent=75.0)
    assert "requires cpu_admission_close_percent" in str(caught.value)
