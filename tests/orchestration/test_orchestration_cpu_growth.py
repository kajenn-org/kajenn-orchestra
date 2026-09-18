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

"""CPU soft admission and demand-driven worker birth.

The stage is the one ``test_orchestration_group_handler`` builds — real child
processes under a real group and a real vertex — and the CPU is DECLARED, not
burned: the fresh commander-side temperature is written on the handler, which
is exactly the channel the judge reads. Implementation tests exercise the
experimental policy directly. A CPU sample may open or
close admission, but never forks. A concrete arrival that finds no open worker
creates exactly one worker and becomes its first user.
"""

from __future__ import annotations

import asyncio
import json
import time as real_time

import pytest

from kajenn_orchestra.orchestration import AssignmentRefused
from kajenn_orchestra.orchestration import group_handler as group_handler_module
from kajenn_orchestra.orchestration.group_policy import GroupPolicyError

from .conftest import kill_process, wait_for

# The stage of the group tests, reused whole: the scripted child, the vertex,
# the group builder. Imported names are pytest fixtures and their dependencies.
from .test_orchestration_group_handler import (
    MEMORY_CEILING,
    WORKER_CEILING,
    known_at_the_vertex,
)
from .test_orchestration_group_handler import commander  # noqa: F401
from .test_orchestration_group_handler import group_settings  # noqa: F401
from .test_orchestration_group_handler import instance_root  # noqa: F401
from .test_orchestration_group_handler import make_group  # noqa: F401

ORDERS_LOGGER = "kajenn.orchestration.orders"
DECISIONS_LOGGER = "kajenn.orchestration.decisions"


async def grown_group(make_group, **policies):
    """One group with the CPU admission policy on, as the experiment runs it."""
    group = make_group(cpu_admission_close_percent=50.0, **policies)
    await group.start_worker()
    return group


def declare_cpu(worker_handler, cpu_temperature_percent: float) -> None:
    """Declare the CPU channel directly; policy tests do not test its clock."""
    worker_handler.cpu_temperature_percent = cpu_temperature_percent
    worker_handler.cpu_temperature_sampled_at = real_time.monotonic()
    worker_handler.cpu_temperature_interval_seconds = 0.1
    worker_handler.get_cpu_temperature_percent = lambda: cpu_temperature_percent


class ControlledTime:
    """The clock of the retirement quiet, advanced by hand — no real waiting.

    Stands in for the ``time`` module INSIDE group_handler only: ``monotonic``
    answers this clock, everything else is the real module — the workers, the
    envelope layer and asyncio keep the real time, so a test advancing the
    quiet moves no timer but the group's own.
    """

    def __init__(self) -> None:
        self.now = real_time.monotonic()

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def __getattr__(self, name):
        return getattr(real_time, name)


@pytest.fixture
def group_clock(monkeypatch):
    """group_handler's clock, controlled: advance() is the only way it moves."""
    clock = ControlledTime()
    monkeypatch.setattr(group_handler_module, "time", clock)
    return clock


def arrival(commander, group, user: str):
    """One new user known at the vertex, ready for this group's placement."""
    known_at_the_vertex(commander, f"c_{user}", user)
    return group.assign_user(user)


# --- CPU judges admission, never process count ------------------------------


async def test_under_the_threshold_nothing_grows_and_admission_stays_open(make_group):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 49.9)

    await group.check_occupancy(now=True)

    assert len(group.worker_handler_map) == 1
    assert group.reception.cpu_admission_open is True


async def test_a_crossing_blocks_the_worker_without_growing(make_group, caplog):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 55.0)

    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        await group.check_occupancy(now=True)

    assert len(group.worker_handler_map) == 1
    assert group.reception.cpu_admission_open is False
    rows = [record.getMessage() for record in caplog.records]
    assert any("cpu_admission" in row and "blocked" in row for row in rows)
    assert not any("order=grow" in row for row in rows)


async def test_admission_decision_names_the_closed_worker(make_group, caplog):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 55.0)

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_occupancy(now=True)

    decisions = [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == DECISIONS_LOGGER
    ]
    scan = [row for row in decisions if row["decision"] == "cpu_admission_scan"][-1]
    assert scan["reason"] == "cpu_admission_transitions"
    assert scan["outcome"] == "updated"
    assert scan["numbers"]["open_workers"] == 0
    assert scan["numbers"]["empty_workers"] == 1
    assert scan["candidates"][0]["name"] == "standard_0001"
    assert scan["candidates"][0]["cpu_admission_open"] is False


async def test_the_journal_explains_a_reopened_full_worker_winning_placement(
    make_group, commander, caplog
):
    group = await grown_group(make_group)
    first = group.reception
    declare_cpu(first, 55.0)
    await group.check_occupancy(now=True)
    declare_cpu(first, 28.0)
    await group.check_occupancy(now=True)

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        placed = await arrival(commander, group, "mario")

    assert placed == first.name
    decisions = [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == DECISIONS_LOGGER
    ]
    placement = [row for row in decisions if row["decision"] == "placement"][-1]
    assert placement["outcome"] == first.name
    assert placement["reason"] == "hottest_cpu_open_candidate"
    assert [candidate["name"] for candidate in placement["candidates"]] == [first.name]
    assert placement["candidates"][0]["cpu_temperature_percent"] == 28.0


async def test_cpu_past_the_restart_setpoint_blocks_without_restarting(make_group):
    group = await grown_group(make_group)
    original = group.reception
    declare_cpu(original, 96.0)

    await group.check_occupancy(now=True)

    assert original.name in group.worker_handler_map
    assert original.state == "running"
    assert original.cpu_admission_open is False
    assert sorted(group.worker_handler_map) == ["standard_0001"]


async def test_snapshots_that_stay_above_the_threshold_are_one_crossing(make_group):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)

    for _ in range(3):
        await group.check_occupancy(now=True)

    assert len(group.worker_handler_map) == 1
    assert group.reception.cpu_admission_open is False


async def test_inside_the_hysteresis_band_the_state_is_kept(make_group):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)

    for cpu_temperature_percent in (45.0, 49.0, 41.0, 49.9):
        declare_cpu(group.reception, cpu_temperature_percent)
        await group.check_occupancy(now=True)

    assert group.reception.cpu_admission_open is False
    assert len(group.worker_handler_map) == 1


async def test_below_the_rearm_threshold_the_worker_reopens(make_group, caplog):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)

    declare_cpu(group.reception, 39.0)
    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        await group.check_occupancy(now=True)

    assert group.reception.cpu_admission_open is True
    assert "reopened" in caplog.text
    assert len(group.worker_handler_map) == 1


async def test_repeated_crossings_without_arrivals_never_create_workers(make_group):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)
    declare_cpu(group.reception, 39.0)
    await group.check_occupancy(now=True)

    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)

    assert len(group.worker_handler_map) == 1


async def test_with_the_policy_off_a_burning_worker_changes_nothing(make_group, commander):
    # 60 is over the experimental threshold and under the admission ceiling:
    # with the policy off, NO road reads it as a reason to grow or to close.
    group = make_group()
    await group.start_worker()
    declare_cpu(group.reception, 60.0)

    await group.check_occupancy(now=True)

    assert len(group.worker_handler_map) == 1
    assert group.reception.cpu_admission_open is True
    assert await arrival(commander, group, "walkin") == "standard_0001"


# --- the placement over the admission ----------------------------------------


async def test_a_new_user_births_capacity_when_the_only_worker_is_blocked(
    make_group, commander
):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)
    # CPU created nothing; this concrete arrival creates and occupies worker 2.
    assert await arrival(commander, group, "first") == "standard_0002"
    assert group.user_worker_map["first"] == "standard_0002"


async def test_the_journal_ties_each_demand_birth_to_its_first_user(
    make_group, commander, caplog
):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)

    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        home = await arrival(commander, group, "first")

    decisions = [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == DECISIONS_LOGGER
    ]
    placement = [row for row in decisions if row["decision"] == "placement"][-1]
    assert home == "standard_0002"
    assert placement["subject"] == "first"
    assert placement["outcome"] == home
    assert placement["reason"] == "new_worker_created_for_placement"
    newborn = next(row for row in placement["candidates"] if row["name"] == home)
    assert newborn["users"] == 1


async def test_the_newborn_takes_new_users_for_as_long_as_the_trigger_stays_hot(
    make_group, commander
):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)

    homes = [await arrival(commander, group, f"user{i}") for i in range(4)]

    assert homes == ["standard_0002"] * 4


async def test_two_open_workers_are_hottest_first(make_group, commander):
    group = await grown_group(make_group)
    second = await group.start_worker()
    declare_cpu(group.reception, 30.0)
    declare_cpu(second, 10.0)
    group.reception.worker_snapshot["rss_bytes"] = int(0.3 * WORKER_CEILING)

    assert await arrival(commander, group, "walkin") == "standard_0001"


async def test_sticky_users_stay_on_the_blocked_worker(make_group, commander):
    group = await grown_group(make_group)
    await arrival(commander, group, "resident")
    declare_cpu(group.reception, 60.0)

    await group.check_occupancy(now=True)

    assert group.user_worker_map["resident"] == "standard_0001"


async def test_the_newborn_crossing_fathers_the_third_and_takes_over(make_group, commander):
    # Every birth has a concrete first user: CPU only closes each hot worker.
    group = await grown_group(make_group)
    await arrival(commander, group, "resident")
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)
    assert sorted(group.worker_handler_map) == ["standard_0001"]

    assert await arrival(commander, group, "second_a") == "standard_0002"
    assert await arrival(commander, group, "second_b") == "standard_0002"

    second = group.worker_handler_map["standard_0002"]
    declare_cpu(second, 55.0)
    await group.check_occupancy(now=True)
    assert sorted(group.worker_handler_map) == ["standard_0001", "standard_0002"]
    assert second.cpu_admission_open is False

    assert await arrival(commander, group, "third_a") == "standard_0003"
    assert await arrival(commander, group, "third_b") == "standard_0003"
    assert group.user_worker_map["resident"] == "standard_0001"
    assert group.user_worker_map["second_a"] == "standard_0002"


async def test_nobody_open_and_growth_allowed_births_the_capacity(make_group, commander):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)
    assert await arrival(commander, group, "first") == "standard_0002"
    second = group.worker_handler_map["standard_0002"]
    declare_cpu(second, 60.0)
    await group.check_occupancy(now=True)
    # Both living workers are now blocked; the placement's own level 2 births.
    for worker_handler in group.living_workers:
        worker_handler.cpu_admission_open = False

    home = await arrival(commander, group, "walkin")

    assert home == "standard_0003"
    assert len(group.worker_handler_map) == 3


async def test_nobody_open_and_growth_refused_falls_back_on_a_blocked_worker(
    make_group, commander, caplog
):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)
    assert len(group.worker_handler_map) == 1
    commander.state = "quitting"  # the growth is refused: _may_grow is False

    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        home = await arrival(commander, group, "walkin")
    commander.state = "running"

    assert home == "standard_0001"
    assert "placement_fallback" in caplog.text
    assert "over the soft limit" in caplog.text


async def test_a_blocked_worker_at_the_hard_limit_still_refuses(make_group, commander):
    # CPU-closed AND past the memory veto: the fallback has nowhere to put him.
    group = await grown_group(make_group, rss_bytes=int(0.9 * MEMORY_CEILING))
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)
    group.reception.cpu_admission_open = False
    commander.state = "quitting"

    known_at_the_vertex(commander, "c_walkin", "walkin")
    with pytest.raises(AssignmentRefused):
        await group.assign_user("walkin")
    commander.state = "running"


async def test_the_fallback_is_never_used_while_an_open_worker_admits(
    make_group, commander, caplog
):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)

    # Add an open worker explicitly: CPU sampling itself created none.
    await group.start_worker()

    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        home = await arrival(commander, group, "walkin")

    assert home == "standard_0002"
    assert "placement_fallback" not in caplog.text


async def test_worker_max_users_still_gates_the_open_worker(make_group, commander):
    group = await grown_group(make_group, worker_max_users=1)
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)

    first = await arrival(commander, group, "one")
    second = await arrival(commander, group, "two")

    assert first == "standard_0002"
    assert second not in ("standard_0001", "standard_0002")  # 2 is full BY HEADS, 1 blocked


# --- concurrency --------------------------------------------------------------


async def test_two_concurrent_cpu_rounds_never_fork(make_group, caplog):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 55.0)

    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        await asyncio.gather(
            group.check_occupancy(now=True), group.check_occupancy(now=True)
        )

    assert len(group.worker_handler_map) == 1
    assert group.reception.cpu_admission_open is False


async def test_concurrent_arrivals_share_one_demand_born_worker(make_group, commander):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)
    known_at_the_vertex(commander, "c_one", "one")
    known_at_the_vertex(commander, "c_two", "two")

    homes = await asyncio.gather(group.assign_user("one"), group.assign_user("two"))

    assert homes == ["standard_0002", "standard_0002"]
    assert len(group.worker_handler_map) == 2


async def test_two_workers_over_the_threshold_are_one_spawn_per_round(make_group):
    group = await grown_group(make_group)
    second = await group.start_worker()
    declare_cpu(group.reception, 60.0)
    declare_cpu(second, 60.0)

    await group.check_occupancy(now=True)
    assert len(group.worker_handler_map) == 2
    assert group.reception.cpu_admission_open is False
    assert second.cpu_admission_open is False

    # More CPU rounds only preserve admission; demand alone creates capacity.
    await group.check_occupancy(now=True)
    assert len(group.worker_handler_map) == 2


async def test_a_blocked_worker_that_dies_takes_its_state_with_it(make_group, commander):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)
    assert await arrival(commander, group, "first") == "standard_0002"
    newborn = group.worker_handler_map["standard_0002"]
    declare_cpu(newborn, 60.0)
    await group.check_occupancy(now=True)

    group.drop_worker("standard_0002")

    assert "standard_0002" not in group.worker_handler_map
    assert await arrival(commander, group, "walkin") == "standard_0003"
    third = group.worker_handler_map["standard_0003"]

    kill_process(newborn.process)
    await wait_for(lambda: not newborn.process.alive)
    await newborn.connector.stop()
    assert third.state == "running"


async def test_a_failed_spawn_leaves_the_fallback_available(make_group, commander, caplog):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)
    group.worker_settings["worker_kwargs"]["behaviour"] = "absent"

    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        home = await arrival(commander, group, "walkin")

    assert home == "standard_0001"
    assert "placement_fallback" in caplog.text
    group.worker_settings["worker_kwargs"]["behaviour"] = "answer"


async def test_memory_refusing_the_growth_is_no_503_while_a_blocked_worker_has_room(
    make_group, commander, caplog
):
    group = await grown_group(
        make_group,
        memory_max_percent=10.0,
        # The worker's own ceiling is kept WIDE: only the group quota refuses
        # here, the worker itself stays under its hard admission limit.
        worker_memory_max_percent=400.0,
        # No restart here: the occupancy is clamped to 100 and never exceeds it.
        restart_occupancy_max_percent=100.0,
        rss_bytes=int(0.2 * MEMORY_CEILING),
    )
    declare_cpu(group.reception, 60.0)
    await group.check_occupancy(now=True)
    assert len(group.worker_handler_map) == 1  # CPU does not attempt growth

    home = await arrival(commander, group, "walkin")

    assert home == "standard_0001"


async def test_permission_that_falls_before_demand_stops_the_spawn(make_group, commander):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 55.0)

    await group.check_occupancy(now=True)
    commander.state = "quitting"
    home = await arrival(commander, group, "walkin")
    commander.state = "running"

    assert len(group.worker_handler_map) == 1
    assert home == "standard_0001"  # soft fallback, not a birth


async def test_a_dead_only_worker_is_replaced_by_the_availability_judge(make_group):
    group = await grown_group(make_group)
    declare_cpu(group.reception, 55.0)

    group.reception.state = "quitted"
    await group.check_occupancy(now=True)

    # With no living worker, the empty-group branch of check_occupancy restores
    # a reception. This birth is not caused by the CPU scan.
    assert len(group.worker_handler_map) == 2
    assert group.worker_handler_map["standard_0002"].cpu_admission_open


# --- the retirement stands apart ----------------------------------------------


async def test_the_newborn_is_not_retired_by_the_next_round(make_group):
    group = await grown_group(make_group, worker_min_life_seconds=60.0)
    await group.start_worker()
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)
    declare_cpu(group.reception, 5.0)

    await group.check_occupancy(now=True)

    assert len(group.worker_handler_map) == 2


async def test_after_the_descent_and_the_quiet_the_spare_worker_is_released(
    make_group, group_clock
):
    group = await grown_group(make_group, cpu_retirement_quiet_seconds=60.0)
    newborn = await group.start_worker()
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)

    # The load is gone: the reception reopens — a CPU event, the quiet restarts.
    declare_cpu(group.reception, 5.0)
    declare_cpu(newborn, 1.0)
    await group.check_occupancy(now=True)
    assert newborn.state == "running"  # suspended: the reopen just spoke

    group_clock.advance(61.0)
    await group.check_occupancy(now=True)  # the quiet elapsed: judged as always

    assert newborn.state in ("quitting", "quitted")


async def test_no_spawn_close_spawn_cycle_without_new_load(make_group, group_clock):
    group = await grown_group(make_group, cpu_retirement_quiet_seconds=60.0)
    newborn = await group.start_worker()
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)

    declare_cpu(group.reception, 5.0)
    declare_cpu(newborn, 1.0)
    await group.check_occupancy(now=True)  # reopen: the quiet restarts
    group_clock.advance(61.0)
    for _ in range(4):
        await group.check_occupancy(now=True)
        group_clock.advance(61.0)

    assert len(group.worker_handler_map) == 2  # quitting handlers stay until death report
    assert newborn.state in ("quitting", "quitted")


async def test_the_thresholds_must_leave_a_hysteresis_band(make_group):
    for grow, rearm in ((50.0, 60.0), (50.0, 50.0), (110.0, 40.0), (50.0, -1.0)):
        with pytest.raises(ValueError, match="hysteresis"):
            make_group(cpu_admission_close_percent=grow, cpu_admission_reopen_percent=rearm)

    make_group(cpu_admission_close_percent=50.0, cpu_admission_reopen_percent=40.0)
    make_group(cpu_admission_reopen_percent=60.0)  # policy off: the band is nobody's business


# --- the retirement stands aside while the CPU speaks ---------------------------


async def test_policy_off_keeps_the_retirement_immediate(make_group):
    # No CPU policy: the gate does not exist and the spare quits at once,
    # exactly as it always did — no cooldown appears from anywhere.
    group = make_group(cpu_retirement_quiet_seconds=3600.0)
    await group.start_worker()
    second = await group.start_worker()
    declare_cpu(group.reception, 1.0)
    declare_cpu(second, 1.0)

    await group.check_occupancy(now=True)

    assert second.state in ("quitting", "quitted")


async def test_policy_off_ignores_even_a_fresh_pressure_stamp(make_group):
    # The gate is inert with the policy off: a timestamp left by a policy that
    # was on — or by an apply that switched it off — holds nothing back.
    group = make_group(cpu_retirement_quiet_seconds=3600.0)
    await group.start_worker()
    second = await group.start_worker()
    declare_cpu(group.reception, 1.0)
    declare_cpu(second, 1.0)
    group.record_cpu_pressure()

    await group.check_occupancy(now=True)

    assert second.state in ("quitting", "quitted")


async def test_policy_on_from_boot_imposes_no_artificial_cooldown(make_group):
    # Policy ON, a huge quiet, and NO CPU event ever — no blocked, grown,
    # refusal or reopened: the pressure clock is still None, both workers are
    # open, and the FIRST check retires the spare as always. Distinct from the
    # policy-off test: here the gate exists and answers None.
    group = await grown_group(make_group, cpu_retirement_quiet_seconds=3600.0)
    second = await group.start_worker()
    declare_cpu(group.reception, 1.0)
    declare_cpu(second, 1.0)
    assert group._cpu_pressure_monotonic is None
    assert group.reception.cpu_admission_open and second.cpu_admission_open

    await group.check_occupancy(now=True)

    assert second.state in ("quitting", "quitted")


async def test_a_cpu_closed_worker_suspends_the_retirement(make_group):
    # Even with a zero quiet, standing demand — a worker still closed — is
    # its own gate: the emptiest worker is not handed back to the hot one.
    group = await grown_group(make_group, cpu_retirement_quiet_seconds=0.0)
    newborn = await group.start_worker()
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)
    declare_cpu(newborn, 1.0)
    declare_cpu(group.reception, 45.0)  # in the band: stays blocked

    await group.check_occupancy(now=True)

    assert newborn.state == "running"
    assert group.reception.cpu_admission_open is False


async def test_a_fresh_admission_transition_holds_retirement_for_the_whole_quiet(
    make_group, group_clock
):
    group = await grown_group(make_group, cpu_retirement_quiet_seconds=60.0)
    newborn = await group.start_worker()
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)  # blocked: pressure spoke
    declare_cpu(group.reception, 5.0)
    declare_cpu(newborn, 1.0)
    await group.check_occupancy(now=True)  # reopen: restarts the quiet

    group_clock.advance(59.0)
    await group.check_occupancy(now=True)  # still inside the quiet

    assert newborn.state == "running"


async def test_a_blocked_admission_is_pressure_even_when_growth_is_impossible(
    make_group, commander, group_clock
):
    group = await grown_group(make_group, cpu_retirement_quiet_seconds=60.0)
    second = await group.start_worker()
    declare_cpu(group.reception, 60.0)
    declare_cpu(second, 1.0)
    commander.state = "saturated"  # _may_grow says no: the growth is refused

    await group.check_occupancy(now=True)

    group_clock.advance(59.0)
    declare_cpu(group.reception, 45.0)  # the band: still blocked, but even if...
    group.reception.cpu_admission_open = True  # ...nobody is closed any more
    await group.check_occupancy(now=True)

    assert second.state == "running"


async def test_a_reopen_restarts_the_whole_quiet(make_group, group_clock):
    # The measured churn defect: a worker closed for LONGER than the quiet,
    # then reopened — the retirement must not meet it at the very next beat.
    group = await grown_group(make_group, cpu_retirement_quiet_seconds=60.0)
    newborn = await group.start_worker()
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)
    declare_cpu(newborn, 1.0)

    group_clock.advance(120.0)  # closed for two whole quiets: pressure is old
    declare_cpu(group.reception, 5.0)
    await group.check_occupancy(now=True)  # THIS round reopens the reception

    assert group.reception.cpu_admission_open is True
    assert newborn.state == "running"  # and closes nobody: the quiet restarted

    group_clock.advance(59.0)
    await group.check_occupancy(now=True)
    assert newborn.state == "running"  # still inside the restarted quiet

    group_clock.advance(2.0)
    await group.check_occupancy(now=True)
    assert newborn.state in ("quitting", "quitted")


async def test_new_pressure_during_the_quiet_restarts_it(make_group, group_clock):
    group = await grown_group(make_group, cpu_retirement_quiet_seconds=60.0)
    newborn = await group.start_worker()
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)
    declare_cpu(group.reception, 5.0)
    declare_cpu(newborn, 1.0)
    await group.check_occupancy(now=True)  # reopen: quiet running

    group_clock.advance(50.0)
    declare_cpu(group.reception, 55.0)  # the CPU speaks again mid-quiet
    await group.check_occupancy(now=True)  # blocked; CPU creates no process
    declare_cpu(group.reception, 5.0)
    await group.check_occupancy(now=True)  # reopened: restarted again

    group_clock.advance(59.0)
    for worker_handler in group.living_workers:
        declare_cpu(worker_handler, 1.0)
    await group.check_occupancy(now=True)

    assert all(w.state == "running" for w in group.living_workers)


async def test_a_worker_with_users_is_consolidated_after_the_quiet(
    make_group, commander, group_clock
):
    # No absolute "a worker with users cannot close": after the descent and
    # the quiet, the spare's user is redistributed — the consolidation stands.
    group = await grown_group(make_group, cpu_retirement_quiet_seconds=60.0)
    newborn = await group.start_worker()
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)
    resident = await arrival(commander, group, "resident")
    assert resident == "standard_0002"

    declare_cpu(group.reception, 5.0)
    declare_cpu(newborn, 2.0)
    await group.check_occupancy(now=True)  # reopen
    group_clock.advance(61.0)
    await group.check_occupancy(now=True)  # quiet over: the spare is judged

    assert newborn.state in ("quitting", "quitted")


async def test_sticky_users_stand_until_the_intentional_retirement(
    make_group, commander, group_clock
):
    group = await grown_group(make_group, cpu_retirement_quiet_seconds=60.0)
    newborn = await group.start_worker()
    declare_cpu(group.reception, 55.0)
    await group.check_occupancy(now=True)
    home = await arrival(commander, group, "settler")
    assert home == "standard_0002"

    declare_cpu(group.reception, 5.0)
    declare_cpu(newborn, 2.0)
    await group.check_occupancy(now=True)  # reopen: suspended
    group_clock.advance(30.0)
    await group.check_occupancy(now=True)  # still quiet: suspended

    assert group.user_worker_map["settler"] == "standard_0002"  # untouched so far
    assert newborn.state == "running"


async def test_a_worker_with_no_temperature_suspends_the_retirement(make_group, caplog):
    # The CPU decides the closure: with one worker unmeasured there is no
    # judgment this round, and the journal says so — nothing is closed.
    group = make_group()
    await group.start_worker()
    second = await group.start_worker()
    declare_cpu(group.reception, 1.0)
    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_occupancy(now=True)
    assert second.state == "running"
    decisions = [json.loads(r.getMessage()) for r in caplog.records if r.name == DECISIONS_LOGGER]
    rows = [d for d in decisions if d["decision"] == "retirement"]
    assert [row["reason"] for row in rows] == ["cpu_temperature_missing"]
    assert rows[0]["numbers"]["missing"] == [second.name]


async def test_a_missing_temperature_is_journaled_before_the_pressure_gate(
    make_group, caplog
):
    # Pressure stands (the reception is CPU-closed) AND the newborn has no
    # temperature yet: the round says the judgment could not be made, not that
    # the pressure holds it — one row, the missing one.
    group = await grown_group(make_group)
    second = await group.start_worker()
    declare_cpu(group.reception, 60.0)
    with caplog.at_level("INFO", logger=DECISIONS_LOGGER):
        await group.check_occupancy(now=True)
    assert group.reception.cpu_admission_open is False
    decisions = [json.loads(r.getMessage()) for r in caplog.records if r.name == DECISIONS_LOGGER]
    rows = [d for d in decisions if d["decision"] == "retirement"]
    assert [r["reason"] for r in rows] == ["cpu_temperature_missing"]
    assert rows[0]["numbers"]["missing"] == [second.name]
    assert second.state == "running"


def test_the_quiet_is_a_duration_and_zero_is_one(make_group):
    # The validation is the policy's, and it lists what is wrong: a negative
    # quiet is out of range, zero is a legitimate duration.
    with pytest.raises(GroupPolicyError, match="cpu_retirement_quiet_seconds"):
        make_group(cpu_retirement_quiet_seconds=-1.0)

    assert make_group(cpu_retirement_quiet_seconds=0.0).cpu_retirement_quiet_seconds == 0.0
    # And a group that names it nowhere runs on the dataclass default.
    assert make_group().cpu_retirement_quiet_seconds == 60.0
