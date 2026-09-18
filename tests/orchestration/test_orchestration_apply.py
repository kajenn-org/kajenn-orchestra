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

"""The apply of the setpoints: the lock, the three stages, the CPU reconciliation.

The stage is the one ``test_orchestration_group_handler`` builds — real child
processes under a real group and a real vertex — and the CPU is DECLARED, not
burned, exactly as ``test_orchestration_cpu_growth`` declares it. Implementation
tests: they declare the separate temperature channel and inspect how apply lands.
"""

from __future__ import annotations

import asyncio
import inspect
import time

import pytest

from kajenn_orchestra.orchestration_profile_store import OrchestrationProfileStore
from kajenn_orchestra.orchestration import AssignmentRefused
from kajenn_orchestra.orchestration.group_policy import GroupPolicyError
from kajenn_orchestra.orchestration.spa_commander import SpaCommander

from .test_orchestration_group_handler import WORKER_CEILING, known_at_the_vertex
from .test_orchestration_group_handler import commander  # noqa: F401
from .test_orchestration_group_handler import group_settings  # noqa: F401
from .test_orchestration_group_handler import instance_root  # noqa: F401
from .test_orchestration_group_handler import make_group  # noqa: F401

ORDERS_LOGGER = "kajenn.orchestration.orders"
VERTEX_LOGGER = "kajenn_orchestra.orchestration.spa_commander"

#: The recipe level of these tests: what the groups here are really built with,
#: so an apply that names nothing leaves those two setpoints where they are.
RECIPE_SETTINGS = {"worker_memory_max_percent": 50.0, "worker_min_life_seconds": 0.0}


@pytest.fixture
def configured(commander, instance_root):  # noqa: F811
    """The vertex with a profiles folder and its two immutable levels."""
    commander.profile_store = OrchestrationProfileStore(instance_root / "profiles")
    commander.recipe_settings = dict(RECIPE_SETTINGS)
    commander.env_settings = {}
    return commander


def declare_cpu(worker_handler, cpu_temperature_percent: float | None) -> None:
    """Declare the CPU channel directly; policy tests do not test its clock."""
    worker_handler.cpu_temperature_percent = cpu_temperature_percent
    worker_handler.cpu_temperature_sampled_at = time.monotonic()
    worker_handler.cpu_temperature_interval_seconds = 0.1
    worker_handler.get_cpu_temperature_percent = lambda: cpu_temperature_percent


async def test_concurrent_applies_serialize_on_the_lock(configured, make_group):  # noqa: F811
    # wf:contract: T10 — two simultaneous applies serialize on
    # wf:contract: _configuration_lock; generation advances by 2; each recomposes
    # wf:contract: defaults ⊕ recipe_settings ⊕ profile ⊕ env_settings from the
    # wf:contract: immutable levels, never from the other's result.
    group = make_group()
    configured.profile_store.write("first", {"worker_memory_admission_percent": 60.0})
    configured.profile_store.write("second", {"cpu_close_percent": 30.0})

    payloads = await asyncio.gather(
        configured.apply_group_settings(profile_name="first", source="reload"),
        configured.apply_group_settings(profile_name="second", source="reload"),
    )

    assert sorted(payload["generation"] for payload in payloads) == [2, 3]
    assert configured.configuration_generation == 3
    first, second = payloads
    # Neither apply carries the other's key: each composed from the levels, and
    # the levels are the only thing an apply reads.
    assert first["effective_settings"]["cpu_close_percent"] is None
    assert second["effective_settings"]["worker_memory_admission_percent"] == 80.0
    # What is in force is the one that landed last, whole.
    last = max(payloads, key=lambda payload: payload["generation"])
    assert group.policy.to_settings() == last["effective_settings"]


async def test_decision_snapshot_and_emitted_order_complete(
    configured, make_group, caplog  # noqa: F811
):
    # wf:contract: T11 — a swap during a round: the in-flight decision either
    # wf:contract: runs entirely on the old policy or suppresses at the
    # wf:contract: checkpoint, never mixed values; an order ALREADY emitted
    # wf:contract: before the swap completes under its own generation and its
    # wf:contract: outcome is recorded; effects AFTER that completion are
    # wf:contract: suppressed when the policy is no longer current.
    group = make_group(rss_bytes=int(0.96 * WORKER_CEILING))
    worker_handler = await group.start_worker()

    # The order that goes out BEFORE any swap: the worker is past the restart
    # setpoint, so the round orders its replacement and that lands whole.
    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        caplog.clear()
        await group.check_occupancy(now=True)

    assert worker_handler.name not in group.worker_handler_map
    assert "restart_worker" in caplog.text
    assert "suppressed" not in caplog.text

    # A placement's effect meets the swap instead: the survivor has no room
    # left, so the arrival would father a worker — and the birth waits on the
    # placement lock while the setpoints change under it.
    survivor = group.worker_handler_map[next(iter(group.worker_handler_map))]
    survivor.worker_snapshot["rss_bytes"] = int(0.79 * WORKER_CEILING)
    known_at_the_vertex(configured, "cid_0", "gino")
    await group._placement_lock.acquire()
    placement = asyncio.create_task(group.assign_user("gino"))
    for _ in range(5):
        await asyncio.sleep(0)

    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        caplog.clear()
        payload = await configured.apply_group_settings(profile={"worker_memory_admission_percent": 75.0})
        group._placement_lock.release()
        with pytest.raises(AssignmentRefused):
            await placement

    assert len(group.worker_handler_map) == 1
    assert "suppressed: policy changed while deciding" in caplog.text
    # And no mixed values: every reader answers the policy of that one apply.
    assert group.policy.to_settings() == payload["effective_settings"]
    assert group.worker_memory_admission_percent == 75.0


async def test_cpu_apply_and_scan_never_create_capacity(configured, make_group):  # noqa: F811
    # Applying a threshold and judging a hot photo only reconcile admission.
    # There is no in-flight CPU fork to coordinate with policy replacement.
    group = make_group(cpu_admission_close_percent=50.0)
    worker_handler = await group.start_worker()
    declare_cpu(worker_handler, 90.0)

    await configured.apply_group_settings(
        profile={
            "cpu_admission_close_percent": 95.0,
            "cpu_admission_reopen_percent": 80.0,
        }
    )
    group._judge_cpu_admission()

    assert list(group.worker_handler_map) == [worker_handler.name]
    assert worker_handler.cpu_admission_open is True


async def test_no_admission_window_on_apply(configured, make_group):  # noqa: F811
    # wf:contract: T13 — a worker above the NEW threshold at apply time has
    # wf:contract: cpu_admission_open False IN the swap: a placement immediately
    # wf:contract: after the apply does not choose it.
    group = make_group()
    hot = await group.start_worker()
    cool = await group.start_worker()
    declare_cpu(hot, 90.0)
    declare_cpu(cool, 10.0)
    known_at_the_vertex(configured, "cid_1", "mario")

    await configured.apply_group_settings(
        profile={"cpu_admission_close_percent": 50.0}
    )

    assert hot.cpu_admission_open is False
    assert cool.cpu_admission_open is True
    assert group._placement_candidate("mario") is cool


async def test_cpu_reconciliation_six_outcomes(configured, make_group):  # noqa: F811
    # wf:contract: T14 — activation closes those above the new threshold at the
    # wf:contract: swap; deactivation opens everyone; thresholds raised reopen
    # wf:contract: those under the new rearm; thresholds lowered close those
    # wf:contract: above the new grow; the intermediate band PRESERVES the
    # wf:contract: worker's current state (hysteresis memory, closed included);
    # wf:contract: a missing snapshot means open. Applying never creates a
    # wf:contract: worker; concrete placement is the sole demand-driven birth.
    group = make_group()
    hot = await group.start_worker()
    cool = await group.start_worker()
    blind = await group.start_worker()
    declare_cpu(hot, 90.0)
    declare_cpu(cool, 10.0)
    blind.worker_snapshot = None
    band = {"cpu_admission_close_percent": 50.0, "cpu_admission_reopen_percent": 20.0}

    # 1) Activation: the one above the new threshold is closed at the swap, the
    # one below the rearm stays open, the one with no photo is open.
    await configured.apply_group_settings(profile=band)
    assert (hot.cpu_admission_open, cool.cpu_admission_open, blind.cpu_admission_open) == (
        False,
        True,
        True,
    )
    # No birth in the swap: the wake was rung, and no round has run.
    assert len(group.worker_handler_map) == 3
    assert group.ping_now_event.is_set()

    # 2) The intermediate band preserves what each worker IS — the closed one
    # included, which is the memory of the hysteresis.
    declare_cpu(hot, 35.0)
    declare_cpu(cool, 35.0)
    await configured.apply_group_settings(profile=band)
    assert (hot.cpu_admission_open, cool.cpu_admission_open) == (False, True)

    # 3) Thresholds raised: 35 is now under the new rearm, so the closed one
    # reopens.
    await configured.apply_group_settings(
        profile={"cpu_admission_close_percent": 80.0, "cpu_admission_reopen_percent": 40.0}
    )
    assert (hot.cpu_admission_open, cool.cpu_admission_open) == (True, True)

    # 4) Thresholds lowered: 35 is now above the new grow, so both close.
    await configured.apply_group_settings(
        profile={"cpu_admission_close_percent": 30.0, "cpu_admission_reopen_percent": 10.0}
    )
    assert (hot.cpu_admission_open, cool.cpu_admission_open) == (False, False)

    # 5) Deactivation: with the policy off nobody is ever closed.
    await configured.apply_group_settings(profile={})
    assert [handler.cpu_admission_open for handler in group.worker_handler_map.values()] == [
        True,
        True,
        True,
    ]


async def test_no_partial_state_observable(configured, make_group):  # noqa: F811
    # wf:contract: T20 — a concurrent task on the same loop never observes the
    # wf:contract: new policy with the old generation or vice versa: the
    # wf:contract: synchronous core never yields the loop.
    group = make_group()
    # The body past its own docstring: what the core DOES, never what it says.
    core = inspect.getsource(SpaCommander._commit_group_settings).rsplit('"""', 1)[1]

    assert not inspect.iscoroutinefunction(SpaCommander._commit_group_settings)
    assert "await " not in core

    # A named apply reads its profile off the loop, so the loop IS yielded while
    # the apply runs: the watcher samples it from the inside.
    configured.profile_store.write("shift", {"worker_memory_admission_percent": 55.0})
    samples = []
    stop = asyncio.Event()

    async def watch() -> None:
        while not stop.is_set():
            samples.append((group.worker_memory_admission_percent, configured.configuration_generation))
            await asyncio.sleep(0)

    watcher = asyncio.create_task(watch())
    await asyncio.sleep(0)
    await configured.apply_group_settings(profile_name="shift", source="reload")
    for _ in range(3):
        await asyncio.sleep(0)
    stop.set()
    await watcher

    assert (80.0, 1) in samples
    assert (55.0, 2) in samples
    assert set(samples) == {(80.0, 1), (55.0, 2)}


async def test_audit_line_per_attempt(configured, make_group, caplog):  # noqa: F811
    # wf:contract: T21 — every attempt reaching the handler (applied and
    # wf:contract: rejected) leaves a log_order line carrying digest, generation,
    # wf:contract: source, changed on success and violations on rejection, with
    # wf:contract: outcome "applied" or "rejected: <first violation>+N".
    make_group()

    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        caplog.clear()
        payload = await configured.apply_group_settings(
            profile={"worker_memory_admission_percent": 65.0}, source="apply"
        )
    applied = caplog.text

    assert "order=apply_group_settings" in applied
    assert f"'digest': '{configured.last_apply['digest']}'" in applied
    assert "'generation': 2" in applied
    assert "'source': 'apply'" in applied
    assert "'changed': {'worker_memory_admission_percent': 65.0}" in applied
    assert "outcome=applied" in applied
    assert payload["outcome"] == "applied"

    with caplog.at_level("INFO", logger=ORDERS_LOGGER):
        caplog.clear()
        with pytest.raises(GroupPolicyError):
            await configured.apply_group_settings(
                profile={"worker_memory_admission_percent": 500.0, "worker_max_number": 0}, source="apply"
            )
    rejected = caplog.text

    assert "'violations':" in rejected
    assert "outcome=rejected: worker_memory_admission_percent: 500.0 is out of range" in rejected
    assert rejected.rstrip().endswith("+1")
    # Nothing moved: the generation and the setpoints are the applied ones.
    assert configured.configuration_generation == 2
    assert configured.last_apply["outcome"].startswith("rejected: ")
    assert configured.last_apply["generation"] == 2


async def test_post_commit_best_effort(configured, make_group, caplog, monkeypatch):  # noqa: F811
    # wf:contract: T28 — a log_order that raises does not prevent ping_now(),
    # wf:contract: and vice versa; the response is the stage-1 payload with the
    # wf:contract: state applied regardless; the module logger is tried as
    # wf:contract: fallback when the orchestration log fails.
    group = make_group()

    def refuse_the_log(*args, **kwargs) -> None:
        raise OSError("the orchestration log is gone")

    monkeypatch.setattr(configured, "log_order", refuse_the_log)
    with caplog.at_level("ERROR", logger=VERTEX_LOGGER):
        caplog.clear()
        payload = await configured.apply_group_settings(profile={"worker_memory_admission_percent": 55.0})

    assert payload["generation"] == 2
    assert group.worker_memory_admission_percent == 55.0
    assert group.ping_now_event.is_set()
    assert "the apply of the setpoints could not be audited" in caplog.text

    monkeypatch.undo()

    def refuse_the_wake() -> None:
        raise RuntimeError("the wake is gone")

    monkeypatch.setattr(group, "ping_now", refuse_the_wake)
    with caplog.at_level("ERROR", logger=VERTEX_LOGGER):
        caplog.clear()
        payload = await configured.apply_group_settings(profile={"worker_memory_admission_percent": 56.0})

    assert payload["generation"] == 3
    assert group.worker_memory_admission_percent == 56.0
    assert "the round after the apply could not be anticipated" in caplog.text


# --- the retirement quiet meets the hot apply -----------------------------------


async def test_applying_the_quiet_alone_moves_no_clock(configured, make_group):  # noqa: F811
    # A new quiet is a setpoint like any other: it changes at once what the gate
    # reads, and it invents no CPU event — an apply is not pressure.
    group = make_group()
    await group.start_worker()
    assert group._cpu_pressure_monotonic is None

    await configured.apply_group_settings(profile={"cpu_retirement_quiet_seconds": 5.0})

    assert group.cpu_retirement_quiet_seconds == 5.0
    assert group._cpu_pressure_monotonic is None


async def test_a_foreign_apply_during_the_quiet_leaves_it_running(
    configured, make_group  # noqa: F811
):
    # A change that has nothing to do with the CPU neither clears nor renews the
    # stamp: the time already elapsed stays elapsed.
    group = make_group(cpu_admission_close_percent=50.0)
    worker = await group.start_worker()
    declare_cpu(worker, 10.0)
    group.record_cpu_pressure()
    stamped = group._cpu_pressure_monotonic

    await configured.apply_group_settings(profile={"worker_memory_admission_percent": 61.0})

    assert group.worker_memory_admission_percent == 61.0
    assert group._cpu_pressure_monotonic == stamped


async def test_switching_the_policy_on_without_a_transition_is_no_pressure(
    configured, make_group  # noqa: F811
):
    # OFF -> ON with nobody's admission actually moving: no cooldown is invented
    # out of the reconfiguration, and the retirement judges as it always did.
    group = make_group()
    worker = await group.start_worker()
    declare_cpu(worker, 10.0)

    await configured.apply_group_settings(
        profile={"cpu_admission_close_percent": 50.0, "cpu_admission_reopen_percent": 20.0}
    )

    assert group.cpu_admission_close_percent == 50.0
    assert worker.cpu_admission_open is True
    assert group._cpu_pressure_monotonic is None
    assert group.get_retirement_suspension(group.policy) is None


async def test_an_apply_that_closes_a_worker_is_pressure(configured, make_group):  # noqa: F811
    # OFF -> ON that really closes somebody: the same fact the periodic judge
    # would have recorded, so the quiet starts here too.
    group = make_group()
    hot = await group.start_worker()
    declare_cpu(hot, 90.0)

    await configured.apply_group_settings(
        profile={
            "cpu_admission_close_percent": 50.0,
            "cpu_admission_reopen_percent": 20.0,
            "cpu_retirement_quiet_seconds": 60.0,
        }
    )

    assert hot.cpu_admission_open is False
    assert group._cpu_pressure_monotonic is not None
    # And the closed worker is its own reason, before the clock is even read.
    assert group.get_retirement_suspension(group.policy) == "a worker is still CPU-closed"


async def test_an_apply_that_reopens_a_worker_is_pressure(configured, make_group):  # noqa: F811
    # A reconciliation that REOPENS is a CPU event as much as one that closes:
    # the reopen is exactly the transition the measured churn came from.
    group = make_group()
    hot = await group.start_worker()
    declare_cpu(hot, 90.0)
    await configured.apply_group_settings(
        profile={"cpu_admission_close_percent": 50.0, "cpu_admission_reopen_percent": 20.0}
    )
    group._cpu_pressure_monotonic = None  # forget the close: judge the reopen alone

    await configured.apply_group_settings(
        profile={
            "cpu_admission_close_percent": 95.0,
            "cpu_admission_reopen_percent": 92.0,
            "cpu_retirement_quiet_seconds": 60.0,
        }
    )

    assert hot.cpu_admission_open is True
    assert group._cpu_pressure_monotonic is not None
    suspension = group.get_retirement_suspension(group.policy)
    assert suspension is not None and "the quiet lasts" in suspension


async def test_switching_the_policy_off_frees_the_retirement_at_once(
    configured, make_group  # noqa: F811
):
    # ON -> OFF: the gate is inert from that instant, and the timestamp the
    # policy left behind holds nothing back.
    group = make_group(
        cpu_admission_close_percent=50.0,
        cpu_retirement_quiet_seconds=3600.0,
    )
    reception = await group.start_worker()
    spare = await group.start_worker()
    declare_cpu(reception, 90.0)
    declare_cpu(spare, 1.0)
    await group.check_occupancy(now=True)  # blocked: pressure, and a growth
    assert group._cpu_pressure_monotonic is not None

    declare_cpu(reception, 1.0)
    stamped = group._cpu_pressure_monotonic
    await configured.apply_group_settings(profile={})  # the policy off

    assert group.cpu_admission_close_percent is None
    # The reopenings this apply performed are the gate being dismantled, not the
    # CPU speaking: the clock stays exactly where the last real event left it.
    assert group._cpu_pressure_monotonic == stamped
    for worker_handler in group.living_workers:
        declare_cpu(worker_handler, 1.0)
    await group.check_occupancy(now=True)

    assert any(
        worker_handler.state in ("quitting", "quitted")
        for worker_handler in group.worker_handler_map.values()
    )


async def test_a_refused_apply_moves_neither_policy_nor_clock(configured, make_group):  # noqa: F811
    # Stage one refuses before anything moves: no policy, no admission, no stamp.
    group = make_group(
        cpu_admission_close_percent=50.0,
        cpu_retirement_quiet_seconds=60.0,
    )
    hot = await group.start_worker()
    declare_cpu(hot, 90.0)
    await group.check_occupancy(now=True)
    before_policy = group.policy
    before_stamp = group._cpu_pressure_monotonic
    before_admission = hot.cpu_admission_open

    with pytest.raises(GroupPolicyError):
        await configured.apply_group_settings(
            profile={"cpu_retirement_quiet_seconds": -1.0}
        )

    assert group.policy is before_policy
    assert group._cpu_pressure_monotonic == before_stamp
    assert hot.cpu_admission_open == before_admission


async def test_the_quiet_travels_through_status_digest_and_changed(
    configured, make_group  # noqa: F811
):
    # The setpoint is in the effective settings, in the changed diff when it
    # moves, and it moves the digest like any other.
    make_group()
    first = await configured.apply_group_settings(profile={"cpu_retirement_quiet_seconds": 90.0})

    assert first["changed_settings"] == {"cpu_retirement_quiet_seconds": 90.0}
    assert first["effective_settings"]["cpu_retirement_quiet_seconds"] == 90.0
    moved_digest = configured.last_apply["digest"]

    second = await configured.apply_group_settings(profile={"cpu_retirement_quiet_seconds": 90.0})
    assert second["changed_settings"] == {}
    assert configured.last_apply["digest"] == moved_digest

    third = await configured.apply_group_settings(profile={})
    assert third["changed_settings"] == {"cpu_retirement_quiet_seconds": 60.0}
    assert configured.last_apply["digest"] != moved_digest


async def test_off_then_on_again_leaves_no_cooldown_behind(configured, make_group):  # noqa: F811
    # The three steps in a row: a worker closed under the policy, the policy
    # switched off — the worker reopens, the clock does not move and the
    # retirement is free — and the policy switched back on with nobody moving,
    # which must not resurrect a quiet out of the old pressure.
    group = make_group()
    hot = await group.start_worker()
    spare = await group.start_worker()
    declare_cpu(hot, 90.0)
    declare_cpu(spare, 1.0)
    band = {
        "cpu_admission_close_percent": 50.0,
        "cpu_admission_reopen_percent": 20.0,
        "cpu_retirement_quiet_seconds": 3600.0,
    }

    # 1) The policy on closes the hot worker: a real transition, so pressure.
    await configured.apply_group_settings(profile=band)
    assert hot.cpu_admission_open is False
    closed_at = group._cpu_pressure_monotonic
    assert closed_at is not None
    assert group.get_retirement_suspension(group.policy) == "a worker is still CPU-closed"

    # 2) The policy off reopens him, and the clock does not move: the gate is
    # gone, so the retirement is free at once despite the huge quiet.
    await configured.apply_group_settings(profile={"cpu_retirement_quiet_seconds": 3600.0})
    assert group.cpu_admission_close_percent is None
    assert hot.cpu_admission_open is True
    assert group._cpu_pressure_monotonic == closed_at

    declare_cpu(hot, 1.0)
    await group.check_occupancy(now=True)
    assert any(
        worker_handler.state in ("quitting", "quitted")
        for worker_handler in group.worker_handler_map.values()
    )

    # 3) The policy back on with nobody moving — both workers are open and cool
    # — stamps nothing: the only pressure on record is still step 1's, so the
    # quiet that governs from here runs from THAT instant and not from the
    # apply. Whatever is left of it is real time already elapsed, never a fresh
    # period the reconfiguration invented.
    for worker_handler in group.living_workers:
        declare_cpu(worker_handler, 1.0)
    await configured.apply_group_settings(profile=band)

    assert group.cpu_admission_close_percent == 50.0
    assert all(w.cpu_admission_open for w in group.living_workers)
    assert group._cpu_pressure_monotonic == closed_at
