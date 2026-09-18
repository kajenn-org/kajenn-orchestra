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

"""The memory a birth is judged against: the cgroup's, and the room still in it.

Two subjects, and the second needs the first. The vertex reads the memory of
the CGROUP wherever there is one — a server in a container that reads the host
sees 64 GiB where it may take 2, and grows until the kernel kills it. And a
group asks TWO gates before forking: its own quota, prospectively (what its
workers hold plus what the newborn may hold), and the room the machine actually
has left, which is the only reading that counts the commander, the templates and
every other tenant of the container.

The cgroup files are read through ``CGROUP_MEMORY_FILES``, so the tests write
real files and point the constant at them: no ``/sys`` of the host is touched,
and the suite says the same thing on a machine that has no cgroup at all.
"""

from __future__ import annotations

from collections import namedtuple
from typing import Any

import psutil
import pytest

from kajenn_orchestra.orchestration import GroupHandler, SpaCommander
from kajenn_orchestra.orchestration import spa_commander as spa_commander_module

MIB = 1024 * 1024

#: The container of these tests: 64 MiB, small enough to be under the memory of
#: any machine the suite runs on, so the limit is always the smaller figure.
LIMIT_BYTES = 64 * MIB

#: The host the pinned baseline describes: far above the container's limit.
HOST_TOTAL = 8 * 1024 * MIB
HOST_AVAILABLE = 5 * 1024 * MIB


@pytest.fixture
def commander(tmp_path):
    return SpaCommander(tmp_path / "frozen_users")


@pytest.fixture
def cgroup(tmp_path, monkeypatch):
    """Write a cgroup layout under the tmp path and make the vertex read THAT one."""

    def install(*, limit: str | None = None, current: str | None = None, v1: bool = False) -> None:
        limit_path = tmp_path / "memory.max"
        current_path = tmp_path / "memory.current"
        if limit is not None:
            limit_path.write_text(limit)
        if current is not None:
            current_path.write_text(current)
        layout = (str(limit_path), str(current_path))
        absent = ("/nowhere/memory.max", "/nowhere/memory.current")
        monkeypatch.setattr(
            spa_commander_module,
            "CGROUP_MEMORY_FILES",
            (absent, layout) if v1 else (layout, absent),
        )

    return install


@pytest.fixture
def host_gauges(commander, monkeypatch):
    """What the machine reads with no cgroup in the way: the baseline of the tests.

    The host reading is pinned: the available memory of a live machine moves
    between two readings, and these tests compare two.
    """
    machine = namedtuple("Machine", "total available")(HOST_TOTAL, HOST_AVAILABLE)
    monkeypatch.setattr(psutil, "virtual_memory", lambda: machine)
    monkeypatch.setattr(
        spa_commander_module,
        "CGROUP_MEMORY_FILES",
        (("/nowhere/memory.max", "/nowhere/memory.current"),),
    )
    return commander._machine_memory_gauges()


class WorkerStub:
    """A worker as the memory gates read it: its state, its last photo, its latches."""

    def __init__(
        self,
        name: str,
        rss_bytes: int,
        cpu_temperature_percent: float | None = None,
        pss_bytes: int | None = None,
    ) -> None:
        self.name = name
        self.state = "running"
        self.worker_snapshot: dict[str, Any] = {"rss_bytes": rss_bytes}
        if pss_bytes is not None:
            self.worker_snapshot["pss_bytes"] = pss_bytes
        self.cpu_temperature_percent = cpu_temperature_percent
        self.cpu_admission_open = True
        self.last_admission_monotonic = None

    def get_cpu_temperature_percent(self) -> float | None:
        return self.cpu_temperature_percent


def build_group(commander, tmp_path, **settings: Any) -> GroupHandler:
    """A group whose entry module is never launched: nothing here forks a process."""
    return GroupHandler(
        commander,
        "standard",
        memory_concession_bytes=LIMIT_BYTES,
        instance_dir=tmp_path / "i",
        frozen_users_path=tmp_path / "frozen_users",
        entry_module="never.launched",
        **settings,
    )


def with_workers(group: GroupHandler, *workers: WorkerStub) -> None:
    """Put these workers in the group, the way a launch that landed would have."""
    for worker in workers:
        group.worker_handler_map[worker.name] = worker


def with_available(monkeypatch, commander, *readings: float) -> None:
    """Make the machine answer these readings in order, the last one for ever after."""
    answers = list(readings)
    monkeypatch.setattr(
        type(commander),
        "memory_available_bytes",
        property(lambda self: answers.pop(0) if len(answers) > 1 else answers[0]),
    )


# --- The gauges: what the vertex reads as "the machine" ----------------------


def test_a_finite_cgroup_limit_takes_the_place_of_the_host_memory(commander, cgroup):
    cgroup(limit=str(LIMIT_BYTES), current=str(48 * MIB))

    gauges = commander._machine_memory_gauges()

    assert gauges["MemTotal"] == LIMIT_BYTES
    assert gauges["MemAvailable"] == 16 * MIB


def test_an_unlimited_cgroup_leaves_the_host_figures_alone(commander, cgroup, host_gauges):
    cgroup(limit="max", current=str(48 * MIB))

    assert commander._machine_memory_gauges() == host_gauges


def test_a_machine_with_no_cgroup_at_all_reads_exactly_what_it_read_before(
    commander, host_gauges
):
    assert commander._machine_memory_gauges() == host_gauges


@pytest.mark.parametrize("written", ["", "garbage", "0", "-1", "nan", "inf", "1.5"])
def test_a_limit_that_is_not_a_number_of_bytes_leaves_the_host_figures_alone(
    commander, cgroup, host_gauges, written
):
    cgroup(limit=written, current=str(48 * MIB))

    assert commander._machine_memory_gauges() == host_gauges


def test_a_charge_past_its_own_limit_leaves_no_room_at_all(commander, cgroup):
    cgroup(limit=str(LIMIT_BYTES), current=str(80 * MIB))

    assert commander._machine_memory_gauges()["MemAvailable"] == 0.0


@pytest.mark.parametrize("charge", [None, "", "garbage", "-1", "nan", "inf", "1.5"])
def test_a_limit_whose_charge_cannot_be_read_proves_no_room_at_all(commander, cgroup, charge):
    cgroup(limit=str(LIMIT_BYTES), current=charge)

    gauges = commander._machine_memory_gauges()

    assert gauges["MemTotal"] == LIMIT_BYTES
    assert gauges["MemAvailable"] == 0.0


def test_a_group_may_not_grow_into_a_container_whose_charge_cannot_be_read(
    commander, tmp_path, cgroup
):
    cgroup(limit=str(LIMIT_BYTES), current="garbage")
    group = build_group(commander, tmp_path, worker_max_number=4)
    with_workers(group, WorkerStub("standard_0001", rss_bytes=LIMIT_BYTES // 4))

    assert group.memory_occupied_percent < group.memory_max_percent
    assert not group._may_grow


def test_the_first_layout_that_answers_is_the_one_read(commander, cgroup):
    cgroup(limit=str(LIMIT_BYTES), current=str(48 * MIB), v1=True)

    assert commander._machine_memory_gauges()["MemTotal"] == LIMIT_BYTES


def test_a_limit_as_wide_as_the_machine_limits_nothing(commander, cgroup, host_gauges):
    cgroup(limit=str(int(host_gauges["MemTotal"])), current="0", v1=True)

    assert commander._machine_memory_gauges() == host_gauges


def test_the_concession_of_the_vertex_follows_the_cgroup_limit(tmp_path, cgroup):
    cgroup(limit=str(LIMIT_BYTES), current="0")
    commander = SpaCommander(tmp_path / "frozen_users", memory_max_percent=50.0)

    assert commander.memory_concession_bytes == LIMIT_BYTES // 2


def test_the_room_the_machine_has_left_is_the_cgroups_own(commander, cgroup):
    cgroup(limit=str(LIMIT_BYTES), current=str(60 * MIB))

    assert commander.memory_available_bytes == 4 * MIB


# --- The gates: what a birth must pass --------------------------------------


def test_a_group_with_room_for_one_more_worker_may_grow(commander, tmp_path, monkeypatch):
    group = build_group(commander, tmp_path, worker_max_number=4)
    with_workers(group, WorkerStub("standard_0001", rss_bytes=LIMIT_BYTES // 4))
    with_available(monkeypatch, commander, LIMIT_BYTES // 2)

    assert group._may_grow


def test_prefork_shared_pages_are_not_charged_once_per_worker(
    commander, tmp_path, monkeypatch
):
    group = build_group(commander, tmp_path, worker_max_number=6)
    with_workers(
        group,
        *(
            WorkerStub(
                f"standard_{index:04d}",
                rss_bytes=LIMIT_BYTES // 3,
                pss_bytes=LIMIT_BYTES // 100,
            )
            for index in range(1, 7)
        ),
    )
    with_available(monkeypatch, commander, LIMIT_BYTES // 2)

    assert group.memory_accounting_kind == "pss"
    assert group.memory_occupied_percent == pytest.approx(6.0, abs=0.01)
    assert group._may_grow


def test_a_worker_without_pss_keeps_the_conservative_rss_fallback(
    commander, tmp_path, monkeypatch
):
    group = build_group(commander, tmp_path, worker_max_number=4)
    with_workers(group, WorkerStub("standard_0001", rss_bytes=3 * LIMIT_BYTES // 4 + 1))
    with_available(monkeypatch, commander, LIMIT_BYTES)

    assert group.memory_accounting_kind == "rss_fallback"
    assert not group._may_grow


def test_a_quota_with_no_room_for_the_newborn_refuses_the_growth(
    commander, tmp_path, monkeypatch
):
    group = build_group(commander, tmp_path, worker_max_number=4)
    # Three quarters of the quota are held: what is left is exactly one ceiling,
    # and one more worker would fill the quota to the brim. The machine has room
    # to spare, so the quota alone answers.
    with_workers(
        group,
        WorkerStub("standard_0001", rss_bytes=LIMIT_BYTES // 2),
        WorkerStub("standard_0002", rss_bytes=LIMIT_BYTES // 4 + 1),
    )
    with_available(monkeypatch, commander, LIMIT_BYTES)

    assert not group._may_grow


def test_a_group_under_its_quota_may_not_grow_into_a_container_with_no_room(
    commander, tmp_path, monkeypatch
):
    group = build_group(commander, tmp_path, worker_max_number=4)
    with_workers(group, WorkerStub("standard_0001", rss_bytes=LIMIT_BYTES // 4))
    # The quota is a quarter used, so the group's own arithmetic says yes. What
    # the group does not see is that the commander and the templates have eaten
    # the container: a ceiling is 16 MiB and 4 MiB are free.
    with_available(monkeypatch, commander, 4 * MIB)

    assert group.memory_occupied_percent < group.memory_max_percent
    assert not group._may_grow


def test_a_vertex_that_is_not_running_refuses_the_growth(commander, tmp_path, monkeypatch):
    group = build_group(commander, tmp_path, worker_max_number=4)
    with_available(monkeypatch, commander, LIMIT_BYTES)
    commander.state = "saturated"

    assert not group._may_grow


def test_cpu_pressure_only_closes_admission_when_the_container_is_full(
    commander, tmp_path, monkeypatch
):
    group = build_group(commander, tmp_path, worker_max_number=4, cpu_admission_close_percent=70.0)
    worker = WorkerStub("standard_0001", rss_bytes=LIMIT_BYTES // 4, cpu_temperature_percent=90.0)
    with_workers(group, worker)
    with_available(monkeypatch, commander, 4 * MIB)

    group._judge_cpu_admission()

    assert len(group.worker_handler_map) == 1
    assert not worker.cpu_admission_open


def test_cpu_scan_never_consults_changing_memory_to_fork(
    commander, tmp_path, monkeypatch
):
    group = build_group(commander, tmp_path, worker_max_number=4, cpu_admission_close_percent=70.0)
    worker = WorkerStub("standard_0001", rss_bytes=LIMIT_BYTES // 4, cpu_temperature_percent=90.0)
    with_workers(group, worker)
    # Memory availability may change, but a CPU scan never starts a fork.
    with_available(monkeypatch, commander, LIMIT_BYTES, 4 * MIB)

    group._judge_cpu_admission()

    assert len(group.worker_handler_map) == 1
    assert not worker.cpu_admission_open


# --- worker_max_number: a divisor of the size, and no cap on the count -------


def six_light_workers(group: GroupHandler, rss_bytes: int) -> None:
    """Fill a group sized for six with six workers, each holding what is asked."""
    with_workers(
        group,
        *(WorkerStub(f"standard_{index:04d}", rss_bytes=rss_bytes) for index in range(1, 7)),
    )


def test_a_group_sized_for_six_takes_a_seventh_when_the_memory_is_there(
    commander, tmp_path, monkeypatch
):
    group = build_group(commander, tmp_path, worker_max_number=6)
    # Each of the six holds a hundredth of the concession, far under the sixth
    # of it each may hold: the quota is barely touched, and the machine has room.
    six_light_workers(group, rss_bytes=LIMIT_BYTES // 100)
    with_available(monkeypatch, commander, LIMIT_BYTES // 2)

    assert len(group.living_workers) == 6
    assert group._may_grow


def test_the_seventh_is_refused_by_the_quota_and_by_nothing_else(
    commander, tmp_path, monkeypatch
):
    group = build_group(commander, tmp_path, worker_max_number=6)
    # Now the same six hold 84% of the concession between them: one more ceiling
    # would not fit. The count of six had nothing to do with either answer.
    six_light_workers(group, rss_bytes=14 * LIMIT_BYTES // 100)
    with_available(monkeypatch, commander, LIMIT_BYTES // 2)

    assert not group._may_grow


def test_the_seventh_is_refused_by_the_machine_when_the_quota_says_yes(
    commander, tmp_path, monkeypatch
):
    group = build_group(commander, tmp_path, worker_max_number=6)
    six_light_workers(group, rss_bytes=LIMIT_BYTES // 100)
    with_available(monkeypatch, commander, MIB)

    assert group.memory_occupied_percent < group.memory_max_percent
    assert not group._may_grow
