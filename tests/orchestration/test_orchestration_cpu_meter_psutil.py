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

"""The worker CPU clock is read through psutil, on every platform.

Implementation tests. One reading is ``(create_time, cpu seconds)``; a process
psutil cannot see is an absent reading, never a zero.
"""

from __future__ import annotations

from types import SimpleNamespace

import psutil

from kajenn_orchestra.orchestration import worker_handler as worker_handler_module
from kajenn_orchestra.orchestration.worker_handler import WorkerHandler


class ProcessDouble:
    """A live process identity for a handler whose clock is under test."""

    def __init__(self, pid: int) -> None:
        self.pid = pid


class ProbeDouble:
    """What ``psutil.Process`` answers for one pid."""

    instances: list[ProbeDouble] = []

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.user = 1.25
        self.system = 0.25
        self.born = 900.5
        ProbeDouble.instances.append(self)

    def cpu_times(self):
        return SimpleNamespace(user=self.user, system=self.system)

    def create_time(self) -> float:
        return self.born


class VanishedProbe(ProbeDouble):
    """A pid whose process is gone by the time it is asked."""

    def cpu_times(self):
        raise psutil.NoSuchProcess(self.pid)


def handler_double() -> WorkerHandler:
    """One handler without a wire: only its process clock belongs to this story."""
    group = SimpleNamespace()
    group.envelope_handler = lambda envelope: envelope
    group.cpu_admission_close_percent = None
    handler = WorkerHandler(
        group,
        "standard_0001",
        instance_dir="/tmp",
        frozen_users_path="/tmp",
        entry_module="unused",
    )
    handler.process = ProcessDouble(4242)
    return handler


def test_one_reading_is_the_process_birth_and_its_cpu_seconds(monkeypatch):
    ProbeDouble.instances.clear()
    monkeypatch.setattr(worker_handler_module.psutil, "Process", ProbeDouble)
    handler = handler_double()

    assert handler.get_process_cpu_reading() == (900.5, 1.5)


def test_the_probe_is_built_once_per_pid(monkeypatch):
    ProbeDouble.instances.clear()
    monkeypatch.setattr(worker_handler_module.psutil, "Process", ProbeDouble)
    handler = handler_double()

    handler.get_process_cpu_reading()
    handler.get_process_cpu_reading()
    assert len(ProbeDouble.instances) == 1

    handler.process = ProcessDouble(4243)
    handler.get_process_cpu_reading()
    assert [probe.pid for probe in ProbeDouble.instances] == [4242, 4243]


def test_a_vanished_process_is_an_absent_reading(monkeypatch):
    ProbeDouble.instances.clear()
    monkeypatch.setattr(worker_handler_module.psutil, "Process", VanishedProbe)
    handler = handler_double()

    assert handler.get_process_cpu_reading() is None


def test_no_process_means_no_reading(monkeypatch):
    ProbeDouble.instances.clear()
    monkeypatch.setattr(worker_handler_module.psutil, "Process", ProbeDouble)
    handler = handler_double()
    handler.process = None

    assert handler.get_process_cpu_reading() is None
    assert ProbeDouble.instances == []


def test_the_process_table_parser_is_gone():
    assert not hasattr(worker_handler_module, "PROCESS_STAT_ROOT")
    assert not hasattr(worker_handler_module, "PROCESS_CLOCK_TICKS")
