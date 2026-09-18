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

"""The two questions, asked of a process born each of the two ways.

A spawned process is asked through the Popen its handler holds; a forked one
through nothing but its pid. Both answer ``alive`` and ``pid``, and only the
spawned one answers ``exit_code``, because only a parent can read a status.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

import pytest

from kajenn_orchestra.orchestration import ForkedProcess, SpawnedProcess, WorkerProcess

#: A child that waits to be killed, so a test can look at it while it is there.
PATIENT_CHILD = [sys.executable, "-c", "import time; time.sleep(30)"]

#: A pid that belongs to the system, not to us: signalling it is not permitted.
FOREIGN_PID = 1


def bury(worker_process: SpawnedProcess) -> None:
    """Kill it and wait until the answer changes; reading ``alive`` reaps it too."""
    os.kill(worker_process.pid, signal.SIGKILL)
    deadline = time.monotonic() + 10.0
    while worker_process.alive and time.monotonic() < deadline:
        time.sleep(0.01)


def test_the_two_questions_have_no_answer_of_their_own():
    process = WorkerProcess()

    with pytest.raises(NotImplementedError):
        process.alive
    with pytest.raises(NotImplementedError):
        process.pid


def test_a_spawned_process_answers_from_its_popen():
    popen = subprocess.Popen(PATIENT_CHILD)
    process = SpawnedProcess(popen)

    assert process.alive is True
    assert process.pid == popen.pid
    assert process.exit_code is None

    bury(process)

    assert process.alive is False
    assert process.exit_code is not None


def test_a_forked_process_answers_from_its_pid_alone():
    popen = subprocess.Popen(PATIENT_CHILD)
    process = ForkedProcess(popen.pid)

    assert process.alive is True
    assert process.pid == popen.pid
    assert not hasattr(process, "exit_code")

    os.kill(process.pid, signal.SIGKILL)
    popen.wait()

    assert process.alive is False


def test_a_forked_process_reads_a_zombie_as_alive():
    """The declared weakness, shown on purpose: this form waits for the reaping.

    A pid still holds a zombie until somebody collects it — the template, in the
    real machine. Until then a signal reaches it and this is the only answer the
    pid can give, which is why the WIRE stays the authoritative death.
    """
    popen = subprocess.Popen(PATIENT_CHILD)
    process = ForkedProcess(popen.pid)
    os.kill(process.pid, signal.SIGKILL)
    time.sleep(0.2)

    assert process.alive is True  # dead, unburied, and reading as alive

    popen.wait()  # the reaping a template would have done

    assert process.alive is False


def test_a_pid_that_is_no_longer_ours_reads_as_gone():
    assert ForkedProcess(FOREIGN_PID).alive is False
