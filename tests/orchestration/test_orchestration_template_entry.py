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

"""The template's whole contract: the first line, the refusals, and one real fork.

Everything here drives ``TemplateEntry`` on paper pipes, which is exactly what it
was given: a line source and somewhere to answer. The last test forks for real —
the child writes what it found and leaves with ``os._exit``, because a pytest
process must not be run twice.
"""

from __future__ import annotations

import gc
import io
import json
import os
import signal
import threading
import time
from typing import Any

import pytest

from kajenn_orchestra.orchestration import TemplateEntry

@pytest.fixture(autouse=True)
def sigchld_put_back():
    """Give ``SIGCHLD`` back to whoever had it, and thaw what was frozen.

    ``serve`` installs the reaper on the PROCESS, and here that process is the
    test runner: left in place it would collect every later test's children, and
    a Popen whose child somebody else buried does not behave.
    """
    had = signal.getsignal(signal.SIGCHLD)
    yield
    signal.signal(signal.SIGCHLD, had)
    # And ``serve`` freezes the heap of the PROCESS too, which here is the runner's:
    # left frozen, no later test's garbage would ever be collected.
    gc.unfreeze()


TEMPLATE_NAME = "template-standard"
WORKER_NAME = "standard_0001"


class EngineFactory:
    """The seam the deployment fills: a class asked once for the group's engine."""

    def __init__(self, mark: str = "plain") -> None:
        self.mark = mark

    def build_group_engine(self) -> str:
        """The engine, played here by a string the tests can recognise."""
        return f"engine-{self.mark}"


FACTORY = f"{__name__}:EngineFactory"


def launch_line(**overrides: Any) -> str:
    """One launch line, with whatever a test wants different in it."""
    config: dict[str, Any] = {"name": TEMPLATE_NAME, "engine_factory": FACTORY}
    config.update(overrides)
    return json.dumps(config) + "\n"


def fork_line(name: str = WORKER_NAME) -> str:
    """One fork request: a worker's spawn payload, cut to what these tests read back."""
    return json.dumps(
        {"name": name, "uds_url": "uds:/nowhere.sock", "frozen_users_path": "/nowhere"}
    ) + "\n"


def template_on(lines: str) -> tuple[TemplateEntry, io.StringIO]:
    """A template reading those lines, and the paper it answers on."""
    answers = io.StringIO()
    return TemplateEntry(pipe_in=io.StringIO(lines), pipe_out=answers), answers


def answers_of(paper: io.StringIO) -> list[dict[str, Any]]:
    """The answer lines, parsed."""
    return [json.loads(line) for line in paper.getvalue().splitlines()]


# ----------------------------------------------------------------------
# The first line
# ----------------------------------------------------------------------


def test_the_launch_line_builds_the_engine_of_the_group():
    template, _ = template_on(launch_line(kwargs={"mark": "standard"}))

    template.run()

    assert template.name == TEMPLATE_NAME
    assert template.group_engine == "engine-standard"


def test_what_the_template_built_is_frozen_before_any_fork():
    template, _ = template_on(launch_line())

    template.run()

    assert gc.get_freeze_count() > 0
    # And what comes after the freeze is the collector's again, which is what a
    # child allocates while it serves.
    watched = len(gc.get_objects())
    fresh = [[i] for i in range(100)]
    assert len(gc.get_objects()) >= watched + len(fresh)


def test_a_launch_line_without_a_factory_ends_the_process():
    template, _ = template_on(json.dumps({"name": TEMPLATE_NAME}) + "\n")

    with pytest.raises(SystemExit, match="engine_factory"):
        template.run()


def test_a_launch_line_that_is_not_a_json_object_ends_the_process():
    template, _ = template_on("[1, 2, 3]\n")

    with pytest.raises(SystemExit, match="must be a JSON object"):
        template.run()


def test_a_launch_line_that_is_not_json_ends_the_process():
    template, _ = template_on("not json at all\n")

    with pytest.raises(SystemExit, match="not valid JSON"):
        template.run()


def test_a_pipe_that_closes_before_the_launch_line_ends_the_process():
    template, _ = template_on("")

    with pytest.raises(SystemExit, match="closed before the launch line"):
        template.run()


def test_a_factory_that_is_not_a_reference_is_refused():
    template, _ = template_on(launch_line(engine_factory="some.module.EngineFactory"))

    with pytest.raises(SystemExit, match="module.path:ClassName"):
        template.run()


# ----------------------------------------------------------------------
# Serving, and refusing
# ----------------------------------------------------------------------


def test_the_pipe_ending_takes_the_template_out():
    template, answers = template_on(launch_line())

    assert template.run() == 0
    assert answers_of(answers) == []


def test_the_invariant_reads_the_threads_of_this_process():
    template, _ = template_on(launch_line())

    assert template.live_thread_count == threading.active_count()


def test_a_second_thread_refuses_the_fork_out_loud():
    class NeverForks(TemplateEntry):
        """A template with company, which records the birth it must never reach."""

        births: list[dict[str, Any]] = []

        @property
        def live_thread_count(self) -> int:
            return 2

        def live_as_worker(self, payload: dict[str, Any]) -> None:
            self.births.append(payload)

    answers = io.StringIO()
    template = NeverForks(
        pipe_in=io.StringIO(launch_line() + fork_line()), pipe_out=answers
    )

    template.run()

    answer = answers_of(answers)[0]
    assert "2 threads alive" in answer["error"]
    assert TEMPLATE_NAME in answer["error"]
    assert NeverForks.births == []


# ----------------------------------------------------------------------
# One real fork
# ----------------------------------------------------------------------


def test_the_forked_child_takes_its_own_session_and_finds_payload_and_engine(tmp_path):
    report = tmp_path / "what_the_child_found.json"

    class ReportingTemplate(TemplateEntry):
        """A template whose child says what it found instead of serving.

        ``become_worker`` is the seam: everything before it — the fork, the session,
        the pipes — is the real thing under test. The thread count is declared
        because a test runner is never a one-thread process, which a template is.
        """

        @property
        def live_thread_count(self) -> int:
            return 1

        def become_worker(self, payload: dict[str, Any]) -> None:
            report.write_text(
                json.dumps(
                    {
                        "worker": payload["name"],
                        "engine": self.group_engine,
                        "session": os.getsid(0),
                        "pipes_closed": self.pipe_in.closed and self.pipe_out.closed,
                    }
                )
            )
            os._exit(0)

    answers = io.StringIO()
    template = ReportingTemplate(
        pipe_in=io.StringIO(launch_line() + fork_line()), pipe_out=answers
    )
    template.run()

    pid = answers_of(answers)[0]["pid"]
    assert pid > 0
    deadline = time.monotonic() + 10.0
    while not report.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    found = json.loads(report.read_text())

    assert found["worker"] == WORKER_NAME
    assert found["engine"] == "engine-plain"
    assert found["pipes_closed"] is True
    assert found["session"] != os.getsid(0)
