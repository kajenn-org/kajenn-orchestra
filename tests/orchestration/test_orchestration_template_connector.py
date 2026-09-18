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

"""A group's template, from its first line to a worker forked out of it.

The last test is the whole chain in one: a real template process builds a real
engine, a real handler asks it for a worker, and the child that comes out is a
real worker presenting itself on its handler's socket, with the engine already
in its hands.
"""

from __future__ import annotations

import json

import pytest

from kajenn_orchestra.orchestration import ForkedProcess, WorkerHandler
from kajenn_orchestra.orchestration.template_connector import (
    TemplateConnector,
    TemplateRefused,
)

from .conftest import kill_process, wait_for
from .group_stub import GroupStub

GROUP = "standard"
WORKER_NAME = "standard_0001"
FACTORY = "tests.orchestration.engine_stub:EngineFactory"
BROKEN_FACTORY = "tests.orchestration.engine_stub:BrokenFactory"
ENGINE_WORKER = "tests.orchestration.engine_stub:EngineWorker"


@pytest.fixture
async def group(short_root):
    """A group under a real vertex, with no template of its own yet."""
    return GroupStub(short_root / "frozen_users", name=GROUP)


@pytest.fixture
async def template(group, repo_on_pythonpath):
    """A connector to a real template process; it does not outlive the test."""
    connector = TemplateConnector(
        group, engine_factory=FACTORY, engine_kwargs={"mark": GROUP}
    )
    yield connector
    await connector.stop()


def test_the_first_line_says_who_the_template_is_and_what_to_build(template):
    assert template.launch_payload == {
        "name": f"template-{GROUP}",
        "engine_factory": FACTORY,
        "kwargs": {"mark": GROUP},
    }


async def test_no_template_is_launched_before_the_first_worker_is_asked_for(template):
    assert template.alive is False
    assert template.process is None


async def test_a_factory_that_cannot_build_takes_the_template_with_it(group, repo_on_pythonpath):
    connector = TemplateConnector(group, engine_factory=BROKEN_FACTORY)

    with pytest.raises(TemplateRefused, match="its pipe ended"):
        await connector.fork_worker({"name": WORKER_NAME})

    await connector.stop()


async def test_a_template_that_died_is_launched_again_by_the_next_request(
    template, short_root, tmp_path
):
    payload = {
        "name": WORKER_NAME,
        "uds_url": f"uds:{short_root}/nobody.sock",
        "frozen_users_path": str(short_root / "frozen_users"),
        "worker_class": ENGINE_WORKER,
        "kwargs": {"group": GROUP, "report_path": str(tmp_path / "first.json")},
    }
    await template.fork_worker(payload)
    born = template.process.pid

    kill_process(template.process)
    await wait_for(lambda: not template.alive)
    payload["kwargs"]["report_path"] = str(tmp_path / "second.json")
    await template.fork_worker(payload)

    assert template.alive is True
    assert template.process.pid != born


async def test_the_worker_forked_off_a_template_serves_with_the_engine_in_hand(
    short_root, tmp_path, repo_on_pythonpath
):
    report = tmp_path / "what_the_worker_got.json"
    group = GroupStub(short_root / "frozen_users", name=GROUP)
    group.template = TemplateConnector(
        group, engine_factory=FACTORY, engine_kwargs={"mark": GROUP}
    )
    handler = WorkerHandler(
        group,
        WORKER_NAME,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module="kajenn_orchestra.orchestration.worker_entry",
        worker_class=ENGINE_WORKER,
        worker_kwargs={"group": GROUP, "report_path": str(report)},
        process_ping_timeout=30.0,
    )
    try:
        await handler.launch_process()

        # Born by fork, not spawned: its handler holds a pid and nothing else.
        assert isinstance(handler.process, ForkedProcess)
        assert handler.state == "running"
        assert handler.process.alive is True

        # And it is a worker of its own, with the engine its template built.
        got = json.loads(report.read_text())
        assert got["worker"] == WORKER_NAME
        assert got["engine"] == f"engine-{GROUP}"
        assert got["pid"] == handler.process.pid
        assert got["pid"] != group.template.process.pid
    finally:
        if handler.process is not None:
            kill_process(handler.process)
        await handler.connector.stop()
        await group.template.stop()


async def test_a_build_that_prints_cannot_dirty_the_answer_channel(
    group, short_root, tmp_path, repo_on_pythonpath
):
    # The bridge's real build prints on stdout. Pre-fix those lines were read
    # as answers: the FIRST forks failed with «Extra data» and the true
    # answers lagged behind by as many lines. Now the channel is the
    # template's own duplicate and every print goes to the logs: the first
    # answer is immediate, and each answer carries the pid of ITS OWN fork.
    connector = TemplateConnector(
        group,
        engine_factory="tests.orchestration.engine_stub:NoisyFactory",
        engine_kwargs={"mark": GROUP},
    )
    try:
        pids = []
        for tag in ("first", "second"):
            report = tmp_path / f"{tag}.json"
            pid = await connector.fork_worker(
                {
                    "name": f"{GROUP}_{tag}",
                    "uds_url": f"uds:{short_root}/nobody.sock",
                    "frozen_users_path": str(short_root / "frozen_users"),
                    "worker_class": ENGINE_WORKER,
                    "kwargs": {"group": GROUP, "report_path": str(report)},
                }
            )
            await wait_for(report.exists)
            got = json.loads(report.read_text())
            assert got["pid"] == pid  # the answer of THIS fork, not a stale one
            assert got["engine"] == f"engine-{GROUP}"
            pids.append(pid)
        assert len(set(pids)) == 2
    finally:
        await connector.stop()


async def test_a_living_child_does_not_mask_the_templates_death(
    short_root, tmp_path, repo_on_pythonpath
):
    # The child closes ITS copy of the answer channel: with the template gone,
    # the GroupHandler's read meets EOF even while a forked worker still
    # serves — a child holding the write end open would hang that read forever.
    import asyncio

    group = GroupStub(short_root / "frozen_users", name=GROUP)
    group.template = TemplateConnector(
        group, engine_factory=FACTORY, engine_kwargs={"mark": GROUP}
    )
    handler = WorkerHandler(
        group,
        WORKER_NAME,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module="kajenn_orchestra.orchestration.worker_entry",
        worker_class=ENGINE_WORKER,
        worker_kwargs={"group": GROUP, "report_path": str(tmp_path / "child.json")},
        process_ping_timeout=30.0,
    )
    try:
        await handler.launch_process()
        assert handler.process.alive is True

        template_process = group.template.process
        template_process.stdin.close()
        raw = await asyncio.wait_for(template_process.stdout.readline(), timeout=10.0)

        assert raw == b""  # EOF: nobody held the write end, the child included
        assert handler.process.alive is True
    finally:
        if handler.process is not None:
            kill_process(handler.process)
        await handler.connector.stop()
        await group.template.stop()
