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

"""The common ground of the orchestration tests: the root, the path, the wait.

``worker_commander_lane`` is a live lane — a real worker on a real group bus under a
real commander — for the tests that exercise the site's verbs, which now place calls
on it. ``short_root`` is a temporary directory whose name fits inside the system's cap
on a UDS path — about a hundred characters, which pytest's own ``tmp_path`` is
already past, and the very reason group names are short. ``repo_on_pythonpath``
puts this repository where a spawned child can import the stub modules of the
test package from. ``wait_for`` polls a condition instead of sleeping a guessed
amount: a process dying, a wire ending, a round landing — none has a duration.
"""

from __future__ import annotations

import asyncio
import functools
import os
import shutil
import signal
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tests.orchestration.message_helpers import incoming, join_worker, read_control

from kajenn_orchestra.orchestration import FreezeHandler, GroupHandler, SpaCommander, SpaWorker
from kajenn_orchestra.orchestration.lane_payload import LanePayload
from kajenn_orchestra.orchestration.worker_connector import (
    ENVELOPE_SLOT_WORKER_EVENTS,
    GROUP_MEMBER,
)
from kajenn_orchestra.orchestration.worker_handler import ANNOUNCE_OP_PATH, WorkerHandler

#: The name the lane's handler and worker share.
WORKER_NAME = "standard_0001"


@pytest.fixture
def short_root():
    """A temporary root short enough for a socket path; it dies with the test."""
    root = Path(tempfile.mkdtemp(prefix="gnrorch_"))
    yield root
    shutil.rmtree(root, ignore_errors=True)


def kill_process(worker_process) -> None:
    """SIGKILL one worker's process by its pid, the way the handler itself does.

    The tests reach a process through the two questions its handler asks — ``alive``
    and ``pid`` — and never through the Popen underneath, because a forked worker
    has none. Waiting for the death is the caller's next line: ``await wait_for(
    lambda: not handler.process.alive)``.

    One already gone is the same outcome, as it is for the handler's own kill.
    """
    try:
        os.kill(worker_process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


async def wait_for(condition, timeout: float = 10.0) -> None:
    """Poll until the condition holds, or give up loudly at the deadline."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("the machine never reached the awaited state")
        await asyncio.sleep(0.01)


@pytest.fixture
def repo_on_pythonpath(monkeypatch):
    """Let a spawned child import the test package: this repository on its path."""
    repo_root = Path(__file__).resolve().parents[2]
    inherited = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv(
        "PYTHONPATH", os.pathsep.join([str(repo_root), inherited]).rstrip(os.pathsep)
    )


class XT_Wire:
    """The group's end of the bus, as a worker sees it: it keeps what is sent.

    It stands as the worker's ``member``. A call the worker places is kept and
    answered at once with an empty result, the way a live group answers it.
    ``serve`` hands the worker one call of the group's the way kbus does — on a
    task of its own — and gives back the reply its answer lands on. Tests read
    each answer and each announcement off ``replies`` and ``placed``.
    """

    class XT_Reply:
        """Where one call's answer lands: ``message``, once it is sent."""

        def __init__(self, wire) -> None:
            self.wire = wire
            self.message = None

        async def send(self, message) -> None:
            if self.message is not None:
                raise RuntimeError("call already answered")
            self.message = message
            self.wire.replies.append(message)

    def __init__(self, worker: SpaWorker) -> None:
        self.worker = worker
        #: Never connected: ``exit_process`` has no connection to close.
        self.connection = None
        self.placed: list[tuple[str, object]] = []
        self.replies: list[object] = []
        self._tasks: set[asyncio.Task] = set()

    async def call(self, route: str, message):
        self.placed.append((route, message))
        return LanePayload().control_message({"result": {}})

    def serve(self, path: str, data=None) -> "XT_Wire.XT_Reply":
        reply = self.XT_Reply(self)
        task = asyncio.get_running_loop().create_task(
            self.worker.serve_call(incoming(path, data), reply)
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return reply

    def calls(self, path: str) -> list:
        return [message for route, message in self.placed if route == f"{GROUP_MEMBER}.{path}"]

    def announced_events(self) -> list[dict]:
        """Every worker event the announcements carried, in order."""
        return [
            event
            for message in self.calls(ANNOUNCE_OP_PATH)
            for event in read_control(message)[ENVELOPE_SLOT_WORKER_EVENTS]
        ]


def attach_wire(worker: SpaWorker) -> XT_Wire:
    """Give a worker a stub member and a slot: the shape a unit test drives it in.

    The member is set directly and not through ``attach_member``, which reads the
    running loop: a sync fixture has none, and the stub needs none.
    """
    wire = XT_Wire(worker)
    worker.member = wire
    worker.open_request_slot()
    return wire


class XT_WorkerCommanderLane:
    """A worker and its real handler on the group's bus, the commander above it.

    Args:
        commander: the vertex the lane calls reach.
        group: the group the handler hangs under.
        freeze_handler: the deposit the worker is built with.
        worker_name: the name shared by the handler and the worker.

    The site's verbs are served on a traffic-pool thread, and a request IS a
    thread here too: ``verb`` runs them on ONE dedicated thread, so everything a
    test does belongs to the same request slot, and ``open_request`` starts a new
    one on it.
    """

    def __init__(self, commander, group, freeze_handler, worker_name=WORKER_NAME) -> None:
        self.commander = commander
        self.worker_name = worker_name
        self.group = group
        self.worker_handler = WorkerHandler(group, worker_name, **group.worker_settings)
        group.worker_handler_map[worker_name] = self.worker_handler
        self.worker = SpaWorker(
            worker_name, freeze_handler=freeze_handler, deposit_lock_retry_interval=0.01
        )
        self.request_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="xt-request")

    async def open(self) -> None:
        """Mint the URL, join the bus, present."""
        connector = self.worker_handler.connector
        await connector.start()
        await join_worker(self.worker, connector.url)
        await self.worker.send_presentation({})
        await connector.wait_connected()
        # A request slot on both sides a test drives verbs from: the request
        # thread, and the loop context the fixture is built in.
        await self.open_request()
        self.worker.open_request_slot()

    async def close(self) -> None:
        self.request_pool.shutdown(wait=True)
        self.worker_handler.connector.stop()
        self.worker.exit_process()
        await self.worker.member.connected.wait_closed()
        self.group.worker_handler_map.pop(self.worker_name, None)

    async def verb(self, name, *args, **kwargs):
        """Call one of the worker's site verbs where the site calls it: off the loop.

        What the verb announced is sent up at once: on a live lane a verb runs
        inside a CALL and its events ride that CALL's reply, and here nothing
        answers the request thread, so the lane plays the reply's part.
        """
        answer = await asyncio.get_running_loop().run_in_executor(
            self.request_pool, functools.partial(getattr(self.worker, name), *args, **kwargs)
        )
        await self.announce()
        return answer

    async def verb_on(self, pool, name, *args, **kwargs):
        """Call a site verb on ANOTHER request thread than ``verb``'s.

        A thread has a request slot of its own, so a test that needs two
        concurrent requests on one worker runs the second one here, on a pool of
        its own; the first verb on that pool opens its slot.
        """
        worker = self.worker

        def in_own_slot():
            try:
                worker.request_slot
            except RuntimeError:
                worker.open_request_slot()
            return getattr(worker, name)(*args, **kwargs)

        return await asyncio.get_running_loop().run_in_executor(pool, in_own_slot)

    async def open_request(self) -> None:
        """Start a fresh request on the thread the verbs run on."""
        await asyncio.get_running_loop().run_in_executor(
            self.request_pool, self.worker.open_request_slot
        )

    async def announce(self) -> None:
        """Send the vertex what the verbs announced, on the request thread and here.

        No CALL answers either context in a test, so their events would never
        leave: this takes them off both slots and sends them up the worker's own
        channel, the way the transfer cycle does for its own.
        """
        worker = self.worker

        def take_events() -> list:
            events = list(worker.worker_events)
            worker.worker_events.clear()
            return events

        events = await asyncio.get_running_loop().run_in_executor(self.request_pool, take_events)
        events += take_events()
        if events:
            await worker.announce_worker_events(events)


@pytest.fixture
async def worker_commander_lane(short_root, tmp_path):
    """A live lane: the site's verbs on a worker, a real handler on a real commander."""
    commander = SpaCommander(short_root / "frozen_users")
    group = GroupHandler(
        commander,
        "standard",
        memory_concession_bytes=8 * 1024 * 1024 * 1024,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module="never.launched",
    )
    lane = XT_WorkerCommanderLane(commander, group, FreezeHandler(tmp_path / "frozen_users"))
    await lane.open()
    yield lane
    await lane.close()
    await group.bus.close()
