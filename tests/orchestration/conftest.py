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

``worker_commander_lane`` is a live lane — a real worker on a real UDS under a real
commander — for the tests that exercise the site's verbs, which now place calls
on it. ``short_root`` is a temporary directory whose name fits inside the system's cap
on a UDS path — about a hundred characters, which pytest's own ``tmp_path`` is
already past, and the very reason worker names are short. ``repo_on_pythonpath``
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

from tests.orchestration.frame_helpers import control_frame, read_control

from kajenn.channel.frame import Frame
from kajenn_orchestra.orchestration import FreezeHandler, GroupHandler, SpaCommander, SpaWorker
from kajenn_orchestra.orchestration import spa_worker as spa_worker_module
from kajenn_orchestra.orchestration.worker_connector import (
    CALL_METHOD,
    ENVELOPE_SLOT_WORKER_EVENTS,
    REPLY_METHOD,
)
from kajenn_orchestra.orchestration.worker_handler import ANNOUNCE_OP_PATH, WorkerHandler

#: The name the lane's handler and worker share: short, because a UDS path is.
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
    """The handler's side of the wire, as a worker sees it: it keeps what is written.

    A CALL the worker places is answered at once with an empty result, on the
    worker's own loop, so ``call`` resolves the way it does on a live lane. Tests
    read each REPLY and each announcement off ``frames``.
    """

    class XT_Writer:
        """What ``exit_process`` closes: the transport under a real ``FrameStream``."""

        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    def __init__(self, worker: SpaWorker) -> None:
        self.worker = worker
        self.frames: list[Frame] = []
        self.writer = self.XT_Writer()

    async def write(self, frame: Frame) -> None:
        self.frames.append(frame)
        if frame.method == CALL_METHOD:
            self.worker.handle_frame(
                control_frame(id=frame.id, method=REPLY_METHOD, path=frame.path, data={"result": {}})
            )

    def replies(self) -> list[Frame]:
        return [frame for frame in self.frames if frame.method == REPLY_METHOD]

    def calls(self, path: str) -> list[Frame]:
        return [
            frame for frame in self.frames if frame.method == CALL_METHOD and frame.path == path
        ]

    def reply_to(self, frame_id: str) -> Frame | None:
        return next((frame for frame in self.replies() if frame.id == frame_id), None)

    def announced_events(self) -> list[dict]:
        """Every worker event the announcements carried, in order."""
        return [
            event
            for frame in self.calls(ANNOUNCE_OP_PATH)
            for event in read_control(frame)[ENVELOPE_SLOT_WORKER_EVENTS]
        ]


def attach_wire(worker: SpaWorker) -> XT_Wire:
    """Give a worker a stub wire and a slot: the shape a unit test drives it in.

    The stream is set directly and not through ``attach_stream``, which reads the
    running loop: a sync fixture has none, and the stub needs none.
    """
    wire = XT_Wire(worker)
    worker.stream = wire
    worker.open_request_slot()
    return wire


class XT_WorkerCommanderLane:
    """A worker and its real handler on one UDS, the commander above it.

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
        self._reader_task = None

    async def open(self) -> None:
        """Bind, connect, present, and put the worker's read loop on the air."""
        connector = self.worker_handler.connector
        await connector.start()
        reader, writer = await asyncio.open_unix_connection(str(connector.socket_path))
        self.worker.attach_stream(spa_worker_module.FrameStream(reader, writer))
        await self.worker.send_presentation({})
        self._reader_task = asyncio.create_task(self.worker.receive_frames())
        await connector.wait_connected()
        # A request slot on both sides a test drives verbs from: the request
        # thread, and the loop context the fixture is built in.
        await self.open_request()
        self.worker.open_request_slot()

    async def close(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        self.request_pool.shutdown(wait=True)
        self.worker.exit_process()
        await self.worker_handler.connector.stop()
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
