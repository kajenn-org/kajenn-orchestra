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

"""WorkerHandler tests: real child processes, a real bus, real kills.

The handler governs an operating-system process, so every test here spawns one: a
scripted child written to disk at test time (``CHILD_SCRIPT``), started by the
handler itself as ``python -m <module>`` and configured by the very payload the
handler puts in ``KAJENN_WORKER``. The child joins the group's bus with the URL
of that payload, and answers every call with a photo, or stays mute on purpose,
or never shows up at all — the three behaviours the surveillance has to tell
apart.

The bus socket lives under a short ``mkdtemp`` root: the system caps a UDS path
at about a hundred characters and pytest's own directory is already past it.

``GroupStub`` is the level above, shared with the other tests of this package: the
handler asks it for the wake it rings when a process of its own has ended, and for
the layer of the chain everything the process says climbs through. The tests read
the wake and the ``state`` the group would read at that round.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import pytest


from kajenn_orchestra.orchestration.worker_connector import (
    ENVELOPE_SLOT_PRESENTATION,
    ENVELOPE_SLOT_WORKER_SNAPSHOT,
)
from kajenn_orchestra.orchestration import WorkerHandler
from kajenn_orchestra.orchestration import worker_handler as worker_handler_module
from .group_stub import GroupStub
from .conftest import kill_process, wait_for
from kajenn_orchestra.orchestration.worker_handler import (
    PING_OP_PATH,
    QUIT_OP_PATH,
    WORKER_ENV_VAR,
)

CHILD_SCRIPT = '''
"""A scripted worker process: presents itself, answers with a photo, leaves when asked."""

import asyncio
import json
import os

from tests.orchestration.message_helpers import join_bus, present

from kajenn_orchestra.orchestration.lane_payload import LanePayload


def photo_of(payload):
    return {{"pid": os.getpid(), "asked_on": "presentation", "rss_mb": 42}}


async def live() -> None:
    payload = json.loads(os.environ["{env_var}"])
    behaviour = payload["kwargs"].get("behaviour", "answer")
    if behaviour == "absent":
        await asyncio.sleep(60)
        return
    leaving = asyncio.Event()

    async def serve(message, reply):
        if behaviour == "mute":
            await asyncio.Event().wait()
        path = message.meta["route"]
        photo = {{"pid": os.getpid(), "asked_on": path, "rss_mb": 42}}
        await reply.send(LanePayload().control_message(
            {{"result": {{"answered": path}}, "{snapshot_key}": photo}}
        ))
        # Asked to leave, it leaves — after answering, like a real worker:
        # the answer is what carries the photo of everybody departing. Unless
        # it is the one that answers and then stays, which is what a
        # departure nobody can wait for any longer looks like.
        if path == "{quit_path}" and behaviour == "answer":
            leaving.set()

    member = await join_bus(payload["kbus_url"], serve)
    await present(member, {{"pid": os.getpid(), "config": payload, "{snapshot_key}": photo_of(payload)}})
    ended = asyncio.ensure_future(member.connected.wait_closed())
    left = asyncio.ensure_future(leaving.wait())
    await asyncio.wait({{ended, left}}, return_when=asyncio.FIRST_COMPLETED)
    await member.close()


asyncio.run(live())
'''.format(
    env_var=WORKER_ENV_VAR, snapshot_key=ENVELOPE_SLOT_WORKER_SNAPSHOT, quit_path=QUIT_OP_PATH
)

CHILD_MODULE = "scripted_child"


@pytest.fixture
def group(instance_root):
    return GroupStub(instance_root / "frozen_users")


@pytest.fixture
def instance_root(monkeypatch):
    """A short root holding the sockets, the deposit and the scripted child."""
    root = Path(tempfile.mkdtemp(prefix="gnrwh_"))
    (root / f"{CHILD_MODULE}.py").write_text(CHILD_SCRIPT)
    inherited = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(root), inherited]).rstrip(os.pathsep))
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
async def make_handler(instance_root, group):
    """Build handlers, and let no process or socket of theirs outlive the test."""
    handlers: list[WorkerHandler] = []

    def build(
        name: str = "standard_0001",
        behaviour: str = "answer",
        process_ping_timeout: float = 10.0,
        **kwargs: Any,
    ):
        handler = WorkerHandler(
            group,
            name,
            instance_dir=instance_root / "i",
            frozen_users_path=instance_root / "frozen_users",
            entry_module=CHILD_MODULE,
            worker_kwargs={"behaviour": behaviour},
            # Wide at birth: the same value bounds the wait for the presentation,
            # and a fresh interpreter on a loaded machine can take a second to
            # get there. The mute scenarios tighten it to what they measure.
            process_ping_timeout=process_ping_timeout,
            **kwargs,
        )
        handlers.append(handler)
        group.worker_handler = handler
        return handler

    yield build
    for handler in handlers:
        handler.connector.stop()
        if handler.process is not None:
            kill_process(handler.process)
            await wait_for(lambda: not handler.process.alive)
    await group.bus.close()


async def test_the_spawn_payload_carries_the_child_whole_configuration(make_handler, instance_root):
    handler = make_handler(
        main_threadpool_size=8,
        aux_threadpool_size=2,
        worker_class="kajenn_orchestra.orchestration.spa_worker:SpaWorker",
    )
    await handler.connector.start()
    token = handler.group_handler.bus.secrets["standard_0001"]

    assert handler.spawn_payload == {
        "name": "standard_0001",
        "kbus_url": f"unix://standard_0001:{token}@{instance_root / 'bus' / 'standard.sock'}",
        "frozen_users_path": str(instance_root / "frozen_users"),
        "main_threadpool_size": 8,
        "aux_threadpool_size": 2,
        "worker_class": "kajenn_orchestra.orchestration.spa_worker:SpaWorker",
        "kwargs": {"behaviour": "answer"},
    }


async def test_the_chain_composes_nothing_to_send_back_down(make_handler, group):
    handler = make_handler()

    assert handler.read_envelope({ENVELOPE_SLOT_PRESENTATION: 4242}) == {}
    assert handler.read_envelope({}) == {}


async def test_a_launched_process_presents_itself_on_the_handlers_socket(make_handler, group):
    handler = make_handler()
    assert handler.state == "starting"

    await handler.launch_process()

    assert handler.process.alive
    assert handler.connector.connected is True
    assert group.bus_path.exists()
    assert handler.state == "running"


async def test_the_photo_arrives_with_the_presentation_and_every_envelope_after(make_handler):
    handler = make_handler()
    await handler.launch_process()

    assert handler.worker_snapshot == {
        "pid": handler.process.pid,
        "asked_on": "presentation",
        "rss_mb": 42,
    }

    await handler.ping_process()

    assert handler.worker_snapshot == {
        "pid": handler.process.pid,
        "asked_on": "/group/ping",
        "rss_mb": 42,
    }
    assert PING_OP_PATH == "/group/ping"


async def test_the_beat_gives_back_what_the_process_answered(make_handler):
    handler = make_handler()
    await handler.launch_process()

    answered = await handler.ping_process()

    assert answered["result"] == {"answered": PING_OP_PATH}
    assert answered[ENVELOPE_SLOT_WORKER_SNAPSHOT]["pid"] == handler.process.pid


async def test_a_mute_process_is_killed_after_one_repeated_beat(make_handler, group, caplog):
    handler = make_handler(behaviour="mute")
    await handler.launch_process()
    handler.process_ping_timeout = 1.0
    process = handler.process

    with caplog.at_level("WARNING"):
        answered = await handler.ping_process()

    assert answered is None
    assert "beat 1 of 2 unanswered" in caplog.text
    assert "beat 2 of 2 unanswered" in caplog.text
    assert not process.alive
    assert handler.process is None
    assert handler.worker_snapshot["pid"] == process.pid
    await wait_for(lambda: group.wakes == ["aborted"])


async def test_a_wild_death_wakes_the_group_with_the_state_that_says_so(make_handler, group):
    handler = make_handler()
    await handler.launch_process()
    handler.hosted_users.update({"mario", "anna"})

    kill_process(handler.process)
    await wait_for(lambda: len(group.wakes) == 1)

    assert group.wakes == ["aborted"]
    assert handler.state == "aborted"
    assert handler.hosted_users == {"mario", "anna"}
    assert handler.connector.connected is False


async def test_a_bare_termination_is_an_abort_nobody_was_waiting_for(make_handler, group):
    handler = make_handler()
    await handler.launch_process()

    await handler.terminate_process()

    assert handler.process is None
    await wait_for(lambda: group.wakes == ["aborted"])


async def test_the_process_asked_to_leave_leaves_and_the_state_says_it_was_ordered(
    make_handler, group
):
    handler = make_handler()
    await handler.launch_process()
    leaving = handler.process

    await handler.quit_process()

    # The end of the WIRE is what the order waits for — the process itself is
    # already on its way out, and the OS catches up an instant later.
    assert handler.state == "quitted"
    await wait_for(lambda: not leaving.alive)
    assert group.wakes == ["quitted"]
    assert handler.connector.connected is False


async def test_the_state_says_quitting_from_the_order_on_not_from_the_death(make_handler):
    handler = make_handler(behaviour="stay")
    await handler.launch_process()

    leaving = asyncio.ensure_future(handler.quit_process())
    await wait_for(lambda: handler.state == "quitting")

    # Still there, still draining as far as anybody knows: the state is what a
    # group asked to place somebody reads, and it says do not send him here.
    assert handler.process.alive
    leaving.cancel()


async def test_a_process_that_answers_the_order_and_stays_is_killed_and_the_abort_is_loud(
    make_handler, group, caplog, monkeypatch
):
    monkeypatch.setattr(worker_handler_module, "QUIT_TIMEOUT_SECONDS", 0.3)
    handler = make_handler(behaviour="stay")
    await handler.launch_process()
    condemned = handler.process

    with caplog.at_level("WARNING"):
        await handler.quit_process()

    assert "still here 0.3s after being asked to leave" in caplog.text
    assert not condemned.alive
    assert handler.process is None
    await wait_for(lambda: group.wakes == ["aborted"])
    assert handler.state == "aborted"


async def test_a_process_mute_to_the_order_to_leave_is_killed_too(
    make_handler, group, caplog, monkeypatch
):
    monkeypatch.setattr(worker_handler_module, "QUIT_TIMEOUT_SECONDS", 0.3)
    handler = make_handler(behaviour="mute")
    await handler.launch_process()
    condemned = handler.process

    with caplog.at_level("WARNING"):
        await handler.quit_process()

    assert not condemned.alive
    await wait_for(lambda: group.wakes == ["aborted"])


async def test_a_second_launch_over_a_living_process_is_refused(make_handler):
    handler = make_handler()
    await handler.launch_process()
    resident = handler.process

    with pytest.raises(RuntimeError, match="is still alive"):
        await handler.launch_process()

    assert handler.process is resident
    assert resident.alive


async def test_a_process_that_never_presents_itself_is_killed_and_the_wait_raises(
    make_handler, group
):
    handler = make_handler(behaviour="absent", process_ping_timeout=1.0)

    with pytest.raises(TimeoutError):
        await handler.launch_process()

    assert handler.process is None
    assert group.wakes == []
