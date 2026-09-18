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

"""The foundations end to end: a worker is born, works, dies wild, and leaves its traces.

The three foundations are exercised together, in one story, on the real things:
a real child process (``child_stub``), a real Unix socket, a real deposit on
disk. Nothing here is doubled except the level above — ``GroupStub``, because
the GroupHandler is Macro 2's — and the test reads the deposit from the parent
side through a FreezeHandler of its own, over the same root the child was given.

The story: the handler launches its process, which presents itself — carrying
its first photo — and is answered with the whole global store; the beat asks
whether it is alive; the child takes a user's semaphore, parks that user's
connection under it and gives the semaphore back; it takes it again, and while it
holds it the process goes mute and the surveillance kills it.

**What the death leaves is the point.** The handler writes ``aborted`` — the
death nobody was waiting for — rings its group's wake and stops there; the users
that were on board are still on its list for whoever reads it at that round. The
parcel is still in the deposit, the semaphore of the interrupted operation is
still held by a process that no longer exists. Macro 1 cleans nothing: the sweep
of the traces belongs to the Commander, which does not exist yet, and this test
is the picture it will inherit.

The sockets and the deposit live under a short ``mkdtemp`` root: the system caps
a UDS path at about a hundred characters and pytest's own directory is already
past it, which is the very reason worker names are short.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import pytest

from kajenn_orchestra.orchestration import FreezeHandler, WorkerHandler

from .child_stub import (
    GO_MUTE_OP,
    RELEASE_LOCK_OP,
    TAKE_LOCK_OP,
    WRITE_CONNECTION_REGISTER_ITEM_OP,
)
from .group_stub import GroupStub
from .conftest import kill_process, wait_for

CHILD_MODULE = "tests.orchestration.child_stub"
PARKED_CONNECTION = {"cid": "c-1", "pages": ["main", "invoices"]}


@pytest.fixture
def group(short_root):
    return GroupStub(short_root / "frozen_users")


@pytest.fixture
def deposit(short_root):
    """The deposit as the parent reads it — the same root the child is given."""
    return FreezeHandler(short_root / "frozen_users")


@pytest.fixture
async def handler(short_root, group, repo_on_pythonpath):
    """The handler under test; no process and no socket of its own outlives the test."""
    worker_handler = WorkerHandler(
        group,
        "standard_0001",
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module=CHILD_MODULE,
        worker_kwargs={"group": "standard"},
        # Wide at birth: the same value bounds the wait for the presentation,
        # and a fresh interpreter on a loaded machine can take seconds to get
        # there. The mute phase tightens it to what it really measures.
        process_ping_timeout=10.0,
    )
    group.worker_handler = worker_handler
    yield worker_handler
    if worker_handler.process is not None:
        kill_process(worker_handler.process)
        await wait_for(lambda: not worker_handler.process.alive)
    await worker_handler.connector.stop()


async def order(handler: WorkerHandler, path: str, data: Any = None) -> Any:
    """Drive one order of the child through the wire and give back what it answered."""
    payload = await handler.connector.call(path, data, timeout=5.0)
    return payload["result"]


async def test_a_worker_is_born_works_dies_wild_and_leaves_its_traces_behind(
    handler, group, deposit, caplog
):
    caplog.set_level(logging.INFO)
    handler.hosted_users.update({"mario", "anna"})

    # It is born: the process presents itself on its handler's own socket, and
    # the presentation already carries its first photo — a live process is never
    # without one.
    await handler.launch_process()
    assert handler.connector.connected is True
    assert handler.worker_snapshot == {"pid": handler.process.pid, "name": "standard_0001"}

    # It is alive: the beat asks that and nothing else, and a fresh photo rides
    # the answer.
    await handler.ping_process()
    assert handler.worker_snapshot == {"pid": handler.process.pid, "name": "standard_0001"}

    # It takes the semaphore of one of its users. Nothing is announced upward:
    # the lock is the deposit's own mechanism, and the vertex already knows —
    # it suspended mario before any of this.
    assert await order(handler, TAKE_LOCK_OP, {"user": "mario"}) == {"taken": True}
    assert deposit.lock_holder("mario") == "standard_0001"
    assert deposit.read_connection_register_item("mario", "c-1") is None

    # It parks that user's connection under the semaphore it holds, and the
    # parent reads back from the deposit exactly what the child wrote.
    written = await order(
        handler,
        WRITE_CONNECTION_REGISTER_ITEM_OP,
        {"user": "mario", "cid": "c-1", "payload": PARKED_CONNECTION},
    )
    assert written == {"written": "c-1"}
    assert deposit.read_connection_register_item("mario", "c-1") == PARKED_CONNECTION
    assert deposit.get_item_header("mario", "c-1") == {
        "writer": "standard_0001",
        "ts": pytest.approx(time.time(), abs=60),
        "cause": "freeze",
        "group": "standard",
    }

    # It gives the semaphore back: the operation is over, the parcel stays.
    assert await order(handler, RELEASE_LOCK_OP, {"user": "mario"}) == {"released": "mario"}
    assert deposit.lock_holder("mario") is None
    assert deposit.read_connection_register_item("mario", "c-1") == PARKED_CONNECTION

    # It takes the semaphore again — this is the operation the death interrupts.
    assert await order(handler, TAKE_LOCK_OP, {"user": "mario"}) == {"taken": True}
    assert deposit.lock_holder("mario") == "standard_0001"

    # It goes mute: up, and no longer serving. That order is the last thing it
    # answers. The beat is repeated once past the timeout and then the process
    # group is killed and its death awaited.
    condemned = handler.process
    handler.process_ping_timeout = 1.0
    assert await order(handler, GO_MUTE_OP) == {"muted": True}
    started = time.monotonic()
    await handler.ping_process()
    elapsed = time.monotonic() - started

    assert elapsed < 6 * handler.process_ping_timeout
    assert not condemned.alive
    assert handler.process is None

    # The death was nobody's order: the handler writes `aborted`, rings its
    # group's wake, and its job ends there — at that round the group reads the
    # state and the users that were on board.
    await wait_for(lambda: group.wakes == ["aborted"])
    assert handler.state == "aborted"
    assert group.users_on_board == [{"mario", "anna"}]
    assert handler.connector.connected is False
    assert handler.worker_snapshot["pid"] == condemned.pid

    # And the deposit is exactly as the dead process left it: the parcel it
    # parked, and the semaphore of the operation it never finished. Macro 1
    # cleans nothing — the traces are the Commander's to sweep.
    assert deposit.user_folders == {deposit.user_to_userkey("mario")}
    assert deposit.read_connection_register_item("mario", "c-1") == PARKED_CONNECTION
    assert deposit.lock_holder("mario") == "standard_0001"
