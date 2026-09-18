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

"""The scripted child of the end-to-end test: a real process using the real foundations.

The worker of Macro 2 does not exist yet, so this is what the WorkerHandler
launches: the smallest process that uses every foundation the way a worker will.
It is TEST-ONLY, and it is not a fake — the wire is the package's own
``FrameStream``, the deposit is the package's own ``FreezeHandler``, and the
configuration is the very ``KAJENN_WORKER`` payload the handler writes.

Its life: connect to the socket it was given, present itself with its pid and
the configuration it received, and keep the whole global store that comes back
in the answer. Its presentation already carries its first photo, and so does
every reply after it: the photo rides the envelope, in the
``worker_snapshot`` slot beside the payload. Then serve the parent — the health
beat with its photo, the three deposit orders, and the order that makes it go
mute. Everything it has to say travels in the answer to what was asked of it:
the wire is asymmetric, orders down and answers up, and nothing here speaks
unbidden.

**It reaches the deposit on its own side.** Nothing hands it a FreezeHandler:
it builds one over ``frozen_users_path``, exactly as a worker in another process
must. What it writes there the parent reads back through a FreezeHandler of its
own, over the same root.

**Going mute is how it dies.** On ``/go_mute`` it answers that once and then
stops answering, while still reading the wire — a process that is up and no
longer serving, which is what the surveillance kills. Whatever it was holding in
the deposit when the SIGKILL lands stays there: nothing in this stub tidies up,
because nothing in the foundations does.

The stub resolves what the parent asks on a routing tree, as a real worker does
(#59): the beat under ``group``, the source filter under ``commander``, and its
own test orders as root-level entries — the keys below are this stub's own.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from genro_routes import RoutingClass, route

from tests.orchestration.frame_helpers import control_frame, read_control

from kajenn.channel.frame import REGISTER_METHOD, REGISTER_PATH, Frame, FrameStream
from kajenn_orchestra.orchestration import FreezeHandler
from kajenn_orchestra.orchestration.worker_connector import (
    CALL_METHOD,
    REPLY_METHOD,
    ENVELOPE_SLOT_WORKER_SNAPSHOT,
)
from kajenn_orchestra.orchestration.worker_handler import (
    WORKER_ENV_VAR,
)

#: The parent tells the child to stop answering: the mute worker of the drill.
#: It answers this one, and nothing after it.
GO_MUTE_OP = "/go_mute"

#: The parent tells the child what to announce: the worker events it is handed
#: come back on the reply, in the slot a real worker fills with what really
#: happened in it. It is how a test says «a user was born in that process» without
#: a whole worker in there.
ANNOUNCE_OP = "/announce"

#: The three deposit orders the parent drives the child through.
TAKE_LOCK_OP = "/take_deposit_lock"
WRITE_CONNECTION_REGISTER_ITEM_OP = "/write_connection_register_item"
RELEASE_LOCK_OP = "/release_deposit_lock"

__all__ = [
    "ANNOUNCE_OP",
    "GO_MUTE_OP",
    "RELEASE_LOCK_OP",
    "TAKE_LOCK_OP",
    "WRITE_CONNECTION_REGISTER_ITEM_OP",
    "ChildStub",
]


class XT_StubGroupOrders(RoutingClass):
    """The ``group`` branch: the beat is the one group order this stub takes."""

    def __init__(self, stub: Any) -> None:
        self.stub = stub

    @route()
    def ping(self) -> dict[str, Any]:
        """The health beat: an answer is the whole point of it."""
        return {}


class ChildStub(RoutingClass):
    """One scripted worker process: its wire, its deposit, and its mute switch."""

    def __init__(self) -> None:
        self.config = json.loads(os.environ[WORKER_ENV_VAR])
        self.name = self.config["name"]
        self.group = self.config["kwargs"]["group"]
        self.freeze_handler = FreezeHandler(self.config["frozen_users_path"])
        self.answering = True
        self.stream: FrameStream | None = None
        self.worker_events: list[dict[str, Any]] = []
        self.add_branches(
            [
                {"name": "group", "instance": XT_StubGroupOrders(self)},
            ]
        )

    @property
    def photo(self) -> dict[str, Any]:
        """What this process honestly knows of itself.

        Returns:
            Its pid and its name — a real worker adds the memory and the clocks
            it can actually measure.
        """
        return {"pid": os.getpid(), "name": self.name}

    async def live(self) -> None:
        """Connect, present, then serve the parent until the wire ends.

        Sets ``stream``; returns when the parent is gone.
        """
        address = self.config["uds_url"].removeprefix("uds:")
        reader, writer = await asyncio.open_unix_connection(address)
        self.stream = FrameStream(reader, writer)
        await self.present()
        while True:
            frame = await self.stream.read()
            if frame is None:
                return
            await self.serve_frame(frame)

    async def present(self) -> None:
        """Say pid and configuration, and wait for the parent's answer."""
        await self.stream.write(
            control_frame(
                method=REGISTER_METHOD,
                path=REGISTER_PATH,
                data={
                    "pid": os.getpid(),
                    "config": self.config,
                    ENVELOPE_SLOT_WORKER_SNAPSHOT: self.photo,
                },
            )
        )
        await self.stream.read()

    async def serve_frame(self, frame: Frame) -> None:
        """Obey one envelope from the parent.

        Args:
            frame: the envelope as it came off the wire.

        A CALL is answered with its REPLY, reusing its id, and whatever this
        process was told to announce rides that answer, as a real worker's own
        worker events do. A mute process answers no CALL at all, and the order
        that mutes it is the last thing it answers.

        Empties ``worker_events``: they are delivered once.
        """
        if frame.method == CALL_METHOD and self.answering:
            result = self.route.node(frame.path)(**(read_control(frame) or {}))
            worker_events, self.worker_events = self.worker_events, []
            await self.stream.write(
                control_frame(
                    id=frame.id,
                    method=REPLY_METHOD,
                    path=frame.path,
                    data={
                        "result": result,
                        "worker_events": worker_events,
                        ENVELOPE_SLOT_WORKER_SNAPSHOT: self.photo,
                    },
                )
            )

    @route()
    def announce(self, worker_events: list[dict[str, Any]]) -> dict[str, Any]:
        """Take the worker events the parent handed down and say them back up.

        Args:
            worker_events: the worker events to make.

        Returns:
            How many were taken.

        Sets ``worker_events``: they leave on the reply to this very order, which
        is the only road anything has out of here.
        """
        self.worker_events.extend(worker_events)
        return {"announcing": len(self.worker_events)}

    @route()
    def take_deposit_lock(self, user: str) -> dict[str, Any]:
        """Take the semaphore of a user.

        Args:
            user: whose folder is being entered.

        Returns:
            Whether the semaphore is now this process's.

        Writes the lock in the deposit. Nothing is announced upward: the lock is
        the deposit's own mechanism, and what the vertex must know it knows from
        the suspension it granted before any of this.
        """
        return {"taken": self.freeze_handler.take_lock(user, self.name)}

    @route()
    def write_connection_register_item(self, user: str, cid: str, payload: Any) -> dict[str, Any]:
        """Write one connection parcel under the semaphore this process holds.

        Args:
            user: whose connection is parked.
            cid: the connection identity.
            payload: what to park.

        Returns:
            The connection identity written.

        Writes the parcel in the deposit.
        """
        self.freeze_handler.write_connection_register_item(
            user,
            cid,
            payload,
            writer=self.name,
            cause="freeze",
            group=self.group,
        )
        return {"written": cid}

    @route()
    def release_deposit_lock(self, user: str) -> dict[str, Any]:
        """Give the semaphore back; whatever was parked stays behind.

        Args:
            user: whose folder is being left.

        Returns:
            The user released.

        Removes the lock from the deposit.
        """
        self.freeze_handler.release_lock(user, self.name)
        return {"released": user}

    @route()
    def go_mute(self) -> dict[str, Any]:
        """Answer this one, then stop answering: the mute worker of the drill.

        Returns:
            That it has gone mute — the last thing this process ever says.

        Sets ``answering``.
        """
        self.answering = False
        return {"muted": True}


if __name__ == "__main__":
    asyncio.run(ChildStub().live())
