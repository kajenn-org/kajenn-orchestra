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

"""One worker's place on its group's bus: presentation, calls both ways, end of the member.

The group owns one ``kbus.Dispatcher`` and every worker joins it as a member
under its own name. This connector is what the group knows of ONE of them: the
URL its process is spawned with, a fresh token per launch, so a predecessor can
no longer join; the presentation, the worker's first call, answered with what
the chain composes; the calls the group places down, mapped at this boundary so
a link that is gone is ``ConnectionError``; the calls the worker places up,
served on the group's own tree; and the end of the member, which the
dispatcher reports and this connector turns into ``on_child_lost``.

Every reply that comes up is folded before its caller receives it: the worker
events and the photo travel in the payload, and the chain reads them there.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import secrets
from typing import Any

import kbus

from .lane_payload import LanePayload

#: The member the group joins its own bus as; every worker calls it.
GROUP_MEMBER = "group"

#: The route of a worker's first call, its presentation.
REGISTER_PATH = "/register"

#: The slot the worker events travel in, as the worker composes it.
ENVELOPE_SLOT_WORKER_EVENTS = "worker_events"

#: The slot the photo rides in, beside whatever payload its envelope carries.
ENVELOPE_SLOT_WORKER_SNAPSHOT = "worker_snapshot"

#: What only a presentation carries: the child says its pid at birth and never
#: again. It is what tells the vertex that this envelope is owed the whole store.
ENVELOPE_SLOT_PRESENTATION = "pid"

#: The kbus errors that mean nobody is on the other end: a link that dropped, a
#: member that never joined or already left, an address nobody answers.
LINK_GONE = (kbus.LinkLost, kbus.NoSuchMember, kbus.Unreachable)

__all__ = [
    "ENVELOPE_SLOT_PRESENTATION",
    "ENVELOPE_SLOT_WORKER_EVENTS",
    "ENVELOPE_SLOT_WORKER_SNAPSHOT",
    "GROUP_MEMBER",
    "LINK_GONE",
    "REGISTER_PATH",
    "CommanderCallFailed",
    "WorkerConnector",
]


class CommanderCallFailed(Exception):
    """The lane carried the call up and the answer was an error, not a result.

    Args:
        path: the routing key the child's CALL was placed on.
        cause: what the parent side said went wrong, for the log.
    """

    def __init__(self, path: str, cause: str) -> None:
        self.path = path
        self.cause = cause
        super().__init__(f"the call to {path} failed: {cause}")


class WorkerConnector:
    """The group's side of one worker member: its URL, its calls, its end.

    Args:
        worker_handler: the handler this member belongs to, and the only thing
            it asks — ``read_envelope`` for every envelope that arrives, whose
            answer is what goes back down to a presentation, ``serve_child_call``
            for a call the worker places, and ``on_child_lost`` when the member
            leaves on its own. Its ``group_handler`` owns the bus.
    """

    def __init__(self, worker_handler: Any) -> None:
        self.worker_handler = worker_handler
        #: The URL the current process was spawned with; None before the first launch.
        self.url: str | None = None
        #: True once ``stop`` ran: the end of the member is announced to nobody.
        self.closing = False
        self._logger = logging.getLogger(__name__)
        self._connected_event = asyncio.Event()
        self._service_tasks: set[asyncio.Task[Any]] = set()
        #: The orders placed down and not finished yet, the abandoned ones included.
        self._order_tasks: set[asyncio.Future[Any]] = set()

    @property
    def connected(self) -> bool:
        """Whether a child is on the bus and has presented itself."""
        return self._connected_event.is_set()

    async def start(self) -> None:
        """Mint a fresh URL for the next process; the previous token stops admitting.

        Opens the group's bus on the first launch of any of its workers.
        """
        group_handler = self.worker_handler.group_handler
        await group_handler.open_bus()
        token = secrets.token_hex(32)
        group_handler.bus.secrets[self.worker_handler.name] = token
        self.closing = False
        self.url = f"unix://{self.worker_handler.name}:{token}@{group_handler.bus_path}"

    def stop(self) -> None:
        """Take this worker off the bus for good — announced to nobody.

        Its token is forgotten, so no process joins under its name again, and the
        calls it placed that are still being served are cancelled.
        """
        self.closing = True
        self.worker_handler.group_handler.bus.secrets.pop(self.worker_handler.name, None)
        self._connected_event.clear()
        self._cancel_child_calls()

    async def wait_connected(self) -> None:
        """Block until a child has presented itself; the wait after a spawn."""
        await self._connected_event.wait()

    async def call(
        self,
        path: str,
        data: Any = None,
        timeout: float | None = None,
        meta: dict[str, Any] | None = None,
    ) -> Any:
        """Call the child on ``path`` with a JSON value and return its payload, folded.

        Args:
            path: the routing key of the call.
            data: the payload, JSON-serializable.
            timeout: the caller's own deadline; None waits until the reply lands
                or the child dies (``ConnectionError``).
            meta: routing fields beside the payload — the identity and the
                freeze verdict of a user's request.

        Returns:
            The child's payload — ``result`` and the envelope slots — with the
            routing fields of the reply's meta beside them, ``error`` included.

        Raises:
            ConnectionError: no child is on the bus, or it died waiting.
        """
        reply, envelope = await self.call_message(
            path, LanePayload().control_message(data, **(meta or {})), timeout=timeout
        )
        return {**envelope, **{key: value for key, value in reply.meta.items() if key != "format"}}

    async def call_message(
        self, path: str, message: kbus.Message, timeout: float | None = None
    ) -> tuple[kbus.Message, dict[str, Any]]:
        """Call the child on ``path`` with a message and fold the envelope of its reply.

        The call runs on a task of its own and the caller waits on it shielded:
        a caller that gives up — its ``timeout`` ran out, or it was cancelled —
        leaves the order running on the worker, and the reply that lands late
        is folded all the same, so the events it carries reach the chain.

        Returns:
            The reply and its envelope — the very dict the fold read, so what the
            fold wrote into the photo is what the caller reads.

        Raises:
            ConnectionError: no child is on the bus, or it died waiting.
            TimeoutError: ``timeout`` ran out; the order goes on on the worker.
        """
        order = asyncio.ensure_future(self._call_and_fold(path, message))
        self._order_tasks.add(order)
        order.add_done_callback(self._forget_order)
        async with asyncio.timeout(timeout):
            return await asyncio.shield(order)

    async def _call_and_fold(
        self, path: str, message: kbus.Message
    ) -> tuple[kbus.Message, dict[str, Any]]:
        """Place the call on the bus, then fold the envelope of its reply."""
        try:
            reply = await self.worker_handler.group_handler.bus_member.call(
                f"{self.worker_handler.name}.{path}", message)
        except (*LINK_GONE, kbus.Overloaded) as exc:
            raise ConnectionError(f"{self.worker_handler.name} is not on the bus: {exc}") from exc
        envelope = LanePayload().envelope(reply)
        self._take_envelope(envelope)
        return reply, envelope

    def _forget_order(self, order: asyncio.Future[Any]) -> None:
        """Drop a finished order; its failure is read here when nobody waits on it."""
        self._order_tasks.discard(order)
        if not order.cancelled():
            order.exception()

    async def serve(self, message: kbus.Message, reply: kbus.Reply) -> None:
        """Serve a call the worker placed: its presentation, then anything on the group's tree.

        A call before the presentation is refused. Anything the tree raises — a
        path it does not serve included — becomes the ``error`` of the reply: the
        child is answered once, always, so nobody is left parked.
        """
        task = asyncio.current_task()
        self._service_tasks.add(task)
        try:
            if message.meta["route"] == REGISTER_PATH:
                await self._present(message, reply)
            elif not self.connected:
                raise kbus.Refused(f"{self.worker_handler.name} has not presented itself")
            else:
                await self._serve_child_call(message, reply)
        finally:
            self._service_tasks.discard(task)

    def lose(self) -> None:
        """The member left the bus: a presented child that was not stopped is a death."""
        presented = self.connected
        self._connected_event.clear()
        self._cancel_child_calls()
        if presented and not self.closing:
            self._logger.info("Member %s left the bus", self.worker_handler.name)
            self.worker_handler.on_child_lost()

    async def _present(self, message: kbus.Message, reply: kbus.Reply) -> None:
        """Answer the presentation with what the chain composed."""
        envelope = LanePayload().decode_control(message.payload)
        await reply.send(LanePayload().control_message(self.worker_handler.read_envelope(envelope)))
        self._connected_event.set()
        self._logger.info("Child presented itself as %s: %s", self.worker_handler.name, envelope)

    def _cancel_child_calls(self) -> None:
        """Drop every call of the gone child still being served up here.

        Nothing can be answered to a member that is gone, and a call left parked
        would go on holding whatever it is parked ON — the store grant, in
        practice, which a dead waiter would win and never give back.
        """
        for task in list(self._service_tasks):
            task.cancel()

    async def _serve_child_call(self, message: kbus.Message, reply: kbus.Reply) -> None:
        """Serve one call the child placed on the group's tree, and answer it.

        The handler is asked, sync or async, and whatever it returns is the
        ``result`` of the reply; whatever it raises is its ``error``. A reply
        over the frame ceiling goes back as an error of its own.
        """
        path = message.meta["route"]
        try:
            payload = LanePayload().decode_control(message.payload) or {}
            answer = self.worker_handler.serve_child_call(path, payload)
            if inspect.isawaitable(answer):
                answer = await answer
            data: dict[str, Any] = {"result": answer}
        except Exception as exc:
            self._logger.exception("The call %s from %s was not served", path, self.worker_handler.name)
            data = {"error": f"{type(exc).__name__}: {exc}"}
        try:
            answer_message = LanePayload().control_message(data)
        except ValueError as exc:
            answer_message = LanePayload().control_message(
                {"error": f"{type(exc).__name__}: child call result is not encodable: {exc}"})
        try:
            await reply.send(answer_message)
        except kbus.FrameTooLarge as exc:
            await reply.send(LanePayload().control_message({"error": str(exc)}))

    def _take_envelope(self, envelope: dict[str, Any]) -> None:
        """Push the envelope into the fold before the caller is answered.

        Whatever the fold composed for the descent is dropped: nothing goes down
        in answer to an answer. A fold that raises is a fault of THIS side — a
        field the two sides name differently, or a bug in a layer of the chain —
        so the exception is logged with its stack and nothing is done to the
        child: the orchestration neither corrects nor masks, and the worker is
        not answerable for it. The events of that envelope stay half applied,
        which is the declared price until the escalation of F48 exists.
        """
        try:
            self.worker_handler.read_envelope(envelope)
        except Exception:
            self._logger.exception("The fold refused the envelope from %s", self.worker_handler.name)
