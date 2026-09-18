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

"""Phase 7 contract: the worker-to-commander CALL lane.

The redesign's foundation (registro 2026-08-20 §1): ``WorkerConnector`` learns
the second dispatch branch — a CALL arriving FROM the child is served as a task
and answered with a REPLY — and the worker learns to place a call and await its
answer. The transport is already full duplex and frames carry ids: the
conversations interleave without confusion. The channel doctrine always had the
two sides («REPLY si risolve inline, CALL/EVENT si servono come task»); this
phase finishes the half the connector implemented.

Bindings (method names, the handler hook the commander exposes) are settled by
the phase: skeletons state the behaviour, the executable shape is the phase's
work. The `ENVELOPE_SLOT_*` renames ride this phase too — they touch the same
file.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from kajenn.channel.frame import Frame
from kajenn_orchestra.orchestration import FreezeHandler, SpaWorker, WorkerConnector
from kajenn_orchestra.orchestration import spa_worker as spa_worker_module
from kajenn_orchestra.orchestration import worker_connector as worker_connector_module
from kajenn_orchestra.orchestration import envelope_handler as envelope_handler_module
from kajenn_orchestra.orchestration.worker_connector import (
    ENVELOPE_SLOT_PRESENTATION,
    ENVELOPE_SLOT_WORKER_EVENTS,
    ENVELOPE_SLOT_WORKER_SNAPSHOT,
    CommanderCallFailed,
)

WORKER_NAME = "standard_0001"
UPWARD_OP_PATH = "/op/upward"
SERVED_OP_PATH = "/op/ask"


class XT_ServingHandler:
    """The WorkerHandler seen by its wire, with the parent half the lane needs.

    Args:
        answers: what the parent hands back, by routing key; a key it does not
            hold is a path the parent does not serve.
    """

    def __init__(self, answers: dict[str, Any] | None = None) -> None:
        self.name = WORKER_NAME
        self.answers = answers or {}
        self.served: list[tuple[str, Any]] = []
        self.gate: asyncio.Event | None = None

    def read_envelope(self, envelope: dict[str, Any]) -> dict[str, Any]:
        """Whatever arrives is taken; nothing of the store goes down these tests."""
        return {}

    def on_child_lost(self) -> None:
        pass

    async def serve_child_call(self, path: str, data: Any) -> Any:
        """Answer one call the child placed, parked on the gate while there is one."""
        self.served.append((path, data))
        if self.gate is not None:
            await self.gate.wait()
        return self.answers[path]


class X_LaneWorker(SpaWorker):
    """A worker with one op that asks the parent before answering its own caller."""

    async def answer_call(self, frame: Frame) -> None:
        if frame.path != UPWARD_OP_PATH:
            await super().answer_call(frame)
            return
        answer = await self.call(SERVED_OP_PATH, {"who": self.name})
        await self.send_reply(frame, result=answer)


class XT_LanePair:
    """A connector and a worker on one real UDS, both on this test's loop.

    Args:
        handler: the parent side of the wire, which serves the child's calls.
        socket_path: where to bind, under the short root the UDS cap needs.
        freeze_handler: the deposit the worker is built with.
    """

    def __init__(
        self, handler: XT_ServingHandler, socket_path: Path, freeze_handler: FreezeHandler
    ) -> None:
        self.handler = handler
        self.connector = WorkerConnector(handler, socket_path)
        self.worker = X_LaneWorker(WORKER_NAME, freeze_handler=freeze_handler)
        self._reader_task: asyncio.Task[None] | None = None

    async def open(self) -> None:
        """Bind, connect, present, and put the worker's read loop on the air."""
        await self.connector.start()
        reader, writer = await asyncio.open_unix_connection(str(self.connector.socket_path))
        self.worker.attach_stream(spa_worker_module.FrameStream(reader, writer))
        await self.worker.send_presentation({})
        self._reader_task = asyncio.create_task(self.worker.receive_frames())
        await self.connector.wait_connected()

    async def close(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        self.worker.exit_process()
        await self.connector.stop()


@pytest.fixture
async def pair(short_root, tmp_path):
    handler = XT_ServingHandler({SERVED_OP_PATH: {"served": "answered"}})
    built = XT_LanePair(
        handler,
        short_root / "i" / f"{WORKER_NAME}.sock",
        FreezeHandler(tmp_path / "frozen_users"),
    )
    await built.open()
    yield built
    await built.close()


async def test_a_worker_call_is_served_and_answered_while_the_parents_call_is_still_open(pair):
    # wf:contract: while the worker is serving a CALL the commander made (the
    # wf:contract: request is mid-flight), the worker places its own CALL on
    # wf:contract: the same wire; the connector serves it as a task, a handler
    # wf:contract: on the parent side answers, and the worker's awaited future
    # wf:contract: resolves with that REPLY — the parent's original CALL is
    # wf:contract: still pending throughout and completes normally afterwards.
    pair.handler.gate = asyncio.Event()
    downward = asyncio.create_task(pair.connector.call(UPWARD_OP_PATH))

    await wait_until(lambda: pair.handler.served)
    assert pair.handler.served == [(SERVED_OP_PATH, {"who": WORKER_NAME})]
    assert not downward.done()

    pair.handler.gate.set()
    reply = await asyncio.wait_for(downward, 5.0)

    assert reply["result"] == {"served": "answered"}
    assert ENVELOPE_SLOT_WORKER_EVENTS in reply


async def test_two_worker_calls_interleave_by_frame_id(pair):
    # wf:contract: two CALLs placed by the worker without awaiting the first
    # wf:contract: resolve each with its own REPLY, matched by frame id, in
    # wf:contract: whatever order the parent answers.
    pair.handler.answers = {"/op/one": "first", "/op/two": "second"}
    gate = asyncio.Event()
    pair.handler.gate = gate

    one = asyncio.create_task(pair.worker.call("/op/one"))
    await wait_until(lambda: pair.handler.served)
    two = asyncio.create_task(pair.worker.call("/op/two"))
    await wait_until(lambda: len(pair.handler.served) == 2)

    # Both are on the wire and neither is answered: the parent is holding the
    # gate, so the order the answers come in is the gate's, not the calls'.
    assert not one.done() and not two.done()
    gate.set()

    assert await asyncio.wait_for(two, 5.0) == "second"
    assert await asyncio.wait_for(one, 5.0) == "first"


async def test_a_worker_call_from_a_pool_thread_reaches_the_loop_and_returns(pair):
    # wf:contract: the request runs on a traffic-pool thread; the worker's
    # wf:contract: call() is reachable from that thread (hop onto the loop,
    # wf:contract: the pre_refactoring pattern of the global lock) and hands
    # wf:contract: the REPLY payload back to the calling thread.
    worker = pair.worker

    def on_the_pool_thread() -> Any:
        return worker.run_on_loop(worker.call(SERVED_OP_PATH, {"from": "the pool"}))

    answer = await asyncio.get_running_loop().run_in_executor(
        worker.traffic_pool, on_the_pool_thread
    )

    assert answer == {"served": "answered"}
    assert pair.handler.served == [(SERVED_OP_PATH, {"from": "the pool"})]


async def test_a_call_the_parent_has_no_handler_for_answers_an_error_not_silence(pair):
    # wf:contract: a CALL path the commander does not serve comes back as an
    # wf:contract: error REPLY the worker can raise on — never a dropped frame
    # wf:contract: and never a warning-and-discard.
    with pytest.raises(CommanderCallFailed) as refusal:
        await asyncio.wait_for(pair.worker.call("/op/nothing_here"), 5.0)

    assert refusal.value.path == "/op/nothing_here"
    assert "KeyError" in refusal.value.cause

    # The wire is unharmed: the next call on a path the parent serves answers.
    assert await asyncio.wait_for(pair.worker.call(SERVED_OP_PATH), 5.0) == {"served": "answered"}


def test_the_envelope_slot_constants_wear_the_family_prefix():
    # wf:contract: the surviving envelope slot constants are named
    # wf:contract: ENVELOPE_SLOT_WORKER_EVENTS, ENVELOPE_SLOT_WORKER_SNAPSHOT,
    # wf:contract: ENVELOPE_SLOT_PRESENTATION, live in worker_connector.py,
    # wf:contract: keep their wire values ("worker_events", "worker_snapshot",
    # wf:contract: "pid"), and no bare string literal writes those slots any
    # wf:contract: more (the two M2/M3 stray literals are gone).
    assert ENVELOPE_SLOT_WORKER_EVENTS == "worker_events"
    assert ENVELOPE_SLOT_WORKER_SNAPSHOT == "worker_snapshot"
    assert ENVELOPE_SLOT_PRESENTATION == "pid"
    for dead in ("WORKER_EVENTS_KEY", "WORKER_SNAPSHOT_KEY", "PRESENTATION_KEY"):
        assert not hasattr(worker_connector_module, dead)
        assert not hasattr(envelope_handler_module, dead)

    source = Path(spa_worker_module.__file__).read_text()
    assert '"worker_events"' not in source
    assert '"worker_snapshot"' not in source
    assert '{"pid": os.getpid()' not in source


async def wait_until(condition, timeout: float = 5.0) -> None:
    """Spin until the condition holds — the frames of a lane land on the loop."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("the lane never got there")
        await asyncio.sleep(0.01)
