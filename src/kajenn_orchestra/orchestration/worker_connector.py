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

"""The local worker's private connection and its two request/reply lanes.

Every Frame carries routing/control info and opaque payload bytes. call_frame
parks a correlated reply without opening application bytes; call is the
explicit JSON endpoint for orchestration control values. The worker's info
(worker_events and snapshot) is folded before the caller receives its reply.
The normalized info accompanies the same payload bytes throughout.

A child presents on its private UDS path, and a second simultaneous connection
is refused. This connector remains local-only: its handler owns supervision.
Link loss fails pending calls without replay. The connector's notification is
a connection fact; connecting to a generic remote service uses the separate
RemoteConnection and grants no authority over that service's process.

Inbound calls are served in separate tasks so they cannot block reply reading.
Each connection owns its pending calls; shutdown cancels service tasks and
releases the private socket. Limits reject excess pending calls before sending.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from pathlib import Path
from typing import Any, Callable

from kajenn.channel.control import ControlPayload
from kajenn.channel.frame import REGISTER_METHOD, Frame, FrameStream
from kajenn.transport_limits import FrameTooLarge

CALL_METHOD = "CALL"
REPLY_METHOD = "REPLY"

#: The slot the worker events travel in, as the worker composes it.
ENVELOPE_SLOT_WORKER_EVENTS = "worker_events"

#: The slot the photo rides in, beside whatever payload its envelope carries.
ENVELOPE_SLOT_WORKER_SNAPSHOT = "worker_snapshot"

#: What only a presentation carries: the child says its pid at birth and never
#: again. It is what tells the vertex that this envelope is owed the whole store.
ENVELOPE_SLOT_PRESENTATION = "pid"

__all__ = [
    "CALL_METHOD",
    "ENVELOPE_SLOT_PRESENTATION",
    "ENVELOPE_SLOT_WORKER_EVENTS",
    "ENVELOPE_SLOT_WORKER_SNAPSHOT",
    "REPLY_METHOD",
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
    """The accept side of one worker's socket: presentation, envelopes, end of wire.

    Args:
        worker_handler: the handler this wire belongs to, and the only thing it
            asks — ``read_envelope`` for every envelope that arrives, whose
            answer is what goes back down, and ``on_child_lost`` when the wire
            dies on its own.
        socket_path: the UDS path to bind, ``<instance_dir>/<name>.sock``.
        max_size: the frame ceiling on this wire.
    """

    def __init__(
        self,
        worker_handler: Any,
        socket_path: str | Path,
        *,
        max_size: int | None = None,
    ) -> None:
        self.worker_handler = worker_handler
        self.socket_path = Path(socket_path)
        self.max_size = max_size
        self._logger = logging.getLogger(__name__)
        self._server: asyncio.Server | None = None
        self._stream: FrameStream | None = None
        self._pending: dict[str, asyncio.Future[Frame]] = {}
        self._pending_paths: dict[str, str] = {}
        self._abandoned: dict[str, str] = {}
        self._connected_event = asyncio.Event()
        self._service_tasks: set[asyncio.Task[None]] = set()
        self._closing = False

    @property
    def address(self) -> str:
        """The address the child is spawned with, in the channel's own form."""
        return f"uds:{self.socket_path}"

    @property
    def connected(self) -> bool:
        """Whether a child is on the wire and has presented itself."""
        return self._connected_event.is_set()

    async def start(self) -> None:
        """Bind the socket and start listening: the directory private, the stale path cleared."""
        self.socket_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.socket_path.unlink(missing_ok=True)
        self._server = await asyncio.start_unix_server(
            self._serve_connection, path=str(self.socket_path)
        )
        self._logger.info("Wire listening on %s", self.socket_path)

    async def stop(self) -> None:
        """Close the wire for good and take the socket away — announced to nobody.

        Acts on the stream, the listening socket, the pending CALLs and the socket
        file. FINAL: this connector does not reopen.
        """
        self._closing = True
        if self._stream is not None:
            await self._stream.close()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        self._connected_event.clear()
        self._fail_pending(f"the wire of {self.socket_path.name} was closed")
        self.socket_path.unlink(missing_ok=True)
        self._logger.info("Wire on %s closed", self.socket_path)

    async def wait_connected(self) -> None:
        """Block until a child has presented itself; the wait after a spawn."""
        await self._connected_event.wait()

    async def call(self, path: str, data: Any = None, timeout: float | None = None) -> Any:
        """CALL the child and await its REPLY; returns the REPLY ``data`` verbatim.

        Args:
            path: the routing key of the call.
            data: the payload, JSON-serializable.
            timeout: the caller's own deadline; None waits until the REPLY lands
                or the child dies (``ConnectionError``).

        Returns:
            The child's payload, untouched — reading it is the caller's job.

        Raises:
            ConnectionError: no child is on the wire, or it died waiting.
        """
        frame = Frame(method=CALL_METHOD, path=path, info={"format": "control-json"},
                      payload=ControlPayload().encode(data))
        reply = await self.call_frame(frame, timeout=timeout)
        return {**(ControlPayload().decode(reply.payload) or {}),
                **{key: value for key, value in reply.info.items() if key != "format"}}

    async def call_frame(self, frame: Frame, timeout: float | None = None) -> Frame:
        """Send opaque bytes once and await this connection's correlated reply."""
        stream = self._live_stream()
        if frame.method != CALL_METHOD:
            raise ValueError("worker request/reply calls require method CALL")
        if frame.id in self._pending or frame.id in self._abandoned:
            raise ValueError("duplicate in-flight correlation id")
        if len(self._pending) >= 256:
            raise ConnectionError("worker call capacity exhausted; request not sent")
        future: asyncio.Future[Frame] = asyncio.get_running_loop().create_future()
        self._pending[frame.id] = future
        self._pending_paths[frame.id] = frame.path
        sent = False
        try:
            sent = True
            await stream.write(frame)
            if timeout is None:
                return await asyncio.shield(future)
            return await asyncio.wait_for(asyncio.shield(future), timeout)
        except (asyncio.CancelledError, TimeoutError):
            if sent and not future.done():
                if len(self._abandoned) >= 256:
                    await stream.close()
                else:
                    self._abandoned[frame.id] = frame.path
            raise
        finally:
            self._pending.pop(frame.id, None)
            self._pending_paths.pop(frame.id, None)
            if not future.done():
                future.cancel()

    def _live_stream(self) -> FrameStream:
        """The child's stream, or ``ConnectionError`` when there is no child."""
        if self._stream is None:
            raise ConnectionError(f"no child on the wire of {self.socket_path.name}")
        return self._stream

    async def _serve_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Per-connection task: take the wire, present the child, then relay."""
        stream = FrameStream(reader, writer, max_size=self.max_size)
        if self._stream is not None:
            self._logger.warning(
                "Connection refused on %s: the wire is already taken", self.socket_path.name
            )
            await stream.close()
            return
        self._stream = stream
        self._abandoned.clear()
        presented = False
        try:
            presented = await self._present(stream)
            if presented:
                self._connected_event.set()
                await self._receive_loop(stream)
        finally:
            self._stream = None
            self._connected_event.clear()
            await stream.close()
            self._fail_pending(f"the wire of {self.socket_path.name} is down")
            self._cancel_child_calls()
            if presented and not self._closing:
                self._logger.info("Wire lost on %s", self.socket_path.name)
                await self._fire(self.worker_handler.on_child_lost)

    async def _present(self, stream: FrameStream) -> bool:
        """Read the presentation and answer it with what the chain composed; False if it is not one."""
        try:
            frame = await stream.read()
        except ValueError:
            self._logger.exception(
                "Protocol violation presenting on %s; refusing the connection",
                self.socket_path.name,
            )
            return False
        if frame is None or frame.method != REGISTER_METHOD:
            self._logger.warning(
                "Connection refused on %s: the first frame is not %s",
                self.socket_path.name,
                REGISTER_METHOD,
            )
            return False
        await stream.write(
            Frame(
                id=frame.id,
                method=REPLY_METHOD,
                path=frame.path,
                info={"format": "control-json"},
                payload=ControlPayload().encode(self.worker_handler.read_envelope(frame.info)),
            )
        )
        self._logger.info("Child presented itself on %s: %s", self.socket_path.name, frame.info)
        return True

    async def _receive_loop(self, stream: FrameStream) -> None:
        """Read the child's frames until the wire ends; EOF is the death signal."""
        while True:
            try:
                frame = await stream.read()
            except ValueError:
                self._logger.exception(
                    "Protocol violation from the child on %s; closing the wire",
                    self.socket_path.name,
                )
                return
            if frame is None:
                return
            self._dispatch(frame)

    def _cancel_child_calls(self) -> None:
        """Drop every CALL of the dead child still being served up here.

        Nothing can be answered on a wire that is gone, and a call left parked
        would go on holding whatever it is parked ON — the store grant, in
        practice, which a dead waiter would win and never give back.
        """
        for task in list(self._service_tasks):
            task.cancel()

    def _dispatch(self, frame: Frame) -> None:
        """Route one inbound frame: a REPLY resolves its caller, a CALL is served as a task."""
        if frame.method == REPLY_METHOD:
            self._validate_reply_route(frame)
            frame = self._take_envelope(frame)
            self._resolve_reply(frame)
        elif frame.method == CALL_METHOD:
            task = asyncio.create_task(self._serve_child_call(frame))
            self._service_tasks.add(task)
            task.add_done_callback(self._service_tasks.discard)
        else:
            self._logger.warning(
                "Unexpected envelope %s from the child on %s", frame.method, self.socket_path.name
            )

    def _validate_reply_route(self, frame: Frame) -> None:
        """Reject a correlated REPLY on the wrong route before folding its metadata."""
        abandoned_path = self._abandoned.get(frame.id)
        if abandoned_path is not None and frame.path != abandoned_path:
            raise ValueError("abandoned REPLY does not belong to its route")
        pending_path = self._pending_paths.get(frame.id)
        if pending_path is not None and frame.path != pending_path:
            raise ValueError("REPLY does not belong to its parked route")

    async def _serve_child_call(self, frame: Frame) -> None:
        """Serve one CALL the child placed, and answer it.

        Args:
            frame: the CALL as it came off the wire.

        The handler is asked, sync or async, and whatever it returns is the
        ``result`` of the REPLY. Anything it raises — a path it does not serve
        included, which reaches here as the ``AttributeError`` of a hook that is
        not there — becomes the ``error`` of that same REPLY: the child is
        answered once, always, so nobody is left parked on a dropped frame.
        """
        try:
            wire_format = frame.info.get("format")
            if wire_format == "wsx-json":
                if frame.path != "/commander/websocket/send":
                    raise ValueError("serialized page data requires the websocket route")
                info = frame.info
                if set(info) != {"format", "page_id", "cid", "reply_path", "data_present"}:
                    raise ValueError("invalid serialized websocket metadata")
                if (not isinstance(info["page_id"], str)
                        or not isinstance(info["reply_path"], str)
                        or (info["cid"] is not None and not isinstance(info["cid"], str))
                        or not isinstance(info["data_present"], bool)):
                    raise ValueError("invalid serialized websocket metadata types")
                if not info["data_present"] and frame.payload:
                    raise ValueError("absent websocket data must have an empty payload")
                payload = {"page_id": info["page_id"], "cid": info.get("cid"),
                           "path": info["reply_path"],
                           "data": frame.payload.decode("utf-8") if info.get("data_present") else None}
            elif wire_format not in (None, "control-json"):
                raise ValueError("unsupported child call payload format")
            else:
                payload = ControlPayload().decode(frame.payload) or {}
            answer = self.worker_handler.serve_child_call(frame.path, payload)
            if inspect.isawaitable(answer):
                answer = await answer
            data: dict[str, Any] = {"result": answer}
        except Exception as exc:
            self._logger.exception(
                "The CALL %s from the child on %s was not served",
                frame.path,
                self.socket_path.name,
            )
            data = {"error": f"{type(exc).__name__}: {exc}"}
        try:
            encoded = ControlPayload().encode(data)
        except Exception as exc:
            encoded = ControlPayload().encode(
                {"error": f"{type(exc).__name__}: child call result is not encodable: {exc}"}
            )
        stream = None
        try:
            stream = self._live_stream()
            reply = Frame(id=frame.id, method=REPLY_METHOD, path=frame.path,
                          info={"format": "control-json"}, payload=encoded)
            try:
                await stream.write(reply)
            except FrameTooLarge as exc:
                await stream.write(Frame(
                    id=frame.id, method=REPLY_METHOD, path=frame.path,
                    info={"format": "control-json"},
                    payload=ControlPayload().encode({"error": str(exc)}),
                ))
        except Exception:
            if stream is not None:
                with contextlib.suppress(Exception):
                    await stream.close()
            self._logger.warning(
                "The answer to %s found no wire on %s", frame.path, self.socket_path.name,
                exc_info=True,
            )

    def _take_envelope(self, frame: Frame) -> Frame:
        """Push the envelope into the fold before the caller is answered.

        Args:
            frame: the REPLY as it came off the wire.

        Whatever the fold composed for the descent is dropped: nothing goes down
        in answer to an answer. A fold that raises is a fault of THIS side — a
        field the two sides name differently, or a bug in a layer of the chain —
        so the exception is logged with its stack and nothing is done to the
        child: the orchestration neither corrects nor masks, and the worker is
        not answerable for it. The events of that envelope stay half applied,
        which is the declared price until the escalation of F48 exists.
        """
        info = frame.info
        try:
            self.worker_handler.read_envelope(info)
        except Exception:
            self._logger.exception(
                "The fold refused the envelope %s from the child on %s",
                frame.id,
                self.socket_path.name,
            )
        return Frame(id=frame.id, method=frame.method, path=frame.path,
                     info=info, payload=frame.payload)


    def _resolve_reply(self, frame: Frame) -> None:
        """Hand the REPLY payload to the parked caller; a caller already gone drops it."""
        abandoned_path = self._abandoned.get(frame.id)
        if abandoned_path is not None:
            if frame.path != abandoned_path:
                raise ValueError("abandoned REPLY does not belong to its route")
            self._abandoned.pop(frame.id, None)
            return
        future = self._pending.get(frame.id)
        if future is None or future.done():
            self._logger.debug(
                "REPLY %s on %s has no parked caller", frame.id, self.socket_path.name
            )
            return
        if frame.path != self._pending_paths[frame.id]:
            raise ValueError("REPLY does not belong to its parked route")
        future.set_result(frame)

    def _fail_pending(self, reason: str) -> None:
        """Fail every CALL still waiting with ``ConnectionError``; each caller pops its own."""
        for future in self._pending.values():
            if not future.done():
                future.set_exception(ConnectionError(reason))

    async def _fire(self, callback: Callable[..., Any], *args: Any) -> None:
        """Tell the handler something, sync or async; its bug must not sever the wire."""
        try:
            result = callback(*args)
            if inspect.isawaitable(result):
                await result
        except Exception:
            self._logger.exception("Wire callback %r failed", callback)
