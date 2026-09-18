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

"""The global store: one dictionary, living ONLY on the commander, behind one lock.

The commander owns ``dict[str, Any]``. Keys are literal strings — a dot in a key
is a character, never a path — and values are opaque: a scalar, a dict, a Bag of
either kind, anything the TYTX codec knows. There are no replicas: every access
a worker makes is a CALL on the lane, served under ONE FIFO lock (issue #74,
owner decision 2026-09-07).

- :class:`GlobalStoreLock` is the commander's lock: an ``asyncio.Lock`` (FIFO by
  construction) plus who holds the TURN — request id, worker and the key the
  turn selected. A simple ``get``/``set``/``delete`` takes the same lock for the
  length of its own operation and records no holder; a turn holds it from the
  grant to the release. No lease and no timer: the holder's channel EOF is the
  whole death protocol, and it applies nothing.
- :class:`GlobalStoreClient` is what a worker holds as ``global_store``: the
  three simple operations, synchronous from a pool thread, and ``for_update``.
- :class:`GlobalStoreLease` is one turn: ``with`` or ``async with``, because the
  vehicle follows the caller. It yields ITSELF, with ``value`` — the private
  working copy the grant decoded — and ``exists``, said at grant time. The exit
  sends the COMPLETE value back (``apply=True``) and the commander replaces the
  selected key, or the whole dictionary when no key was selected; a body that
  raises, or a lease that cannot decode its grant or encode its value, releases
  with ``apply=False`` and the master is exactly as the grant found it.

**A turn recognises its own context.** The client keeps a ``ContextVar`` set
while a lease is in force: a second ``for_update`` or a simple operation from
the same task or pool thread raises at once instead of parking on the lock it
already holds. Other threads of the same worker wait normally.

**The wire is TYTX.** Every value travels ``to_tytx(..., "json")`` and is
hydrated by its reader, so a Bag stays a Bag and a datetime a datetime; a
``get`` reply carries ``exists`` beside ``value``, so a stored ``None`` and an
absent key are two answers.

**A commit whose answer never came is uncertain, and says so.** The commit is a
CALL like any other and waits as long as the wire lives; when the wire fails
after the commit was sent, the commander may already have published the value.
The lease then raises :class:`GlobalStoreCommitUnconfirmed` — never a retry, and
no abort attempt on a wire that is gone. A commander that REFUSED the commit is
not this case: that is ``CommanderCallFailed``, and nothing was published.
"""

from __future__ import annotations

import asyncio
import contextvars
import uuid
from types import TracebackType
from typing import Any

from genro_tytx import from_tytx, to_tytx

#: The routing keys of the global store on the commander's dispatcher.
GLOBAL_STORE_SET_OP_PATH = "/commander/store/set"
GLOBAL_STORE_DEL_OP_PATH = "/commander/store/del"
GLOBAL_STORE_GET_OP_PATH = "/commander/store/get"
GLOBAL_STORE_LOCK_OP_PATH = "/commander/store/lock"
GLOBAL_STORE_UNLOCK_OP_PATH = "/commander/store/unlock"

__all__ = [
    "GLOBAL_STORE_DEL_OP_PATH",
    "GLOBAL_STORE_GET_OP_PATH",
    "GLOBAL_STORE_LOCK_OP_PATH",
    "GLOBAL_STORE_SET_OP_PATH",
    "GLOBAL_STORE_UNLOCK_OP_PATH",
    "GlobalStoreClient",
    "GlobalStoreCommitUnconfirmed",
    "GlobalStoreLease",
    "GlobalStoreLock",
]


class GlobalStoreCommitUnconfirmed(Exception):
    """The commit of a turn was sent and its answer never came: the value MAY be published.

    Args:
        request_id: the turn whose commit is unconfirmed.
        key: the key it selected, None for the whole dictionary.
        cause: what ended the wait — the transport failure, for the log.

    The caller must not repeat the write on its own: the commander may hold it
    already. Raised by ``GlobalStoreLease`` on the commit path alone.
    """

    def __init__(self, request_id: str, key: str | None, cause: BaseException) -> None:
        self.request_id = request_id
        self.key = key
        self.cause = cause
        target = "the whole store" if key is None else f"key {key!r}"
        super().__init__(
            f"the commit of turn {request_id} on {target} got no answer "
            f"({type(cause).__name__}: {cause}): the value may have been published"
        )


class GlobalStoreLock:
    """The commander's lock on the dictionary: FIFO, one holder, no lease and no timer."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        # The turn in force: its request id, the worker whose channel death
        # releases it, and the key it selected (None = the whole dictionary).
        self.holder: str | None = None
        self.holder_worker: str | None = None
        self.holder_key: str | None = None

    async def acquire(self, worker: str, request_id: str, key: str | None = None) -> None:
        """Park until the lock is this request's, then record whose turn it is.

        ``asyncio.Lock`` wakes its waiters in arrival order, so the FIFO the
        protocol promises is the primitive's own and nothing here queues.
        """
        await self.lock.acquire()
        self.holder = request_id
        self.holder_worker = worker
        self.holder_key = key

    def holds(self, request_id: str) -> bool:
        """Whether this request is the turn in force.

        A release for a turn no longer in force is a real case, not a protocol
        violation: the holder's channel died while its release was on the wire,
        and the death released it first. Such a release must touch NOTHING —
        neither the master nor a newer turn.
        """
        return self.holder == request_id

    def held_by(self, worker: str) -> bool:
        """Whether this worker holds the turn — the death check."""
        return self.holder_worker == worker

    def release(self) -> None:
        """Let the next waiter in; the caller has established who holds it."""
        self.holder = None
        self.holder_worker = None
        self.holder_key = None
        self.lock.release()


class GlobalStoreClient:
    """A worker's side of the global store: three simple operations and the turn.

    Args:
        worker: the ``SpaWorker`` whose lane the CALLs travel on.

    The simple operations are synchronous and block a pool thread on the
    worker's loop; ``for_update`` answers a lease usable with ``with`` from a
    pool thread or ``async with`` on the loop.
    """

    def __init__(self, worker: Any) -> None:
        self.worker = worker
        self.active_turn: contextvars.ContextVar[GlobalStoreLease | None] = (
            contextvars.ContextVar(f"global_store_turn:{worker.name}", default=None)
        )

    def refuse_inside_turn(self) -> None:
        """Raise when this context already holds a turn: a CALL would wait on itself."""
        turn = self.active_turn.get()
        if turn is not None:
            raise RuntimeError(
                f"the global store is already held by this context (turn {turn.request_id})"
            )

    def get(self, key: str, default: Any = None) -> Any:
        """Read one key: the stored value, ``None`` included, or ``default`` when absent."""
        self.refuse_inside_turn()
        reply = self.worker.run_on_loop(
            self.worker.call(GLOBAL_STORE_GET_OP_PATH, {"key": key})
        )
        return from_tytx(reply["value"], "json") if reply["exists"] else default

    def set(self, key: str, value: Any = None) -> None:
        """Write one key; the master holds the value when this returns."""
        self.refuse_inside_turn()
        self.worker.run_on_loop(
            self.worker.call(
                GLOBAL_STORE_SET_OP_PATH, {"key": key, "value": to_tytx(value, "json")}
            )
        )

    def delete(self, key: str) -> None:
        """Remove one key; an absent key is a no-op."""
        self.refuse_inside_turn()
        self.worker.run_on_loop(self.worker.call(GLOBAL_STORE_DEL_OP_PATH, {"key": key}))

    def for_update(self, key: str | None = None) -> GlobalStoreLease:
        """One read-modify-write turn on ``key``, or on the whole dictionary when None."""
        return GlobalStoreLease(self, key)


class GlobalStoreLease:
    """One turn on the global store: ``with`` or ``async with``, yielding itself.

    Args:
        client: the worker's ``GlobalStoreClient``.
        key: the selected key, or None for the whole dictionary.

    ``value`` is the private working copy the grant decoded — assign it or
    mutate it, the master sees nothing until the exit; ``exists`` says whether
    the key was there at grant time (always True for the whole dictionary).
    A body that raises releases with ``apply=False``; so does a grant that
    cannot be decoded or a value that cannot be encoded, the original error
    re-raised; so does a turn on which ``abort`` was called, whatever the body
    did to ``value`` afterwards — the lock stays held until the exit either way.
    """

    def __init__(self, client: GlobalStoreClient, key: str | None) -> None:
        self.client = client
        self.key = key
        self.request_id = uuid.uuid4().hex
        self.value: Any = None
        self.exists = False
        self.aborted = False
        self._token: contextvars.Token[GlobalStoreLease | None] | None = None

    def abort(self) -> None:
        """Mark this turn as not to be published: the exit sends ``apply=False``.

        The lock stays held until the ``with`` block exits; once called, nothing
        the body does to ``value`` reaches the master.
        """
        self.aborted = True

    async def _acquire(self) -> None:
        worker = self.client.worker
        reply = await worker.call(
            GLOBAL_STORE_LOCK_OP_PATH,
            {"worker": worker.name, "request_id": self.request_id, "key": self.key},
        )
        try:
            self.value = from_tytx(reply["value"], "json")
        except Exception:
            await self._abort()
            raise
        self.exists = reply["exists"]

    async def _release(self, exc_type: type[BaseException] | None) -> None:
        if exc_type is not None or self.aborted:
            await self._abort()
            return
        try:
            text = to_tytx(self.value, "json")
        except Exception:
            await self._abort()
            raise
        try:
            await self.client.worker.call(
                GLOBAL_STORE_UNLOCK_OP_PATH,
                {"request_id": self.request_id, "apply": True, "value": text},
            )
        except ConnectionError as exc:
            # The wire ended after the commit left: a parked CALL is failed with
            # ConnectionError, a write on a dead socket raises one of its
            # subclasses. Anything else — the commander's own refusal included —
            # propagates as it is.
            raise GlobalStoreCommitUnconfirmed(self.request_id, self.key, exc) from exc

    async def _abort(self) -> None:
        await self.client.worker.call(
            GLOBAL_STORE_UNLOCK_OP_PATH, {"request_id": self.request_id, "apply": False}
        )

    def _mark_active(self) -> None:
        self.client.refuse_inside_turn()
        self._token = self.client.active_turn.set(self)

    def _mark_closed(self) -> None:
        if self._token is not None:
            self.client.active_turn.reset(self._token)
            self._token = None

    async def __aenter__(self) -> GlobalStoreLease:
        self._mark_active()
        try:
            await self._acquire()
        except BaseException:
            self._mark_closed()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._mark_closed()
        await self._release(exc_type)

    def __enter__(self) -> GlobalStoreLease:
        self._mark_active()
        try:
            self.client.worker.run_on_loop(self._acquire())
        except BaseException:
            self._mark_closed()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._mark_closed()
        self.client.worker.run_on_loop(self._release(exc_type))
