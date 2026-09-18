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

"""The three register rows as classes: a ``dict`` each, carrying what it knows of itself.

A row is still a dict — ``row["field"]`` reads and writes as it always did,
the parcel pickles a plain dict built from it, the census serialises it, the
hosted site receives it as the daemon's item — but the class says what the
worker used to hard-code about the row (#59, block 3):

- ``default_fields``: what the row is born with beside what the caller passes
  (the row's own ``item_lock`` for every kind);
- ``fields_left_behind``: what the parcel does NOT carry — the reserved id, the
  edges the folder already says, the lock, and the live objects bound to the
  Bags of the process the row lives in, which the birth on the other side
  makes anew;
- ``fields_replayed``: what travels in the parcel but cannot be passed to the
  birth, and ``replay_fields`` puts it back on the row once it exists;
- ``announcement_fields``: what the ``new_page`` worker event carries beside
  the identities.

A consumer subclasses a row and names it on its registry
(``RegisterRegistry.page_row_class`` and siblings); the worker asks the row and
knows nothing of the fields. The three rows of the core carry no data of a
hosted site: what a page queues, subscribes or watches is the consumer's row
class's to add.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

__all__ = ["ConnectionRow", "PageRow", "RegisterRow", "UserRow"]


class RegisterRow(dict):
    """One register row: a dict born with its defaults, then the caller's fields.

    Args:
        fields: what the caller passes; wins over ``default_fields``.
    """

    #: What the parcel leaves behind: the reserved id and the row's own lock.
    fields_left_behind: frozenset[str] = frozenset({"register_item_id", "item_lock"})
    #: What travels but is put back after the birth, in order.
    fields_replayed: tuple[str, ...] = ()

    def __init__(self, fields: dict[str, Any] | None = None) -> None:
        super().__init__(self.default_fields())
        if fields:
            self.update(fields)

    def default_fields(self) -> dict[str, Any]:
        """The fields the row is born with: a fresh lock, exclusive and re-entrant."""
        return {"item_lock": threading.RLock()}

    def replay_fields(self, registry: Any, fields: dict[str, Any]) -> None:
        """Put back what travelled and could not be passed to the birth: nothing here."""

    def announcement_fields(self) -> dict[str, Any]:
        """What the birth announces beside the identities: nothing here."""
        return {}


class UserRow(RegisterRow):
    """The user's row: the top of the chain. Its parcel is the store alone."""


class ConnectionRow(RegisterRow):
    """The connection's row: the parcel leaves its two edges behind, the folder says them."""

    fields_left_behind = RegisterRow.fields_left_behind | {"user", "pages"}


class PageRow(RegisterRow):
    """The page's row: the parcel leaves the edge to the connection behind.

    ``wsx`` is how this page uses its channel, and it is the command
    ``openchannel`` that writes it: absent until the page opened its channel,
    then ``True`` for the ordinary page or a dict of parameters for one that
    asked for something. A message for a page that never opened its channel is
    refused, so the field is also the proof that the browser and the row agree
    on who this page is. It TRAVELS in the parcel — it is ``True`` or a dict of
    plain data — because a user parked for being idle and woken by his next
    request never lost his websocket: the browser noticed nothing, and a row
    that came back without its channel would refuse the very next message of a
    page that is still connected.
    """

    fields_left_behind = RegisterRow.fields_left_behind | {"connection_id", "call_lock"}

    def default_fields(self) -> dict[str, Any]:
        """The row's own lock, its channel, and the queue its calls wait in.

        ``call_lock`` is an ``asyncio.Lock``, and it is a different thing from
        ``item_lock``: that one guards the row itself and is taken on whatever
        thread touches it, this one serialises the CALLS of this page, which
        are tasks on the worker's loop. It is taken only when the page asked to
        be served one call at a time, and it never travels — a page that comes
        back from the deposit gets a fresh one.
        """
        return {**super().default_fields(), "wsx": None, "call_lock": asyncio.Lock()}
