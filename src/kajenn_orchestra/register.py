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

"""Register: one in-process dataset with secondary indexes.

Items are schemaless dicts keyed by an arbitrary string: ``create(key,
**fields)`` stores the fields passed plus the reserved ``register_item_id``
(the legacy name), seeded with the key itself so an item fetched through an
index or a scan always knows how to address itself. ``index_attrs`` names the
fields kept in a secondary index, so ``keys_by(attr, value)`` is an O(1)
lookup instead of a scan.

The one invariant every mutation upholds: the indexes stay coherent with
``_items`` at all times. ``create``/``update``/``drop`` each re-index exactly
what changed, and a bucket is pruned the moment it empties — a long-lived
register never accumulates empty entries. ``reindex`` and ``add_index``
rebuild from scratch when there is no incremental delta to apply.

Items are LIVE, never copies: ``create``/``get``/``update``/``drop`` return
the stored dict itself. Mutating non-indexed fields in place is the intended
idiom, inherited from the legacy daemon (appending to ``dbevents``,
adding to subscription sets, touching timestamps). INDEXED fields are the one
exception: they change ONLY through ``update``, which moves the key between
buckets — writing them directly desyncs the index silently. Known trap when
porting legacy code: the daemon's ``change_connection_user`` assigns
``item['user']`` in place (safe there — no indexes, lookups are scans); here
the user-move must go through ``update``.

Impossible cases are explicit errors, never silently handled: ``create`` on
an existing key, ``update``/``drop`` on a missing key, ``keys_by`` on an attr
that was never indexed, passing ``register_item_id`` to ``create``/``update``.
"""

from __future__ import annotations

from typing import Any

__all__ = ["Register"]


class Register:
    """One in-process dataset with secondary indexes."""

    def __init__(
        self, name: str, index_attrs: tuple[str, ...] = (), row_class: type = dict
    ) -> None:
        """Initialize an empty register named ``name`` indexing ``index_attrs``."""
        self._name = name
        self._index_attrs = index_attrs
        #: The class every item is built as: a dict, or a consumer's dict subclass
        #: that seeds its own defaults (see ``register_row``).
        self.row_class = row_class
        self._items: dict[str, dict[str, Any]] = {}
        self._indexes: dict[str, dict[Any, set[str]]] = {attr: {} for attr in index_attrs}

    @property
    def name(self) -> str:
        """The kind of item this register holds."""
        return self._name

    @property
    def index_attrs(self) -> tuple[str, ...]:
        """The item fields kept in a secondary index."""
        return self._index_attrs

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, key: str) -> bool:
        return key in self._items

    def get(self, key: str) -> dict[str, Any] | None:
        """Return the item stored under ``key``, or ``None`` if absent."""
        return self._items.get(key)

    def keys(self) -> list[str]:
        """A snapshot of every key held, safe to walk while dropping from it."""
        return list(self._items)

    def create(self, key: str, **fields: Any) -> dict[str, Any]:
        """Create the item of ``key``, seed its ``register_item_id`` and index it.

        Raises ``ValueError`` if ``key`` already exists — call sites decide
        whether to ``update`` or ``drop`` first, Register never overwrites
        silently — or if ``fields`` carries the reserved ``register_item_id``.
        """
        if key in self._items:
            raise ValueError(f"key already exists in register {self._name!r}: {key!r}")
        if "register_item_id" in fields:
            raise ValueError(f"register_item_id is reserved, seeded by create: {key!r}")
        item = self.row_class({"register_item_id": key, **fields})
        self._items[key] = item
        for attr in self._index_attrs:
            self._indexes[attr].setdefault(item.get(attr), set()).add(key)
        return item

    def update(self, key: str, **fields: Any) -> dict[str, Any]:
        """Merge ``fields`` into the item under ``key``, reindexing what changed.

        Raises ``KeyError`` if ``key`` is absent, ``ValueError`` on an attempt
        to change the reserved ``register_item_id``. Only indexed attrs whose
        value actually changes are moved between buckets.
        """
        if "register_item_id" in fields:
            raise ValueError(f"register_item_id is reserved, never updated: {key!r}")
        item = self._items[key]
        changed = [
            attr for attr in self._index_attrs if attr in fields and fields[attr] != item.get(attr)
        ]
        for attr in changed:
            self._deindex(attr, item.get(attr), key)
        item.update(fields)
        for attr in changed:
            self._indexes[attr].setdefault(item.get(attr), set()).add(key)
        return item

    def drop(self, key: str) -> dict[str, Any]:
        """Remove and return the item under ``key``, de-indexing it.

        Raises ``KeyError`` if ``key`` is absent.
        """
        item = self._items.pop(key)
        for attr in self._index_attrs:
            self._deindex(attr, item.get(attr), key)
        return item

    def _deindex(self, attr: str, value: Any, key: str) -> None:
        """Drop ``key`` from the ``attr`` bucket for ``value``, pruning it if empty."""
        bucket = self._indexes[attr][value]
        bucket.discard(key)
        if not bucket:
            del self._indexes[attr][value]

    def keys_by(self, attr: str, value: Any) -> list[str]:
        """Return the keys of items whose ``attr`` equals ``value``.

        Raises ``KeyError`` if ``attr`` is not indexed.
        """
        return list(self._indexes[attr].get(value, ()))

    def reindex(self) -> None:
        """Rebuild every secondary index from the current items."""
        self._indexes = {attr: {} for attr in self._index_attrs}
        for key, item in self._items.items():
            for attr in self._index_attrs:
                self._indexes[attr].setdefault(item.get(attr), set()).add(key)

    def add_index(self, attr: str) -> None:
        """Add ``attr`` to the indexed attrs and reindex; idempotent if already indexed."""
        if attr in self._index_attrs:
            return
        self._index_attrs = (*self._index_attrs, attr)
        self.reindex()
