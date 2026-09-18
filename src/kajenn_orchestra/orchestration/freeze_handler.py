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

"""FreezeHandler: the freezer on disk, and the only direct filesystem access.

A user who leaves memory leaves it here. One DIRECTORY per user — named by
``user_to_userkey``, which goes ONE WAY: every reader starts from the identity
and computes the name forward, nothing ever derives an identity back from a
directory name. Inside: ``user_register_item.pickle`` for the user's own store, one
``connection_register_item_<cid>.pickle`` per connection, carrying that connection AND
its pages. Beside them the semaphore, ``.lock``.

**The freezer only through this surface.** The house rule says the filesystem
is reached through storage nodes; this class is the declared exception, and the
exception is what buys it: the semaphore needs real exclusive creation
(``O_CREAT|O_EXCL``), which no logical-volume surface offers. The deal is that
NOBODY else computes a path under the root — freezing, adoption, the sweep and
the photo all speak to this surface.

**The semaphore is the only coherence mechanism.** There is no temporary file
and no rename: whoever holds the lock writes DIRECTLY over the destination.
Nobody can read half a file, because a reader waits for the lock before
looking; the half file a crash leaves behind is covered elsewhere — the dead
worker's folder is discarded by the cleanup that follows its death, and every
server start wipes the working freezer anyway. ONE declared exception, accepted
by weighed probability (2026-08-18): the vertex reads headers and drops folders
WITHOUT the lock. A parcel is complete before the vertex ever marks its user
frozen, so the only collision left is a drop (expiry, forget) against a lazy
wake in flight — and it ends in a loud error on a user the machine was
forgetting anyway, never in silent corruption.

**Waiting is the caller's, on its own loop.** ``take_lock`` tries once and says
yes or no. Whoever finds it taken retries as a coroutine — never holding a
thread of the service pool, which exists for real disk work and would starve
itself waiting for the operation meant to release the lock.

**The lock owns the empty folder.** Dropping items never touches the lock: a
folder is alive while an operation runs, whatever it has left inside. It is
``release_lock`` — the end of that operation — that removes the folder when
nothing but the lock remains, so the root holds the frozen and nothing else.

**A drop asks for absence.** Every ``drop_*`` says what must no longer be
there, and a thing already gone is that same outcome: no error. The cleanup
after a dead worker is the ordinary caller, and it walks over parcels the dead
one may or may not have written — it must not have to ask first.

**The header is diagnostic and only that.** Every payload goes to disk wrapped
with who wrote it, when, for which cause and from which group. It is read for
counting and for the sysop; no decision is ever taken on it — what is true
about a user is the mark in the indexes, never the file.
"""

from __future__ import annotations

import os
import pickle
import shutil
import time
import urllib.parse
from pathlib import Path
from typing import Any

LOCK_NAME = ".lock"
USER_REGISTER_ITEM_NAME = "user_register_item.pickle"
CONNECTION_REGISTER_ITEM_PREFIX = "connection_register_item_"
COMMANDER_REGISTER_ITEM_NAME = "commander_register_item.pickle"

__all__ = [
    "COMMANDER_REGISTER_ITEM_NAME",
    "CONNECTION_REGISTER_ITEM_PREFIX",
    "LOCK_NAME",
    "USER_REGISTER_ITEM_NAME",
    "FreezeHandler",
]


class FreezeHandler:
    """The freezer: one directory per frozen user, under one root.

    Args:
        root_path: the freezer root, created private (0700) if missing.
    """

    def __init__(self, root_path: str | Path):
        self.root_path = Path(root_path)
        self.root_path.mkdir(mode=0o700, parents=True, exist_ok=True)

    def user_to_userkey(self, user: str) -> str:
        """The directory name ``user`` is filed under: its identity, percent-encoded.

        Args:
            user: the user identity.

        Returns:
            The key, with every separator quoted away — no identity can name a
            directory outside the root. ONE WAY: no reverse exists.
        """
        return urllib.parse.quote(user, safe="")

    @property
    def storage_free_percent(self) -> float:
        """How much of the storage the freezer lives on is still free, in percent.

        Returns:
            The free share of the whole filesystem, not the room the freezer
            takes: what runs out is the storage, and the freezer is only one of
            the things filling it.
        """
        usage = shutil.disk_usage(self.root_path)
        return 100.0 * usage.free / usage.total

    @property
    def user_folders(self) -> set[str]:
        """The keys of every folder in the freezer, as one set.

        Returns:
            The folder names, unopened. The sweep subtracts the keys of the
            users it manages from this set and discards the difference whole.
        """
        return {entry.name for entry in os.scandir(self.root_path) if entry.is_dir()}

    def take_lock(self, user: str, holder: str) -> bool:
        """Try ONCE to take the semaphore of ``user``, creating the folder if needed.

        Args:
            user: the user whose folder is being entered.
            holder: the name answering for the operation, written inside the lock.

        Returns:
            True if the semaphore is now this holder's, False if somebody holds it.

        Creates the user folder and the lock file.
        """
        folder = self._user_folder(user)
        folder.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            fd = os.open(folder / LOCK_NAME, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w") as lock_file:
            lock_file.write(holder)
        return True

    def release_lock(self, user: str, holder: str) -> None:
        """Give back the semaphore of ``user``, and the folder if nothing else is left.

        Args:
            user: the user whose folder is being left.
            holder: the name that took it — a mismatch is a protocol break.

        Raises:
            RuntimeError: the semaphore is not this holder's.

        Removes the lock file, and the folder when the lock was all it had.
        """
        current = self.lock_holder(user)
        if current != holder:
            raise RuntimeError(
                f"release of {user}: the semaphore is {current!r}, not {holder!r}"
            )
        folder = self._user_folder(user)
        os.remove(folder / LOCK_NAME)
        if not os.listdir(folder):
            os.rmdir(folder)

    def lock_holder(self, user: str) -> str | None:
        """Who holds the semaphore of ``user`` right now.

        Args:
            user: the user whose folder is asked about.

        Returns:
            The holder name, or None if the semaphore is free.
        """
        try:
            return (self._user_folder(user) / LOCK_NAME).read_text()
        except FileNotFoundError:
            return None

    def write_user_register_item(
        self, user: str, payload: Any, *, writer: str, cause: str, group: str
    ) -> None:
        """Write the user's own store, directly over whatever was there.

        Args:
            user: the user the store belongs to.
            payload: the store, pickled as it comes.
            writer: the name answering for the write.
            cause: why it is being written (freeze, login, ...).
            group: the group the writer belongs to.

        Writes the file. The caller holds the semaphore.
        """
        self._write_item(self._user_folder(user) / USER_REGISTER_ITEM_NAME, payload, writer, cause, group)

    def write_connection_register_item(
        self, user: str, cid: str, payload: Any, *, writer: str, cause: str, group: str
    ) -> None:
        """Write one connection of ``user`` — the connection and its pages.

        Args:
            user: the user the connection belongs to.
            cid: the connection identity.
            payload: the connection with its pages, pickled as it comes.
            writer: the name answering for the write.
            cause: why it is being written (freeze, login, ...).
            group: the group the writer belongs to.

        Writes the file. The caller holds the semaphore.
        """
        self._write_item(
            self._connection_path(user, cid), payload, writer, cause, group
        )

    def write_commander_register_item(self, payload: Any, *, writer: str, cause: str) -> None:
        """Write the vertex's own item at the ROOT — a file, never a folder.

        Args:
            payload: what the vertex saves of itself, pickled as it comes.
            writer: the name answering for the write.
            cause: why it is being written.

        Writes the file. It sits beside the user folders and outside them, so
        the sweep — which only ever looks at directories — cannot see it.
        """
        self._write_item(self.root_path / COMMANDER_REGISTER_ITEM_NAME, payload, writer, cause, "")

    def read_commander_register_item(self) -> Any:
        """The vertex's own item, or None when there is no such file."""
        envelope = self._read_envelope(self.root_path / COMMANDER_REGISTER_ITEM_NAME)
        return None if envelope is None else envelope["payload"]

    def drop_commander_register_item(self) -> None:
        """Remove the vertex's own item; a thing already gone is that same outcome."""
        (self.root_path / COMMANDER_REGISTER_ITEM_NAME).unlink(missing_ok=True)

    def wipe_root(self) -> None:
        """Empty the root and stand it up again — what every start does first (F4).

        Removes everything the root held and recreates it bare, with the same
        permissions ``__init__`` gives it.
        """
        shutil.rmtree(self.root_path, ignore_errors=True)
        self.root_path.mkdir(mode=0o700, parents=True, exist_ok=True)

    def drop_root(self) -> None:
        """Remove the root itself, whole; a root already gone is that same outcome."""
        shutil.rmtree(self.root_path, ignore_errors=True)

    def rename_root(self, destination: str | Path) -> None:
        """Move this whole root to *destination*, and follow it.

        Args:
            destination: where the root goes — the same filesystem, so the move
                is one atomic rename and never a copy.

        Acts on the disk and on this handler, which points at the new name
        afterwards. The rename is the only commit this class has: a directory
        that appears under its final name is complete by construction.
        """
        destination = Path(destination)
        os.rename(self.root_path, destination)
        self.root_path = destination

    def read_user_register_item(self, user: str) -> Any:
        """Read back the user's own store.

        Args:
            user: the user the store belongs to.

        Returns:
            The payload as it was written, or None if there is no such file.
        """
        envelope = self._read_envelope(self._user_folder(user) / USER_REGISTER_ITEM_NAME)
        return None if envelope is None else envelope["payload"]

    def read_connection_register_item(self, user: str, cid: str) -> Any:
        """Read back one connection of ``user`` with its pages.

        Args:
            user: the user the connection belongs to.
            cid: the connection identity.

        Returns:
            The payload as it was written, or None if there is no such file.
        """
        envelope = self._read_envelope(self._connection_path(user, cid))
        return None if envelope is None else envelope["payload"]

    def get_item_header(self, user: str, cid: str | None = None) -> dict[str, Any] | None:
        """The diagnostic header of an item — for counting and for the sysop.

        Args:
            user: the user the item belongs to.
            cid: a connection identity, or None for the user's own store.

        Returns:
            The header (writer, ts, cause, group), or None if there is no such
            file. Never a ground for a decision.
        """
        path = (
            self._user_folder(user) / USER_REGISTER_ITEM_NAME if cid is None
            else self._connection_path(user, cid)
        )
        envelope = self._read_envelope(path)
        return None if envelope is None else envelope["header"]

    def drop_user_register_item(self, user: str) -> None:
        """Discard the user's own store, adopted or spent.

        Args:
            user: the user the store belongs to.

        Removes the file if it is there — an absence is the same outcome, not an
        error. The folder stays until the semaphore is released.
        """
        (self._user_folder(user) / USER_REGISTER_ITEM_NAME).unlink(missing_ok=True)

    def drop_connection_register_item(self, user: str, cid: str) -> None:
        """Discard one connection of ``user``, adopted or spent.

        Args:
            user: the user the connection belongs to.
            cid: the connection identity.

        Removes the file if it is there — an absence is the same outcome, not an
        error. The folder stays until the semaphore is released.
        """
        self._connection_path(user, cid).unlink(missing_ok=True)

    def drop_user_folder(self, user: str) -> bool:
        """Discard everything ``user`` has in the freezer, semaphore included.

        Args:
            user: the user leaving the freezer for good.

        Returns:
            Whether the freezer was holding anything of his.

        Raises:
            RuntimeError: the folder survived its own removal.

        Removes the folder and verifies it is gone.
        """
        return self._drop_folder(self.user_to_userkey(user))

    def cleanup_frozen(self, claimed: set[str]) -> list[str]:
        """Sweep the freezer of everything that belongs to nobody.

        Args:
            claimed: the keys somebody still answers for — the caller computes
                them forward from its own identities, since no identity ever
                comes back from a folder name.

        Returns:
            The keys swept away, so the caller can count and name them.

        Removes those folders, each verified gone.
        """
        orphans = sorted(self.user_folders - claimed)
        for userkey in orphans:
            self._drop_folder(userkey)
        return orphans

    def _drop_folder(self, userkey: str) -> bool:
        """Remove one folder of the freezer by its key, verify it is gone, say if it was."""
        folder = self.root_path / userkey
        was_there = folder.exists()
        shutil.rmtree(folder, ignore_errors=True)
        if folder.exists():
            raise RuntimeError(f"freezer folder {userkey}: it survived its removal")
        return was_there

    def _user_folder(self, user: str) -> Path:
        return self.root_path / self.user_to_userkey(user)

    def _connection_path(self, user: str, cid: str) -> Path:
        name = f"{CONNECTION_REGISTER_ITEM_PREFIX}{self.user_to_userkey(cid)}.pickle"
        return self._user_folder(user) / name

    def _write_item(self, path: Path, payload: Any, writer: str, cause: str, group: str) -> None:
        header = {"writer": writer, "ts": time.time(), "cause": cause, "group": group}
        path.write_bytes(pickle.dumps({"header": header, "payload": payload}))

    def _read_envelope(self, path: Path) -> dict[str, Any] | None:
        try:
            return pickle.loads(path.read_bytes())
        except FileNotFoundError:
            return None
