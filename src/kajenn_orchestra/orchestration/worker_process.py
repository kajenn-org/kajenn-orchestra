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

"""The two questions a handler asks about its worker's process, however it was born.

A worker is born in one of two ways, and the difference is who its parent is. A
**spawned** worker is a child of its own handler, which holds its ``Popen``. A
**forked** worker is a child of its group's template, and the handler has only
its pid — POSIX grants no adoption, and none is needed.

None is needed because the handler never watched the process to begin with: the
death it acts on is the death of the WIRE (``on_child_lost``), not a ``waitpid``.
Of the ``Popen`` it used exactly two things — whether the process is still there,
and the pid to aim ``os.killpg`` at, which does not require being the parent. The
exit code it never read. So those two things are the whole interface, and both
births can answer them.

``SpawnedProcess`` answers one more, ``exit_code``, because it can: it is the
parent, so the status is its own to read. ``ForkedProcess`` does not have it, and
that absence is the truth — the template is the one that reaps a forked child, so
nobody else can tell how it went.

Two weaknesses come with the forked form, and they are accepted, not hidden.
``os.kill(pid, 0)`` sees a zombie as alive, so a forked child reads as alive until
its template has reaped it. And a pid, once reaped, can be handed to a stranger:
the wire stays the authoritative signal, and the pid check is hygiene.
"""

from __future__ import annotations

import os
import subprocess

__all__ = ["ForkedProcess", "SpawnedProcess", "WorkerProcess"]


class WorkerProcess:
    """One worker's process, seen through the only two questions asked of it."""

    @property
    def alive(self) -> bool:
        """Whether the process is still there."""
        raise NotImplementedError

    @property
    def pid(self) -> int:
        """Its pid: what a signal is aimed at, and what a snapshot reports."""
        raise NotImplementedError


class SpawnedProcess(WorkerProcess):
    """A worker spawned by its own handler, which is therefore its parent.

    Args:
        process: the ``Popen`` the handler holds.
    """

    def __init__(self, process: subprocess.Popen[bytes]) -> None:
        self.process = process

    @property
    def alive(self) -> bool:
        """Whether it is still there; the read also buries it once it is gone."""
        return self.process.poll() is None

    @property
    def pid(self) -> int:
        """Its pid."""
        return self.process.pid

    @property
    def exit_code(self) -> int | None:
        """How it went, or None while it is still going — only a parent can say."""
        return self.process.poll()


class ForkedProcess(WorkerProcess):
    """A worker forked by its group's template, which is its parent instead.

    Args:
        pid: the pid the template answered with.
    """

    def __init__(self, pid: int) -> None:
        self._pid = pid

    @property
    def alive(self) -> bool:
        """Whether this handler's own child is still there; a zombie answers yes.

        Two ways to answer no. The pid is gone, which is the ordinary death. Or
        the pid is there and cannot be signalled, which means it now belongs to
        somebody else: the child was reaped and the number handed on, so this
        handler's process is gone either way.
        """
        try:
            os.kill(self.pid, 0)
        except (ProcessLookupError, PermissionError):
            return False
        return True

    @property
    def pid(self) -> int:
        """Its pid, which is all this handler was ever given."""
        return self._pid
