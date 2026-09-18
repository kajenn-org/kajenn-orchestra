# Copyright 2025 Softwell S.r.l.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The cadence of a periodic method, declared on the method itself.

Every rung of the pool has work that must happen not at every turn but every so
many: the group reads its own shape every few turns, the vertex prunes the frozen
every few minutes. ``@every`` puts that number ON the method, where the knowledge
is, so nobody above has to keep a table of who is due when — a rung gives its
periodic methods a turn and each one decides for itself.

**The turn is counted on the instance.** Two groups are two counts, so the count
cannot live on the function: a function object is one per class, and hanging the
count there would give N instances one shared clock and a rotation nobody asked
for. Each object carries its own ``beat_counts``, one row per periodic method.

**A periodic failure never touches whoever gave the turn.** The wrapper logs the
traceback and counts the failure in that row: a task with a bad disk raising at
every turn must not take down the siblings of its turn — least of all the check
that would have said the disk is bad. So the row IS the dashboard: how many turns
it has seen, how many times it ran, how many failed, and what the last failure
said. A run that goes well clears that last one, so the row speaks of now.

**Whoever wants it now says so.** ``now=True`` runs the method regardless of the
count — what a monitor asking for a fresh reading needs. The failure is swallowed
there too: the answer is in the row, and asking is never a way to be hurt.
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Callable, Coroutine

__all__ = ["every"]

Periodic = Callable[..., Coroutine[Any, Any, Any]]


def every(beats: int) -> Callable[[Periodic], Periodic]:
    """Let a periodic method through one turn in ``beats``, or now if asked.

    Args:
        beats: how many turns of its owner pass between two runs. It is read at
            every call off the wrapper's own ``every_beats``, so a test can move
            the cadence without touching the caller.

    Returns:
        The method wrapped in its count.
    """

    def decorate(method: Periodic) -> Periodic:
        @functools.wraps(method)
        async def periodic(self: Any, *, now: bool = False, **kwargs: Any) -> Any:
            row = self.beat_counts.setdefault(
                method.__name__, {"turns": 0, "runs": 0, "errors": 0, "last_error": None}
            )
            row["turns"] += 1
            if not now and row["turns"] % periodic.every_beats:
                return None
            row["runs"] += 1
            try:
                answer = await method(self, **kwargs)
            except Exception as failure:
                row["errors"] += 1
                row["last_error"] = f"{type(failure).__name__}: {failure}"
                logging.getLogger(method.__module__).exception(
                    "%s: %s failed", type(self).__name__, method.__name__
                )
                return None
            row["last_error"] = None
            return answer

        periodic.every_beats = beats
        return periodic

    return decorate
