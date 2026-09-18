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

"""The group of the tests: the level that does not exist yet, and what it must do.

``GroupHandler`` is built in its own phase. Until then a handler still needs a
group above it — for the wake it rings when its process ends, and for the layer of
the chain everything it hears climbs through — so this is that group, and it is
NOT a fake: the layer it carries is the real ``GroupEnvelopeHandler`` over a real
``SpaCommander``, so a worker event born in a child process lands in the real
indexes.

What it stands in for is only the group's OWN work, and this is therefore the
list of what the real one owes the chain: the wake (``ping_now``), how full a
photo reads and the setpoint past which it brings the round forward, the
placement of its users
(``user_worker_map``, ``None`` meaning "to be assigned", written by the chain
straight into the map) and taking a dead handler out of the group
(``drop_worker`` — silent here on a name it does not carry, where the real one is
LOUD). Everything the tests assert about the group is asserted on these.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from kajenn_orchestra.orchestration import GroupEnvelopeHandler, SpaCommander
from kajenn_orchestra.orchestration.group_handler import GroupDispatcher


class GroupStub:
    """The GroupHandler seen from below: its wake, its placements, its layer of the chain.

    Args:
        frozen_users_path: the deposit root the vertex reads — the same one the
            workers are given.
        name: the group's name.
        spa_commander: an existing vertex to hang under; one of its own when None.
    """

    def __init__(
        self,
        frozen_users_path: str | Path,
        *,
        name: str = "standard",
        spa_commander: SpaCommander | None = None,
    ) -> None:
        self.name = name
        self.spa_commander = spa_commander or SpaCommander(frozen_users_path)
        self.envelope_handler = GroupEnvelopeHandler(self, self.spa_commander.envelope_handler)
        self.group_dispatcher = GroupDispatcher(self)
        self.user_worker_map: dict[str, str | None] = {}
        #: No template: a handler under this stub spawns its process, as the
        #: handlers of a group with no engine factory declared do.
        self.template = None
        #: What the handler was in when it rang, in the order the wakes came.
        self.wakes: list[str] = []
        #: Who was on board at each of those wakes.
        self.users_on_board: list[set[str]] = []
        #: The names of the handlers taken out of the group, in order.
        self.dropped_workers: list[str] = []
        #: Whether the next photo counts as urgent — the tests set it, and the
        #: reading below turns it into the occupancy the chain judges.
        self.urgent_snapshots = False
        self.restart_occupancy_max_percent = 95.0
        #: CPU admission stays OFF under this stub, as on a real group by
        #: default: the threshold decides whether a photo rings the wake (#43).
        self.cpu_admission_close_percent: float | None = None
        self.cpu_admission_reopen_percent = 40.0
        #: The handler under this group; the tests assign it after construction.
        self.worker_handler: Any = None

    @property
    def worker_handler_map(self) -> dict[str, Any]:
        """The one handler under this stub, by name — what ``group/announce`` looks up."""
        if self.worker_handler is None:
            return {}
        return {self.worker_handler.name: self.worker_handler}

    def ping_now(self) -> None:
        """The wake: at this round the group reads the state and who was on board."""
        self.wakes.append(self.worker_handler.state)
        self.users_on_board.append(set(self.worker_handler.hosted_users))

    def get_memory_occupancy_percent(
        self, worker_snapshot: dict[str, Any] | None
    ) -> float:
        """How full the worker of this photo is: full when the tests want it urgent."""
        return 100.0 if self.urgent_snapshots else 0.0

    def drop_worker(self, worker: str) -> None:
        """Take a dead handler out of the group, with the placements that pointed at it."""
        self.dropped_workers.append(worker)
        for user in list(self.user_worker_map):
            if self.user_worker_map[user] == worker:
                del self.user_worker_map[user]
