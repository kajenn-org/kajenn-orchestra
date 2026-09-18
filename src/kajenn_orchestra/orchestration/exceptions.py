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

"""What the orchestration raises instead of answering.

A refusal here is not a failure: it is the ANSWER, and the caller's own next step
is written in which exception arrived. So the reasons are types, not codes to
read out of a return value, and nobody has to remember what a False meant.

``UserOnHold`` is the gate of the waiting room: the user is mid-departure, so the
request that just arrived cannot be routed at his old address and must not open a
new one either — it waits, and how long a request may wait belongs to whoever
answers requests. The row at the vertex carries the cause, and the ONLY way that
field is read is by catching this.

``AssignmentRefused`` and its family are the placement: a group asks its workers
one at a time and each of them answers by taking the user or by RAISING, so a
walk is a ``try`` and the reason a worker said no is its class. The base is what
the group itself raises when it has asked everybody — nobody took him, and
whoever asked answers 503. On its way out to a request it carries ``retry_after``,
composed where the clocks are so that whoever writes the header has none.

``SiteFailedRequest`` is the other end: the placement was sound and the wire
is up, but the site inside the process failed the request. A refusal says come
back later; this one says the upstream is broken, which is a different answer.
"""

from __future__ import annotations

__all__ = [
    "AssignmentRefused",
    "NoRoomError",
    "UserOnHold",
    "SiteFailedRequest",
    "WorkerQuittingError",
]


class UserOnHold(Exception):
    """This user is between two homes: whatever asked for him has to wait.

    Args:
        user: the identity that is on hold.
        cause: what put him there, for the log and for the sysop — never for a
            caller to branch on.
    """

    def __init__(self, user: str, cause: str) -> None:
        super().__init__(f"{user} is on hold: {cause}")
        self.user = user
        self.cause = cause


class AssignmentRefused(Exception):
    """Nobody took this user: the answer to a placement, never a failure of it.

    Args:
        user: the identity that was not placed.
        cause: who refused and why, for the log — the branching is on the class.
        retry_after: how many seconds before the machine will have decided again;
            None on the refusals of a single worker, which the walk catches and
            never lets out. Whoever answers requests reads it off the class and
            has no clock of its own to consult.
    """

    def __init__(self, user: str, cause: str, retry_after: float | None = None) -> None:
        super().__init__(f"{user} was not placed: {cause}")
        self.user = user
        self.cause = cause
        self.retry_after = retry_after


class NoRoomError(AssignmentRefused):
    """He does not fit: this worker plus what he is expected to cost is over the setpoint."""


class WorkerQuittingError(AssignmentRefused):
    """This worker's process is leaving, or has left: it will never take anybody again."""


class SiteFailedRequest(Exception):
    """The worker answered a request of the site with a failure instead of an answer.

    Args:
        user: whose request it was.
        cause: what the child said went wrong.
        status: what the front should answer, when the worker knew — a REFUSAL
            of the client knows (a page that never opened its channel is a 409,
            and its words are meant for the browser). ``None`` for everything
            else, which is the upstream's own failure and stays a 502 with the
            fixed text.

    The placement is sound and the wire is up: what failed is the site inside the
    process. It is the upstream's failure and never the client's, which is why it
    is a class of its own and not one of the refusals — except when the child
    named a status, and then those words ARE the answer.
    """

    def __init__(self, user: str, cause: str, status: int | None = None) -> None:
        super().__init__(f"the worker of {user} failed the request: {cause}")
        self.user = user
        self.cause = cause
        self.status = status
