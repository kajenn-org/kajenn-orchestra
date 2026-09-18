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

"""Macro 2 end to end: the worker is born, serves, parks a user, wakes him, leaves.

One story on the real things: a real child process running the package's own
``WorkerEntry``, a real Unix socket under the M1 ``WorkerHandler``, a real
deposit on disk read back from the parent side through a ``FreezeHandler`` of
its own. What is doubled is only what Macro 3 will bring: the group above the
handler (``GroupStub``) and the driver of the story, which plays the fold — it
is the test that says who is on board and that orders the departure.

The worker in the child is ``DrivenWorker``, a ``SpaWorker`` subclass of this
test package: it fills the ``wsgi_app`` seam with a tiny site, the way the
genropy-asgi bridge fills it with a whole one, and it answers the two orders the
protocol does not carry — see its docstring, which declares those routing keys as
the test's own.

The story, in order: it is born with a photo and the global store on board; it
serves http calls through the seam, and the rows are born and the clocks
stamped; the driver orders the shot, and inside it the user who went silent is
flagged for cession by the VALVE — one more reason for a ``'T'``, not a road of
its own — while the one who has just spoken is kept; past the gate the silent
one is in the deposit with his placement to be assigned and his row is gone from
memory entirely; his next call carries the verdict and he comes home — to
whatever worker the vertex will name, which here is this one because there is
only one; the handler asks the process to leave and the reply to that order
carries the flagged photo at once; past the gate everybody is in the deposit and
the process ENDS BY ITSELF, exit code 0 and nobody killed it. That death is the
one somebody was waiting for, so the handler writes ``quitted`` — not a word of
WILD anywhere — and the successor launched on the same name and socket presents
itself with a fresh photo over a deposit nothing has swept. That last picture is
what Macro 3 inherits.

The sockets and the deposit live under a short ``mkdtemp`` root: the system caps
a UDS path at about a hundred characters and pytest's own directory is already
past it, which is the very reason worker names are short.
"""

from __future__ import annotations

import logging
import time
from http.cookies import SimpleCookie
from typing import Any

import pytest
from tests.orchestration.frame_helpers import call_endpoint, read_control

from kajenn_orchestra.spa_app import SPA_CONNECTION_ID_COOKIE
from kajenn_orchestra.orchestration import (
    FreezeHandler,
    SpaWorker,
    UserOnHold,
    WorkerHandler,
)
from kajenn_orchestra.orchestration.worker_connector import ENVELOPE_SLOT_WORKER_SNAPSHOT

from .group_stub import GroupStub
from .conftest import kill_process, wait_for

WORKER_NAME = "standard_0001"
GROUP = "standard"
ENTRY_MODULE = "kajenn_orchestra.orchestration.worker_entry"
DRIVEN_WORKER = f"{__name__}:DrivenWorker"

#: The two orders of this story that the protocol does NOT carry. They are the
#: TEST's own routing keys: the shot and the transfer cycle are verbs of the
#: worker, and in the machine proper they are not orders at all — the shot is
#: taken by whoever composes a due photo, and the cycle follows it. WHO is ceded
#: is named from above, by the group, which this story does not have: so the
#: driver names him, through a subclass that answers for these two. The
#: departure, one of these in Macro 2, is now the protocol's own ``/op/quit`` and
#: is ordered through the handler's ``quit_process``.
PLAN_ORDER = "/op/plan_transfers"
EXECUTE_ORDER = "/op/execute_transfers"

#: The gate the departures wait out, shrunk from its own default through the
#: spawn grammar, so the story is told in fractions of a second.
GATE_DELAY = 0.5

#: The bounds every wait of this test is given: multiples of the times above and
#: of the handler's own beat timeout, never a measure of anything.
CALL_TIMEOUT = 10.0
DEATH_TIMEOUT = 15.0


class DrivenWorker(SpaWorker):
    """The worker of the story: it hosts a tiny site, and it takes three orders.

    The site is the real seam — ``wsgi_app``, what the genropy-asgi bridge
    assigns — and it answers with the facts of the request plus the global store
    this process was handed at its presentation, which is how the parent reads
    back what the child holds.

    The two orders are this test's own. ``plan_transfers`` and
    ``execute_transfers`` are verbs of ``SpaWorker`` that no op routes to — the
    machine takes the shot when a photo is due, and the cycle follows it. Rather
    than teach the protocol something the design has not decided, the subclass —
    the place a consumer already extends the worker — answers for them, exactly as
    the M1 child stub answers for the deposit orders it is driven with.
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        self.wsgi_app = self.tiny_site

    def tiny_site(self, environ: dict[str, Any], start_response: Any) -> list[bytes]:
        """The WSGI callable: say back who asked, for what, and what this process holds."""
        identity = environ["genro.identity"]
        cid = cid_of(environ)
        if identity is not None and self.connection_register.get(cid) is None:
            # A real site registers its connection while serving (doctrine of
            # 2026-08-21: the rows are the site's) — keyed by the cookie, the
            # core-only world where no site renames it.
            self.new_connection(cid, user=identity)
        start_response(
            "200 OK",
            [
                ("Content-Type", "text/plain"),
                ("X-Worker", self.name),
            ],
        )
        return [f"{environ['REQUEST_METHOD']} {environ['PATH_INFO']} for {identity}".encode()]

    async def answer_call(self, frame: Any) -> None:
        """Answer the two orders of the driver, and hand everything else upstairs.

        Args:
            frame: the CALL as it came off the wire.

        The shot is taken BEFORE the reply is composed, which is the whole point
        of the shot logic: the photo riding the reply is the one the transfers
        just flagged. Whom it cedes travels in the order, since nothing below the
        group judges that. The cycle order is answered when the cycle is OVER, so
        the reply carries what the departures said and the picture they left.
        """
        if frame.path == PLAN_ORDER:
            self.plan_transfers(transfer_users=(read_control(frame) or {}).get("users", ()))
            await self.send_reply(frame, result={})
        elif frame.path == EXECUTE_ORDER:
            await self.execute_transfers()
            await self.send_reply(frame, result={})
        else:
            await super().answer_call(frame)


def http_call(cid: str, identity: str, *, path: str, **payload: Any) -> dict[str, Any]:
    """The http CALL form as the front packs it: the request, and who it is for."""
    return {
        "http": {
            "method": "GET",
            "path": path,
            "query_string": "",
            "headers": [
                ["host", "site.example:8080"],
                ["cookie", f"{SPA_CONNECTION_ID_COOKIE}={cid}"],
            ],
            "body": "",
            "cid": cid,
        },
        "identity": identity,
        **payload,
    }


def cid_of(environ: dict[str, Any]) -> str | None:
    """The connection a request carries, read where a real site reads it."""
    morsel = SimpleCookie(environ.get("HTTP_COOKIE", "")).get(SPA_CONNECTION_ID_COOKIE)
    return None if morsel is None else morsel.value


def known_at_the_vertex(commander: Any, cid: str, user: str) -> None:
    """What the login will do in Macro 4: this cid is that person's, and he has a row.

    The vertex mints guests from a cid on its own; a person with a name of his own
    is the login's business, and the login is not built. So the story writes the
    identity and lets the vertex mint the row under it.
    """
    commander.connection_user_map[cid] = user
    commander.resolve_user(cid)


def body_of(reply: dict[str, Any]) -> str:
    """The site's answer, decoded out of the wire form."""
    return reply["result"]["body"].decode()


def announced(reply: dict[str, Any]) -> list[str]:
    """The protocol names the reply carried up, in order."""
    return [event["op"] for event in reply["worker_events"]]


@pytest.fixture
def group(short_root):
    """The group of the story, with the real chain and the real vertex above it."""
    return GroupStub(short_root / "frozen_users")


@pytest.fixture
def deposit(short_root):
    """The deposit as the parent reads it — the same root the child is given."""
    return FreezeHandler(short_root / "frozen_users")


@pytest.fixture
async def handler(short_root, group, repo_on_pythonpath):
    """The handler under test; no process and no socket of its own outlives the test."""
    worker_handler = WorkerHandler(
        group,
        WORKER_NAME,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module=ENTRY_MODULE,
        main_threadpool_size=4,
        aux_threadpool_size=1,
        worker_class=DRIVEN_WORKER,
        worker_kwargs={
            "group": GROUP,
            "transfer_start_delay": GATE_DELAY,
            # Every envelope carries a photo: this story reads what the photo
            # says, and the throttle has its own tests one phase down.
            "worker_snapshot_ttl": 0,
        },
        process_ping_timeout=CALL_TIMEOUT,
    )
    group.worker_handler = worker_handler
    yield worker_handler
    if worker_handler.process is not None:
        kill_process(worker_handler.process)
        await wait_for(lambda: not worker_handler.process.alive)
    await worker_handler.connector.stop()


async def test_the_worker_is_born_serves_parks_wakes_departs_and_a_successor_takes_over(
    handler, group, deposit, caplog
):
    caplog.set_level(logging.INFO)
    # The fold is REAL now — the chain of the envelope over the vertex of Macro 3 —
    # so the two people of the story are known up there the way the login will make
    # them known. Who the handler has on board is still the driver's word.
    commander = group.spa_commander
    known_at_the_vertex(commander, "cid-a", "mario")
    known_at_the_vertex(commander, "cid-b", "anna")
    handler.hosted_users.update({"mario", "anna"})

    # BORN. The handler opens the wire and spawns the child, which presents
    # itself with a photo that already knows which process it is — and holds
    # nobody yet.
    await handler.launch_process()
    born = handler.process
    assert handler.connector.connected is True
    assert handler.worker_snapshot["pid"] == born.pid
    assert handler.worker_snapshot["name"] == WORKER_NAME
    assert handler.worker_snapshot["group"] == GROUP
    assert handler.worker_snapshot["user_count"] == 0
    assert handler.worker_snapshot["users"] == {}
    assert handler.worker_snapshot["connections"] == {}

    # SERVES. The request crosses the process boundary inside the envelope and
    # comes back as the site answered it — headers and body whole. The rows are
    # born on the way in, and the worker events say so.
    before = time.time()
    reply = await call_endpoint(
        handler.connector,
        "/site/invoices",
        http_call("cid-a", "mario", path="/invoices"),
        timeout=CALL_TIMEOUT,
    )

    assert reply["result"]["status"] == 200
    assert ["X-Worker", WORKER_NAME] in reply["result"]["headers"]
    assert body_of(reply) == "GET /invoices for mario"
    assert announced(reply) == ["new_user", "new_connection"]
    photo = reply[ENVELOPE_SLOT_WORKER_SNAPSHOT]
    assert sorted(photo["users"]["mario"]) == ["item", "transfer_flag"]
    assert photo["users"]["mario"]["transfer_flag"] is None
    assert photo["users"]["mario"]["item"]["state"] == "active"
    assert photo["connections"]["cid-a"]["user"] == "mario"
    assert photo["connections"]["cid-a"]["last_rpc_ts"] >= before
    # The fold read that envelope: the births it announced are the ones the vertex
    # had already written at the minting, so they change nothing — and the photo is
    # filed on the handler, which is the only thing the bottom layer does with it.
    assert commander.user_map["mario"] == {"group": None, "frozen": False, "on_hold": None}
    assert handler.worker_snapshot == photo

    # A second user arrives: one of them is about to be parked, the other has
    # just spoken.
    arrival = await call_endpoint(
        handler.connector,
        "/site/orders",
        http_call("cid-b", "anna", path="/orders"),
        timeout=CALL_TIMEOUT,
    )
    assert announced(arrival) == ["new_user", "new_connection"]

    # THE SHOT FLAGS HIM. The driver orders the shot and names whom it cedes —
    # the judgment is the group's rung, which this story has not got — and the
    # one nobody named is kept. The decision travels on the photo that answers
    # the order.
    planned = await handler.connector.call(PLAN_ORDER, {"users": ["mario"]}, timeout=CALL_TIMEOUT)

    flagged = planned[ENVELOPE_SLOT_WORKER_SNAPSHOT]["users"]
    assert {user: pair["transfer_flag"] for user, pair in flagged.items()} == {
        "mario": "T",
        "anna": None,
    }
    # And the vertex read that decision off the photo: whoever is on his way out is
    # in the waiting room, so a request of his does not get routed to a process that
    # is emptying. The one who was kept is untouched.
    with pytest.raises(UserOnHold):
        commander.resolve_user("cid-a")
    assert commander.resolve_user("cid-b") == "anna"

    # HE DEPARTS. Past the gate he goes to the deposit like anybody else
    # leaving: the placement the worker event carries is NOBODY'S — the vertex
    # decides where he wakes — and his row leaves memory whole, connection and
    # all. His store and his connection are readable from the parent side.
    parked = await handler.connector.call(EXECUTE_ORDER, timeout=CALL_TIMEOUT)

    # The cycle answers no CALL: what it announced went up on the worker's own
    # channel and was folded before the order was answered — the order's own
    # reply carries nothing (kajenn #60).
    assert parked["worker_events"] == []
    assert deposit.read_user_register_item("mario") is not None
    assert deposit.read_connection_register_item("mario", "cid-a") is not None
    assert deposit.get_item_header("mario")["writer"] == WORKER_NAME
    # The fold turned that worker event into the two facts it is: at the vertex the
    # mark says his state is on disk and the wait is over, in the group his
    # placement is to be assigned again.
    assert commander.user_is_frozen("mario") is True
    assert commander.user_map["mario"]["on_hold"] is None
    assert group.user_worker_map == {"mario": None, "anna": "standard_0001"}
    # The photo rode the announcement, and the handler holds it as the last one.
    photo = handler.worker_snapshot
    assert "mario" not in photo["users"]
    assert photo["users"]["anna"]["item"]["state"] == "active"
    assert list(photo["connections"]) == ["cid-b"]

    # WAKES BY VERDICT. His next call carries the verdict, and only that
    # authorises the trip: the store comes home, the connection finds itself in
    # the deposit and is born again through the ordinary worker events, both
    # parcels are taken away and the folder goes with the last of them. WHICH
    # worker he comes home to is the vertex's to say — here it is this one
    # because this story runs a single worker.
    woken = await call_endpoint(
        handler.connector,
        "/site/invoices",
        http_call("cid-a", "mario", path="/invoices", user_frozen=True),
        timeout=CALL_TIMEOUT,
    )

    assert announced(woken) == ["user_adopted", "new_connection"]
    assert body_of(woken) == "GET /invoices for mario"
    assert deposit.read_user_register_item("mario") is None
    assert deposit.read_connection_register_item("mario", "cid-a") is None
    assert deposit.user_folders == set()
    assert woken[ENVELOPE_SLOT_WORKER_SNAPSHOT]["users"]["mario"]["item"]["state"] == "active"
    # The vertex turned the mark off on the same worker event: he lives in a process
    # again, so his next request is routed there and not to the deposit.
    assert commander.user_is_frozen("mario") is False

    # QUITS ON ORDER. The handler asks the process to leave and waits for it to
    # be gone. The answer to that order came back AT ONCE, carrying the photo the
    # transfers had just flagged — everybody ceded, nobody kept — and the wire
    # filed it: the decision travels in the shot it was taken in, which is what
    # the fold acts on while the departures are still running.
    await handler.quit_process()

    # Each departure of the cycle was announced on the worker's own channel and
    # folded as it happened (kajenn #60): the vertex marks both frozen, and
    # the last photo the handler holds shows the process empty.
    assert commander.user_is_frozen("mario") is True
    assert commander.user_is_frozen("anna") is True
    assert handler.worker_snapshot["user_count"] == 0

    # Past the gate everybody is in the deposit and the process ends BY ITSELF:
    # the exit code is its own clean 0, and nothing in this test killed it. What
    # the freezes would have announced — the placement of each, nobody's — dies
    # with the wire the worker closes behind itself.
    await wait_for(lambda: not born.alive, timeout=DEATH_TIMEOUT)

    assert born.exit_code == 0
    assert deposit.user_folders == {
        deposit.user_to_userkey("mario"),
        deposit.user_to_userkey("anna"),
    }
    assert deposit.read_user_register_item("anna") is not None
    assert deposit.read_connection_register_item("anna", "cid-b") is not None
    assert deposit.lock_holder("mario") is None
    assert deposit.lock_holder("anna") is None

    # THE SEAM M2 DECLARED IS CLOSED. That departure is the death somebody was
    # waiting for: the wait the order parked is what classifies it, so the state
    # says `quitted` and nothing in the log says WILD. The group was woken to read
    # exactly that, with the users the vertex still had on board — nobody: each
    # departure was announced and folded as it happened (kajenn #60).
    await wait_for(lambda: group.wakes == ["quitted"])
    assert handler.state == "quitted"
    assert group.users_on_board == [set()]
    assert "WILD death" not in caplog.text
    assert handler.connector.connected is False

    # AND THE ROUND CONSUMES IT. What the group does at that round is Macro 3's
    # own; what the CHAIN does with it exists already, so the driver plays the
    # round: the ended state becomes the worker event, the group takes the handler
    # out, and the freezer marks of the two — written when their departures were
    # announced — stay as they are.
    handler.envelope_handler.report_death()

    assert group.dropped_workers == [WORKER_NAME]
    assert commander.user_is_frozen("mario") is True
    assert commander.user_is_frozen("anna") is True
    assert group.user_worker_map == {"mario": None, "anna": None}

    # RELAUNCH. The same handler launches a successor on the same name and the
    # same socket: it presents itself with a fresh photo of its own process,
    # holding nobody. The deposit is untouched by the rebirth — the parcels are
    # where the dead one left them, waiting for whoever will be told to take
    # them. This is the picture Macro 3 inherits.
    await handler.launch_process()

    assert handler.process.pid != born.pid
    assert handler.connector.connected is True
    assert handler.worker_snapshot["pid"] == handler.process.pid
    assert handler.worker_snapshot["user_count"] == 0
    assert deposit.read_user_register_item("mario") is not None
    assert deposit.read_user_register_item("anna") is not None
