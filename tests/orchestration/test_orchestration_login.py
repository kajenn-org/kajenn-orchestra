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

"""The login: what the site's call changes at once, and what the call's tail does.

The story in one line: a visitor browses as a guest and his store fills up; he
logs in and the connection changes owner IN THAT INSTANT, because the caller
reads the row back in the same breath; the request goes on being served under the
new identity; and only when the call is over does the connection travel to the
deposit, carrying what the guest had accumulated. His next request — wherever the
pool puts him — finds it.

The deposit is a real ``FreezeHandler`` on a real directory: what a login leaves
on disk is the whole point, and a fake filesystem would judge nothing.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from tests.orchestration.frame_helpers import read_control
from genro_bag import Bag

from kajenn_orchestra.orchestration import (
    FreezeHandler,
    GroupHandler,
    SpaCommander,
    SpaWorker,
)
from kajenn_orchestra.orchestration.worker_handler import ANNOUNCE_OP_PATH, WorkerHandler
from kajenn_orchestra.orchestration.spa_worker import GUEST_PREFIX

from .conftest import attach_wire

WORKER_NAME = "standard_0001"


@pytest.fixture
def deposit(tmp_path):
    return FreezeHandler(tmp_path / "frozen_users")


@pytest.fixture
def worker(deposit):
    worker = SpaWorker(WORKER_NAME, freeze_handler=deposit, deposit_lock_retry_interval=0.01)
    worker.open_request_slot()
    return worker


def browsing_guest(worker: SpaWorker, cid: str = "a1b2", pages: int = 1) -> str:
    """A visitor who arrived anonymous, opened a page and filled his store a little."""
    worker.add_connection(cid)
    guest = f"{GUEST_PREFIX}{cid}"
    for index in range(pages):
        worker.add_page(f"page-{index}", cid)
    worker.user_register.get(guest)["store"]["cart.item"] = "a lamp"
    return guest


def events_of(worker: SpaWorker, op: str) -> list[dict]:
    """The announcements of one kind this worker has ready to send."""
    return [event for event in worker.worker_events if event["op"] == op]


# -- what the call sees, in the instant of the login --


async def test_the_connection_changes_owner_in_the_same_breath(worker):
    guest = browsing_guest(worker)

    worker.change_connection_user("a1b2", "mario", user_tags="admin")

    # The caller reads the row back at once and must see the new identity.
    assert worker.connection_register.get("a1b2")["user"] == "mario"
    assert worker.connection_register.get("a1b2")["user_tags"] == "admin"
    assert worker.user_register.get("mario")["connections"] == {"a1b2"}
    # The guest entry did not stay behind empty: it BECAME mario's, key and all.
    assert guest not in worker.user_register
    # The pages were not touched: their owner is derived through the connection.
    assert worker.page_register.get("page-0")["connection_id"] == "a1b2"
    assert worker.registry.page_user("page-0") == "mario"


async def test_the_new_identity_inherits_the_guest_store(worker):
    """The guest item follows its first real identity — the registry's own rule."""
    guest = browsing_guest(worker)
    guest_store = worker.user_register.get(guest)["store"]
    guest_store["draft"] = "half typed"

    worker.change_connection_user("a1b2", "mario")

    assert worker.user_register.get("mario")["store"] is guest_store
    assert worker.user_register.get("mario")["store"]["draft"] == "half typed"


async def test_nobody_logs_in_as_a_guest(worker):
    browsing_guest(worker)

    with pytest.raises(ValueError):
        worker.change_connection_user("a1b2", "guest_somebody")


async def test_a_connection_this_worker_never_saw_is_loud(worker):
    with pytest.raises(KeyError):
        worker.change_connection_user("nobody", "mario")


async def test_the_departure_promised_to_the_guest_is_dropped(worker):
    """A guest that is ceasing to exist is not carried to the deposit."""
    guest = browsing_guest(worker)
    worker.plan_transfers(transfer_users=[guest])

    worker.change_connection_user("a1b2", "mario")

    assert worker._transfer_flags == {}


async def test_the_departure_promised_to_a_real_identity_survives_his_avatar_switch(worker):
    """R8 admits the real prior, and a prior that stays keeps the departure he was promised."""
    worker.add_connection("a1b2")
    worker.add_connection("c3d4")
    worker.change_connection_user("a1b2", "mario")
    worker.change_connection_user("c3d4", "mario")
    worker.plan_transfers(transfer_users=["mario"])

    worker.change_connection_user("a1b2", "carlo")

    assert worker._transfer_flags == {"mario": "T"}


async def test_an_avatar_switch_does_not_strand_the_wait_on_the_person_who_stays(tmp_path):
    """The consequence of the flag: without it the wait on him has nothing to release it.

    The vertex parks whoever the photo shows on his way out, and the ONLY things
    that let that wait go are the marks a departure or a homecoming brings. An
    avatar switch that cancelled the departure of a real identity would leave him
    on hold with nothing left able to clear it: every other browser of his would
    wait out the whole budget and be answered 503, at every request, forever.
    """
    group, worker_handler = login_rungs(tmp_path)
    vertex = group.spa_commander
    worker = SpaWorker(WORKER_NAME, freeze_handler=FreezeHandler(tmp_path / "frozen_users"))
    wire = attach_wire(worker)
    worker.add_connection("a1b2")
    worker.add_connection("c3d4")
    worker.change_connection_user("a1b2", "mario")
    worker.change_connection_user("c3d4", "mario")
    worker.worker_events.clear()
    vertex.connection_user_map["c3d4"] = "mario"
    vertex.user_map["mario"] = vertex._new_row()

    worker.plan_transfers(transfer_users=["mario"])
    vertex.hold_user("mario", "transfer_flag T")
    worker.change_connection_user("a1b2", "carlo")
    await worker.execute_transfers()
    for announcement in wire.calls(ANNOUNCE_OP_PATH):
        worker_handler.read_envelope(read_control(announcement))

    assert vertex.user_map["mario"]["on_hold"] is None
    assert vertex.user_hold_event_map == {}
    assert vertex.resolve_user("c3d4") == "mario"


async def test_the_login_travels_on_the_reply(worker):
    guest = browsing_guest(worker)

    worker.change_connection_user("a1b2", "mario")

    assert events_of(worker, "connection_user_changed") == [
        {
            "op": "connection_user_changed",
            "worker": WORKER_NAME,
            "user": "mario",
            "previous_user": guest,
            "connection_id": "a1b2",
        }
    ]


async def test_an_avatar_switch_onto_a_stranger_leaves_a_row_the_photo_can_read(worker):
    """The identity a real prior hands his connection to is born the worker's way.

    Found on 2026-09-04: the registry brought him into being bare, and the next
    photo raised on his missing ``state`` — so the reply that would have carried
    it never left.
    """
    worker.add_connection("a1b2", "mario")

    worker.change_connection_user("a1b2", "carlo")

    row = worker.user_register.get("carlo")
    assert row["state"] == "active"
    assert worker.worker_snapshot["users"]["carlo"]["item"]["state"] == "active"


async def test_a_login_makes_the_photo_due(worker):
    """The population is a list of NAMES, and the login changes which names.

    The count does not move — a guest goes, a person arrives — but a stale photo
    would name somebody who no longer exists and miss somebody who does, and every
    reader of that photo hangs on the names: the flagged are held by name, a death
    is settled by intersecting names, the estimate is divided over them.
    """
    browsing_guest(worker)
    # A photo just sent and nothing changed since: the ONLY thing that can make
    # one due now is the population, which is what this test is about.
    worker._population_changed = False
    worker._snapshot_sent_ts = time.time()
    assert worker._snapshot_due is False

    worker.change_connection_user("a1b2", "mario")

    assert worker._snapshot_due is True


# -- what the tail of the call does --


async def test_nothing_leaves_before_the_call_is_over(worker, deposit):
    browsing_guest(worker)
    worker.open_request("guest_a1b2")

    worker.change_connection_user("a1b2", "mario")

    # Mid-call the rows are all still here: the site goes on serving under the
    # new identity, and the deposit has seen nothing.
    assert "a1b2" in worker.connection_register
    assert deposit.user_folders == set()


async def test_the_tail_carries_the_connection_and_the_guests_store(worker, deposit):
    guest = browsing_guest(worker)

    worker.change_connection_user("a1b2", "mario")
    assert await worker.freeze_connection("a1b2", "guest_a1b2") is True

    # On disk, under the identity he logged in as.
    parcel = deposit.read_connection_register_item("mario", "a1b2")
    assert parcel["store"]["cart.item"] == "a lamp"
    assert set(parcel["pages"]) == {"page-0"}
    # And nothing of them is left in memory — the guest included.
    assert "a1b2" not in worker.connection_register
    assert "page-0" not in worker.page_register
    assert guest not in worker.user_register
    assert "mario" not in worker.user_register


async def test_an_avatar_switch_carries_no_store(worker, deposit):
    """A real identity is not a guest: what is his stays his."""
    worker.add_connection("a1b2", "mario")
    worker.add_page("page-0", "a1b2")
    worker.add_connection("c3d4", "mario")
    worker.user_register.get("mario")["store"]["cart.item"] = "a lamp"

    worker.change_connection_user("a1b2", "carlo")
    assert await worker.freeze_connection("a1b2", "mario") is True

    parcel = deposit.read_connection_register_item("carlo", "a1b2")
    assert "store" not in parcel
    # He kept his other connection, so he is still here with his store.
    assert worker.user_register.get("mario")["store"]["cart.item"] == "a lamp"


async def test_a_deposit_that_refuses_leaves_everything_alive(worker, monkeypatch):
    guest = browsing_guest(worker)
    worker.change_connection_user("a1b2", "mario")

    def refuse(*args, **kwargs):
        raise OSError("the disk is full")

    monkeypatch.setattr(worker.freeze_handler, "write_connection_register_item", refuse)

    assert await worker.freeze_connection("a1b2", "guest_a1b2") is False

    # The legitimate degraded shape: he is resident here with his connection.
    assert worker.connection_register.get("a1b2")["user"] == "mario"
    assert "mario" in worker.user_register
    assert worker.page_register.get("page-0") is not None
    assert guest not in worker.user_register  # it became mario's at the login
    assert worker.freeze_failures == 1
    assert events_of(worker, "user_frozen") == []


async def test_a_folder_that_never_comes_free_leaves_everything_alive_too(tmp_path):
    """The other way a departure does not happen, and it must end where the first one ends.

    The contract of this method is that a departure that gives up leaves the
    machine as it was. Of its two ways of giving up, only the refused write used
    to honour it: the folder that never comes free returned early, keeping the
    claim it had taken on the previous identity. A claim nobody gives back is a
    departure that never ends, and the worker's own ``quit`` waits on exactly
    that — so a process that had once served a login under a busy folder could
    never finish leaving, and the drain of a replacement would stall on it.
    """
    deposit = FreezeHandler(tmp_path / "frozen_users")
    worker = SpaWorker(
        WORKER_NAME,
        freeze_handler=deposit,
        deposit_lock_retry_interval=0.01,
        deposit_lock_wait_limit=0.05,
    )
    worker.open_request_slot()
    guest = browsing_guest(worker)
    worker.change_connection_user("a1b2", "mario")
    # Somebody else is inside mario's folder and does not come out.
    assert deposit.take_lock("mario", "standard_0002") is True

    assert await worker.freeze_connection("a1b2", "guest_a1b2") is False

    # The declared degraded shape, exactly as for a refused write.
    assert worker.connection_register.get("a1b2")["user"] == "mario"
    assert "mario" in worker.user_register
    assert worker.page_register.get("page-0") is not None
    assert guest not in worker.user_register  # it became mario's at the login
    assert worker.freeze_failures == 1
    assert events_of(worker, "user_frozen") == []
    # And nothing of the attempt is left behind to block what comes next.
    assert worker._departing_users == set()
    assert deposit.lock_holder("mario") == "standard_0002"


async def test_a_departure_that_gave_up_does_not_hold_the_process_here(tmp_path):
    """The consequence: a worker that gave up on a folder can still finish leaving.

    ``quit`` parks everybody and then waits for the departures to be OVER, which
    is no flag left and nobody still on his way out. A claim kept by a departure
    that gave up is somebody on his way out for ever, so that wait would never
    return and the process would never leave — the drain of a replacement, which
    waits on the same thing, would stall behind it.
    """
    deposit = FreezeHandler(tmp_path / "frozen_users")
    worker = SpaWorker(
        WORKER_NAME,
        freeze_handler=deposit,
        deposit_lock_retry_interval=0.01,
        deposit_lock_wait_limit=0.05,
        transfer_start_delay=0.0,
    )
    attach_wire(worker)
    browsing_guest(worker)
    worker.change_connection_user("a1b2", "mario")
    deposit.take_lock("mario", "standard_0002")
    await worker.freeze_connection("a1b2", "guest_a1b2")

    await asyncio.wait_for(worker.quit(), timeout=5.0)

    assert worker._transfers_done.is_set() is True
    assert worker._departing_users == set()


# -- what the next request finds, wherever the pool puts him --


async def test_the_guests_store_becomes_his_own_on_a_row_just_born(worker, deposit):
    browsing_guest(worker)
    worker.change_connection_user("a1b2", "mario")
    await worker.freeze_connection("a1b2", "guest_a1b2")

    await worker.adopt_connection("mario", "a1b2")

    assert worker.user_register.get("mario")["store"]["cart.item"] == "a lamp"
    assert "page-0" in worker.page_register


async def test_a_resident_keeps_his_own_store_and_the_guests_dies(worker, deposit):
    """The RESIDENT wins, applied where the resident is."""
    browsing_guest(worker)
    worker.change_connection_user("a1b2", "mario")
    await worker.freeze_connection("a1b2", "guest_a1b2")
    # He is already living here under another connection of his own.
    worker.add_connection("c3d4", "mario")
    worker.user_register.get("mario")["store"]["cart.item"] = "his own lamp"

    await worker.adopt_connection("mario", "a1b2")

    assert worker.user_register.get("mario")["store"]["cart.item"] == "his own lamp"


async def test_a_parked_connection_comes_home_on_its_own_id(worker, deposit):
    """One identity, one key: the deposit files the parcel under the connection id
    the cookie carries, and the wake asks for that one and no other."""
    browsing_guest(worker)
    worker.change_connection_user("a1b2", "mario")
    assert await worker.freeze_connection("a1b2", "guest_a1b2") is True

    await worker.adopt_connection("mario", "a1b2")

    assert "a1b2" in worker.connection_register
    assert deposit.read_connection_register_item("mario", "a1b2") is None


async def test_the_user_parcel_is_installed_before_the_connection_one(worker, deposit):
    """THE RATIFIED INVARIANT — it is what makes the resident win on a wake.

    A user whose own state was in the freezer comes home FIRST; only then is the
    connection installed, and the store it carries meets a row that is already
    full and dies. Inverting the two would let a login's leftovers overwrite days
    of hibernated state. A failure here is a STOP, never a test to adapt.
    """
    browsing_guest(worker)
    worker.change_connection_user("a1b2", "mario")
    await worker.freeze_connection("a1b2", "guest_a1b2")
    deposit.take_lock("mario", "standard_0002")
    hibernated = Bag()
    hibernated["cart.item"] = "what he had before"
    deposit.write_user_register_item(
        "mario", hibernated, writer="standard_0002", cause="freeze", group="standard"
    )
    deposit.release_lock("mario", "standard_0002")

    await worker._resolve_row("mario", "a1b2", {"user_frozen": True, "http": {}})

    assert worker.user_register.get("mario")["store"]["cart.item"] == "what he had before"


# -- what the vertex folds --


async def test_the_fold_repoints_the_cookie_and_forgets_the_guest(tmp_path):
    vertex = SpaCommander(tmp_path / "frozen_users")
    guest = "guest_a1b2"
    vertex.record_connection_user("a1b2", guest)

    vertex.change_connection_user("a1b2", "mario", guest)

    assert vertex.connection_user_map["a1b2"] == "mario"
    assert "mario" in vertex.user_map
    assert guest not in vertex.user_map


async def test_the_fold_keeps_a_real_previous_identity(tmp_path):
    vertex = SpaCommander(tmp_path / "frozen_users")
    vertex.record_connection_user("a1b2", "mario")

    vertex.change_connection_user("a1b2", "carlo", "mario")

    assert vertex.connection_user_map["a1b2"] == "carlo"
    assert "mario" in vertex.user_map


def login_rungs(tmp_path) -> tuple[GroupHandler, WorkerHandler]:
    """The two rungs under the vertex, over a group whose child is never launched."""
    vertex = SpaCommander(tmp_path / "frozen_users")
    group = GroupHandler(
        vertex,
        "standard",
        memory_concession_bytes=1_000_000,
        instance_dir=tmp_path / "i",
        frozen_users_path=tmp_path / "frozen_users",
        entry_module="never.launched",
    )
    return group, WorkerHandler(group, WORKER_NAME, **group.worker_settings)


def user_changed_envelope(user: str, previous_user: str) -> dict[str, Any]:
    """The envelope a login climbs on, as the worker composes it."""
    return {
        "worker_events": [
            {
                "op": "connection_user_changed",
                "worker": WORKER_NAME,
                "user": user,
                "previous_user": previous_user,
                "connection_id": "a1b2",
            }
        ]
    }


async def test_the_handler_swaps_who_it_holds(tmp_path):
    """A death between the login and the tail must not report a guest nobody knows."""
    _group, worker_handler = login_rungs(tmp_path)
    worker_handler.read_envelope(
        {"worker_events": [{"op": "new_user", "worker": WORKER_NAME, "user": "guest_a1b2"}]}
    )

    worker_handler.read_envelope(user_changed_envelope("mario", "guest_a1b2"))

    assert worker_handler.hosted_users == {"mario"}


async def test_a_previous_identity_who_is_no_guest_keeps_his_place(tmp_path):
    """An avatar switch moves ONE connection: the person stays where he lives.

    The two rungs under the vertex used to read every previous identity as a
    guest ceasing to exist, so a real one lost both his place in the process and
    his placement in the group — and his next request, landing anywhere on a row
    just born, would throw away the store he still had. R8 admits the real prior,
    and the register the worker keeps says so too: he stays, and the idleness
    sweep is what parks him.
    """
    group, worker_handler = login_rungs(tmp_path)
    worker_handler.read_envelope(
        {"worker_events": [{"op": "new_user", "worker": WORKER_NAME, "user": "mario"}]}
    )
    group.user_worker_map["mario"] = WORKER_NAME

    worker_handler.read_envelope(user_changed_envelope("mario_admin", "mario"))

    assert worker_handler.hosted_users == {"mario", "mario_admin"}
    assert group.user_worker_map["mario"] == WORKER_NAME
    assert group.spa_commander.connection_user_map["a1b2"] == "mario_admin"
