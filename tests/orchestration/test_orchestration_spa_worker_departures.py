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

"""SpaWorker tests: how a user leaves — the flags, the gate, the order, the quit.

WHO leaves is never decided here: the group names him and the tests of the group
say how it judges. What these say is that this rung executes and keeps no policy
of its own.

The deposit is a real ``FreezeHandler`` on a real directory here too: a departure
is judged by what is on disk when it is over, and by the fact that the adoption
can read it back. The gate is watched with a delay shrunk to a tenth of a second
— the point being that it is waited for, not how long it is — and the clocks are
pushed into the past by hand, because a silence measured by really waiting would
make the suite sleep for the age it is meant to prove.

Three deposits are played against here besides the plain one: one that refuses
every write, one whose disk takes a visible half second to answer, and — through
a semaphore taken by a name that is not this worker's — one whose folder is
simply not available. What they are for is what happens on the loop MEANWHILE.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time

import pytest

from kajenn_orchestra.orchestration import FreezeHandler, SpaWorker
from kajenn_orchestra.orchestration import spa_worker as spa_worker_module
from kajenn_orchestra.orchestration.worker_connector import ENVELOPE_SLOT_WORKER_SNAPSHOT

from .conftest import attach_wire

WORKER_NAME = "standard_0001"
OTHER_WORKER = "standard_0002"
GROUP = "standard"
CLOCKS = ("last_refresh_ts", "last_user_ts", "last_rpc_ts")


class RefusingDeposit(FreezeHandler):
    """A real deposit whose disk has stopped taking writes."""

    def write_user_register_item(self, user, payload, **header):
        raise OSError("no space left on device")


class BreakingDeposit(FreezeHandler):
    """A deposit that goes wrong where the departure catches nothing: on the way out."""

    def release_lock(self, user, holder):
        raise RuntimeError("the semaphore file vanished under us")


class SlowDeposit(FreezeHandler):
    """A real deposit whose disk takes a visible moment to take a store.

    ``STALL`` is long enough that anything waiting behind it is unmistakable in
    a measurement, and ``writing`` says when the wait has really begun — the
    write runs on the service pool, so the flag is read from the loop.
    """

    STALL = 0.5

    def __init__(self, root_path):
        super().__init__(root_path)
        self.writing = threading.Event()

    def write_user_register_item(self, user, payload, **header):
        self.writing.set()
        time.sleep(self.STALL)
        super().write_user_register_item(user, payload, **header)


@pytest.fixture
def deposit(tmp_path):
    return FreezeHandler(tmp_path / "frozen_users")


def build_worker(deposit, **kwargs):
    """A worker whose gate is short enough to watch inside a test."""
    kwargs.setdefault("transfer_start_delay", 0.1)
    worker = SpaWorker(
        WORKER_NAME,
        freeze_handler=deposit,
        group=GROUP,
        deposit_lock_retry_interval=0.01,
        **kwargs,
    )
    attach_wire(worker)
    return worker


@pytest.fixture
def worker(deposit):
    return build_worker(deposit)


def announced(worker):
    """The protocol names the departures announced up the worker's own channel, in order."""
    return [event["op"] for event in worker.stream.announced_events()]


def offered(worker):
    """The protocol names a verb called HERE, as an op would, left for this context's reply."""
    return [event["op"] for event in worker.worker_events]


async def wait_until(condition, timeout=5.0):
    """Give the loop back until something else has happened, or give up."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() >= deadline:
            raise TimeoutError("the departure never reached the awaited state")
        await asyncio.sleep(0.005)


def age_user(worker, user, seconds):
    """Push a user and everything under him that many seconds into the past."""
    item = worker.user_register.get(user)
    items = [item]
    for cid in item["connections"]:
        connection = worker.connection_register.get(cid)
        items.append(connection)
        items.extend(worker.page_register.get(page_id) for page_id in connection["pages"])
    for one in items:
        for clock in CLOCKS:
            one[clock] -= seconds


# ----------------------------------------------------------------------
# The flags: who is kept and who is ceded — named from above, never here
# ----------------------------------------------------------------------


def test_the_photo_pairs_every_user_with_his_flag(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.add_page("page-2", "cid-b", "anna")

    transfers = worker.plan_transfers(transfer_users=["mario"])

    assert transfers["mario"] == (worker.user_register.get("mario"), "T")
    assert transfers["anna"] == (worker.user_register.get("anna"), None)


def test_a_worker_alone_cedes_nobody_of_his_own(worker):
    worker.add_page("page-1", "cid-a", "mario")
    age_user(worker, "mario", 24 * 3600)

    transfers = worker.plan_transfers()

    # However long he has been silent: the silence is judged one rung up, and
    # this worker has no gauge to judge it with.
    assert transfers["mario"][1] is None
    assert worker.worker_snapshot["users"]["mario"]["transfer_flag"] is None


async def test_a_row_that_is_not_active_is_left_to_the_vertex(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.user_register.get("mario")["state"] = "unfreezing"

    transfers = worker.plan_transfers(transfer_users=["mario"])

    assert transfers["mario"][1] is None
    assert worker.worker_snapshot["users"]["mario"]["transfer_flag"] is None


async def test_a_flag_posed_puts_the_photo_on_the_next_envelope_out(deposit):
    # A photo that stays fresh for an hour: nothing the throttle knows about
    # would put another one on the wire before then.
    worker = build_worker(deposit, worker_snapshot_ttl=3600)
    worker.add_page("page-1", "cid-a", "mario")
    worker._outbound({})
    assert ENVELOPE_SLOT_WORKER_SNAPSHOT not in worker._outbound({})

    worker.plan_transfers(transfer_users=["mario"])

    # A flag is a promise the vertex has to read: without the photo carrying it,
    # a departure settled on the last one would count this user as kept.
    envelope = worker._outbound({})
    assert envelope[ENVELOPE_SLOT_WORKER_SNAPSHOT]["users"]["mario"]["transfer_flag"] == "T"


# ----------------------------------------------------------------------
# The gate: nothing departs in the turn it was announced
# ----------------------------------------------------------------------


async def test_the_gate_holds_the_departure_until_its_delay_has_passed(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")
    worker.plan_transfers(transfer_users=["mario"])
    transfer = asyncio.ensure_future(worker.execute_transfers())

    await asyncio.sleep(worker.transfer_start_delay / 4)
    still_here = "mario" in worker.user_register
    await transfer

    assert still_here
    assert "mario" not in worker.user_register
    assert deposit.read_user_register_item("mario") is not None


async def test_a_ceded_user_leaves_with_his_placement_to_be_assigned(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.stream.frames.clear()
    worker.plan_transfers(transfer_users=["mario"])

    await worker.execute_transfers()

    assert announced(worker) == ["user_frozen", "drop_pages"]
    assert worker.stream.announced_events()[0] == {
        "op": "user_frozen",
        "worker": WORKER_NAME,
        "user": "mario",
        "placement": None,
    }
    assert worker.connection_register.keys() == []
    assert worker.page_register.keys() == []


# ----------------------------------------------------------------------
# The end of a call is where a deferred departure happens
# ----------------------------------------------------------------------


async def test_a_call_in_flight_defers_the_departure_to_its_end(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")
    worker.plan_transfers(transfer_users=["mario"])

    await worker.execute_transfers()

    assert "mario" in worker.user_register
    assert deposit.read_user_register_item("mario") is None

    await worker.close_request("mario")

    assert "mario" not in worker.user_register
    assert deposit.read_user_register_item("mario") is not None
    assert "user_frozen" in announced(worker)


async def test_the_departure_waits_for_the_last_of_several_calls(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")
    worker.open_request("mario")
    worker.plan_transfers(transfer_users=["mario"])
    await worker.execute_transfers()

    await worker.close_request("mario")
    assert "mario" in worker.user_register

    await worker.close_request("mario")
    assert "mario" not in worker.user_register


async def test_a_call_closing_before_the_gate_takes_nobody_away(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")
    worker.plan_transfers(transfer_users=["mario"])

    await worker.close_request("mario")

    assert "mario" in worker.user_register
    assert "user_frozen" not in announced(worker)


async def test_a_call_closing_on_a_user_nobody_asked_for_changes_nothing(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")
    worker.stream.frames.clear()

    await worker.close_request("mario")

    assert worker.user_register.get("mario")["state"] == "active"
    assert announced(worker) == []


# ----------------------------------------------------------------------
# What the freeze writes, the adoption reads back
# ----------------------------------------------------------------------


async def test_what_the_freeze_writes_the_adoption_reads_back(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")
    worker.add_page("page-2", "cid-a")
    worker.user_register.get("mario")["store"]["cart.total"] = 12
    human_event = worker.refresh_chain("page-1", "last_user_ts")

    assert await worker.freeze_user("mario")
    assert (
        worker.user_register.keys(),
        worker.connection_register.keys(),
        worker.page_register.keys(),
    ) == ([], [], [])

    item = await worker.adopt_user("mario")
    connection = await worker.adopt_connection("mario", "cid-a")

    assert item["store"]["cart.total"] == 12
    assert connection["user"] == "mario"
    assert connection["pages"] == {"page-1", "page-2"}
    assert connection["last_user_ts"] == human_event
    assert worker.page_register.get("page-1")["last_user_ts"] == human_event
    assert worker.page_register.get("page-2")["connection_id"] == "cid-a"
    assert deposit.user_folders == set()


# ----------------------------------------------------------------------
# The deposit that refuses: nobody is killed who could not be saved
# ----------------------------------------------------------------------


async def test_a_deposit_that_refuses_leaves_the_user_alive(tmp_path):
    deposit = RefusingDeposit(tmp_path / "frozen_users")
    worker = build_worker(deposit)
    worker.add_page("page-1", "cid-a", "mario")
    worker.worker_events.clear()

    frozen = await worker.freeze_user("mario")

    assert frozen is False
    assert worker.user_register.get("mario")["state"] == "active"
    assert worker.connection_register.get("cid-a")["pages"] == {"page-1"}
    assert offered(worker) == []
    assert worker.freeze_failures == 1
    assert deposit.lock_holder("mario") is None
    assert deposit.user_folders == set()


async def test_a_refused_departure_does_not_keep_coming_back(tmp_path):
    deposit = RefusingDeposit(tmp_path / "frozen_users")
    worker = build_worker(deposit)
    worker.add_page("page-1", "cid-a", "mario")
    worker.plan_transfers(transfer_users=["mario"])

    await worker.execute_transfers()
    worker.open_request("mario")
    await worker.close_request("mario")

    assert worker.user_register.get("mario")["state"] == "active"
    assert worker.freeze_failures == 1


# ----------------------------------------------------------------------
# A row mid-adoption is nobody's to park
# ----------------------------------------------------------------------


async def test_a_row_being_adopted_is_not_parked_under_the_adoption(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")
    worker.worker_events.clear()
    worker.user_register.get("mario")["state"] = "unfreezing"

    assert await worker.freeze_user("mario") is False
    assert worker.user_register.get("mario")["state"] == "unfreezing"
    assert offered(worker) == []
    assert deposit.read_user_register_item("mario") is None
    assert deposit.user_folders == set()


# ----------------------------------------------------------------------
# One departure at a time, and the disk held by nobody
# ----------------------------------------------------------------------


async def test_the_cycle_and_the_hook_never_park_the_same_user_twice(tmp_path):
    deposit = SlowDeposit(tmp_path / "frozen_users")
    worker = build_worker(deposit)
    worker.add_page("page-1", "cid-a", "mario")
    worker.stream.frames.clear()
    worker.plan_transfers(transfer_users=["mario"])
    cycle = asyncio.ensure_future(worker.execute_transfers())
    await wait_until(deposit.writing.is_set)

    # A call opens and closes while his departure is already on the disk: the
    # hook finds it claimed and comes straight back, instead of queueing behind
    # a semaphore this worker itself is holding.
    worker.open_request("mario")
    await asyncio.wait_for(worker.close_request("mario"), SlowDeposit.STALL / 2)
    await cycle

    assert announced(worker) == ["user_frozen", "drop_pages"]
    assert "mario" not in worker.user_register
    assert deposit.lock_holder("mario") is None


async def test_a_call_born_while_the_parcels_were_written_does_not_stop_the_departure(tmp_path):
    deposit = SlowDeposit(tmp_path / "frozen_users")
    worker = build_worker(deposit)
    worker.add_page("page-1", "cid-a", "mario")
    worker.stream.frames.clear()
    worker.plan_transfers(transfer_users=["mario"])
    cycle = asyncio.ensure_future(worker.execute_transfers())
    await wait_until(deposit.writing.is_set)

    # What keeps a call of his from being born while the disk writes is the
    # BLOCK whoever ordered the departure raised at the vertex. Staged by hand
    # here, the call changes nothing: the parcels stay where they were written
    # and he leaves, where the abandoned departure of before took them back off.
    worker.open_request("mario")
    await cycle

    assert "mario" not in worker.user_register
    assert announced(worker) == ["user_frozen", "drop_pages"]
    assert deposit.read_user_register_item("mario") is not None
    assert deposit.read_connection_register_item("mario", "cid-a") is not None
    assert worker.freeze_failures == 0

    # The tail of that call finds him gone, and there is nothing left to park.
    await worker.close_request("mario")

    assert "mario" not in worker.user_register
    assert announced(worker) == ["user_frozen", "drop_pages"]


async def test_the_parcels_are_photographed_before_the_disk_takes_them(tmp_path):
    deposit = SlowDeposit(tmp_path / "frozen_users")
    worker = build_worker(deposit)
    worker.add_page("page-1", "cid-a", "mario")
    worker.user_register.get("mario")["store"]["cart.total"] = 12
    freezing = asyncio.ensure_future(worker.freeze_user("mario"))
    await wait_until(deposit.writing.is_set)

    # The disk is slow and the store is live. What a mutation writes now must
    # NOT reach the parcel: the deposit pickles on the service pool, with no
    # lock of the worker's, so what crosses over has to be a photograph.
    worker.user_register.get("mario")["store"]["cart.total"] = 99

    assert await freezing is True
    assert deposit.read_user_register_item("mario")["cart.total"] == 12


async def test_a_slow_write_does_not_stall_the_loop(tmp_path):
    deposit = SlowDeposit(tmp_path / "frozen_users")
    worker = build_worker(deposit)
    worker.add_page("page-1", "cid-a", "mario")
    freezing = asyncio.ensure_future(worker.freeze_user("mario"))
    await wait_until(deposit.writing.is_set)

    started = time.monotonic()
    worker.add_page("page-2", "cid-b", "anna")
    elapsed = time.monotonic() - started

    assert elapsed < SlowDeposit.STALL / 2
    assert await freezing is True
    assert "anna" in worker.user_register


# ----------------------------------------------------------------------
# The semaphore: a voice on the first miss, and a floor under the wait
# ----------------------------------------------------------------------


async def test_a_held_folder_says_once_who_is_holding_it(worker, deposit, caplog):
    caplog.set_level(logging.WARNING)
    worker.add_page("page-1", "cid-a", "mario")
    deposit.take_lock("mario", OTHER_WORKER)
    freezing = asyncio.ensure_future(worker.freeze_user("mario"))

    await asyncio.sleep(worker.deposit_lock_retry_interval * 10)
    deposit.release_lock("mario", OTHER_WORKER)

    assert await freezing is True
    warnings = [record.getMessage() for record in caplog.records if record.levelno >= logging.WARNING]
    assert len(warnings) == 1
    assert "mario" in warnings[0]
    assert OTHER_WORKER in warnings[0]


async def test_a_folder_nobody_gives_back_ends_the_departure_loud(deposit):
    worker = build_worker(deposit, deposit_lock_wait_limit=0.05)
    worker.add_page("page-1", "cid-a", "mario")
    worker.worker_events.clear()
    deposit.take_lock("mario", OTHER_WORKER)

    assert await asyncio.wait_for(worker.freeze_user("mario"), 2.0) is False
    assert worker.user_register.get("mario")["state"] == "active"
    assert worker.freeze_failures == 1
    assert offered(worker) == []
    assert deposit.lock_holder("mario") == OTHER_WORKER


async def test_a_folder_nobody_gives_back_makes_the_adoption_raise(deposit):
    worker = build_worker(deposit, deposit_lock_wait_limit=0.05)
    deposit.take_lock("mario", OTHER_WORKER)

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(worker.adopt_user("mario"), 2.0)

    # The wait limit is one more failed pull: no row of his stays behind, and
    # the verdict on his next request is what tries the folder again.
    assert "mario" not in worker.user_register
    assert deposit.lock_holder("mario") == OTHER_WORKER


# ----------------------------------------------------------------------
# The freeze order: the worker only executes
# ----------------------------------------------------------------------


async def test_the_freeze_order_parks_a_user_with_no_call_in_flight(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")
    worker.worker_events.clear()

    outcome = await worker.freeze_designated_user("mario")

    assert outcome == {"frozen": "mario"}
    assert "mario" not in worker.user_register
    assert deposit.read_user_register_item("mario") is not None
    assert offered(worker) == ["user_frozen", "drop_pages"]


async def test_the_freeze_order_does_not_return_before_the_call_in_flight_ends(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")
    order = asyncio.ensure_future(worker.freeze_designated_user("mario"))
    await asyncio.sleep(0.05)

    assert not order.done()
    assert deposit.read_user_register_item("mario") is None

    await worker.close_request("mario")

    assert await order == {"frozen": "mario"}
    assert "mario" not in worker.user_register
    assert deposit.read_user_register_item("mario") is not None


async def test_the_freeze_order_waits_for_the_pull_bringing_him_home(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")
    item = worker.user_register.get("mario")
    item["state"] = "unfreezing"
    pull = worker._unfreeze_waits["mario"] = asyncio.Event()
    order = asyncio.ensure_future(worker.freeze_designated_user("mario"))
    await asyncio.sleep(0.05)

    assert not order.done()

    item["state"] = "active"
    del worker._unfreeze_waits["mario"]
    pull.set()

    assert await order == {"frozen": "mario"}
    assert deposit.read_user_register_item("mario") is not None


async def test_the_freeze_order_for_a_stranger_is_refused_by_name(worker):
    with pytest.raises(KeyError, match="no user 'nobody' here"):
        await worker.freeze_designated_user("nobody")


# ----------------------------------------------------------------------
# The mass cycle and the quit
# ----------------------------------------------------------------------


async def test_quit_parks_everybody_and_reaches_the_exit(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")
    worker.add_page("page-2", "cid-b", "anna")
    worker.stream.frames.clear()

    await worker.quit()

    assert worker.exited
    assert worker.user_register.keys() == []
    assert deposit.read_user_register_item("mario") is not None
    assert deposit.read_user_register_item("anna") is not None
    assert announced(worker) == ["user_frozen", "drop_pages", "user_frozen", "drop_pages"]
    assert all(
        event["placement"] is None
        for event in worker.stream.announced_events()
        if event["op"] == "user_frozen"
    )


async def test_quit_does_not_leave_while_a_call_is_in_flight(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")
    leaving = asyncio.ensure_future(worker.quit())

    await asyncio.sleep(worker.transfer_start_delay * 3)
    still_here = not worker.exited
    await worker.close_request("mario")
    await leaving

    assert still_here
    assert worker.exited
    assert worker.user_register.keys() == []


async def test_a_shot_taken_during_the_quit_does_not_free_the_straggler(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")
    leaving = asyncio.ensure_future(worker.quit())
    await asyncio.sleep(worker.transfer_start_delay * 3)

    # An ordinary shot, naming nobody for cession: once the quit has begun the
    # plan is its own, and this one may not take the straggler off it.
    transfers = worker.plan_transfers()

    assert transfers["mario"][1] == "T"
    assert not worker.exited

    # The cycle is given its turn on the shot and goes back to sleep on him,
    # and only THEN does his call end — inside what a restarted gate would
    # cost. So the hook is the only thing that can free him, and a gate the
    # shot had shut again would leave the quit with nobody to wake it.
    await asyncio.sleep(worker.transfer_start_delay / 4)
    await worker.close_request("mario")
    await asyncio.wait_for(leaving, 5.0)

    assert worker.exited
    assert worker.user_register.keys() == []
    assert deposit.read_user_register_item("mario") is not None


async def test_a_user_born_during_the_quit_is_added_to_the_departing(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")
    leaving = asyncio.ensure_future(worker.quit())
    await asyncio.sleep(worker.transfer_start_delay * 3)

    # Somebody arrives while the worker is leaving: his own call is what puts
    # him on the registers, and the shot that answers it flags him too.
    worker.add_page("page-2", "cid-b", "anna")
    worker.open_request("anna")
    transfers = worker.plan_transfers()

    assert transfers["anna"][1] == "T"

    await worker.close_request("anna")
    await worker.close_request("mario")
    await asyncio.wait_for(leaving, 5.0)

    assert worker.exited
    assert worker.user_register.keys() == []
    assert deposit.read_user_register_item("anna") is not None


async def test_a_user_flagged_after_the_first_pass_leaves_with_the_quit_all_the_same(
    worker, deposit
):
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")
    leaving = asyncio.ensure_future(worker.quit())
    await asyncio.sleep(worker.transfer_start_delay * 3)

    # Somebody arrives while the worker is leaving, is served, and his call is
    # already CLOSED by the time the shot names him: no hook of his will ever
    # fire again, so the cycle itself is what has to come back for him.
    worker.add_page("page-2", "cid-b", "anna")
    worker.open_request("anna")
    await worker.close_request("anna")
    transfers = worker.plan_transfers()

    assert transfers["anna"][1] == "T"

    await worker.close_request("mario")
    await asyncio.wait_for(leaving, 5.0)

    assert worker.exited
    assert worker.user_register.keys() == []
    assert deposit.read_user_register_item("mario") is not None
    assert deposit.read_user_register_item("anna") is not None


async def test_the_quit_waits_for_a_pull_still_on_its_way(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")

    # anna's store is still travelling: her pull is parked on a folder somebody
    # else is holding, so her row is neither active nor gone — she is a
    # straggler like the man with a call open, and a quit that left now would
    # shut the pool her own trip is running on.
    deposit.take_lock("anna", OTHER_WORKER)
    pull = asyncio.ensure_future(worker.adopt_user("anna"))
    await wait_until(lambda: (worker.user_register.get("anna") or {}).get("state") == "unfreezing")

    leaving = asyncio.ensure_future(worker.quit())
    await asyncio.sleep(worker.transfer_start_delay * 3)

    assert not worker.exited

    deposit.release_lock("anna", OTHER_WORKER)
    await asyncio.wait_for(pull, 5.0)
    await asyncio.wait_for(leaving, 5.0)

    assert worker.exited
    assert worker.user_register.keys() == []
    assert deposit.read_user_register_item("mario") is not None
    assert deposit.read_user_register_item("anna") is not None


async def test_a_departure_that_falls_over_does_not_keep_the_worker_alive(tmp_path):
    deposit = BreakingDeposit(tmp_path / "frozen_users")
    worker = build_worker(deposit)
    worker.add_page("page-1", "cid-a", "mario")
    worker.add_page("page-2", "cid-b", "anna")

    await asyncio.wait_for(worker.quit(), 5.0)

    # What breaks here breaks on the way OUT — the parcels are already written
    # and the rows already released under the semaphore — so both men really
    # left; what the containment buys is the exit being reached anyway, with
    # the two falls counted and shouted rather than raised at the quit.
    assert worker.exited
    assert worker.freeze_failures == 2
    assert worker.user_register.keys() == []
    assert deposit.read_user_register_item("mario") is not None
    assert deposit.read_user_register_item("anna") is not None


# ----------------------------------------------------------------------
# The flag is the promise (owner, 2026-08-16): only a departure that
# happened, a counted failure or the man's own absence consumes it
# ----------------------------------------------------------------------

class SlowReadDeposit(FreezeHandler):
    """A real deposit whose disk takes a visible moment to give a store back."""

    STALL = 0.3

    def __init__(self, root_path):
        super().__init__(root_path)
        self.reading = threading.Event()

    def read_user_register_item(self, user):
        self.reading.set()
        time.sleep(self.STALL)
        return super().read_user_register_item(user)


async def test_a_bounced_hook_drops_no_flag_between_two_hands(worker, deposit):
    # The two-porters race: the hook fires while somebody else still holds the
    # claim; it bounces — and the bounce must consume NOTHING, because the flag
    # is the promise and only a completed departure takes it away.
    worker.add_page("page-1", "cid-a", "mario")
    worker.plan_transfers(transfer_users=["mario"])
    worker.open_request("mario")

    worker._departing_users.add("mario")
    await worker.close_request("mario")
    assert "mario" in worker._transfer_flags

    worker._release_departure("mario")
    await asyncio.sleep(worker.transfer_start_delay + 0.05)
    await worker.execute_transfers()
    assert "mario" not in worker.user_register
    assert deposit.read_user_register_item("mario") is not None
    assert "mario" not in worker._transfer_flags


async def test_the_quit_is_not_stopped_by_a_call_born_at_its_edge(tmp_path):
    # A call born while the quit writes his parcels no longer takes them back:
    # the flag is consumed by the departure that HAPPENED, and the process
    # leaves with him parked, never with him on board.
    deposit = SlowDeposit(tmp_path / "frozen_users")
    worker = build_worker(deposit)
    worker.add_page("page-1", "cid-a", "mario")

    leaving = asyncio.ensure_future(worker.quit())
    await asyncio.get_running_loop().run_in_executor(None, deposit.writing.wait)
    worker.open_request("mario")
    await wait_until(lambda: "mario" not in worker._departing_users)
    assert worker._transfer_flags == {}
    await worker.close_request("mario")
    await asyncio.wait_for(leaving, 5.0)

    assert worker.exited
    assert "mario" not in worker.user_register
    assert deposit.read_user_register_item("mario") is not None


async def test_the_pendings_cover_the_adoption_itself(tmp_path):
    # open_request comes before the row is put in order: while the store is
    # still travelling up from the deposit, the call is already visible in the
    # pendings, so no departure can wake in the gap between the loading and
    # the serving of the same call.
    deposit = SlowReadDeposit(tmp_path / "frozen_users")
    worker = build_worker(deposit)
    def tiny_site(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"ok"]

    worker.wsgi_app = tiny_site
    deposit.take_lock("mario", "test")
    deposit.write_user_register_item("mario", {"cart": 1}, writer="t", cause="t", group="g")
    deposit.release_lock("mario", "test")

    payload = {
        "http": {
            "method": "GET",
            "path": "/",
            "query_string": "",
            "headers": [["host", "site.example:8080"]],
            "body": "",
            "cid": "cid-a",
        },
        "identity": "mario",
        "user_frozen": True,
    }
    serving = asyncio.ensure_future(worker._serve_request(payload))
    await asyncio.get_running_loop().run_in_executor(None, deposit.reading.wait)
    assert worker._pendings.get("mario")
    await asyncio.wait_for(serving, 5.0)
    assert not worker._pendings


async def test_the_quit_writes_its_parcels_where_it_is_told(worker, deposit, tmp_path):
    """The soft quit parks in the reboot directory, and the working deposit stays empty."""
    worker.add_page("page-1", "cid-a", "mario")
    reboot_temp = tmp_path / "reboot_temp"

    await worker.quit(freezer_path=str(reboot_temp))

    assert worker.exited
    assert FreezeHandler(reboot_temp).read_user_register_item("mario") is not None
    assert deposit.user_folders == set()


async def test_a_straggler_is_cut_past_the_grace_and_parked_without_his_call(
    worker, deposit, monkeypatch
):
    """A call that never ends does not hold the quit: past the grace he is parked."""
    monkeypatch.setattr(spa_worker_module, "PENDING_CALL_GRACE_SECONDS", 0.3)
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")

    await asyncio.wait_for(worker.quit(), timeout=5.0)

    assert worker.exited
    assert deposit.read_user_register_item("mario") is not None
    assert worker.user_register.keys() == []


async def test_the_cut_wakes_the_freeze_order_that_was_waiting_for_the_call(
    worker, deposit, monkeypatch
):
    """The wait an ordered freeze parks on is cut with the call it was waiting for.

    The order is on the event the END of that call sets — and that end never
    comes, because the quit gives the call up. Left alone the order would wait
    for ever on a process that has already left; woken with the pendings it
    finds its user free and parks him.
    """
    monkeypatch.setattr(spa_worker_module, "PENDING_CALL_GRACE_SECONDS", 0.3)
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")
    order = asyncio.ensure_future(worker.freeze_designated_user("mario"))
    await asyncio.sleep(0.05)
    assert not order.done()

    await asyncio.wait_for(worker.quit(), timeout=5.0)

    assert await asyncio.wait_for(order, timeout=1.0) == {"frozen": "mario"}
    assert deposit.read_user_register_item("mario") is not None


async def test_a_call_that_ends_after_its_user_was_cut_closes_quietly(worker, monkeypatch):
    """The stuck call finishes into a process that already left: nothing to close."""
    monkeypatch.setattr(spa_worker_module, "PENDING_CALL_GRACE_SECONDS", 0.3)
    worker.add_page("page-1", "cid-a", "mario")
    worker.open_request("mario")
    await asyncio.wait_for(worker.quit(), timeout=5.0)

    await worker.close_request("mario")


# ----------------------------------------------------------------------
# The wire that falls
# ----------------------------------------------------------------------


async def test_a_worker_that_loses_its_wire_saves_nothing_and_leaves(worker, deposit):
    """A process nobody can vouch for writes no parcel: the vertex drops its users."""
    worker.add_page("page-1", "cid-a", "mario")
    worker.add_page("page-2", "cid-b", "anna")

    await worker.on_wire_lost()

    assert worker.exited
    assert deposit.user_folders == set()
    assert worker.user_register.keys() == ["mario", "anna"]
