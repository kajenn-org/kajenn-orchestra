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

"""SpaWorker tests: the row story, told from the birth of a guest to a drop.

The deposit under these tests is a real ``FreezeHandler`` on a real directory —
the adoption is judged by what is left on disk afterwards, and a fake filesystem
would judge nothing. The only thing wrapped is the counting of the reads, which
is how "one trip to the freezer, however wide the burst" becomes assertable.
"""

from __future__ import annotations

import asyncio

import pytest
from genro_bag import Bag

from kajenn_orchestra.orchestration import FreezeHandler, SpaWorker
from kajenn_orchestra.orchestration.spa_worker import GUEST_PREFIX

WORKER_NAME = "standard_0001"


class CountingDeposit(FreezeHandler):
    """A real deposit that also says how many times it was read."""

    def __init__(self, root_path):
        super().__init__(root_path)
        self.user_reads = 0
        self.connection_reads = 0

    def read_user_register_item(self, user):
        self.user_reads += 1
        return super().read_user_register_item(user)

    def read_connection_register_item(self, user, cid):
        self.connection_reads += 1
        return super().read_connection_register_item(user, cid)


@pytest.fixture
def deposit(tmp_path):
    return CountingDeposit(tmp_path / "frozen_users")


@pytest.fixture
def worker(deposit):
    worker = SpaWorker(WORKER_NAME, freeze_handler=deposit, deposit_lock_retry_interval=0.01)
    worker.open_request_slot()
    return worker


def freeze_store(deposit, user, store):
    """Park a user store in the deposit, the way the freeze cycle will."""
    deposit.take_lock(user, "standard_0002")
    deposit.write_user_register_item(
        user, store, writer="standard_0002", cause="freeze", group="standard"
    )
    deposit.release_lock(user, "standard_0002")


def freeze_connection(deposit, user, cid, parcel):
    """Park one connection with its pages in the deposit."""
    deposit.take_lock(user, "standard_0002")
    deposit.write_connection_register_item(
        user, cid, parcel, writer="standard_0002", cause="freeze", group="standard"
    )
    deposit.release_lock(user, "standard_0002")


def announced(worker):
    """The protocol names queued for the envelope out, in order."""
    return [event["op"] for event in worker.worker_events]


# ----------------------------------------------------------------------
# Births: whoever shows up is a user in full
# ----------------------------------------------------------------------


def test_a_connection_arriving_anonymous_is_a_user_in_full(worker):
    item = worker.add_connection("cid-a")

    assert item["user"] == GUEST_PREFIX + "cid-a"
    assert announced(worker) == ["new_user", "new_connection"]
    assert worker.worker_events[0] == {
        "op": "new_user",
        "worker": WORKER_NAME,
        "user": GUEST_PREFIX + "cid-a",
    }
    assert worker.user_register.get(GUEST_PREFIX + "cid-a")["state"] == "active"


def test_a_named_connection_hangs_from_its_user(worker):
    worker.add_connection("cid-a", "mario")

    assert worker.connection_register.get("cid-a")["user"] == "mario"
    assert worker.user_register.get("mario")["connections"] == {"cid-a"}
    assert announced(worker) == ["new_user", "new_connection"]


def test_a_page_brings_the_whole_chain_into_being_bottom_up(worker):
    worker.add_page("page-1", "cid-a", "mario")

    assert announced(worker) == ["new_user", "new_connection", "new_page"]
    assert worker.page_register.get("page-1")["connection_id"] == "cid-a"
    assert worker.connection_register.get("cid-a")["pages"] == {"page-1"}
    assert worker.user_register.get("mario")["connections"] == {"cid-a"}


def test_a_second_page_of_a_known_connection_announces_only_itself(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.worker_events.clear()

    worker.add_page("page-2", "cid-a")

    assert announced(worker) == ["new_page"]
    assert worker.worker_events[0]["user"] == "mario"
    assert worker.connection_register.get("cid-a")["pages"] == {"page-1", "page-2"}


# ----------------------------------------------------------------------
# The three clocks
# ----------------------------------------------------------------------


def test_a_page_call_stamps_the_three_clocks_up_the_chain(worker):
    worker.add_page("page-1", "cid-a", "mario")

    now = worker.refresh_chain("page-1", "last_user_ts", "last_rpc_ts")

    for item in (
        worker.page_register.get("page-1"),
        worker.connection_register.get("cid-a"),
        worker.user_register.get("mario"),
    ):
        assert item["last_refresh_ts"] == now
        assert item["last_user_ts"] == now
        assert item["last_rpc_ts"] == now


def test_the_beat_stamps_the_technical_clock_and_nothing_else(worker):
    worker.add_page("page-1", "cid-a", "mario")
    user_item = worker.user_register.get("mario")
    born_human = user_item["last_user_ts"]
    born_rpc = user_item["last_rpc_ts"]

    now = worker.refresh_chain("page-1")

    assert user_item["last_refresh_ts"] == now
    assert user_item["last_user_ts"] == born_human
    assert user_item["last_rpc_ts"] == born_rpc


def test_a_stamp_for_a_page_nobody_holds_is_an_error(worker):
    with pytest.raises(KeyError):
        worker.refresh_chain("page-nowhere", "last_rpc_ts")


# ----------------------------------------------------------------------
# The unified row: the pull, and the burst that shares it
# ----------------------------------------------------------------------


async def test_the_authorised_store_comes_home_and_the_parcel_goes(worker, deposit):
    store = Bag()
    store["cart.total"] = 12
    freeze_store(deposit, "mario", store)

    item = await worker.adopt_user("mario")

    assert item["state"] == "active"
    assert item["store"]["cart.total"] == 12
    assert announced(worker) == ["user_adopted"]
    assert deposit.read_user_register_item("mario") is None
    assert deposit.user_folders == set()


async def test_the_folder_stays_while_the_user_has_other_parcels(worker, deposit):
    freeze_store(deposit, "mario", Bag())
    freeze_connection(deposit, "mario", "cid-a", {"connection": {}, "pages": {}})

    await worker.adopt_user("mario")

    assert deposit.user_folders == {deposit.user_to_userkey("mario")}
    assert deposit.read_connection_register_item("mario", "cid-a") is not None


async def test_a_user_the_worker_never_saw_is_added_frozen_and_pulled(worker, deposit):
    store = Bag()
    store["cart.total"] = 3
    freeze_store(deposit, "mario", store)
    assert "mario" not in worker.user_register

    item = await worker.adopt_user("mario")

    assert item is worker.user_register.get("mario")
    assert item["state"] == "active"
    assert item["store"]["cart.total"] == 3


async def test_a_user_announced_frozen_with_no_parcel_still_wakes(worker, deposit):
    item = await worker.adopt_user("mario")

    assert item["state"] == "active"
    assert len(item["store"]) == 0
    assert announced(worker) == ["user_adopted"]


async def test_a_second_authorised_request_finds_the_row_already_home(worker, deposit):
    freeze_store(deposit, "mario", Bag())
    await worker.adopt_user("mario")
    worker.worker_events.clear()

    await worker.adopt_user("mario")

    assert deposit.user_reads == 1
    assert announced(worker) == []


async def test_a_burst_on_a_frozen_row_makes_one_trip_and_the_sisters_wait(worker, deposit):
    store = Bag()
    store["cart.total"] = 7
    freeze_store(deposit, "mario", store)
    # Somebody else is in the folder: the first caller parks on the semaphore,
    # which is what gives the whole burst the time to pile up behind it.
    deposit.take_lock("mario", "standard_0002")

    burst = [asyncio.create_task(worker.adopt_user("mario")) for _ in range(5)]
    await asyncio.sleep(0.02)
    assert worker.user_register.get("mario")["state"] == "unfreezing"
    assert deposit.user_reads == 0

    deposit.release_lock("mario", "standard_0002")
    items = await asyncio.wait_for(asyncio.gather(*burst), 2.0)

    assert deposit.user_reads == 1
    assert all(item["state"] == "active" for item in items)
    assert all(item["store"]["cart.total"] == 7 for item in items)
    assert announced(worker) == ["user_adopted"]


async def test_a_pull_that_fails_leaves_no_row_and_the_next_request_retries(
    worker, deposit, monkeypatch
):
    store = Bag()
    store["cart.total"] = 7
    freeze_store(deposit, "mario", store)

    def unreadable(user):
        raise OSError("the deposit is unreachable")

    monkeypatch.setattr(deposit, "read_user_register_item", unreadable)

    with pytest.raises(OSError, match="unreachable"):
        await worker.adopt_user("mario")

    # No row of his is resident: his parcel is still on disk and the mark is
    # still on at the vertex, so the verdict comes back with his next request
    # and the trip is retried by the shape of the unified row itself.
    assert "mario" not in worker.user_register
    assert announced(worker) == []
    assert deposit.lock_holder("mario") is None

    monkeypatch.undo()
    assert deposit.read_user_register_item("mario")["cart.total"] == 7
    item = await worker.adopt_user("mario")

    assert item["state"] == "active"
    assert item["store"]["cart.total"] == 7
    assert announced(worker) == ["user_adopted"]


async def test_a_pull_that_fails_wakes_the_sisters_with_a_failure_of_their_own(
    worker, deposit, monkeypatch
):
    freeze_store(deposit, "mario", Bag())
    deposit.take_lock("mario", "standard_0002")

    def unreadable(user):
        raise OSError("the deposit is unreachable")

    monkeypatch.setattr(deposit, "read_user_register_item", unreadable)
    burst = [asyncio.create_task(worker.adopt_user("mario")) for _ in range(3)]
    await asyncio.sleep(0.02)
    deposit.release_lock("mario", "standard_0002")

    outcomes = await asyncio.wait_for(asyncio.gather(*burst, return_exceptions=True), 2.0)

    # One of them made the trip and carries what the deposit said; the sisters
    # awaited a transition that ended in nothing and are told so, never handed
    # a row that is not there.
    assert any(isinstance(one, OSError) for one in outcomes)
    assert all(isinstance(one, Exception) for one in outcomes)
    assert "mario" not in worker.user_register


async def test_a_parcel_no_envelope_authorises_is_never_touched(worker, deposit):
    store = Bag()
    store["cart.total"] = 99
    freeze_store(deposit, "mario", store)

    # The request is served without the verdict: the row is born on the spot,
    # empty, and the residue on disk stays exactly where it was.
    worker.add_page("page-1", "cid-a", "mario")
    worker.refresh_chain("page-1", "last_rpc_ts")

    assert deposit.user_reads == 0
    assert len(worker.user_register.get("mario")["store"]) == 0
    assert deposit.read_user_register_item("mario")["cart.total"] == 99
    assert announced(worker) == ["new_user", "new_connection", "new_page"]


# ----------------------------------------------------------------------
# The connection, which needs no verdict
# ----------------------------------------------------------------------


async def test_a_connection_found_in_the_deposit_arrives_with_its_pages(worker, deposit):
    worker.add_page("page-1", "cid-a", "mario")
    worker.worker_events.clear()
    freeze_connection(
        deposit,
        "mario",
        "cid-b",
        {"connection": {"last_user_ts": 111.0}, "pages": {"page-2": {"last_rpc_ts": 222.0}}},
    )

    item = await worker.adopt_connection("mario", "cid-b")

    assert announced(worker) == ["new_connection", "new_page"]
    assert item["last_user_ts"] == 111.0
    assert worker.page_register.get("page-2")["last_rpc_ts"] == 222.0
    assert worker.user_register.get("mario")["connections"] == {"cid-a", "cid-b"}
    assert deposit.read_connection_register_item("mario", "cid-b") is None


async def test_a_connection_the_deposit_never_had_is_nobodys_to_bear(worker, deposit):
    """The doctrine of 2026-08-21: the rows are the site's — nothing parked
    means NO row and no announcement; the site baptises again while served."""
    worker.add_page("page-1", "cid-a", "mario")
    worker.worker_events.clear()

    item = await worker.adopt_connection("mario", "cid-b")

    assert item is None
    assert announced(worker) == []
    assert "cid-b" not in worker.connection_register
    assert deposit.connection_reads == 1


async def test_a_stranger_gets_no_row_out_of_the_adoption(worker, deposit):
    assert await worker.adopt_connection("mario", "cid-a") is None
    assert "mario" not in worker.user_register


async def test_a_connection_already_held_costs_no_trip(worker, deposit):
    worker.add_connection("cid-a", "mario")
    worker.worker_events.clear()

    item = await worker.adopt_connection("mario", "cid-a")

    assert item is worker.connection_register.get("cid-a")
    assert deposit.connection_reads == 0
    assert announced(worker) == []


# ----------------------------------------------------------------------
# Departures from the registers: the cascade and its plurals
# ----------------------------------------------------------------------


def test_dropping_a_page_takes_what_it_was_the_last_of(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.worker_events.clear()

    worker.drop_page("mario", "page-1")

    assert announced(worker) == ["drop_page", "drop_connection", "drop_user"]
    assert worker.page_register.keys() == []
    assert worker.connection_register.keys() == []
    assert worker.user_register.keys() == []


def test_dropping_a_page_with_a_sister_leaves_the_chain_standing(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.add_page("page-2", "cid-a")
    worker.worker_events.clear()

    worker.drop_page("mario", "page-1")

    assert announced(worker) == ["drop_page"]
    assert worker.connection_register.get("cid-a")["pages"] == {"page-2"}


def test_dropping_a_connection_speaks_the_plural_for_its_pages(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.add_page("page-2", "cid-a")
    worker.add_connection("cid-b", "mario")
    worker.worker_events.clear()

    worker.drop_connection("mario", "cid-a")

    assert announced(worker) == ["drop_pages", "drop_connection"]
    assert worker.worker_events[0]["page_ids"] == ["page-1", "page-2"]
    assert worker.page_register.keys() == []
    assert worker.user_register.get("mario")["connections"] == {"cid-b"}


def test_dropping_a_user_takes_everything_under_him(worker):
    worker.add_page("page-1", "cid-a", "mario")
    worker.add_page("page-2", "cid-b", "mario")
    worker.worker_events.clear()

    worker.drop_user("mario")

    assert announced(worker) == ["drop_pages", "drop_connections", "drop_user"]
    assert worker.worker_events[0]["page_ids"] == ["page-1", "page-2"]
    assert worker.worker_events[1]["connection_ids"] == ["cid-a", "cid-b"]
    assert worker.page_register.keys() == []
    assert worker.connection_register.keys() == []
    assert worker.user_register.keys() == []


def test_dropping_a_bare_user_says_only_that(worker):
    worker.add_user("mario")
    worker.worker_events.clear()

    worker.drop_user("mario")

    assert announced(worker) == ["drop_user"]


def test_a_drop_asks_for_absence(worker):
    worker.drop_page("nobody", "page-nowhere")
    worker.drop_user("nobody")

    assert announced(worker) == []
    # A connection is the one drop that answers absence with an error: the site
    # names the connection it is logging out, and a name nobody holds is a bug
    # upstream, not a departure that already happened.
    with pytest.raises(KeyError, match="cid-nowhere"):
        worker.drop_connection("nobody", "cid-nowhere")

    worker.add_page("page-1", "cid-a", "mario")
    worker.drop_page("mario", "page-1")
    worker.worker_events.clear()

    worker.drop_page("mario", "page-1")
    worker.drop_user("mario")

    assert announced(worker) == []
