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

"""Phase 1 contract: the shared registry becomes the worker's three registers.

Derived from ``tests/test_spa_stores.py`` (the rows born with live stores) and
``tests/spa/orchestration/test_orchestration_spa_worker.py`` (births, announcements,
the freeze cycle). The pre_refactoring originals stay untouched: they remain the
sentinels of that stack until Macro 6.

What this phase owes: ``SpaWorker`` builds its rows through ``build_registry``
(the pre_refactoring hook name, kept so the bridge overrides nothing) and its
three registers — ``user_register``, ``connection_register``, ``page_register``
— ARE the registry's registers, with the register idioms (``get``, ``keys``,
``keys_by``, ``in``). The worker's own fields (``state``, the clocks, the
transfer flag) survive as extra fields on the same rows, and the freeze cycle
keeps working on them.
"""

from __future__ import annotations

import threading

import pytest
from genro_bag import Bag

from kajenn_orchestra.orchestration import FreezeHandler, SpaWorker
from kajenn_orchestra.orchestration.spa_worker import GUEST_PREFIX
from kajenn_orchestra.register_registry import RegisterRegistry

WORKER_NAME = "standard_0001"


@pytest.fixture
def deposit(tmp_path):
    return FreezeHandler(tmp_path / "frozen_users")


@pytest.fixture
def worker(deposit):
    worker = SpaWorker(WORKER_NAME, freeze_handler=deposit, deposit_lock_retry_interval=0.01)
    worker.open_request_slot()
    return worker


def announced(worker):
    """The protocol names queued for the envelope out, in order."""
    return [event["op"] for event in worker.worker_events]


# ----------------------------------------------------------------------
# The registry is the worker's: built by the hook, exposed as registers
# ----------------------------------------------------------------------


def test_the_worker_builds_its_registry_through_the_hook(worker):
    assert isinstance(worker.registry, RegisterRegistry)
    assert worker.user_register is worker.registry.user_items
    assert worker.connection_register is worker.registry.connection_items
    assert worker.page_register is worker.registry.page_items


def test_build_registry_is_the_seam_a_subclass_replaces(deposit):
    class BridgeRegistry(RegisterRegistry):
        pass

    class Seamed(SpaWorker):
        def build_registry(self):
            return BridgeRegistry()

    worker = Seamed(WORKER_NAME, freeze_handler=deposit)
    assert isinstance(worker.registry, BridgeRegistry)


def test_rows_are_read_with_the_register_idioms(worker):
    worker.add_page("p1", "cid-a")

    user = GUEST_PREFIX + "cid-a"
    assert user in worker.user_register
    assert worker.user_register.get(user) is not None
    assert worker.connection_register.get("cid-a") is not None
    assert worker.page_register.get("p1") is not None
    assert worker.user_register.get("nobody") is None
    assert set(worker.page_register.keys()) == {"p1"}


def test_the_pages_of_a_connection_answer_through_the_secondary_index(worker):
    worker.add_page("p1", "cid-a")
    worker.add_page("p2", "cid-a")
    worker.add_page("p3", "cid-b")

    assert sorted(worker.page_register.keys_by("connection_id", "cid-a")) == ["p1", "p2"]
    assert worker.page_register.keys_by("connection_id", "cid-b") == ["p3"]


# ----------------------------------------------------------------------
# The rows are born with the data plane already on them
# ----------------------------------------------------------------------


def test_rows_are_born_with_live_stores(worker):
    worker.add_page("p1", "cid-a")

    user = worker.user_register.get(GUEST_PREFIX + "cid-a")
    page = worker.page_register.get("p1")
    assert isinstance(user["store"], Bag)
    assert isinstance(page["store"], Bag)


def test_caller_fields_are_stored_verbatim_on_the_rows(worker):
    worker.add_page("p1", "cid-a", start_ts=1000.0, custom="kept")

    page = worker.page_register.get("p1")
    assert page["start_ts"] == 1000.0
    assert page["custom"] == "kept"


def test_the_workers_own_fields_ride_the_same_rows(worker):
    """The core's fields and the data plane's live together on one row."""
    worker.add_connection("cid-a")

    user = worker.user_register.get(GUEST_PREFIX + "cid-a")
    # The worker's own bookkeeping fields survive the registry move.
    assert user["state"] == "active"
    assert isinstance(user["store"], Bag)


# ----------------------------------------------------------------------
# Nothing already working regresses: announcements, clocks, the freezer
# ----------------------------------------------------------------------


def test_a_connection_arriving_anonymous_still_announces_the_cascade(worker):
    item = worker.add_connection("cid-a")

    assert item["user"] == GUEST_PREFIX + "cid-a"
    assert announced(worker) == ["new_user", "new_connection"]


def test_dropping_a_user_still_clears_the_whole_chain(worker):
    """The drop verbs in the SITE forms are Phase 2 matter: here only the
    registry-backed removal is asserted, through the unchanged ``drop_user``."""
    worker.add_page("p1", "cid-a")
    worker.worker_events.clear()

    worker.drop_user(GUEST_PREFIX + "cid-a")

    assert "drop_user" in announced(worker)
    assert worker.page_register.get("p1") is None
    assert worker.connection_register.get("cid-a") is None
    assert worker.user_register.get(GUEST_PREFIX + "cid-a") is None


def test_refresh_chain_still_stamps_the_three_levels(worker):
    worker.add_page("p1", "cid-a")

    instant = worker.refresh_chain("p1")

    user = GUEST_PREFIX + "cid-a"
    assert worker.page_register.get("p1")["last_refresh_ts"] == instant
    assert worker.connection_register.get("cid-a")["last_refresh_ts"] == instant
    assert worker.user_register.get(user)["last_refresh_ts"] == instant


async def test_the_freeze_cycle_still_parks_the_registry_rows(worker, deposit):
    """Must-not-break line 3: the rows stay packageable — the freezer proves it."""
    worker.add_page("p1", "cid-a")
    user = GUEST_PREFIX + "cid-a"

    assert await worker.freeze_user(user) is True

    assert worker.user_register.get(user) is None
    assert worker.page_register.get("p1") is None
    assert deposit.read_user_register_item(user) is not None
    assert deposit.read_connection_register_item(user, "cid-a") is not None


# ----------------------------------------------------------------------
# Every row is born with its own exclusive re-entrant lock
# ----------------------------------------------------------------------


def _acquired_from_another_thread(lock):
    """Whether a second thread can take ``lock`` right now, without waiting."""
    outcome = []

    def attempt():
        taken = lock.acquire(blocking=False)
        if taken:
            lock.release()
        outcome.append(taken)

    thread = threading.Thread(target=attempt)
    thread.start()
    thread.join()
    return outcome[0]


def test_every_register_row_is_born_with_an_item_lock(worker):
    worker.add_page("p1", "cid-a")

    for row in (
        worker.page_register.get("p1"),
        worker.connection_register.get("cid-a"),
        worker.user_register.get(GUEST_PREFIX + "cid-a"),
    ):
        lock = row["item_lock"]
        assert lock.acquire()
        assert lock.acquire()
        assert _acquired_from_another_thread(lock) is False
        lock.release()
        lock.release()
        assert _acquired_from_another_thread(lock) is True


def test_two_rows_hold_independent_locks(worker):
    worker.add_page("p1", "cid-a")
    worker.add_page("p2", "cid-b")

    first = worker.page_register.get("p1")["item_lock"]
    second = worker.page_register.get("p2")["item_lock"]

    assert first is not second
    with first:
        assert _acquired_from_another_thread(second) is True
