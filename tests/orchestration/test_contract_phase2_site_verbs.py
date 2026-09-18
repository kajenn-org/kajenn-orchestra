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

"""Phase 2 contract: the lifecycle verbs in the forms the site calls.

Derived from the pre_refactoring worker's own verb signatures
(``src/kajenn/spa/worker.py``: ``new_connection`` :1917, ``new_page`` :1999,
``drop_page`` :2078, ``drop_connection`` :2084) and from
``tests/test_spa_stores.py`` (the login relabels in place). The identity is the
first positional argument, everything else travels by name — exactly how
``siteregister_client.py`` calls them. The ``cascade`` the site passes on
``drop_page`` is added by the bridge and never reaches the worker.

``add_user`` / ``add_connection`` / ``add_page`` stay what they are: the
mutators that announce upward. The new verbs are facades in the site's shapes
over those same mutators, so the announcements keep rising unchanged.
"""

from __future__ import annotations

import pytest
from genro_bag import Bag

from kajenn_orchestra.orchestration import FreezeHandler, SpaWorker
from kajenn_orchestra.orchestration.spa_worker import GUEST_PREFIX

WORKER_NAME = "standard_0001"


@pytest.fixture
def worker(tmp_path):
    deposit = FreezeHandler(tmp_path / "frozen_users")
    worker = SpaWorker(WORKER_NAME, freeze_handler=deposit, deposit_lock_retry_interval=0.01)
    worker.open_request_slot()
    return worker


def announced(worker):
    return [event["op"] for event in worker.worker_events]


# ----------------------------------------------------------------------
# new_connection / new_page: identity first, the chain built from below
# ----------------------------------------------------------------------


def test_new_connection_names_the_anonymous_guest_and_announces_the_cascade(worker):
    entry = worker.new_connection("sess-1")

    assert entry["user"] == GUEST_PREFIX + "sess-1"
    assert announced(worker) == ["new_user", "new_connection"]
    assert worker.connection_register.get("sess-1") is not None


def test_new_page_builds_the_whole_chain_and_announces_it_in_order(worker):
    entry = worker.new_page("alice", page_id="p1", connection_id="s1")

    assert entry["connection_id"] == "s1"
    assert announced(worker) == ["new_user", "new_connection", "new_page"]
    page_event = worker.worker_events[-1]
    assert page_event["page_id"] == "p1"
    assert page_event["connection_id"] == "s1"
    assert page_event["user"] == "alice"


def test_new_page_keeps_the_fields_the_site_writes(worker):
    """``start_ts`` is the site's own stamp: the row carries it verbatim."""
    worker.new_page("alice", page_id="p1", connection_id="s1", start_ts=1000.0)

    assert worker.page_register.get("p1")["start_ts"] == 1000.0


def test_a_second_page_on_a_known_connection_announces_only_itself(worker):
    worker.new_page("alice", page_id="p1", connection_id="s1")
    worker.worker_events.clear()

    worker.new_page("alice", page_id="p2", connection_id="s1")

    assert announced(worker) == ["new_page"]


def test_the_new_rows_carry_the_data_plane_from_birth(worker):
    worker.new_page("alice", page_id="p1", connection_id="s1")

    page = worker.page_register.get("p1")
    assert isinstance(page["store"], Bag)


# ----------------------------------------------------------------------
# drop_page / drop_connection: the pre_refactoring forms, cascade announced
# ----------------------------------------------------------------------


def test_drop_page_takes_the_page_and_announces_what_its_departure_empties(worker):
    worker.new_page("alice", page_id="p1", connection_id="s1")
    worker.worker_events.clear()

    worker.drop_page("alice", "p1")

    ops = announced(worker)
    assert ops[0] == "drop_page"
    assert "drop_connection" in ops
    assert "drop_user" in ops
    assert worker.page_register.get("p1") is None
    assert worker.connection_register.get("s1") is None
    assert worker.user_register.get("alice") is None


def test_drop_page_leaves_the_surviving_sibling_alone(worker):
    worker.new_page("alice", page_id="p1", connection_id="s1")
    worker.new_page("alice", page_id="p2", connection_id="s1")
    worker.worker_events.clear()

    worker.drop_page("alice", "p1")

    assert announced(worker) == ["drop_page"]
    assert worker.page_register.get("p2") is not None
    assert worker.connection_register.get("s1") is not None


def test_drop_connection_demolishes_pages_first_then_connection_then_user(worker):
    worker.new_page("alice", page_id="p1", connection_id="s1")
    worker.new_page("alice", page_id="p2", connection_id="s1")
    worker.worker_events.clear()

    worker.drop_connection("alice", connection_id="s1")

    ops = announced(worker)
    assert ops[-1] == "drop_user"
    assert "drop_connection" in ops
    assert worker.page_register.get("p1") is None
    assert worker.page_register.get("p2") is None
    assert worker.connection_register.get("s1") is None
    assert worker.user_register.get("alice") is None


def test_drop_connection_on_an_unknown_connection_is_an_explicit_error(worker):
    with pytest.raises(KeyError, match="ghost"):
        worker.drop_connection("alice", connection_id="ghost")


# ----------------------------------------------------------------------
# The login: change_connection_user keeps its compatible form
# ----------------------------------------------------------------------


def test_the_login_relabels_the_connection_in_place(worker):
    worker.new_page("guest_sess-1", page_id="p1", connection_id="sess-1")
    connection = worker.connection_register.get("sess-1")

    worker.change_connection_user("sess-1", user="alice")

    assert worker.connection_register.get("sess-1") is connection
    assert connection["user"] == "alice"
    assert worker.user_register.get("guest_sess-1") is None
    assert worker.user_register.get("alice") is not None


def test_the_guest_store_follows_its_first_real_identity(worker):
    worker.new_page("guest_sess-1", page_id="p1", connection_id="sess-1")
    guest_store = worker.user_register.get("guest_sess-1")["store"]
    guest_store["draft"] = "half typed"

    worker.change_connection_user("sess-1", user="alice")

    entry = worker.user_register.get("alice")
    assert entry["store"] is guest_store
    assert entry["store"]["draft"] == "half typed"


# ----------------------------------------------------------------------
# The old mutators stay: no collision with the site's verbs
# ----------------------------------------------------------------------


def test_add_user_keeps_its_own_form_beside_the_site_verbs(worker):
    item = worker.add_user("alice", custom="kept")

    assert item["custom"] == "kept"
    assert announced(worker) == ["new_user"]


def test_refresh_chain_answers_the_sites_single_argument_call(worker):
    worker.new_page("alice", page_id="p1", connection_id="s1")

    instant = worker.refresh_chain("p1")

    assert worker.user_register.get("alice")["last_refresh_ts"] == instant
