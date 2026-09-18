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

"""Per-user service counters: the worker counts, the envelope layer derives.

Implementation tests of the observation added for the CPU offload judge. The
worker accumulates ``served_call_count`` and ``service_seconds`` on the user
register item — inside the ``finally`` of the actual stitching, so a call that
fails or runs long is counted like any other — and projects them into the
photo with ``pending_call_count``. One layer up, ``WorkerEnvelopeHandler``
turns the cumulatives into ``recent_call_count`` / ``recent_service_seconds``,
the deltas between two photos. The worker keeps no window and takes no
decision.
"""

from __future__ import annotations

import pytest

from kajenn_orchestra.orchestration import FreezeHandler, SpaWorker, WorkerHandler
from kajenn_orchestra.orchestration.worker_connector import ENVELOPE_SLOT_WORKER_SNAPSHOT

from .group_stub import GroupStub

WORKER_NAME = "standard_0001"


@pytest.fixture
def worker(tmp_path):
    worker = SpaWorker(
        WORKER_NAME,
        freeze_handler=FreezeHandler(tmp_path / "frozen_users"),
        deposit_lock_retry_interval=0.01,
    )
    worker.open_request_slot()
    return worker


def serving_app(environ, start_response):
    start_response("200 OK", [("Content-Type", "text/plain")])
    return [b"ok"]


def failing_app(environ, start_response):
    raise RuntimeError("the site broke while serving")


def http_payload(cid: str, identity: str) -> dict:
    return {"http": {"cid": cid, "path": "/", "method": "GET"}, "identity": identity}


# ----------------------------------------------------------------------
# The worker's cumulatives
# ----------------------------------------------------------------------


async def test_a_served_call_grows_the_users_counters(worker):
    worker.wsgi_app = serving_app
    worker.add_connection("cid-a", "mario")

    await worker._serve_request(http_payload("cid-a", "mario"))
    await worker._serve_request(http_payload("cid-a", "mario"))

    item = worker.user_register.get("mario")
    assert item["served_call_count"] == 2
    assert item["service_seconds"] > 0.0


async def test_a_call_that_fails_is_counted_all_the_same(worker):
    worker.wsgi_app = failing_app
    worker.add_connection("cid-a", "mario")

    with pytest.raises(RuntimeError):
        await worker._serve_request(http_payload("cid-a", "mario"))

    item = worker.user_register.get("mario")
    assert item["served_call_count"] == 1
    assert item["service_seconds"] > 0.0


async def test_an_anonymous_call_counts_nobody(worker):
    worker.wsgi_app = serving_app

    await worker._serve_request({"http": {"cid": "cid-x", "path": "/", "method": "GET"}})

    assert len(worker.user_register) == 0


def test_the_photo_projects_the_counters_and_the_pendings(worker):
    worker.add_connection("cid-a", "mario")
    worker._record_service("mario", 0.25)
    worker.open_request("mario")

    row = worker.worker_snapshot["users"]["mario"]["item"]
    assert row["served_call_count"] == 1
    assert row["service_seconds"] == 0.25
    assert row["pending_call_count"] == 1


def test_a_user_never_served_reads_zero_in_the_photo(worker):
    worker.add_connection("cid-a", "mario")

    row = worker.worker_snapshot["users"]["mario"]["item"]
    assert row["served_call_count"] == 0
    assert row["service_seconds"] == 0.0
    assert row["pending_call_count"] == 0


def test_the_parcel_of_a_freeze_carries_no_counter(worker):
    """The counters are observation: the freeze persists store and connections."""
    worker.add_connection("cid-a", "mario")
    worker._record_service("mario", 0.25)

    store, connection_parcels = worker._get_user_parcels(worker.user_register.get("mario"))

    assert "served_call_count" not in str(store)
    for parcel in connection_parcels.values():
        assert set(parcel) == {"connection_id", "connection", "pages"}
        assert "served_call_count" not in parcel["connection"]


# ----------------------------------------------------------------------
# The envelope layer's deltas
# ----------------------------------------------------------------------


def photo_with(user: str, service_seconds: float, served_call_count: int) -> dict:
    return {
        "users": {
            user: {
                "transfer_flag": None,
                "item": {
                    "state": "active",
                    "service_seconds": service_seconds,
                    "served_call_count": served_call_count,
                },
            }
        }
    }


@pytest.fixture
def handler(tmp_path):
    group = GroupStub(tmp_path / "frozen_users")
    return WorkerHandler(
        group,
        WORKER_NAME,
        instance_dir=tmp_path / "i",
        frozen_users_path=tmp_path / "frozen_users",
        entry_module="unused_child",
    )


def filed(handler, photo):
    handler.envelope_handler.work_on_envelope({ENVELOPE_SLOT_WORKER_SNAPSHOT: photo})
    return handler.worker_snapshot["users"]


def test_the_first_photo_of_a_user_reads_zero_recent_work(handler):
    rows = filed(handler, photo_with("mario", 3.0, 7))

    assert rows["mario"]["item"]["recent_service_seconds"] == 0.0
    assert rows["mario"]["item"]["recent_call_count"] == 0


def test_two_photos_apart_the_deltas_are_the_work_between_them(handler):
    filed(handler, photo_with("mario", 3.0, 7))
    rows = filed(handler, photo_with("mario", 4.5, 10))

    assert rows["mario"]["item"]["recent_service_seconds"] == pytest.approx(1.5)
    assert rows["mario"]["item"]["recent_call_count"] == 3


def test_a_counter_gone_backwards_is_clamped_to_zero(handler):
    """A row reborn after a freeze restarts its cumulatives; no negative delta."""
    filed(handler, photo_with("mario", 3.0, 7))
    rows = filed(handler, photo_with("mario", 0.5, 1))

    assert rows["mario"]["item"]["recent_service_seconds"] == 0.0
    assert rows["mario"]["item"]["recent_call_count"] == 0


def test_a_user_gone_from_the_photo_costs_no_memory_and_restarts_clean(handler):
    filed(handler, photo_with("mario", 3.0, 7))
    filed(handler, {"users": {}})

    assert handler.envelope_handler._user_service_read == {}

    rows = filed(handler, photo_with("mario", 9.0, 20))
    assert rows["mario"]["item"]["recent_service_seconds"] == 0.0


def test_a_row_without_counters_gets_no_delta(handler):
    photo = {"users": {"mario": {"transfer_flag": None, "item": {"state": "active"}}}}
    rows = filed(handler, photo)

    assert "recent_service_seconds" not in rows["mario"]["item"]
    assert "recent_call_count" not in rows["mario"]["item"]
