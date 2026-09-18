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

"""Contract: the seams a consumer overrides to pair its own data with the core (#59, block 3).

The core knows four opaque data: the vertex's, the user's, the connection's,
the page's. Every row is a ``dict`` of the registry's row class — ``UserRow``,
``ConnectionRow``, ``PageRow`` — so ``row["field"]`` reads everywhere as
before, and the class carries what the core used to hard-code about the row:
the fields it is born with (``default_fields``), the ones the parcel leaves
behind (``fields_left_behind``), the ones that travel but are put back after
the birth (``fields_replayed``, ``replay_fields``), and what the birth
announces (``announcement_fields``). A consumer subclasses the row and names it
on its registry (``page_row_class``); the worker asks the row and knows nothing
of the fields. Beside the rows: the request slot comes from
``SpaWorker.build_request_slot``, the vertex's data from
``SpaCommander.new_global_store`` (a dict, whose values are the consumer's own
types), every served request ends in
``SpaWorker.on_request_served``, and a process that has just presented itself
is told to the vertex through ``SpaCommander.on_worker_presented`` — the seam
the source filter of a hosted site is pushed from, once per process, on the
envelope that carries the presentation and no other. The last layer of the
envelope chain is the commander's ``envelope_handler`` property: a consumer
returns its subclass of ``CommanderEnvelopeHandler`` and reads, in an
``on_<op>`` that calls the core's, what a worker event carries for it.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from genro_bag import Bag
from genro_tytx import from_tytx, to_tytx

from kajenn_orchestra import RegisterRegistry
from kajenn_orchestra.orchestration import FreezeHandler, GroupHandler, SpaCommander, SpaWorker
from kajenn_orchestra.orchestration.envelope_handler import CommanderEnvelopeHandler
from kajenn_orchestra.orchestration.spa_worker import RequestSlot
from kajenn_orchestra.register_row import ConnectionRow, PageRow, UserRow

from .conftest import XT_WorkerCommanderLane, attach_wire

STORE_GET = "/commander/store/get"
STORE_LOCK = "/commander/store/lock"
STORE_UNLOCK = "/commander/store/unlock"


class XT_PageRow(PageRow):
    """A consumer's page row: one field of its own in every category."""

    fields_left_behind = PageRow.fields_left_behind | {"xt_live"}
    fields_replayed = (*PageRow.fields_replayed, "xt_replayed", "xt_tables")

    def default_fields(self) -> dict[str, Any]:
        return {
            **super().default_fields(),
            "xt_marker": "born",
            "xt_live": object(),
            "xt_tables": set(),
        }

    def replay_fields(self, registry: Any, fields: dict[str, Any]) -> None:
        super().replay_fields(registry, fields)
        self["xt_replayed_seen"] = fields.get("xt_replayed")
        self["xt_tables"].update(fields.get("xt_tables", ()))

    def announcement_fields(self) -> dict[str, Any]:
        return {
            **super().announcement_fields(),
            "xt_marker": self["xt_marker"],
            "xt_tables": sorted(self["xt_tables"]),
        }


class XT_Registry(RegisterRegistry):
    page_row_class = XT_PageRow


class XT_Slot(RequestSlot):
    def __init__(self) -> None:
        super().__init__()
        self.xt_field = "mine"


class XT_Worker(SpaWorker):
    """A consumer's worker: its registry, its slot, its end-of-request hook."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.served: list[str] = []

    def build_registry(self) -> RegisterRegistry:
        return XT_Registry()

    def build_request_slot(self) -> RequestSlot:
        return XT_Slot()

    def on_request_served(self) -> None:
        super().on_request_served()
        self.served.append(type(self.request_slot).__name__)


class XT_EnvelopeHandler(CommanderEnvelopeHandler):
    """A consumer's last layer: it reads what a page's birth carries for it."""

    def on_new_page(self, worker_event: dict[str, Any]) -> None:
        super().on_new_page(worker_event)
        self.spa_commander.announced_pages.append(
            (worker_event["page_id"], worker_event["xt_tables"])
        )


class XT_Commander(SpaCommander):
    """A consumer's commander: it builds the vertex data and applies the writes itself.

    The data stays a Bag: the grant carries the whole store TYTX-encoded down
    the lane, so whatever type a consumer chooses must be one the codec knows.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.built: list[str] = []
        super().__init__(*args, **kwargs)
        self.applied: list[list[dict[str, Any]]] = []
        self.presented: list[str] = []
        self.announced_pages: list[tuple[str, list[str]]] = []

    @property
    def envelope_handler(self) -> CommanderEnvelopeHandler:
        return XT_EnvelopeHandler(self)

    def new_global_store(self) -> dict[str, Any]:
        self.built.append("xt")
        return {"seed": Bag({"a": 0})}

    def on_worker_presented(self, worker_handler: Any) -> None:
        self.presented.append(worker_handler.name)


@pytest.fixture
def worker(tmp_path):
    worker = XT_Worker("standard_0001", freeze_handler=FreezeHandler(tmp_path / "frozen_users"))
    attach_wire(worker)
    yield worker
    worker.exit_process()


# ----------------------------------------------------------------------
# The rows
# ----------------------------------------------------------------------


def test_rows_are_dicts_of_the_registrys_row_classes():
    registry = RegisterRegistry()
    page = registry.new_page("p1", user="mario", connection_id="c1")
    assert isinstance(page, PageRow) and isinstance(page, dict)
    assert isinstance(registry.connection_items.get("c1"), ConnectionRow)
    assert isinstance(registry.user_items.get("mario"), UserRow)
    assert page["register_item_id"] == "p1" and "item_lock" in page


def test_a_row_born_with_fields_keeps_them_over_the_defaults():
    registry = RegisterRegistry()
    page = registry.new_page("p1", user="mario", connection_id="c1", last_refresh_ts=1.0)
    assert page["last_refresh_ts"] == 1.0


def test_the_consumers_row_class_is_what_the_registry_builds(worker):
    worker.add_connection("c1", "mario")
    page = worker.add_page("p1", "c1")
    assert isinstance(page, XT_PageRow)
    assert page["xt_marker"] == "born"
    assert page["xt_tables"] == set()


def test_the_parcel_leaves_behind_what_the_row_says_and_carries_the_rest(worker):
    worker.add_connection("c1", "mario")
    worker.add_page("p1", "c1")
    parcel = worker._connection_parcel("c1")
    page = parcel["pages"]["p1"]
    assert "xt_live" not in page and "item_lock" not in page
    assert "connection_id" not in page
    assert page["xt_marker"] == "born"


def test_the_birth_announces_what_the_row_says(worker):
    worker.add_connection("c1", "mario")
    worker.add_page("p1", "c1")
    announced = [event for event in worker.request_slot.worker_events if event["op"] == "new_page"]
    assert announced[-1]["xt_marker"] == "born"
    assert announced[-1]["xt_tables"] == []


def test_the_row_class_replays_what_travelled(worker):
    worker.add_connection("c1", "mario")
    page = worker.add_page("p1", "c1")
    page.replay_fields(worker.registry, {"xt_replayed": 7, "xt_tables": ["t"]})
    assert page["xt_replayed_seen"] == 7
    assert page["xt_tables"] == {"t"}


# ----------------------------------------------------------------------
# The slot, the vertex data, the end of the request
# ----------------------------------------------------------------------


async def test_every_request_slot_is_the_consumers(worker):
    assert isinstance(worker.open_request_slot(), XT_Slot)
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        on_thread = await worker._run_in_pool(pool, lambda: type(worker.request_slot).__name__)
    finally:
        pool.shutdown(wait=True)
    assert on_thread == "XT_Slot"


async def test_on_request_served_runs_after_every_request_failed_ones_included(worker):
    # Rewritten in phase 4a of #68: the stitching goes through
    # `hosted_app_seam` now, and `_serve_on_thread` is gone with the dict
    # entrance it served. The assertion is the one it always was — the hook
    # runs on a pool thread, in the request's own slot, whatever the site did.
    def wsgi_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"ok"]

    def failing_app(environ, start_response):
        raise RuntimeError("the site failed")

    payload = {"http": {"method": "GET", "path": "/", "cid": "c1"}, "identity": "mario"}
    worker.wsgi_app = wsgi_app
    await worker._serve_request(payload)
    worker.wsgi_app = failing_app
    with pytest.raises(RuntimeError):
        await worker._serve_request(payload)
    assert worker.served == ["XT_Slot", "XT_Slot"]


async def test_the_vertex_data_is_the_commanders_seam(short_root, tmp_path):
    # The consumer fills the dictionary at birth with values of its own type; a
    # turn's release publishes the complete value the worker sent, whatever it is.
    commander = XT_Commander(short_root / "frozen_users")
    assert commander.built == ["xt"]
    group = GroupHandler(
        commander,
        "standard",
        memory_concession_bytes=8 * 1024 * 1024 * 1024,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module="never.launched",
    )
    lane = XT_WorkerCommanderLane(commander, group, FreezeHandler(tmp_path / "frozen_users"))
    await lane.open()
    try:
        grant = await lane.worker.call(
            STORE_LOCK, {"worker": lane.worker_name, "request_id": "r1", "key": "seed"}
        )
        seed = from_tytx(grant["value"], "json")
        seed["a"] = 1
        reply = await lane.worker.call(
            STORE_UNLOCK, {"request_id": "r1", "apply": True, "value": to_tytx(seed, "json")}
        )
    finally:
        await lane.close()
    assert reply == {"applied": True}
    assert isinstance(commander.global_register["seed"], Bag)
    assert commander.global_register["seed"]["a"] == 1
    await asyncio.sleep(0)


async def test_a_newborn_process_is_told_to_the_vertex_once(short_root, tmp_path):
    commander = XT_Commander(short_root / "frozen_users")
    group = GroupHandler(
        commander,
        "standard",
        memory_concession_bytes=8 * 1024 * 1024 * 1024,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module="never.launched",
    )
    lane = XT_WorkerCommanderLane(commander, group, FreezeHandler(tmp_path / "frozen_users"))
    await lane.open()
    try:
        assert commander.presented == [lane.worker_name]
        await lane.worker.call(STORE_GET, {"key": "a"})
        await lane.announce()
    finally:
        await lane.close()
    assert commander.presented == [lane.worker_name]


async def test_the_consumers_envelope_layer_reads_what_a_pages_birth_carries(short_root, tmp_path):
    commander = XT_Commander(short_root / "frozen_users")
    group = GroupHandler(
        commander,
        "standard",
        memory_concession_bytes=8 * 1024 * 1024 * 1024,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module="never.launched",
    )
    lane = XT_WorkerCommanderLane(commander, group, FreezeHandler(tmp_path / "frozen_users"))
    await lane.open()
    try:
        # The lane's worker is the core's, so the event a consumer's row would
        # announce is sent by hand, on the worker's own channel.
        await lane.worker.announce_worker_events(
            [{"op": "new_page", "worker": lane.worker_name, "page_id": "p1", "connection_id": "c1", "xt_tables": ["t"]}]
        )
    finally:
        await lane.close()
    assert commander.page_connection_map == {"p1": "c1"}
    assert commander.announced_pages == [("p1", ["t"])]
