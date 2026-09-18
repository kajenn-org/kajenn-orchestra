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

"""The chain of the envelope: who reads what, and what goes back down.

Every worker event of the census is exercised here, one test each, on the real
three layers over a real ``SpaCommander``: the handler is a real
``WorkerHandler`` with no process under it — construction alone builds its layer
of the chain — and the group is the stub that stands in for the level not yet
built, whose own verbs are the contract that level will owe.

The last test is the whole thing with a REAL child process: a worker event born
in another process lands in the vertex's indexes, the store answers the
presentation and only it, and a fold that refuses an envelope is denounced
without severing the wire.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest


from kajenn_orchestra.orchestration import UserOnHold, WorkerHandler
from kajenn_orchestra.orchestration.worker_connector import (
    ENVELOPE_SLOT_PRESENTATION,
    ENVELOPE_SLOT_WORKER_EVENTS,
    ENVELOPE_SLOT_WORKER_SNAPSHOT,
)

from .child_stub import ANNOUNCE_OP
from .conftest import kill_process, wait_for
from .group_stub import GroupStub

CHILD_MODULE = "tests.orchestration.child_stub"
WORKER_NAME = "standard_0001"
CALL_TIMEOUT = 5.0



def minted(commander, cid: str) -> str:
    """The identity the site would baptise for this cookie, learned by the vertex.

    The old mint died with the doctrine (the cookie routes, the site names):
    tests stage the junction the fold of ``new_connection`` would have written.
    """
    user = f"guest_{cid}"
    commander.record_connection_user(cid, user)
    return user


def envelope(*worker_events: dict[str, Any], photo: dict[str, Any] | None = None) -> dict[str, Any]:
    """One envelope as a child composes it: its worker events, and its photo if due."""
    made: dict[str, Any] = {ENVELOPE_SLOT_WORKER_EVENTS: list(worker_events)}
    if photo is not None:
        made[ENVELOPE_SLOT_WORKER_SNAPSHOT] = photo
    return made


def presentation(**payload: Any) -> dict[str, Any]:
    """The envelope of a child being born: what only a presentation carries."""
    return {ENVELOPE_SLOT_PRESENTATION: 4242, **payload}


def photo_of(**users: str | None) -> dict[str, Any]:
    """A photo carrying one row per user, each with the flag the shot decided."""
    return {
        "name": WORKER_NAME,
        "user_count": len(users),
        "users": {user: {"item": {}, "transfer_flag": flag} for user, flag in users.items()},
    }


@pytest.fixture
def group(short_root):
    """The group of the tests, with the real chain and the real vertex above it."""
    return GroupStub(short_root / "frozen_users")


@pytest.fixture
def commander(group):
    """The vertex the chain writes through."""
    return group.spa_commander


@pytest.fixture
async def handler(short_root, group):
    """A real handler with no process under it: enough to own its layer of the chain."""
    worker_handler = WorkerHandler(
        group,
        WORKER_NAME,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module=CHILD_MODULE,
        worker_kwargs={"group": "standard"},
        process_ping_timeout=1.0,
    )
    group.worker_handler = worker_handler
    yield worker_handler
    if worker_handler.process is not None:
        kill_process(worker_handler.process)
        await wait_for(lambda: not worker_handler.process.alive)
    await worker_handler.connector.stop()


async def test_the_photo_is_filed_by_the_bottom_layer_and_may_wait_for_its_round(handler, group):
    photo = photo_of(mario=None)

    handler.read_envelope(envelope(photo=photo))

    assert handler.worker_snapshot == photo
    assert group.wakes == []


async def test_an_urgent_photo_brings_the_groups_round_forward(handler, group):
    group.urgent_snapshots = True

    handler.read_envelope(envelope(photo=photo_of(mario=None)))

    assert group.wakes == ["starting"]


async def test_a_user_the_photo_shows_leaving_is_put_in_the_waiting_room(handler, commander):
    minted(commander, "cid-a")
    minted(commander, "cid-b")

    handler.read_envelope(
        envelope(photo=photo_of(**{"guest_cid-a": "T", "guest_cid-b": None}))
    )

    with pytest.raises(UserOnHold) as refusal:
        commander.resolve_user("cid-a")
    assert refusal.value.user == "guest_cid-a"
    assert "T" in refusal.value.cause
    assert commander.resolve_user("cid-b") == "guest_cid-b"


async def test_the_births_of_the_reception_find_the_rows_already_written(handler, commander):
    user = minted(commander, "cid-a")
    row = dict(commander.user_map[user])

    handler.read_envelope(
        envelope(
            {"op": "new_user", "worker": WORKER_NAME, "user": user},
            {"op": "new_connection", "worker": WORKER_NAME, "user": user, "connection_id": "cid-a"},
        )
    )

    assert commander.user_map[user] == row
    assert commander.connection_user_map == {"cid-a": user}


async def test_a_page_is_written_where_it_belongs_and_forgotten_one_by_one(handler, commander):
    handler.read_envelope(
        envelope(
            {"op": "new_page", "worker": WORKER_NAME, "page_id": "p1", "connection_id": "cid-a"},
            {"op": "new_page", "worker": WORKER_NAME, "page_id": "p2", "connection_id": "cid-a"},
        )
    )
    assert commander.page_connection_map == {"p1": "cid-a", "p2": "cid-a"}

    handler.read_envelope(
        envelope({"op": "drop_page", "worker": WORKER_NAME, "page_id": "p1"})
    )
    assert commander.page_connection_map == {"p2": "cid-a"}


async def test_a_cascade_of_pages_goes_in_one_worker_event(handler, commander):
    handler.read_envelope(
        envelope(
            {"op": "new_page", "worker": WORKER_NAME, "page_id": "p1", "connection_id": "cid-a"},
            {"op": "new_page", "worker": WORKER_NAME, "page_id": "p2", "connection_id": "cid-a"},
            {"op": "drop_pages", "worker": WORKER_NAME, "page_ids": ["p1", "p2"]},
        )
    )

    assert commander.page_connection_map == {}


async def test_a_connection_leaves_its_pages_and_keeps_its_identity(handler, commander):
    user = minted(commander, "cid-a")
    handler.read_envelope(
        envelope({"op": "new_page", "worker": WORKER_NAME, "page_id": "p1", "connection_id": "cid-a"})
    )

    handler.read_envelope(
        envelope({"op": "drop_connection", "worker": WORKER_NAME, "connection_id": "cid-a"})
    )

    assert commander.page_connection_map == {}
    assert commander.connection_user_map == {"cid-a": user}
    assert minted(commander, "cid-a") == user


async def test_several_connections_leave_in_one_worker_event(handler, commander):
    minted(commander, "cid-a")
    minted(commander, "cid-b")
    handler.read_envelope(
        envelope(
            {"op": "new_page", "worker": WORKER_NAME, "page_id": "p1", "connection_id": "cid-a"},
            {"op": "new_page", "worker": WORKER_NAME, "page_id": "p2", "connection_id": "cid-b"},
        )
    )

    handler.read_envelope(
        envelope(
            {"op": "drop_connections", "worker": WORKER_NAME, "connection_ids": ["cid-a", "cid-b"]}
        )
    )

    assert commander.page_connection_map == {}
    assert sorted(commander.connection_user_map) == ["cid-a", "cid-b"]


async def test_a_user_who_is_gone_leaves_nothing_behind(handler, commander, group):
    user = minted(commander, "cid-a")
    group.user_worker_map[user] = WORKER_NAME
    handler.read_envelope(
        envelope({"op": "new_page", "worker": WORKER_NAME, "page_id": "p1", "connection_id": "cid-a"})
    )

    handler.read_envelope(envelope({"op": "drop_user", "worker": WORKER_NAME, "user": user}))

    assert commander.user_map == {}
    assert commander.connection_user_map == {}
    assert commander.page_connection_map == {}
    assert group.user_worker_map == {}


async def test_the_photo_is_read_before_the_worker_events_of_its_own_envelope(
    handler, commander
):
    user = minted(commander, "cid-a")

    handler.read_envelope(
        envelope(
            {"op": "user_frozen", "worker": WORKER_NAME, "user": user, "placement": None},
            photo={"users": {user: {"transfer_flag": "T"}}},
        )
    )

    # One envelope, taken in the order it was shot: the photo parks him first,
    # the event that follows settles him — nobody is left waiting for ever.
    assert commander.user_map[user]["on_hold"] is None
    assert commander.user_is_frozen(user) is True


async def test_a_freeze_is_a_mark_above_and_a_placement_to_assign_below(handler, commander, group):
    user = minted(commander, "cid-a")
    group.user_worker_map[user] = WORKER_NAME

    handler.read_envelope(
        envelope(
            {"op": "user_frozen", "worker": WORKER_NAME, "user": user, "placement": None},
            photo={"users": {"somebody-else": {}}},
        )
    )

    assert commander.user_is_frozen(user) is True
    assert group.user_worker_map == {user: None}


async def test_a_batch_of_freezes_marks_them_all(handler, commander, group):
    first = minted(commander, "cid-a")
    second = minted(commander, "cid-b")

    handler.read_envelope(
        envelope(
            {"op": "user_frozen", "worker": WORKER_NAME, "user": first, "placement": None},
            {"op": "user_frozen", "worker": WORKER_NAME, "user": second, "placement": None},
            photo={"users": {"somebody-else": {}}},
        )
    )

    assert commander.user_is_frozen(first) is True
    assert commander.user_is_frozen(second) is True


async def test_an_adoption_turns_the_mark_off_and_drains_what_was_waiting(handler, commander):
    user = minted(commander, "cid-a")
    handler.read_envelope(
        envelope(
            {"op": "user_frozen", "worker": WORKER_NAME, "user": user, "placement": None}
        )
    )

    handler.read_envelope(
        envelope({"op": "user_adopted", "worker": WORKER_NAME, "user": user})
    )

    assert commander.user_is_frozen(user) is False


async def test_a_hold_is_lifted_by_the_freeze_it_was_waiting_for(handler, commander):
    user = minted(commander, "cid-a")
    handler.read_envelope(envelope(photo=photo_of(**{user: "T"})))
    with pytest.raises(UserOnHold):
        commander.resolve_user("cid-a")

    handler.read_envelope(
        envelope({"op": "user_frozen", "worker": WORKER_NAME, "user": user, "placement": None})
    )

    assert minted(commander, "cid-a") == user


async def test_a_worker_event_no_layer_knows_is_ignored(handler, commander):
    handler.read_envelope(
        envelope({"op": "something_nobody_reads", "worker": WORKER_NAME, "user": "mario"})
    )

    assert commander.user_map == {}


async def test_the_ordered_death_freezes_the_flagged_and_discards_the_rest(
    handler, commander, group
):
    staying = minted(commander, "cid-a")
    leaving = minted(commander, "cid-b")
    handler.hosted_users.update({staying, leaving})
    group.user_worker_map[staying] = WORKER_NAME
    group.user_worker_map[leaving] = WORKER_NAME
    handler.read_envelope(envelope(photo=photo_of(**{staying: None, leaving: "T"})))
    handler.state = "quitted"

    handler.envelope_handler.report_death()

    assert commander.user_is_frozen(leaving) is True
    assert staying not in commander.user_map
    assert group.user_worker_map == {leaving: None}
    assert group.dropped_workers == [WORKER_NAME]


async def test_a_death_does_not_take_whoever_was_only_passing_through(handler, commander, group):
    """Only whoever LIVED here dies here: the crossing of the two lists.

    A person is in a process's memory for reasons other than living in it — the
    login of a second browser puts him there for the length of one call, while
    his home, his store and his deposit folder are on another process. A death
    read off that memory alone would erase somebody who is perfectly well, and a
    death read off the placement alone would erase somebody the group had sent
    here who had not yet arrived, whose parcel is untouched in the deposit.
    """
    passing = minted(commander, "cid-a")
    expected = minted(commander, "cid-b")
    resident = minted(commander, "cid-c")
    handler.hosted_users.update({passing, resident})   # in this process's memory
    group.user_worker_map[passing] = "standard_0002"   # but he lives elsewhere
    group.user_worker_map[expected] = WORKER_NAME      # sent here, never arrived
    group.user_worker_map[resident] = WORKER_NAME
    handler.state = "aborted"

    handler.envelope_handler.report_death()

    assert resident not in commander.user_map
    assert passing in commander.user_map
    assert expected in commander.user_map
    assert commander.connection_user_map["cid-a"] == passing


async def test_the_wild_death_saves_nobody_and_its_parcels_are_discarded(
    handler, commander, group, caplog
):
    caplog.set_level(logging.INFO)
    user = minted(commander, "cid-a")
    handler.hosted_users.add(user)
    group.user_worker_map[user] = WORKER_NAME          # this worker is where he LIVES
    # What a freeze leaves on disk, written the way a worker writes it: under the
    # semaphore of that user's own folder.
    commander.freeze_handler.take_lock(user, WORKER_NAME)
    commander.freeze_handler.write_user_register_item(
        user, {"store": "whatever"}, writer=WORKER_NAME, cause="freeze", group="standard"
    )
    commander.freeze_handler.release_lock(user, WORKER_NAME)
    handler.read_envelope(envelope(photo=photo_of(**{user: "T"})))
    handler.state = "aborted"

    handler.envelope_handler.report_death()

    assert commander.user_map == {}
    assert commander.freeze_handler.user_folders == set()
    assert commander.counters["frozen_users_discarded"] == 1
    assert group.dropped_workers == [WORKER_NAME]
    assert "order=drop_user" in caplog.text
    assert "outcome=process_aborted" in caplog.text


async def test_a_death_reported_for_a_living_process_is_refused(handler):
    handler.state = "running"

    with pytest.raises(ValueError, match="not dead"):
        handler.envelope_handler.report_death()


async def test_nothing_at_all_travels_back_down_the_chain(handler, commander):
    # The store used to ride the presentation; it lives on the lane now, so the
    # descent carries no payload of its own for any kind of envelope.
    commander.global_register["counters.invoices"] = 3

    assert handler.read_envelope(presentation()) == {}
    assert handler.read_envelope(envelope()) == {}
    assert handler.read_envelope(envelope(photo=photo_of(mario=None))) == {}


async def test_a_real_child_announces_and_the_vertex_learns_it(
    handler, commander, group, repo_on_pythonpath, caplog
):
    caplog.set_level(logging.INFO)
    user = minted(commander, "cid-a")

    # BORN. The photo it presents itself with is all this side knows of it, and
    # the answer to that presentation carries nothing back.
    await handler.launch_process()
    assert handler.worker_snapshot["pid"] == handler.process.pid

    # IT ANNOUNCES. What happened in that process rides the answer to the order,
    # climbs the three layers inline, and lands in the indexes of the vertex.
    reply = await handler.connector.call(
        ANNOUNCE_OP,
        {
            "worker_events": [
                {"op": "new_page", "worker": WORKER_NAME, "page_id": "p1", "connection_id": "cid-a"},
                {"op": "user_frozen", "worker": WORKER_NAME, "user": user, "placement": None},
            ]
        },
        timeout=CALL_TIMEOUT,
    )

    assert reply["result"] == {"announcing": 2}
    assert commander.page_connection_map == {"p1": "cid-a"}
    assert commander.user_is_frozen(user) is True
    assert group.user_worker_map == {user: None}

    # A FOLD THAT REFUSES ANSWERS THE CALLER AND LEAVES THE CHILD ALONE. What
    # raised is a fault of THIS side — a field the two sides name differently, or
    # a bug in a layer of the chain — so it is logged with its stack and the
    # process is not touched: the orchestration neither corrects nor masks, and
    # the worker is not answerable for a defect of the parent.
    refused = await handler.connector.call(
        ANNOUNCE_OP,
        {
            "worker_events": [
                {"op": "user_frozen", "worker": WORKER_NAME, "user": "nobody", "placement": None}
            ]
        },
        timeout=CALL_TIMEOUT,
    )

    assert refused["result"] == {"announcing": 1}
    assert "The fold refused the envelope" in caplog.text
    assert handler.process is not None
    assert handler.state == "running"
