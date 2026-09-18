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

"""The vertex on its own: who it writes, what it answers, what it discards.

The chain's tests look at the fold from above — an envelope arrives and the
indexes move. These look at the same object from the front: the birth of a row
for whoever shows up, the predicates the rest of the machine reads it
through, the waiting room that is a raised exception and not a field, and the two
things the vertex does that nobody below it can — discarding what a dead process
left on disk, and writing the account of every order.
"""

from __future__ import annotations

import asyncio
import json
import logging
import pickle

import pytest


from kajenn_orchestra.orchestration import SpaCommander, UserOnHold
from kajenn_orchestra.orchestration import FreezeHandler
from kajenn_orchestra.orchestration.freeze_handler import USER_REGISTER_ITEM_NAME
from kajenn_orchestra.orchestration.spa_commander import GUEST_PREFIX

WORKER_NAME = "standard_0001"



def minted(commander, cid: str) -> str:
    """The identity the site would baptise for this cookie, learned by the vertex.

    The old mint died with the doctrine (the cookie routes, the site names):
    tests stage the junction the fold of ``new_connection`` would have written.
    """
    user = f"guest_{cid}"
    commander.record_connection_user(cid, user)
    return user


@pytest.fixture
def commander(short_root):
    return SpaCommander(short_root / "frozen_users")


def parked_state(commander: SpaCommander, user: str) -> None:
    """What a freeze leaves on disk, written the way a worker writes it."""
    commander.freeze_handler.take_lock(user, WORKER_NAME)
    commander.freeze_handler.write_user_register_item(
        user, {"store": "whatever"}, writer=WORKER_NAME, cause="freeze", group="standard"
    )
    commander.freeze_handler.release_lock(user, WORKER_NAME)


def test_whoever_shows_up_is_minted_before_anything_descends(commander):
    user = minted(commander, "cid-a")

    assert user == f"{GUEST_PREFIX}cid-a"
    assert commander.connection_user_map == {"cid-a": user}
    assert commander.user_map[user] == {"group": None, "frozen": False, "on_hold": None}


def test_a_cid_already_known_is_answered_and_nothing_is_written_twice(commander):
    first = minted(commander, "cid-a")
    commander.user_map[first]["group"] = "kept"

    assert minted(commander, "cid-a") == first
    assert commander.user_map[first]["group"] == "kept"


def test_a_cookie_that_outlived_its_row_is_still_that_person(commander):
    commander.connection_user_map["cid-a"] = "mario"

    assert commander.resolve_user("cid-a") == "mario"
    assert commander.user_map["mario"]["frozen"] is False


def test_a_user_on_his_way_out_is_not_routed_but_raised(commander):
    user = minted(commander, "cid-a")
    commander.hold_user(user, "transfer_flag T")

    with pytest.raises(UserOnHold) as refusal:
        commander.resolve_user("cid-a")

    assert refusal.value.user == user
    assert refusal.value.cause == "transfer_flag T"
    assert str(refusal.value) == f"{user} is on hold: transfer_flag T"


def test_the_cause_of_a_hold_is_the_one_that_explains_the_wait(commander):
    user = minted(commander, "cid-a")
    commander.hold_user(user, "transfer_flag T")
    commander.hold_user(user, "transfer_flag X")

    assert commander.user_map[user]["on_hold"] == "transfer_flag T"


def test_nobody_is_frozen_until_it_is_written_down(commander):
    user = minted(commander, "cid-a")

    assert commander.user_is_frozen("somebody nobody knows") is False
    assert commander.user_is_frozen(user) is False

    commander.mark_user_frozen(user)

    assert commander.user_is_frozen(user) is True


def test_a_freeze_ends_the_wait_it_was_the_reason_for(commander):
    user = minted(commander, "cid-a")
    commander.hold_user(user, "transfer_flag T")

    commander.mark_user_frozen(user)

    assert minted(commander, "cid-a") == user


def test_an_adoption_takes_the_mark_off(commander):
    user = minted(commander, "cid-a")
    commander.mark_user_frozen(user)

    commander.mark_user_adopted(user)

    assert commander.user_is_frozen(user) is False


def test_dropping_what_is_already_gone_is_that_same_outcome(commander):
    commander.drop_page("never-existed")
    commander.drop_connection("never-existed")
    commander.drop_user("never-existed")

    assert commander.page_connection_map == {}
    assert commander.user_map == {}


def test_a_user_who_is_gone_takes_his_connections_pages_and_freezer_state_with_him(commander):
    user = minted(commander, "cid-a")
    commander.connection_user_map["cid-b"] = user
    commander.page_connection_map["p1"] = "cid-a"
    commander.page_connection_map["p2"] = "cid-b"
    parked_state(commander, user)

    assert commander.drop_user(user) is True

    assert commander.user_map == {}
    assert commander.connection_user_map == {}
    assert commander.page_connection_map == {}
    # Nothing of an identity nobody answers for is left behind: what the sweep of
    # the freezer finds later is only what a row lost WITHOUT a drop.
    assert commander.freeze_handler.user_folders == set()
    assert commander.counters["frozen_users_discarded"] == 1


def test_what_a_dead_process_left_on_disk_is_discarded_and_counted(commander, caplog):
    caplog.set_level(logging.INFO)
    user = minted(commander, "cid-a")
    parked_state(commander, user)
    without_state = minted(commander, "cid-b")

    commander.drop_users([user, without_state], cause="process_aborted")

    assert commander.freeze_handler.user_folders == set()
    assert commander.user_map == {}
    assert commander.counters["frozen_users_discarded"] == 1
    assert caplog.text.count("order=drop_user") == 2


def test_every_order_leaves_its_row_on_the_file_of_the_orders(short_root):
    log_path = short_root / "orchestration.log"
    commander = SpaCommander(short_root / "frozen_users", orchestration_log_path=log_path)

    commander.log_order(
        "standard",
        "quit_process",
        WORKER_NAME,
        numbers={"occupancy_percent": 12.0},
        outcome="quitted",
    )

    row = log_path.read_text().strip()
    assert "decided_by=standard" in row
    assert "order=quit_process" in row
    assert f"subject={WORKER_NAME}" in row
    assert "numbers={'occupancy_percent': 12.0}" in row
    assert "outcome=quitted" in row


def test_every_order_also_leaves_a_structured_decision(short_root):
    log_path = short_root / "orchestration.log"
    commander = SpaCommander(short_root / "frozen_users", orchestration_log_path=log_path)

    commander.log_order(
        "standard",
        "quit_process",
        WORKER_NAME,
        numbers={"occupancy_percent": 12.0},
        outcome="quitted",
        reason="worker_was_ordered_away",
    )

    decision_path = log_path.with_suffix(".decisions.jsonl")
    row = json.loads(decision_path.read_text())
    assert row["schema"] == 1
    assert row["decision_id"].endswith("-1")
    assert row["decided_by"] == "standard"
    assert row["decision"] == "quit_process"
    assert row["subject"] == WORKER_NAME
    assert row["outcome"] == "quitted"
    assert row["reason"] == "worker_was_ordered_away"
    assert row["numbers"] == {"occupancy_percent": 12.0}
    assert row["candidates"] == []
    assert row["timestamp"].endswith("+00:00")


def test_a_calculation_can_be_recorded_without_inventing_an_order(short_root):
    log_path = short_root / "orchestration.log"
    commander = SpaCommander(short_root / "frozen_users", orchestration_log_path=log_path)

    commander.log_decision(
        "standard",
        "placement_candidates",
        "standard_0002",
        reason="hottest_cpu_open_candidate",
        subject="mario",
        candidates=[
            {"name": "standard_0001", "cpu_admission_open": False},
            {"name": "standard_0002", "cpu_admission_open": True},
        ],
    )

    assert log_path.read_text() == ""
    row = json.loads(log_path.with_suffix(".decisions.jsonl").read_text())
    assert row["decision"] == "placement_candidates"
    assert row["outcome"] == "standard_0002"
    assert row["reason"] == "hottest_cpu_open_candidate"
    assert [candidate["name"] for candidate in row["candidates"]] == [
        "standard_0001",
        "standard_0002",
    ]


def test_one_process_has_one_vertex_and_the_log_is_its_own(short_root):
    first_path = short_root / "first.log"
    second_path = short_root / "second.log"
    SpaCommander(short_root / "frozen_users", orchestration_log_path=first_path)
    second = SpaCommander(short_root / "frozen_users", orchestration_log_path=second_path)

    second.log_order("standard", "quit_process", WORKER_NAME, outcome="quitted")

    assert len(logging.getLogger("kajenn.orchestration.orders").handlers) == 1
    assert "order=quit_process" in second_path.read_text()
    assert first_path.read_text() == ""
    assert "quit_process" in second_path.with_suffix(".decisions.jsonl").read_text()
    assert first_path.with_suffix(".decisions.jsonl").read_text() == ""


def test_the_machine_starts_running_and_holds_the_master_of_the_store(commander):
    assert commander.state == "running"
    assert commander.global_register == {}


def test_the_concession_is_this_servers_share_of_the_whole_machine(commander, monkeypatch):
    monkeypatch.setattr(
        commander,
        "_machine_memory_gauges",
        lambda: {"MemTotal": 8_000_000_000.0, "MemAvailable": 6_000_000_000.0},
    )
    commander.memory_max_percent = 25.0

    assert commander.memory_concession_bytes == 2_000_000_000
    # And the alarm line is read against the machine, not against the concession.
    assert commander._machine_memory_used_percent() == 25.0


def test_the_machine_total_is_read_off_the_platform_itself(commander):
    # No monkeypatch: os.sysconf answers on every platform this suite runs on,
    # /proc/meminfo or not.
    assert commander.memory_concession_bytes > 0


def test_the_elected_group_receives_the_newcomer(commander):
    commander.group_map["stable"] = object()
    commander.group_map["canary"] = object()

    # Nobody elected: the first declared is the one the recipe named first.
    assert commander.default_group == "stable"

    commander._default_group = "canary"
    assert commander.default_group == "canary"


def test_a_vertex_with_nowhere_to_put_a_newcomer_says_so(commander):
    with pytest.raises(KeyError):
        commander.default_group

    commander._default_group = "nobody"
    commander.group_map["stable"] = object()
    with pytest.raises(KeyError):
        commander.default_group


def test_the_group_of_a_user_is_recorded_where_the_placement_decided_it(commander):
    user = minted(commander, "cid-a")

    commander.record_user_group(user, "stable")

    assert commander.user_map[user]["group"] == "stable"


async def test_a_request_for_a_user_on_hold_waits_for_his_release(commander):
    user = minted(commander, "cid-a")
    commander.hold_user(user, "transfer_flag T")

    waiting = asyncio.ensure_future(commander.await_user_release(user, timeout=5.0))
    await asyncio.sleep(0)
    assert not waiting.done()

    commander.mark_user_adopted(user)

    await waiting
    assert commander.user_hold_event_map == {}
    assert minted(commander, "cid-a") == user


async def test_the_freezer_mark_releases_the_wait_too(commander):
    user = minted(commander, "cid-a")
    commander.hold_user(user, "transfer_flag F")
    waiting = asyncio.ensure_future(commander.await_user_release(user, timeout=5.0))
    await asyncio.sleep(0)

    commander.mark_user_frozen(user)

    await waiting
    assert commander.user_hold_event_map == {}


async def test_a_user_dropped_while_held_wakes_whoever_waited_for_him(commander):
    user = minted(commander, "cid-a")
    commander.hold_user(user, "transfer_flag T")
    waiting = asyncio.ensure_future(commander.await_user_release(user, timeout=5.0))
    await asyncio.sleep(0)

    commander.drop_users([user], cause="expired")

    await waiting
    assert commander.user_hold_event_map == {}


async def test_a_wait_that_outlives_its_own_deadline_gives_up(commander):
    user = minted(commander, "cid-a")
    commander.hold_user(user, "transfer_flag T")

    with pytest.raises(TimeoutError):
        await commander.await_user_release(user, timeout=0.01)

    # The hold is still up: giving up on a wait decides nothing about the user.
    assert user in commander.user_hold_event_map


async def test_nobody_waits_on_a_user_who_is_not_held(commander):
    user = minted(commander, "cid-a")

    await commander.await_user_release(user, timeout=0.01)


def test_a_hold_already_up_keeps_its_first_cause_and_its_own_door(commander):
    user = minted(commander, "cid-a")
    commander.hold_user(user, "transfer_flag T")
    door = commander.user_hold_event_map[user]

    commander.hold_user(user, "transfer_flag X")

    assert commander.user_map[user]["on_hold"] == "transfer_flag T"
    assert commander.user_hold_event_map[user] is door


class QuietGroup:
    """A group that records the order it was given and parks nobody."""

    def __init__(self) -> None:
        self.ordered_into: list[str] = []

    async def quit_all(self, freezer_path: str) -> None:
        self.ordered_into.append(freezer_path)


async def test_the_quit_orders_every_group_and_commits_the_photo_by_renaming(commander):
    group = QuietGroup()
    commander.group_map["standard"] = group
    commander.record_connection_user("cid-a", "mario")

    await commander.quit()

    assert group.ordered_into == [str(commander.reboot_temp_path)]
    assert not commander.reboot_temp_path.exists()
    assert commander.reboot_data_path.exists()
    saved = FreezeHandler(commander.reboot_data_path).read_commander_register_item()
    assert saved["connection_user_map"] == {"cid-a": "mario"}


async def test_the_saved_rows_are_normalised_for_a_boot_that_adopts_nobody(commander):
    commander.group_map["standard"] = QuietGroup()
    commander.record_connection_user("cid-a", "mario")
    commander.hold_user("mario", "moving")

    await commander.quit()

    saved = FreezeHandler(commander.reboot_data_path).read_commander_register_item()
    row = saved["user_map"]["mario"]
    assert row["frozen"] is True
    assert row["on_hold"] is None


async def test_a_quit_that_dies_before_the_rename_leaves_no_photo(commander, monkeypatch):
    commander.group_map["standard"] = QuietGroup()

    def refuse(*args, **kwargs):
        raise OSError("the disk said no")

    monkeypatch.setattr(FreezeHandler, "write_commander_register_item", refuse)

    with pytest.raises(OSError):
        await commander.quit()

    assert commander.reboot_temp_path.exists()
    assert not commander.reboot_data_path.exists()


def photographed(commander, user="mario", cid="cid-a", ts=None):
    """A photo on disk as a soft quit leaves it: one parcel, and the vertex's item."""
    photo = FreezeHandler(commander.reboot_data_path)
    photo.take_lock(user, WORKER_NAME)
    photo.write_user_register_item(
        user, {"store": "whatever"}, writer=WORKER_NAME, cause="quit", group="standard"
    )
    photo.release_lock(user, WORKER_NAME)
    if ts is not None:
        path = photo.root_path / photo.user_to_userkey(user) / USER_REGISTER_ITEM_NAME
        envelope = pickle.loads(path.read_bytes())
        envelope["header"]["ts"] = ts
        path.write_bytes(pickle.dumps(envelope))
    photo.write_commander_register_item(
        {
            "user_map": {user: dict(commander._new_row(), frozen=True, group="standard")},
            "connection_user_map": {cid: user},
            "page_connection_map": {},
            "global_register": {},
            "quit_ts": 0.0,
        },
        writer="vertex",
        cause="quit",
    )
    return photo


def test_a_boot_with_frozen_registers_becomes_them_and_leaves_the_parcels_where_wakes_look(
    commander,
):
    photographed(commander)

    commander.adopt_frozen_registers()

    assert commander.connection_user_map == {"cid-a": "mario"}
    assert commander.user_is_frozen("mario") is True
    assert not commander.reboot_data_path.exists()
    assert commander.freeze_handler.read_user_register_item("mario") is not None


def test_a_boot_with_no_frozen_registers_wipes_the_working_deposit_and_starts_clean(commander):
    parked_state(commander, "mario")

    commander.adopt_frozen_registers()

    assert commander.user_map == {}
    assert commander.freeze_handler.user_folders == set()


def test_frozen_registers_that_cannot_be_read_boot_clean(commander):
    photo = photographed(commander)
    (photo.root_path / "commander_register_item.pickle").write_bytes(b"not a pickle")

    commander.adopt_frozen_registers()

    assert commander.user_map == {}
    assert not commander.reboot_data_path.exists()
    assert commander.freeze_handler.user_folders == set()


def test_a_reboot_temp_left_by_a_dead_quit_is_never_read(commander):
    FreezeHandler(commander.reboot_temp_path)

    commander.adopt_frozen_registers()

    assert not commander.reboot_temp_path.exists()
    assert commander.user_map == {}


async def test_a_user_past_his_expiry_is_dropped_at_the_boot_not_woken(commander):
    photographed(commander, ts=0.0)
    commander.adopt_frozen_registers()

    await commander.drop_expired_users(now=True)

    assert "mario" not in commander.user_map
    assert commander.freeze_handler.user_folders == set()


async def test_the_cookie_survives_a_quit_and_the_boot_that_follows(commander):
    """The whole point: the same cid still names the same person on the other side."""
    commander.group_map["standard"] = QuietGroup()
    commander.record_connection_user("cid-a", "mario")
    parked_state(commander, "mario")
    commander.mark_user_frozen("mario")
    await commander.quit()

    reborn = SpaCommander(commander.freeze_handler.root_path)
    reborn.adopt_frozen_registers()

    assert reborn.resolve_user("cid-a") == "mario"
    assert reborn.user_is_frozen("mario") is True
