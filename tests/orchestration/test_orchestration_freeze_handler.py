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

"""FreezeHandler tests: the semaphore, the two item kinds, the empty folder.

Everything here runs against a real directory in ``tmp_path``: the deposit is
the one place that talks to the filesystem directly, so a fake filesystem would
assert nothing. The protocol under test is always the same round — announce,
take the lock, operate, release — and what the tests pin down is what happens
at its edges: a second holder finding the lock taken, a folder left with the
lock alone, a header travelling with a payload nobody reads it for.
"""

from __future__ import annotations

import os
import time

import pytest

from kajenn_orchestra.orchestration import FreezeHandler


@pytest.fixture
def deposit(tmp_path):
    return FreezeHandler(tmp_path / "freezed_users")


def test_root_is_created_private(tmp_path):
    root = tmp_path / "deposit" / "freezed_users"
    FreezeHandler(root)
    assert root.is_dir()
    assert os.stat(root).st_mode & 0o777 == 0o700


def test_lock_is_mutually_exclusive(deposit):
    assert deposit.take_lock("mario", "standard_0001") is True
    assert deposit.take_lock("mario", "standard_0002") is False
    assert deposit.lock_holder("mario") == "standard_0001"

    deposit.release_lock("mario", "standard_0001")
    assert deposit.lock_holder("mario") is None
    assert deposit.take_lock("mario", "standard_0002") is True


def test_release_by_a_holder_that_does_not_hold_is_an_error(deposit):
    deposit.take_lock("mario", "standard_0001")
    with pytest.raises(RuntimeError, match="standard_0002"):
        deposit.release_lock("mario", "standard_0002")
    assert deposit.lock_holder("mario") == "standard_0001"


def test_two_writers_share_one_folder_one_at_a_time(deposit):
    """The whole freeze writes the user store, a login writes one connection."""
    assert deposit.take_lock("mario", "standard_0001") is True
    deposit.write_user_register_item(
        "mario", {"store": "whole"}, writer="standard_0001", cause="freeze", group="standard"
    )
    assert deposit.take_lock("mario", "standard_0002") is False
    deposit.release_lock("mario", "standard_0001")

    assert deposit.take_lock("mario", "standard_0002") is True
    deposit.write_connection_register_item(
        "mario", "cid-a", {"pages": ["p1"]}, writer="standard_0002", cause="login", group="standard"
    )
    deposit.release_lock("mario", "standard_0002")

    assert deposit.read_user_register_item("mario") == {"store": "whole"}
    assert deposit.read_connection_register_item("mario", "cid-a") == {"pages": ["p1"]}


def test_reading_what_was_never_written_gives_nothing(deposit):
    assert deposit.read_user_register_item("mario") is None
    assert deposit.read_connection_register_item("mario", "cid-a") is None
    assert deposit.get_item_header("mario") is None


def test_the_header_travels_with_the_payload(deposit):
    before = time.time()
    deposit.take_lock("mario", "standard_0001")
    deposit.write_connection_register_item(
        "mario", "cid-a", {"pages": []}, writer="standard_0001", cause="freeze", group="standard"
    )
    deposit.release_lock("mario", "standard_0001")

    header = deposit.get_item_header("mario", "cid-a")
    assert header["writer"] == "standard_0001"
    assert header["cause"] == "freeze"
    assert header["group"] == "standard"
    assert header["ts"] >= before


def test_the_release_takes_away_the_folder_left_with_the_lock_alone(deposit):
    deposit.take_lock("mario", "standard_0001")
    deposit.write_connection_register_item(
        "mario", "cid-a", {"pages": []}, writer="standard_0001", cause="freeze", group="standard"
    )
    deposit.write_user_register_item(
        "mario", {"store": {}}, writer="standard_0001", cause="freeze", group="standard"
    )
    assert deposit.user_folders == {deposit.user_to_userkey("mario")}

    deposit.drop_connection_register_item("mario", "cid-a")
    deposit.drop_user_register_item("mario")
    assert deposit.lock_holder("mario") == "standard_0001"
    assert deposit.user_folders == {deposit.user_to_userkey("mario")}

    deposit.release_lock("mario", "standard_0001")
    assert deposit.user_folders == set()


def test_the_release_leaves_a_folder_that_still_has_items(deposit):
    deposit.take_lock("mario", "standard_0001")
    deposit.write_user_register_item(
        "mario", {"store": {}}, writer="standard_0001", cause="freeze", group="standard"
    )
    deposit.release_lock("mario", "standard_0001")

    assert deposit.user_folders == {deposit.user_to_userkey("mario")}
    assert deposit.read_user_register_item("mario") == {"store": {}}


def test_the_whole_folder_goes_with_its_user(deposit):
    deposit.take_lock("mario", "standard_0001")
    deposit.write_user_register_item(
        "mario", {"store": {}}, writer="standard_0001", cause="freeze", group="standard"
    )
    deposit.write_connection_register_item(
        "mario", "cid-a", {"pages": []}, writer="standard_0001", cause="freeze", group="standard"
    )

    deposit.drop_user_folder("mario")
    assert deposit.user_folders == set()
    assert deposit.lock_holder("mario") is None


def test_dropping_a_folder_nobody_wrote_is_no_work(deposit):
    deposit.drop_user_folder("mario")
    assert deposit.user_folders == set()


def test_dropping_items_nobody_wrote_is_no_work(deposit):
    deposit.take_lock("mario", "standard_0001")

    deposit.drop_user_register_item("mario")
    deposit.drop_connection_register_item("mario", "cid-a")

    assert deposit.lock_holder("mario") == "standard_0001"
    deposit.release_lock("mario", "standard_0001")
    assert deposit.user_folders == set()


def test_the_folders_of_every_user_come_as_one_set(deposit):
    for user in ("mario", "guest_abc", "anna@example.com"):
        deposit.take_lock(user, "standard_0001")
        deposit.write_user_register_item(
            user, {"store": {}}, writer="standard_0001", cause="freeze", group="standard"
        )
        deposit.release_lock(user, "standard_0001")

    assert deposit.user_folders == {
        deposit.user_to_userkey(user) for user in ("mario", "guest_abc", "anna@example.com")
    }


def test_the_key_goes_one_way_only(deposit):
    assert deposit.user_to_userkey("anna@example.com") == "anna%40example.com"
    assert not hasattr(deposit, "userkey_to_user")


def test_an_identity_carrying_separators_stays_in_its_folder(deposit):
    hostile = "../mario/../../etc"
    deposit.take_lock(hostile, "standard_0001")
    deposit.write_user_register_item(
        hostile, {"store": {}}, writer="standard_0001", cause="freeze", group="standard"
    )
    deposit.release_lock(hostile, "standard_0001")

    assert deposit.user_folders == {deposit.user_to_userkey(hostile)}
    assert "/" not in deposit.user_to_userkey(hostile)


def test_two_connections_of_one_user_are_two_files(deposit):
    deposit.take_lock("mario", "standard_0001")
    deposit.write_connection_register_item(
        "mario", "cid-a", {"pages": ["a"]}, writer="standard_0001", cause="freeze", group="standard"
    )
    deposit.write_connection_register_item(
        "mario", "cid-b", {"pages": ["b"]}, writer="standard_0001", cause="freeze", group="standard"
    )
    deposit.release_lock("mario", "standard_0001")

    assert deposit.read_connection_register_item("mario", "cid-a") == {"pages": ["a"]}
    assert deposit.read_connection_register_item("mario", "cid-b") == {"pages": ["b"]}

    deposit.take_lock("mario", "standard_0001")
    deposit.drop_connection_register_item("mario", "cid-a")
    deposit.release_lock("mario", "standard_0001")

    assert deposit.read_connection_register_item("mario", "cid-a") is None
    assert deposit.read_connection_register_item("mario", "cid-b") == {"pages": ["b"]}
