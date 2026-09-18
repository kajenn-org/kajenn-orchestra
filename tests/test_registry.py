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

"""Register tests: create/get/drop/update roundtrip, index coherence, errors."""

from __future__ import annotations

import pytest

from kajenn_orchestra import Register


def test_create_get_roundtrip() -> None:
    register = Register("users")
    item = register.create("alice", role="admin")
    assert item == {"register_item_id": "alice", "role": "admin"}
    assert register.get("alice") == {"register_item_id": "alice", "role": "admin"}
    assert "alice" in register
    assert len(register) == 1


def test_get_missing_returns_none() -> None:
    register = Register("users")
    assert register.get("nobody") is None


def test_create_seeds_register_item_id() -> None:
    register = Register("users")
    item = register.create("alice")
    assert item == {"register_item_id": "alice"}


def test_create_reserved_register_item_id_raises_value_error() -> None:
    register = Register("users")
    with pytest.raises(ValueError, match="register_item_id"):
        register.create("alice", register_item_id="other")


def test_update_reserved_register_item_id_raises_value_error() -> None:
    register = Register("users")
    register.create("alice")
    with pytest.raises(ValueError, match="register_item_id"):
        register.update("alice", register_item_id="other")


def test_create_duplicate_key_raises_value_error() -> None:
    register = Register("users")
    register.create("alice", role="admin")
    with pytest.raises(ValueError):
        register.create("alice", role="viewer")


def test_update_missing_key_raises_key_error() -> None:
    register = Register("users")
    with pytest.raises(KeyError):
        register.update("nobody", role="admin")


def test_drop_missing_key_raises_key_error() -> None:
    register = Register("users")
    with pytest.raises(KeyError):
        register.drop("nobody")


def test_drop_returns_removed_item() -> None:
    register = Register("users")
    register.create("alice", role="admin")
    dropped = register.drop("alice")
    assert dropped == {"register_item_id": "alice", "role": "admin"}
    assert "alice" not in register
    assert len(register) == 0


def test_keys_by_non_indexed_attr_raises_key_error() -> None:
    register = Register("users")
    register.create("alice", role="admin")
    with pytest.raises(KeyError):
        register.keys_by("role", "admin")


def test_keys_by_reflects_create() -> None:
    register = Register("pages", index_attrs=("user",))
    register.create("p1", user="alice")
    register.create("p2", user="alice")
    register.create("p3", user="bob")
    assert set(register.keys_by("user", "alice")) == {"p1", "p2"}
    assert register.keys_by("user", "bob") == ["p3"]
    assert register.keys_by("user", "carol") == []


def test_keys_by_reflects_update_that_changes_indexed_value() -> None:
    register = Register("pages", index_attrs=("user",))
    register.create("p1", user="alice")
    register.update("p1", user="bob")
    assert register.keys_by("user", "alice") == []
    assert register.keys_by("user", "bob") == ["p1"]


def test_update_unchanged_indexed_value_does_not_grow_bucket() -> None:
    register = Register("pages", index_attrs=("user",))
    register.create("p1", user="alice")
    register.update("p1", user="alice", extra="x")
    assert register.keys_by("user", "alice") == ["p1"]
    assert register.get("p1") == {"register_item_id": "p1", "user": "alice", "extra": "x"}


def test_update_of_only_non_indexed_fields_leaves_indexes_alone() -> None:
    register = Register("pages", index_attrs=("user",))
    register.create("p1", user="alice")
    item = register.update("p1", label="home")
    assert item["label"] == "home"
    assert register.keys_by("user", "alice") == ["p1"]


def test_item_without_an_indexed_field_indexes_under_none() -> None:
    register = Register("pages", index_attrs=("user",))
    register.create("p1")
    assert register.keys_by("user", None) == ["p1"]


def test_keys_by_reflects_drop() -> None:
    register = Register("pages", index_attrs=("user",))
    register.create("p1", user="alice")
    register.drop("p1")
    assert register.keys_by("user", "alice") == []


def test_drop_and_update_prune_emptied_index_buckets() -> None:
    register = Register("pages", index_attrs=("user",))
    register.create("p1", user="alice")
    register.create("p2", user="bob")
    register.update("p1", user="carol")
    register.drop("p2")
    assert set(register._indexes["user"]) == {"carol"}


def test_drop_keeps_bucket_shared_with_surviving_key() -> None:
    register = Register("pages", index_attrs=("user",))
    register.create("p1", user="alice")
    register.create("p2", user="alice")
    register.drop("p1")
    assert register.keys_by("user", "alice") == ["p2"]


def test_add_index_on_populated_register_indexes_existing_rows() -> None:
    register = Register("pages")
    register.create("p1", connection_id="c1")
    register.create("p2", connection_id="c2")
    register.add_index("connection_id")
    assert register.keys_by("connection_id", "c1") == ["p1"]
    assert register.keys_by("connection_id", "c2") == ["p2"]


def test_add_index_keeps_existing_indexes_working() -> None:
    register = Register("pages", index_attrs=("user",))
    register.create("p1", user="alice", connection_id="c1")
    register.add_index("connection_id")
    assert register.index_attrs == ("user", "connection_id")
    assert register.keys_by("user", "alice") == ["p1"]
    assert register.keys_by("connection_id", "c1") == ["p1"]


def test_add_index_is_idempotent_when_already_indexed() -> None:
    register = Register("pages", index_attrs=("user",))
    register.create("p1", user="alice")
    register.add_index("user")
    assert register.keys_by("user", "alice") == ["p1"]
    assert register.index_attrs == ("user",)


def test_reindex_rebuilds_indexes_from_items() -> None:
    register = Register("pages", index_attrs=("user",))
    register.create("p1", user="alice")
    register.reindex()
    assert register.keys_by("user", "alice") == ["p1"]


def test_name_and_index_attrs_properties() -> None:
    register = Register("pages", index_attrs=("user", "connection_id"))
    assert register.name == "pages"
    assert register.index_attrs == ("user", "connection_id")
