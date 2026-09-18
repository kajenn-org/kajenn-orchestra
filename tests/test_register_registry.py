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

"""RegisterRegistry tests: generic registers, the extension seam, lifecycle."""

from __future__ import annotations

from typing import Any

import pytest

from kajenn_orchestra import Register, RegisterRegistry
from kajenn_orchestra.orchestration import FreezeHandler, SpaWorker


def test_generic_registers_exist_with_ratified_indexes() -> None:
    registry = RegisterRegistry()
    assert isinstance(registry.user_items, Register)
    assert registry.user_items.name == "user_items"
    assert registry.user_items.index_attrs == ()
    assert registry.connection_items.name == "connection_items"
    assert registry.connection_items.index_attrs == ()
    assert registry.page_items.name == "page_items"
    assert registry.page_items.index_attrs == (
        "connection_id",
        "root_page_id",
    )


def test_generic_registers_are_stable_references() -> None:
    registry = RegisterRegistry()
    registry.user_items.create("alice")
    # Born as the registry's row class: the reserved id, plus the row's own lock.
    alice = registry.user_items.get("alice")
    assert alice["register_item_id"] == "alice"
    assert set(alice) == {"register_item_id", "item_lock"}
    assert len(registry.user_items) == 1


def test_new_register_returns_a_hosted_working_register() -> None:
    registry = RegisterRegistry()
    connections = registry.new_register("connections", index_attrs=("user", "connection_id"))
    assert isinstance(connections, Register)
    assert connections.name == "connections"
    connections.create("c1", user="alice", connection_id="s1")
    assert connections.keys_by("user", "alice") == ["c1"]
    registry.add_index("connections", "root_page_id")
    assert connections.index_attrs == ("user", "connection_id", "root_page_id")


def test_new_register_duplicate_name_raises_value_error() -> None:
    registry = RegisterRegistry()
    registry.new_register("connections")
    with pytest.raises(ValueError, match="connections"):
        registry.new_register("connections")


def test_new_register_cannot_shadow_a_generic_register() -> None:
    registry = RegisterRegistry()
    with pytest.raises(ValueError, match="page_items"):
        registry.new_register("page_items")


def test_add_index_on_pages_indexes_a_passthrough_field() -> None:
    registry = RegisterRegistry()
    registry.page_items.create("p1", user="alice", connection_id="c1")
    registry.page_items.create("p2", user="bob", connection_id="c2")
    registry.add_index("page_items", "connection_id")
    assert registry.page_items.keys_by("connection_id", "c1") == ["p1"]
    assert registry.page_items.keys_by("connection_id", "c2") == ["p2"]


def test_add_index_unknown_register_raises_key_error() -> None:
    registry = RegisterRegistry()
    with pytest.raises(KeyError):
        registry.add_index("connections", "user")


def test_new_user_duplicate_raises_value_error() -> None:
    registry = RegisterRegistry()
    registry.new_user("alice")
    with pytest.raises(ValueError, match="alice"):
        registry.new_user("alice")


def test_drop_page_cascade_destroys_an_explicit_user_entry() -> None:
    registry = RegisterRegistry()
    registry.new_user("alice", role="admin")
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.drop_page("p1")
    assert "alice" not in registry.user_items


def test_new_page_creates_the_user_entry_when_unseen() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    assert "alice" in registry.user_items
    registry.new_page("p2", user="alice", connection_id="s1")
    assert len(registry.user_items) == 1


def test_new_page_root_defaults_and_tree_lookup() -> None:
    registry = RegisterRegistry()
    root_a = registry.new_page("a", user="alice", connection_id="s1")
    assert root_a["root_page_id"] == "a"
    assert root_a["parent_page_id"] is None
    registry.new_page("a1", user="alice", connection_id="s1", root_page_id="a", parent_page_id="a")
    registry.new_page("b", user="alice", connection_id="s1")
    assert sorted(registry.page_items.keys_by("root_page_id", "a")) == ["a", "a1"]
    assert registry.page_items.keys_by("root_page_id", "b") == ["b"]
    assert sorted(registry.page_items.keys_by("connection_id", "s1")) == ["a", "a1", "b"]


def test_new_page_child_without_root_raises_value_error() -> None:
    registry = RegisterRegistry()
    registry.new_page("a", user="alice", connection_id="s1")
    with pytest.raises(ValueError, match="a1"):
        registry.new_page("a1", user="alice", connection_id="s1", parent_page_id="a")


def test_new_page_born_fields_and_passthrough() -> None:
    registry = RegisterRegistry()
    page = registry.new_page("p1", user="alice", connection_id="c1")
    assert page["avatar_key"] == "root"
    assert page["data"] is None
    assert page["connection_id"] == "c1"
    other = registry.new_page("p2", user="bob", connection_id="s2")
    assert other["item_lock"] is not page["item_lock"]
    registry.add_index("page_items", "connection_id")
    assert registry.page_items.keys_by("connection_id", "c1") == ["p1"]


def test_new_page_avatar_key_is_explicit_when_given() -> None:
    registry = RegisterRegistry()
    page = registry.new_page("p1", user="alice", connection_id="s1", avatar_key="admin")
    assert page["avatar_key"] == "admin"


def test_update_page_merges_and_reindexes() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    page = registry.update_page("p1", connection_id="s2", label="home")
    assert page["label"] == "home"
    assert registry.page_items.keys_by("connection_id", "s2") == ["p1"]
    assert registry.page_items.keys_by("connection_id", "s1") == []


def test_drop_page_cascades_only_on_the_last_page() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s1")
    dropped = registry.drop_page("p1")
    assert dropped["register_item_id"] == "p1"
    assert "alice" in registry.user_items
    registry.drop_page("p2")
    assert "alice" not in registry.user_items
    assert len(registry.page_items) == 0


def test_drop_user_removes_every_page_of_that_user() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s1")
    registry.new_page("p3", user="bob", connection_id="s2")
    registry.drop_user("alice")
    assert "alice" not in registry.user_items
    assert "p1" not in registry.page_items and "p2" not in registry.page_items
    assert "p3" in registry.page_items
    assert "bob" in registry.user_items


def test_drop_user_unknown_raises_key_error() -> None:
    registry = RegisterRegistry()
    with pytest.raises(KeyError):
        registry.drop_user("alice")


# ----------------------------------------------------------------------
# The chain page -> connection -> user
# ----------------------------------------------------------------------


def test_new_page_builds_the_whole_chain_bottom_up() -> None:
    registry = RegisterRegistry()
    page = registry.new_page("p1", user="s1", connection_id="s1")
    assert page["connection_id"] == "s1"
    connection = registry.connection_items.get("s1")
    assert connection["user"] == "s1"
    assert registry.user_items.get("s1")["store"] is not None
    assert registry.user_items.get("s1")["connections"] == {"s1"}
    assert connection["pages"] == {"p1"}


def test_new_connection_is_born_guest_with_its_own_user_entry() -> None:
    registry = RegisterRegistry()
    connection = registry.new_connection("s1")
    assert connection["user"] == "guest_s1"
    assert "guest_s1" in registry.user_items
    assert registry.user_items.get("guest_s1")["store"] is not None


def test_new_connection_is_born_with_its_own_live_store() -> None:
    registry = RegisterRegistry()
    connection = registry.new_connection("s1")
    assert connection["store"] is not None
    assert connection["store"] is not registry.user_items.get("guest_s1")["store"]


def test_new_connection_honours_a_supplied_store() -> None:
    registry = RegisterRegistry()
    carried = registry.new_store()
    carried["arrived.here"] = "yes"
    connection = registry.new_connection("s1", store=carried)
    assert connection["store"] is carried
    assert connection["store"]["arrived.here"] == "yes"


def test_new_connection_under_a_named_user_reuses_the_entry() -> None:
    registry = RegisterRegistry()
    registry.new_user("alice", role="admin")
    registry.new_connection("s1", user="alice")
    assert registry.user_items.get("alice")["connections"] == {"s1"}
    assert registry.user_items.get("alice")["role"] == "admin"


def test_two_pages_of_one_connection_share_it() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s1")
    assert len(registry.connection_items) == 1
    assert registry.connection_items.get("s1")["pages"] == {"p1", "p2"}


def test_two_connections_of_one_user_share_the_user_entry() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s2")
    assert len(registry.user_items) == 1
    assert registry.user_items.get("alice")["connections"] == {"s1", "s2"}


def test_drop_page_climbs_the_chain_only_on_the_last_page() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s1")
    registry.drop_page("p1")
    assert "s1" in registry.connection_items
    assert "alice" in registry.user_items
    registry.drop_page("p2")
    assert "s1" not in registry.connection_items
    assert "alice" not in registry.user_items


def test_drop_page_of_one_connection_leaves_the_sibling_connection() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s2")
    registry.drop_page("p1")
    assert "s1" not in registry.connection_items
    assert "s2" in registry.connection_items
    assert "alice" in registry.user_items


def test_drop_connection_takes_its_pages_and_the_last_user() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s1")
    connection = registry.drop_connection("s1")
    assert connection["user"] == "alice"
    assert len(registry.page_items) == 0
    assert "alice" not in registry.user_items


def test_drop_connection_unknown_raises_key_error() -> None:
    registry = RegisterRegistry()
    with pytest.raises(KeyError, match="ghost"):
        registry.drop_connection("ghost")


def test_drop_connection_never_climbs_back_down_and_up_again() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s2")
    registry.drop_connection("s1")
    assert "alice" in registry.user_items
    assert registry.user_items.get("alice")["connections"] == {"s2"}
    assert registry.connection_items.get("s2")["pages"] == {"p2"}


def test_drop_user_takes_every_connection_and_page() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s2")
    registry.new_page("p3", user="bob", connection_id="s3")
    registry.drop_user("alice")
    assert "alice" not in registry.user_items
    assert "s1" not in registry.connection_items and "s2" not in registry.connection_items
    assert "p1" not in registry.page_items and "p2" not in registry.page_items
    assert registry.user_items.get("bob")["connections"] == {"s3"}
    assert registry.connection_items.get("s3")["pages"] == {"p3"}


def test_drop_page_without_cascade_leaves_the_chain_standing() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.drop_page("p1", cascade=False)
    assert "s1" in registry.connection_items
    assert "alice" in registry.user_items


def test_drop_connection_without_cascade_leaves_the_user_standing() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.drop_connection("s1", cascade=False)
    assert "alice" in registry.user_items
    assert len(registry.page_items) == 0


def test_drop_connection_detaches_every_page_it_takes() -> None:
    class SeamRegistry(RegisterRegistry):
        def __init__(self) -> None:
            super().__init__()
            self.detached: list[str] = []

        def detach_page(self, page: dict[str, Any]) -> None:
            self.detached.append(page["register_item_id"])

    registry = SeamRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s1")
    registry.drop_connection("s1")
    assert sorted(registry.detached) == ["p1", "p2"]


# ----------------------------------------------------------------------
# The tree lives in the items: both directions agree after every mutator
# ----------------------------------------------------------------------


def assert_tree(registry: RegisterRegistry, tree: dict[str, dict[str, set[str]]]) -> None:
    """Check the whole registry against ``{user: {connection: {page, ...}}}``.

    Both directions are read: the downward edge sets carried by the items and
    the upward parent keys carried by their children, plus the three counts, so
    a row the tree does not name is a failure too.
    """
    assert len(registry.user_items) == len(tree)
    assert len(registry.connection_items) == sum(len(c) for c in tree.values())
    assert len(registry.page_items) == sum(len(p) for c in tree.values() for p in c.values())
    for user, connections in tree.items():
        assert registry.user_items.get(user)["connections"] == set(connections)
        for connection_id, pages in connections.items():
            connection = registry.connection_items.get(connection_id)
            assert connection["user"] == user
            assert connection["pages"] == pages
            for page_id in pages:
                assert registry.page_items.get(page_id)["connection_id"] == connection_id
                assert registry.page_user(page_id) == user


def test_the_two_directions_agree_after_every_creation() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s1")
    registry.new_page("p3", user="alice", connection_id="s2")
    registry.new_connection("s3")
    assert_tree(
        registry,
        {"alice": {"s1": {"p1", "p2"}, "s2": {"p3"}}, "guest_s3": {"s3": set()}},
    )


def test_the_two_directions_agree_after_a_drop_from_the_bottom() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s1")
    registry.new_page("p3", user="alice", connection_id="s2")
    registry.drop_page("p1")
    assert_tree(registry, {"alice": {"s1": {"p2"}, "s2": {"p3"}}})
    registry.drop_page("p2")
    assert_tree(registry, {"alice": {"s2": {"p3"}}})


def test_the_two_directions_agree_after_a_drop_from_the_top() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    registry.new_page("p2", user="alice", connection_id="s2")
    registry.new_page("p3", user="bob", connection_id="s3")
    registry.drop_connection("s1")
    assert_tree(registry, {"alice": {"s2": {"p2"}}, "bob": {"s3": {"p3"}}})
    registry.drop_user("alice")
    assert_tree(registry, {"bob": {"s3": {"p3"}}})


def test_the_login_moves_the_connection_between_the_two_users_sets() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="sess-1", connection_id="sess-1")
    registry.new_page("p2", user="sess-2", connection_id="sess-2")
    registry.change_connection_user("sess-1", "alice")
    assert_tree(
        registry,
        {"alice": {"sess-1": {"p1"}}, "sess-2": {"sess-2": {"p2"}}},
    )
    registry.change_connection_user("sess-2", "alice")
    assert_tree(registry, {"alice": {"sess-1": {"p1"}, "sess-2": {"p2"}}})


def test_a_login_target_carrying_the_guest_prefix_is_refused() -> None:
    """The prefix is reserved: nobody logs in as a guest, and the row stays put."""
    registry = RegisterRegistry()
    registry.new_connection("s1")
    with pytest.raises(ValueError, match="reserved"):
        registry.change_connection_user("s1", "guest_mario")
    assert registry.connection_items.get("s1")["user"] == "guest_s1"


def test_page_user_walks_up_the_chain() -> None:
    registry = RegisterRegistry()
    registry.new_page("p1", user="alice", connection_id="s1")
    assert registry.page_user("p1") == "alice"
    registry.change_connection_user("s1", "bob")
    assert registry.page_user("p1") == "bob"


def test_page_user_unknown_raises_key_error() -> None:
    registry = RegisterRegistry()
    with pytest.raises(KeyError, match="nope"):
        registry.page_user("nope")


def test_the_factory_seams_own_every_store_and_capture() -> None:
    """A subclass owning the three seams owns every live object a row is born with."""

    class SeamRegistry(RegisterRegistry):
        def __init__(self) -> None:
            super().__init__()
            self.stores: list[Any] = []
            self.subscribed_pages: list[str] = []

        def new_store(self) -> Any:
            store = super().new_store()
            self.stores.append(store)
            return store

        def subscribe_page_store(self, page: dict[str, Any]) -> None:
            super().subscribe_page_store(page)
            self.subscribed_pages.append(page["register_item_id"])

    registry = SeamRegistry()
    registry.new_user("alice")
    page = registry.new_page("p1", user="sess-1", connection_id="sess-1")
    registry.change_connection_user("sess-1", "alice")

    # Every store born through the seam: alice's, the guest's, the page's.
    assert registry.user_items.get("alice")["store"] in registry.stores
    assert page["store"] in registry.stores
    # The page's own store goes through its own seam, once.
    assert registry.subscribed_pages == ["p1"]


def test_connection_parcel_leaves_the_row_locks_behind(tmp_path) -> None:
    """A lock is neither pickled nor deep-copied: the parcel must not carry it."""
    worker = SpaWorker(
        "standard_0001",
        freeze_handler=FreezeHandler(tmp_path / "frozen_users"),
        deposit_lock_retry_interval=0.01,
    )
    worker.open_request_slot()
    worker.add_page("p1", "cid-a")

    parcel = worker._connection_parcel("cid-a")

    assert "item_lock" not in parcel["connection"]
    assert [page for page in parcel["pages"].values() if "item_lock" in page] == []
