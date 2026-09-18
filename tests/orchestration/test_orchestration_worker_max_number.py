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

"""worker_max_number: how many workers a group's quota is sized for.

A divisor of the size and nothing else — the per-worker memory ceiling becomes
quota / worker_max_number, while the number of living processes stays a
reading, never a setting. It replaces the bridge-era RAM-share-over-workers
derivation with one intuitive count of slots; an explicit
``worker_memory_max_percent`` wins over the derivation (owner decisions,
2026-08-21).
"""

from __future__ import annotations

from typing import Any

import pytest

from kajenn import AsgiConfigBuilder, ConfigurationHandler
from kajenn_orchestra.spa_app import SpaApplication
from kajenn_orchestra.orchestration import GroupHandler, SpaCommander
from kajenn_orchestra.orchestration.group_handler import WORKER_MAX_NUMBER

GIB = 1024 * 1024 * 1024


@pytest.fixture
def commander(tmp_path):
    return SpaCommander(tmp_path / "frozen_users")


def build_group(commander, tmp_path, **kwargs: Any) -> GroupHandler:
    return GroupHandler(
        commander,
        "standard",
        memory_concession_bytes=12 * GIB,
        instance_dir=tmp_path / "i",
        frozen_users_path=tmp_path / "frozen_users",
        entry_module="never.launched",
        **kwargs,
    )


def test_a_group_that_declares_nothing_is_sized_for_six_workers(commander, tmp_path):
    group = build_group(commander, tmp_path)

    assert WORKER_MAX_NUMBER == 6
    assert group.worker_max_number == 6
    assert group.worker_memory_max_percent == 100.0 / 6


def test_worker_max_number_divides_the_quota(commander, tmp_path):
    group = build_group(commander, tmp_path, worker_max_number=4)

    assert group.worker_memory_max_percent == 25.0
    # The ceiling in bytes is the quota split over the declared slots.
    assert group.memory_quota_bytes * group.worker_memory_max_percent / 100.0 == 12 * GIB / 4


def test_an_explicit_worker_share_wins_over_the_derivation(commander, tmp_path):
    group = build_group(
        commander, tmp_path, worker_max_number=4, worker_memory_max_percent=40.0
    )

    assert group.worker_memory_max_percent == 40.0


class WorkerMaxNumberConfig(AsgiConfigBuilder):
    """A real recipe carrying the word on one group and leaving it off the other."""

    def main(self, root: Any) -> None:
        cfg = root.configuration()
        cfg.server(host="127.0.0.1", port=8000)
        front = cfg.applications().application(
            code="shop", mount="", app_class=SpaApplication
        )
        commander = front.orchestration().commander(
            frozen_users_path="/srv/shop/frozen_users",
            instance_dir="/srv/shop/instance",
        )
        groups = commander.groups()
        groups.group(
            name="stable",
            entry_module="kajenn_orchestra.orchestration.worker_entry",
            worker_max_number=4,
        )
        groups.group(
            name="canary",
            entry_module="kajenn_orchestra.orchestration.worker_entry",
        )


def test_the_word_travels_from_the_recipe_to_the_group(tmp_path):
    groups = ConfigurationHandler(WorkerMaxNumberConfig).group_kwargs("shop")

    assert groups["stable"]["worker_max_number"] == 4
    assert "worker_max_number" not in groups["canary"]

    commander = SpaCommander(tmp_path / "frozen_users")
    sized = GroupHandler(
        commander,
        "stable",
        memory_concession_bytes=12 * GIB,
        **{**groups["stable"], "frozen_users_path": tmp_path / "frozen_users"},
    )
    assert sized.worker_memory_max_percent == 25.0
