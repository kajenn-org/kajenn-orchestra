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

"""The global store's own classes, tested bare.

The store lives ONLY on the commander — one dictionary behind one FIFO lock —
and the whole surface (get/set/delete on the lane, the turn whose grant carries
the selected value and whose release publishes it whole) is pinned end to end by
the orchestration contract tests (``tests/spa/orchestration/test_contract_global_store_dict.py``,
``test_contract_phase10_global_store.py``, ``test_orchestration_store_get.py``).
What belongs here is the lock itself: who holds the turn, and what a release
for a turn no longer in force must not do.
"""

from __future__ import annotations

from kajenn_orchestra.global_store import GlobalStoreLock


async def test_the_turn_records_its_request_worker_and_key() -> None:
    lock = GlobalStoreLock()
    await lock.acquire("standard_0001", "r1", "config")

    assert lock.holds("r1") and lock.held_by("standard_0001") and lock.holder_key == "config"
    assert not lock.holds("r2") and not lock.held_by("standard_0002")

    lock.release()
    assert lock.holder is None and lock.holder_worker is None and lock.holder_key is None
    assert not lock.lock.locked()


async def test_a_whole_store_turn_has_no_key() -> None:
    lock = GlobalStoreLock()
    await lock.acquire("standard_0001", "r1")

    assert lock.holder_key is None
    lock.release()


async def test_a_release_for_a_turn_no_longer_in_force_is_told_apart() -> None:
    """The commander asks ``holds`` before it touches anything: a stale id says no."""
    lock = GlobalStoreLock()
    await lock.acquire("standard_0001", "current")

    assert lock.holds("stale") is False
    assert lock.holds("current") is True
    lock.release()
