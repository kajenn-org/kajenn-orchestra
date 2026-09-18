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

"""PSS is the worker memory currency; RSS is its explicit portable fallback."""

from __future__ import annotations

from collections import namedtuple

import psutil
import pytest

from kajenn_orchestra.orchestration import GroupHandler, SpaCommander, SpaWorker

CONCESSION = 1_000_000

LinuxFullInfo = namedtuple("LinuxFullInfo", "rss vms uss pss swap")
DarwinFullInfo = namedtuple("DarwinFullInfo", "rss vms pfaults pageins uss")
MemInfo = namedtuple("MemInfo", "rss vms")


@pytest.fixture
def worker() -> SpaWorker:
    """An uninitialised worker is enough: the gauges read only psutil."""
    return object.__new__(SpaWorker)


def full_info(monkeypatch, value) -> None:
    """Make psutil's full memory info of this process answer this value."""
    monkeypatch.setattr(psutil.Process, "memory_full_info", lambda self: value)


def refused(*args, **kwargs):
    raise psutil.AccessDenied(pid=1)


def group(tmp_path) -> GroupHandler:
    commander = SpaCommander(tmp_path / "frozen")
    return GroupHandler(
        commander,
        "pool",
        memory_concession_bytes=CONCESSION,
        worker_memory_max_percent=100.0,
        instance_dir=tmp_path / "instance",
        frozen_users_path=tmp_path / "frozen",
        entry_module="never.launched",
    )


def test_pss_is_the_pss_field_of_psutil_full_memory_info(worker, monkeypatch):
    full_info(monkeypatch, LinuxFullInfo(900 * 1024, 5_000_000, 400 * 1024, 321 * 1024, 0))

    assert worker.pss_bytes == 321 * 1024


def test_a_platform_without_a_pss_field_reports_no_pss(worker, monkeypatch):
    full_info(monkeypatch, DarwinFullInfo(900 * 1024, 5_000_000, 0, 0, 400 * 1024))

    assert worker.pss_bytes is None


def test_a_refused_full_memory_reading_reports_no_pss(worker, monkeypatch):
    monkeypatch.setattr(psutil.Process, "memory_full_info", refused)

    assert worker.pss_bytes is None


def test_rss_is_the_rss_field_of_psutil_memory_info(worker, monkeypatch):
    monkeypatch.setattr(psutil.Process, "memory_info", lambda self: MemInfo(900 * 1024, 5_000_000))

    assert worker.rss_bytes == 900 * 1024


def test_a_refused_memory_reading_reports_no_rss(worker, monkeypatch):
    monkeypatch.setattr(psutil.Process, "memory_info", refused)

    assert worker.rss_bytes is None


def test_valid_pss_wins_over_a_much_larger_rss(tmp_path):
    subject = group(tmp_path)
    photo = {"rss_bytes": 900_000, "pss_bytes": 200_000}

    assert subject.get_memory_accounting(photo) == (200_000.0, "pss")
    assert subject.get_memory_occupancy_percent(photo) == 20.0


@pytest.mark.parametrize(
    "bad_pss", [None, -1, float("nan"), float("inf"), 10**1000, True, "3"]
)
def test_invalid_or_missing_pss_falls_back_to_rss(tmp_path, bad_pss):
    subject = group(tmp_path)
    photo = {"rss_bytes": 400_000, "pss_bytes": bad_pss}

    assert subject.get_memory_accounting(photo) == (400_000.0, "rss_fallback")
    assert subject.get_memory_occupancy_percent(photo) == 40.0


def test_a_photo_with_no_valid_memory_gauge_is_unmeasured(tmp_path):
    subject = group(tmp_path)

    assert subject.get_memory_accounting({"pss_bytes": -1, "rss_bytes": "unknown"}) == (
        None,
        "unmeasured",
    )
    assert subject.get_memory_occupancy_percent({"pss_bytes": -1, "rss_bytes": "unknown"}) == 0.0
