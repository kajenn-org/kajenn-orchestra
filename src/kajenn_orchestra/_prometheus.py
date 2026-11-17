# Copyright 2026 Softwell S.r.l.
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

"""The Prometheus families of a SPA pool, read from the commander at call time.

Private to ``kajenn_orchestra``: ``SpaApplication.prometheus_metrics`` writes
``pool_text(commander)`` before the families of its base.

Nothing here counts: every value is read when the text is written, from what
the commander already holds.

- Per group, over the group's living workers (``GroupHandler.living_workers``):
  how many there are, and the sum and the largest value of what their last
  photo (``WorkerHandler.worker_snapshot``) and the commander's CPU reading
  (``WorkerHandler.get_cpu_temperature_percent``, None when stale) carry. A living worker that has
  sent no photo yet counts as a worker and adds nothing. A largest value with
  no worker to read it from has no sample; PSS and CPU, which a worker may not
  have (PSS off Linux, CPU before its second reading), have no sum sample
  either when no living worker of the group has them.
- There is no ``worker`` label: worker names are never reused, so a label per
  worker would grow the series with every worker born. The per-worker detail
  stays in the JSON monitor.
- The served calls and the service seconds of a worker are cumulative over the
  users it hosts; their sums are gauges, since they drop when a user leaves or
  a worker closes.
- Per group, the memory quota and the concession it is a share of.
- For the pool: users, connections and pages the commander knows
  (``user_map``, ``connection_user_map``, ``page_connection_map``; a
  connection stays in its map until its user is dropped), and the
  commander's own counts (``counters``) as one counter family by ``event``,
  the known events at 0 before their first count.

The text is written with ``generate_latest`` on a ``CollectorRegistry``
created for each call, never the module-level ``REGISTRY``. The families are
built without a creation time, so the text carries no ``_created`` series.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, NamedTuple

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.metrics_core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

if TYPE_CHECKING:
    from .orchestration import SpaCommander
    from .orchestration.worker_handler import WorkerHandler

__all__: list[str] = []

PREFIX = "kajenn_orchestra_"
"""The prefix of every family this package writes."""

COMMANDER_EVENTS = ("frozen_users_discarded", "orphan_folders_discarded", "requests_refused")
"""The keys ``SpaCommander`` counts in ``counters``, written at 0 before the first.

``SpaCommander`` holds no list of them: each key is written where it is counted.
A key counted that is not listed here is still written, from the first count.
"""


def photo_value(worker_handler: WorkerHandler, key: str) -> float | None:
    """One scalar of the worker's last photo; None without a photo, or when it is None there."""
    photo = worker_handler.worker_snapshot
    if photo is None:
        return None
    value: float | None = photo[key]
    return value


def photo_users_total(worker_handler: WorkerHandler, key: str) -> float | None:
    """One service counter summed over the users of the worker's last photo; None without a photo."""
    photo = worker_handler.worker_snapshot
    if photo is None:
        return None
    total: float = sum(row["item"][key] for row in photo["users"].values())
    return total


def cpu_ratio(worker_handler: WorkerHandler) -> float | None:
    """The smoothed CPU share the commander measured, as a ratio of one core.

    None before the first reading and when the last one is stale.
    """
    percent = worker_handler.get_cpu_temperature_percent()
    return None if percent is None else percent / 100.0


class WorkerValue(NamedTuple):
    """One value read on every living worker of a group."""

    #: The family name after ``group_`` and ``group_max_worker_``.
    stem: str
    #: What the value measures, closing the help text.
    subject: str
    #: Reads it on one worker; None when the worker has no such value.
    read: Callable[[WorkerHandler], float | None]
    #: Whether the largest value of one worker is written beside the sum.
    with_max: bool
    #: Whether the sum is written when no living worker has the value.
    always: bool


WORKER_VALUES: tuple[WorkerValue, ...] = (
    WorkerValue("resident_memory_bytes", "resident memory (RSS), in bytes",
                lambda worker: photo_value(worker, "rss_bytes"), True, True),
    WorkerValue("proportional_memory_bytes", "proportional set size (PSS), in bytes",
                lambda worker: photo_value(worker, "pss_bytes"), True, False),
    WorkerValue("cpu_ratio",
                "CPU share measured by the commander, smoothed, as a ratio of one core",
                cpu_ratio, True, False),
    WorkerValue("users", "users held",
                lambda worker: photo_value(worker, "user_count"), True, True),
    WorkerValue("connections", "connections held",
                lambda worker: photo_value(worker, "connection_count"), True, True),
    WorkerValue("pages", "pages held",
                lambda worker: photo_value(worker, "page_count"), True, True),
    WorkerValue("pending_calls", "calls of the users held still in progress",
                lambda worker: photo_users_total(worker, "pending_call_count"), True, True),
    WorkerValue("served_calls", "calls served to the users held now",
                lambda worker: photo_users_total(worker, "served_call_count"), False, True),
    WorkerValue("service_seconds", "time spent serving the users held now, in seconds",
                lambda worker: photo_users_total(worker, "service_seconds"), False, True),
)


class FamiliesCollector(Collector):
    """A collector that yields a fixed list of metric families."""

    def __init__(self, families: Iterable[Metric]) -> None:
        self.families = list(families)

    def collect(self) -> Iterator[Metric]:
        """Yield the families given at construction."""
        return iter(self.families)


def group_families(commander: SpaCommander) -> list[Metric]:
    """The per-group families: living workers, sums and largest values, quota, concession."""
    workers = GaugeMetricFamily(
        f"{PREFIX}group_workers", "Living workers of the group.", labels=["group"])
    sums = [
        GaugeMetricFamily(
            f"{PREFIX}group_{value.stem}",
            f"Sum over the living workers of the group: {value.subject}.", labels=["group"])
        for value in WORKER_VALUES
    ]
    largest = [
        GaugeMetricFamily(
            f"{PREFIX}group_max_worker_{value.stem}",
            f"Largest value of one living worker of the group: {value.subject}.",
            labels=["group"])
        if value.with_max else None
        for value in WORKER_VALUES
    ]
    quota = GaugeMetricFamily(
        f"{PREFIX}group_memory_quota_bytes",
        "Memory the group may hold, its share of the concession, in bytes.", labels=["group"])
    concession = GaugeMetricFamily(
        f"{PREFIX}group_memory_concession_bytes",
        "Memory of the machine conceded to the pool, of which the group's quota is a share, "
        "in bytes.", labels=["group"])
    for name, group_handler in commander.group_map.items():
        living = group_handler.living_workers
        workers.add_metric([name], len(living))
        for value, total, top in zip(WORKER_VALUES, sums, largest, strict=True):
            readings = [reading for reading in map(value.read, living) if reading is not None]
            if readings or value.always:
                total.add_metric([name], sum(readings))
            if top is not None and readings:
                top.add_metric([name], max(readings))
        quota.add_metric([name], group_handler.memory_quota_bytes)
        concession.add_metric([name], group_handler.memory_concession_bytes)
    return [workers, *sums, *(family for family in largest if family is not None),
            quota, concession]


def pool_families(commander: SpaCommander) -> list[Metric]:
    """The pool-wide families: users, connections, pages, and the commander's counts."""
    events = CounterMetricFamily(
        f"{PREFIX}commander_events",
        "Events counted by the commander since it started, per event.", labels=["event"])
    counts = {**dict.fromkeys(COMMANDER_EVENTS, 0), **commander.counters}
    for event, count in sorted(counts.items()):
        events.add_metric([event], count)
    return [
        GaugeMetricFamily(
            f"{PREFIX}users", "Users the commander knows, frozen and on hold included.",
            value=len(commander.user_map)),
        GaugeMetricFamily(
            f"{PREFIX}connections",
            "Connections the commander knows; a connection stays until its user is dropped.",
            value=len(commander.connection_user_map)),
        GaugeMetricFamily(
            f"{PREFIX}pages", "Pages the commander knows.",
            value=len(commander.page_connection_map)),
        events,
    ]


def pool_text(commander: SpaCommander) -> str:
    """The exposition text of the pool families, on a registry of its own."""
    registry = CollectorRegistry()
    registry.register(FamiliesCollector([*group_families(commander), *pool_families(commander)]))
    return generate_latest(registry).decode()
