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

"""The pool's Prometheus families, read the way Prometheus reads them.

A recipe builds a real ``AsgiServer`` with one SPA front and two groups; its
lifespan builds the pool and starts the reception, a REAL child process. Every
read is ``GET /_server/metrics`` with a ``gak_`` token of a user of ``_auth``
who holds only ``SERVER_METRICS``, parsed with ``prometheus_client``'s own parser.

What the suite pins:

- every pool family is in the text with ``app=<front code>``;
- per group: the living workers (one in the elected group, none in the other),
  the RSS the worker declares, summed and as the largest value, no largest
  value for a group without workers;
- after two clicks of a browser: the user, the connection, the served calls
  are seen per group, and the commander knows the user and the connection;
  a page the site opens is seen per group and by the commander;
- the CPU share the commander measures is written as a ratio of one core;
- the commander's known events are written at 0 before their first count.
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
import uuid
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any

import pytest
from prometheus_client.parser import text_string_to_metric_families

from kajenn import AsgiServer, AuthApplicationBase
from kajenn.config.builder import AsgiConfigBuilder
from kajenn_orchestra.spa_app import SPA_CONNECTION_ID_COOKIE, SpaApplication

from ..conftest import MemoryAuthStore, ask_app, start_server
from .x_spa_worker import X_SpaWorker

APP_CODE = "shop"
BASE_GROUP = "std"
OTHER_GROUP = "alt"
ENTRY_MODULE = "kajenn_orchestra.orchestration.worker_entry"
WORKER_CLASS = f"{__name__}:X_SpaWorker_metrics"
DECLARED_RSS_BYTES = 35_000_000
#: The commander's CPU cadence: wide enough that a reading stays the same
#: across one scrape, short enough that the first one comes within seconds.
CPU_SAMPLE_SECONDS = 0.5
PAGE_PATH = "/page"
PAGE_ID = "page-1"

POOL_FAMILIES = {
    "kajenn_orchestra_group_workers",
    "kajenn_orchestra_group_resident_memory_bytes",
    "kajenn_orchestra_group_proportional_memory_bytes",
    "kajenn_orchestra_group_cpu_ratio",
    "kajenn_orchestra_group_users",
    "kajenn_orchestra_group_connections",
    "kajenn_orchestra_group_pages",
    "kajenn_orchestra_group_pending_calls",
    "kajenn_orchestra_group_served_calls",
    "kajenn_orchestra_group_service_seconds",
    "kajenn_orchestra_group_max_worker_resident_memory_bytes",
    "kajenn_orchestra_group_max_worker_proportional_memory_bytes",
    "kajenn_orchestra_group_max_worker_cpu_ratio",
    "kajenn_orchestra_group_max_worker_users",
    "kajenn_orchestra_group_max_worker_connections",
    "kajenn_orchestra_group_max_worker_pages",
    "kajenn_orchestra_group_max_worker_pending_calls",
    "kajenn_orchestra_group_memory_quota_bytes",
    "kajenn_orchestra_group_memory_concession_bytes",
    "kajenn_orchestra_users",
    "kajenn_orchestra_connections",
    "kajenn_orchestra_pages",
    "kajenn_orchestra_commander_events",
}


class X_SpaWorker_metrics(X_SpaWorker):
    """A site that opens a connection for whoever has none and answers 200."""

    def site(self, environ: dict[str, Any], start_response: Any) -> list[bytes]:
        """Serve one page: the site mints its connection the first time, as genropy does."""
        cookie = SimpleCookie(environ.get("HTTP_COOKIE", ""))
        morsel = cookie.get(SPA_CONNECTION_ID_COOKIE)
        connection_id = None if morsel is None else morsel.value
        if connection_id is None or self.connection_register.get(connection_id) is None:
            connection_id = f"site-{uuid.uuid4().hex}"
            self.new_connection(connection_id, user=environ["genro.identity"])
        if environ["PATH_INFO"] == PAGE_PATH:
            with self.dispatch_lock:
                owner = self.connection_register.get(connection_id)["user"]
            self.new_page(owner, PAGE_ID, connection_id=connection_id)
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"ok"]


def recipe_for(root: Path) -> type[AsgiConfigBuilder]:
    """One front with two groups, the elected one declared second, and ``_auth``."""

    class MetricsConfiguration(AsgiConfigBuilder):
        default_config = False

        def main(self, configuration_root: Any) -> None:
            cfg = configuration_root.configuration()
            applications = cfg.applications()
            applications.application(code="_auth", app_class=AuthApplicationBase).store(
                store_class=MemoryAuthStore)
            front = applications.application(
                code=APP_CODE, mount="", app_class=SpaApplication)
            commander = front.orchestration().commander(
                frozen_users_path=str(root / "frozen_users"),
                instance_dir=str(root / "i"),
                orchestration_log_path=str(root / "orchestration.log"),
                cpu_temperature_sample_seconds=CPU_SAMPLE_SECONDS,
            )
            groups = commander.groups(default=BASE_GROUP)
            for name in (OTHER_GROUP, BASE_GROUP):
                groups.group(
                    name=name,
                    worker_memory_max_percent=50.0,
                    worker_memory_admission_percent=80.0,
                    restart_occupancy_max_percent=95.0,
                    entry_module=ENTRY_MODULE,
                    worker_class=WORKER_CLASS,
                    main_threadpool_size=2,
                    aux_threadpool_size=1,
                    worker_kwargs={
                        "declared_rss_bytes": DECLARED_RSS_BYTES,
                        "worker_snapshot_ttl": 0,
                    },
                )

    return MetricsConfiguration


@pytest.fixture
async def server(repo_on_pythonpath):
    """The server of the recipe, started; its lifespan is shut down after the test."""
    root = Path(tempfile.mkdtemp(prefix="gnrpm_"))
    yield await start_server(AsgiServer(config=recipe_for(root)))
    shutil.rmtree(root, ignore_errors=True)


async def browse(server: AsgiServer, cid: str | None = None, path: str = "/catalog") -> str:
    """One click; returns the connection id the answer's cookie names."""
    answer = await ask_app(
        server, path, cookies={SPA_CONNECTION_ID_COOKIE: cid} if cid else None)
    assert answer["status"] == 200
    if cid is not None:
        return cid
    for name, value in answer["headers"]:
        if name.lower() == "set-cookie" and value.startswith(f"{SPA_CONNECTION_ID_COOKIE}="):
            return str(SimpleCookie(value)[SPA_CONNECTION_ID_COOKIE].value)
    raise AssertionError("the answer names no connection")


async def scrape(server: AsgiServer) -> dict[str, dict[tuple[tuple[str, str], ...], float]]:
    """GET /_server/metrics as Prometheus does; the samples of every family, by labels."""
    store = server.applications["_auth"].store
    store.save_user({
        "identity": "prometheus", "groups": [], "primary_group": None,
        "tags": ["SERVER_METRICS"], "status": "confirmed", "system": False,
        "password_hash": None, "email": None, "external_ids": [],
    })
    key = store.issue_token("prometheus", ["SERVER_METRICS"])
    scope = {
        "type": "http", "method": "GET", "path": "/_server/metrics", "query_string": b"",
        "headers": [(b"host", b"site.example"), (b"authorization", f"Bearer {key}".encode())],
        "scheme": "http", "client": ("10.0.0.1", 5555),
    }
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await server(scope, receive, send)
    assert next(m for m in sent if m["type"] == "http.response.start")["status"] == 200
    text = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return {
        family.name: {
            tuple(sorted(sample.labels.items())): sample.value for sample in family.samples
        }
        for family in text_string_to_metric_families(text.decode())
    }


def group_sample(families: dict, name: str, group: str) -> float | None:
    """The sample of a per-group family for ``group`` under the front's ``app``."""
    return families[name].get((("app", APP_CODE), ("group", group)))


async def test_every_pool_family_is_in_the_text_with_the_front_app_label(server):
    families = await scrape(server)

    assert POOL_FAMILIES <= set(families)
    assert families["kajenn_orchestra_users"] == {(("app", APP_CODE),): 0.0}
    assert families["kajenn_orchestra_commander_events"] == {
        (("app", APP_CODE), ("event", event)): 0.0
        for event in ("frozen_users_discarded", "orphan_folders_discarded", "requests_refused")
    }


async def test_the_cpu_share_is_written_as_a_ratio_of_one_core(server):
    worker = server.applications[APP_CODE].commander.group_map[BASE_GROUP].reception
    async with asyncio.timeout(10):
        while True:
            before = worker.get_cpu_temperature_percent()
            families = await scrape(server)
            after = worker.get_cpu_temperature_percent()
            if before is not None and before == after:
                break
            await asyncio.sleep(0.05)

    assert group_sample(families, "kajenn_orchestra_group_cpu_ratio", BASE_GROUP) == before / 100
    assert group_sample(
        families, "kajenn_orchestra_group_max_worker_cpu_ratio", BASE_GROUP) == before / 100


async def test_living_workers_and_declared_memory_per_group(server):
    await browse(server)
    families = await scrape(server)

    assert group_sample(families, "kajenn_orchestra_group_workers", BASE_GROUP) == 1
    assert group_sample(families, "kajenn_orchestra_group_workers", OTHER_GROUP) == 0
    assert group_sample(
        families, "kajenn_orchestra_group_resident_memory_bytes", BASE_GROUP
    ) == DECLARED_RSS_BYTES
    assert group_sample(
        families, "kajenn_orchestra_group_max_worker_resident_memory_bytes", BASE_GROUP
    ) == DECLARED_RSS_BYTES
    assert group_sample(
        families, "kajenn_orchestra_group_resident_memory_bytes", OTHER_GROUP) == 0
    assert group_sample(
        families, "kajenn_orchestra_group_max_worker_resident_memory_bytes", OTHER_GROUP
    ) is None
    # The worker declares no PSS: no sum and no largest value.
    assert group_sample(
        families, "kajenn_orchestra_group_proportional_memory_bytes", BASE_GROUP) is None
    quota = group_sample(families, "kajenn_orchestra_group_memory_quota_bytes", BASE_GROUP)
    concession = group_sample(
        families, "kajenn_orchestra_group_memory_concession_bytes", BASE_GROUP)
    assert 0 < quota <= concession


async def test_a_browser_is_seen_per_group_and_by_the_commander(server):
    cid = await browse(server)
    await browse(server, cid)
    families = await scrape(server)

    assert group_sample(families, "kajenn_orchestra_group_users", BASE_GROUP) == 1
    assert group_sample(families, "kajenn_orchestra_group_max_worker_users", BASE_GROUP) == 1
    assert group_sample(families, "kajenn_orchestra_group_connections", BASE_GROUP) == 1
    assert group_sample(families, "kajenn_orchestra_group_served_calls", BASE_GROUP) >= 1
    assert group_sample(families, "kajenn_orchestra_group_service_seconds", BASE_GROUP) > 0
    assert group_sample(families, "kajenn_orchestra_group_pending_calls", BASE_GROUP) >= 0
    assert group_sample(families, "kajenn_orchestra_group_users", OTHER_GROUP) == 0
    assert families["kajenn_orchestra_users"] == {(("app", APP_CODE),): 1.0}
    assert families["kajenn_orchestra_connections"] == {(("app", APP_CODE),): 1.0}
    assert families["kajenn_orchestra_pages"] == {(("app", APP_CODE),): 0.0}


async def test_a_page_the_site_opens_is_seen_per_group_and_by_the_commander(server):
    cid = await browse(server)
    await browse(server, cid, PAGE_PATH)
    await browse(server, cid)
    families = await scrape(server)

    assert group_sample(families, "kajenn_orchestra_group_pages", BASE_GROUP) == 1
    assert group_sample(families, "kajenn_orchestra_group_max_worker_pages", BASE_GROUP) == 1
    assert families["kajenn_orchestra_pages"] == {(("app", APP_CODE),): 1.0}
