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

"""The ``_server/inspector`` section: mounted by the env var, and by nothing else.

Contract: the mount IS the gate — without ``KAJENN_INSPECTOR`` the section
does not exist at all; with it the page is HTML and the census is JSON, keyed by
the code of the front whose pool it watches. And in neither case does the
inspector touch the hosted site: no connection cookie ever comes back from it.

The front attaches it, on its own startup, so every server here goes through its
lifespan — the section does not exist before the pool does.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from kajenn import AsgiServer
from kajenn.config.builder import AsgiConfigBuilder
from kajenn.lifespan import STOPPING, FatalBootError
from kajenn_server_app import ServerApplication
from kajenn_orchestra.inspector_section import INSPECTOR_ENV_VAR
from kajenn_orchestra.orchestration import SpaCommander
from kajenn_orchestra.spa_app import SPA_CONNECTION_ID_COOKIE, SpaApplication

from .conftest import LifespanRunner


class InspectorScriptedCommander(SpaCommander):
    """A pool that comes up without processes: no wire, no beat, no worker."""

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


class InspectorScriptedFront(SpaApplication):
    """The front under test: the real startup, over a pool that launches nothing."""

    commander_class = InspectorScriptedCommander


def inspector_recipe_for(root) -> type[AsgiConfigBuilder]:
    """A recipe with the ``_server`` app and one spa front with its pool at the root.

    The server application is declared like any other since D-SA-10; the
    inspector needs it mounted, because the front attaches its section there.
    """

    class FrontConfig(AsgiConfigBuilder):
        def main(self, configuration_root: Any) -> None:
            cfg = configuration_root.configuration()
            applications = cfg.applications()
            applications.application(code="_server", app_class=ServerApplication)
            front = applications.application(
                code="site", mount="", app_class=InspectorScriptedFront
            )
            commander = front.orchestration().commander(
                frozen_users_path=str(root / "frozen_users"),
                instance_dir=str(root / "i"),
            )
            commander.groups(default="standard").group(
                name="standard", entry_module="never.launched"
            )

    return FrontConfig


@pytest.fixture
async def inspector_server(tmp_path, monkeypatch):
    """A started server whose front attached the inspector on startup."""
    monkeypatch.setenv(INSPECTOR_ENV_VAR, "1")
    server = AsgiServer(config=inspector_recipe_for(tmp_path))
    runner = LifespanRunner(server)
    await runner.startup()
    yield server
    await runner.shutdown()


@pytest.fixture
async def plain_server(tmp_path, monkeypatch):
    """The same server with the variable unset: the section is not there."""
    monkeypatch.delenv(INSPECTOR_ENV_VAR, raising=False)
    server = AsgiServer(config=inspector_recipe_for(tmp_path))
    runner = LifespanRunner(server)
    await runner.startup()
    yield server
    await runner.shutdown()


async def test_without_the_env_var_the_page_is_not_there(
    plain_server, http_request, response_status
):
    sent = await http_request(plain_server, "/_server/inspector/page")

    assert response_status(sent) == 404


async def test_the_page_is_html(
    inspector_server, http_request, response_status, response_headers, response_body
):
    sent = await http_request(inspector_server, "/_server/inspector/page")

    assert response_status(sent) == 200
    assert response_headers(sent)[b"content-type"].startswith(b"text/html")
    assert b"worker-grid" in response_body(sent)


async def test_the_census_is_json_under_the_front_code(
    inspector_server, http_request, response_status, response_headers, response_body
):
    sent = await http_request(inspector_server, "/_server/inspector/census")

    assert response_status(sent) == 200
    assert response_headers(sent)[b"content-type"].startswith(b"application/json")
    census = json.loads(response_body(sent))
    assert set(census) == {"site"}
    assert census["site"]["user_map"] == {}


async def test_the_inspector_mints_no_cookie(inspector_server, http_request, response_headers):
    for path in ("/_server/inspector/page", "/_server/inspector/census"):
        headers = await http_request(inspector_server, path)

        cookie = response_headers(headers).get(b"set-cookie", b"")
        assert SPA_CONNECTION_ID_COOKIE.encode() not in cookie


async def test_the_stream_opens_with_the_census(inspector_server, sse_request):
    connection = await sse_request(inspector_server, "/_server/inspector/stream")

    frames = await connection.wait_frames(2)
    await connection.close()

    assert b"retry: 2000" in frames[0]
    assert b"event: census" in frames[1]


async def test_the_stream_ends_when_the_server_leaves(inspector_server, sse_request):
    """The observation stream watches the server and ends without being cancelled."""
    connection = await sse_request(inspector_server, "/_server/inspector/stream")
    await connection.wait_frames(2)
    commander = inspector_server.applications["site"].commander

    inspector_server.state = STOPPING
    await asyncio.wait_for(connection.task, timeout=2.0)

    assert commander._observation_queues == set()


async def test_the_page_carries_its_containers_and_its_endpoints(
    inspector_server, http_request, response_body
):
    page = response_body(await http_request(inspector_server, "/_server/inspector/page"))

    for container_id in ("commander-panel", "worker-grid", "event-log", "last-read", "toggle-stream"):
        assert f'id="{container_id}"'.encode() in page
    assert b'"/census"' in page
    assert b'"/stream"' in page


async def test_no_server_app_leaves_the_inspector_unmounted(inspector_server, caplog):
    """The variable set and no ``_server`` to host it: said once, nothing attached."""
    front = inspector_server.applications["site"]
    del inspector_server.applications["_server"]

    with caplog.at_level("WARNING"):
        front.mount_inspector()

    assert INSPECTOR_ENV_VAR in caplog.text
    assert "nowhere to go" in caplog.text


async def test_a_second_front_on_one_server_does_not_boot(tmp_path, monkeypatch):
    """A server has ONE orchestrated application: the second attach is fatal."""
    monkeypatch.setenv(INSPECTOR_ENV_VAR, "1")
    server = AsgiServer(config=inspector_recipe_for(tmp_path))
    front = server.applications["site"]
    await LifespanRunner(server).startup()

    second = InspectorScriptedFront(code="site2", mount="other")
    server.register_application(second)

    with pytest.raises(FatalBootError, match="already attached"):
        second.mount_inspector()

    server_app = server.applications["_server"]
    assert isinstance(server_app, ServerApplication)
    assert server_app.sections["inspector"].application is front
