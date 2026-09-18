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

"""The new front, from the outside: what a browser gets, and what the recipe builds.

Everything here goes through the doors production goes through — the recipe builds
the server, the lifespan builds the pool, an ASGI call gets an answer — because
this front's whole job is to be that seam. The pool below is a SUBCLASS whose
``serve_request`` is scripted: what the chain does with a request has its own
tests one folder over, and what belongs here is only what the front does with the
answer, or with the refusal.
"""

from __future__ import annotations

import base64
from typing import Any

import pytest

from tests.orchestration.frame_helpers import read_http_request, http_reply
from genro_routes import route

from kajenn import AsgiServer
from kajenn.config.builder import AsgiConfigBuilder
from kajenn_orchestra.spa_app import (
    CONNECTION_COOKIE_MAX_AGE,
    ERR_503_TEXT,
    SPA_CONNECTION_ID_COOKIE,
    ERR_502_TEXT,
    SpaApplication,
)
from kajenn.server import QUITTING, REFUSED_RETRY_AFTER_SECONDS, STOPPING
from kajenn_orchestra.orchestration import AssignmentRefused, SiteFailedRequest, SpaCommander

from .conftest import LifespanRunner, ask_app, get_answer_header


class ScriptedCommander(SpaCommander):
    """A pool that answers from a script: no processes, no wire, no beat."""

    #: What the next request gets: a reply, or an exception raised instead.
    reply: dict[str, Any] = {"result": {"status": 200, "headers": [], "body": ""}}
    failure: Exception | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        #: One entry per request, as (cid, http).
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def serve_request(
        self, cid: str, http: dict[str, Any], *, hold_timeout: float
    ) -> dict[str, Any]:
        self.calls.append((cid, read_http_request(http)))
        if self.failure is not None:
            raise self.failure
        return http_reply(http, self.reply)


class ScriptedFront(SpaApplication):
    """The front under test: its own route, and a pool that answers from a script."""

    commander_class = ScriptedCommander

    @route()
    def ping(self) -> dict[str, bool]:
        return {"ping": True}


def recipe_for(root, fronts: int = 1) -> type[AsgiConfigBuilder]:
    """A recipe with the pool section and as many fronts as asked for."""

    class FrontConfig(AsgiConfigBuilder):
        def main(self, configuration_root: Any) -> None:
            cfg = configuration_root.configuration()
            applications = cfg.applications()
            for index in range(fronts):
                front = applications.application(
                    code=f"site{index}",
                    mount="" if not index else f"other{index}",
                    app_class=ScriptedFront,
                )
                # The pool belongs to the front that owns it: its whole
                # orchestration hangs here, under its own node.
                commander = front.orchestration().commander(
                    frozen_users_path=str(root / f"frozen_users{index}"),
                    instance_dir=str(root / f"i{index}"),
                )
                groups = commander.groups(default="standard")
                groups.group(name="standard", entry_module="never.launched")

    return FrontConfig


@pytest.fixture
def server(tmp_path):
    """A server built from a recipe, with its front mounted and not yet started."""
    return AsgiServer(config=recipe_for(tmp_path))


@pytest.fixture
async def started(server):
    """The same server, through its own lifespan: the pool exists and is up."""
    runner = LifespanRunner(server)
    await runner.startup()
    yield server
    await runner.shutdown()


# -- the pool is born with the server --


async def test_the_pool_is_built_from_the_recipe_when_the_server_starts(server):
    front = server.applications["site0"]
    runner = LifespanRunner(server)

    with pytest.raises(RuntimeError):
        front.commander

    await runner.startup()

    assert isinstance(front.commander, ScriptedCommander)
    assert front.commander.started is True
    # What the recipe said, where it had to arrive.
    assert front.commander.default_group == "standard"
    assert set(front.commander.group_map) == {"standard"}

    pool = front.commander
    await runner.shutdown()
    # The pool was taken down AND let go: the front holds no vertex out of the
    # lifespan, so a later startup builds a new one instead of finding a dead one.
    assert pool.started is False
    assert front._commander is None
    with pytest.raises(RuntimeError):
        front.commander


async def test_two_fronts_each_own_their_pool(tmp_path):
    """A pool belongs to the front that owns it: two fronts are two pools."""
    server = AsgiServer(config=recipe_for(tmp_path, fronts=2))
    runner = LifespanRunner(server)

    await runner.startup()

    first = server.applications["site0"].commander
    second = server.applications["site1"].commander
    assert first is not second
    assert first.freeze_handler.root_path != second.freeze_handler.root_path

    await runner.shutdown()


# -- the demux --


async def test_its_own_route_answers_natively(started):
    front = started.applications["site0"]

    answer = await ask_app(front, "/ping")

    assert answer["status"] == 200
    assert front.commander.calls == []


async def test_a_path_of_the_site_is_forwarded(started):
    front = started.applications["site0"]
    front.commander.reply = {
        "result": {
            "status": 201,
            "headers": [["content-type", "text/html"]],
            "body": base64.b64encode(b"<h1>the site</h1>").decode(),
        }
    }

    answer = await ask_app(front, "/invoices/42")

    assert answer["status"] == 201
    assert answer["body"] == b"<h1>the site</h1>"
    assert get_answer_header(answer, "content-type") == "text/html"
    cid, http = front.commander.calls[0]
    assert http["path"] == "/invoices/42"
    assert http["cid"] == cid


# -- the cookie --


async def test_a_request_with_no_cookie_travels_with_none_and_mints_nothing(started):
    front = started.applications["site0"]

    await ask_app(front, "/invoices")

    cid, http = front.commander.calls[0]
    assert cid is None
    assert http["cid"] is None
    # Nothing of ours is added to what the browser sent.
    assert all(SPA_CONNECTION_ID_COOKIE not in value for _, value in http["headers"])


async def test_the_connection_the_site_named_becomes_the_cookie(started):
    front = started.applications["site0"]
    front.commander.reply = {"result": {"status": 200, "connection_id": "site-1"}}

    answer = await ask_app(front, "/invoices")

    cookie = get_answer_header(answer, "set-cookie")
    assert f"{SPA_CONNECTION_ID_COOKIE}=site-1" in cookie
    # The same life the hosted site gives its own connection cookie.
    assert f"Max-Age={CONNECTION_COOKIE_MAX_AGE}" in cookie


async def test_a_request_that_reused_its_connection_is_answered_without_a_cookie(started):
    front = started.applications["site0"]
    front.commander.reply = {"result": {"status": 200, "connection_id": "site-1"}}

    answer = await ask_app(front, "/invoices", cookies={SPA_CONNECTION_ID_COOKIE: "site-1"})

    assert get_answer_header(answer, "set-cookie") is None
    assert front.commander.calls[0][0] == "site-1"


async def test_a_connection_the_site_replaced_overwrites_the_cookie(started):
    """The site validates its own cookie and creates a fresh connection when it
    does not: the browser must be told, or it would route on a dead id forever."""
    front = started.applications["site0"]
    front.commander.reply = {"result": {"status": 200, "connection_id": "site-2"}}

    answer = await ask_app(front, "/invoices", cookies={SPA_CONNECTION_ID_COOKIE: "site-1"})

    assert f"{SPA_CONNECTION_ID_COOKIE}=site-2" in get_answer_header(answer, "set-cookie")


# -- the two refusals --


async def test_a_pool_that_takes_nobody_is_a_polite_503(started):
    front = started.applications["site0"]
    front.commander.failure = AssignmentRefused("mario", "no worker admits him", retry_after=30.0)

    answer = await ask_app(front, "/invoices")

    assert answer["status"] == 503
    assert get_answer_header(answer, "retry-after") == "30"
    assert answer["body"] == ERR_503_TEXT.encode()


async def test_the_inside_of_the_house_never_reaches_the_browser(started):
    front = started.applications["site0"]
    front.commander.failure = SiteFailedRequest(
        "mario", "ProgrammingError: relation invoices_2024 does not exist"
    )

    answer = await ask_app(front, "/invoices")

    assert answer["status"] == 502
    assert answer["body"] == ERR_502_TEXT.encode()
    assert b"invoices_2024" not in answer["body"]


async def test_a_wire_that_is_gone_while_the_server_quits_is_a_503(started, monkeypatch):
    """The wire died because the server is leaving: a refusal, not a breakage."""
    front = started.applications["site0"]
    monkeypatch.setattr(front.server, "state", QUITTING)
    front.commander.failure = ConnectionError("no child on the wire")

    answer = await ask_app(front, "/invoices")

    assert answer["status"] == 503
    assert get_answer_header(answer, "retry-after") == str(REFUSED_RETRY_AFTER_SECONDS)


async def test_a_wire_that_is_gone_is_the_same_502(started):
    front = started.applications["site0"]
    front.commander.failure = ConnectionError("no child on the wire")

    answer = await ask_app(front, "/invoices")

    assert answer["status"] == 502


async def test_a_refusal_names_no_connection_and_writes_no_cookie(started):
    """The site never served it, so there is nothing to name — and a refusal must
    not overwrite the connection the browser already holds."""
    front = started.applications["site0"]
    front.commander.failure = AssignmentRefused("mario", "no worker admits him", retry_after=30.0)

    answer = await ask_app(front, "/invoices", cookies={SPA_CONNECTION_ID_COOKIE: "site-1"})

    assert get_answer_header(answer, "set-cookie") is None


async def test_the_front_takes_the_photo_only_when_the_server_is_quitting(started, monkeypatch):
    """QUITTING gets the soft quit; any other way out is dry."""
    front = started.applications["site0"]
    called = []

    async def note_quit(self):
        called.append("quit")

    async def note_stop(self):
        called.append("stop")

    monkeypatch.setattr(ScriptedCommander, "quit", note_quit)
    monkeypatch.setattr(ScriptedCommander, "stop", note_stop)

    # One way out per pool: the front lets go of its vertex on the way down, so
    # the second exit is a second pool, built by its own startup.
    monkeypatch.setattr(front.server, "state", STOPPING)
    await front.on_shutdown()
    await front.on_startup()
    monkeypatch.setattr(front.server, "state", QUITTING)
    await front.on_shutdown()

    assert called == ["stop", "quit"]
    assert front._commander is None
