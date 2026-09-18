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

"""A message of a page, from the browser to its worker and back (#68 phase 4).

Contract tests, entered where a real message enters: the socket. A message
becomes a synthetic request, the front packs it, the vertex places it, the
worker serves it on the row of the page it belongs to — and the answer comes
back with the id the browser correlates on.

``openchannel`` is what a page must send first, and it is the one command both
halves of the machine touch: the front validates the page against
``page_connection_map`` and writes the channel on its row through the worker;
the CONNECTION binds the page to its socket, and only because the front
answered 200 (owner, N30).

The pool here is real — a commander, a group, a worker on a UDS — through the
``worker_commander_lane`` fixture; the front is a real ``SpaApplication`` with
that commander in place.
"""

from __future__ import annotations

import asyncio
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from kajenn import BaseServer, MiddlewareMixin
from kajenn_orchestra.spa_app import SPA_CONNECTION_ID_COOKIE, SpaApplication
from kajenn.exceptions import HTTPBadRequest
from kajenn.request import Request
from kajenn.types import Message, Scope
from kajenn.wsx import WsxConnection, WsxEnvelope

PREFIX = "WSX://"


async def no_body() -> Message:
    """A receive that hands over an empty body: the request carries none."""
    return {"type": "http.request", "body": b"", "more_body": False}
CID = "cid-a"
USER = "mario"
PAGE = "page-1"


class WsServer(MiddlewareMixin, BaseServer):
    """The composition a websocket needs."""


class XT_Front(SpaApplication):
    """The real front, with the pool of the fixture already in place."""

    def __init__(self, commander: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._commander = commander
        self.mount_channel_control()


class XT_Browser:
    """A browser that stays connected until the test lets it go."""

    def __init__(self) -> None:
        self.sent: list[Message] = []
        self._incoming: asyncio.Queue[Message] = asyncio.Queue()
        self._incoming.put_nowait({"type": "websocket.connect"})

    async def receive(self) -> Message:
        return await self._incoming.get()

    async def send(self, message: Message) -> None:
        self.sent.append(message)

    def says(self, envelope: WsxEnvelope) -> None:
        self._incoming.put_nowait({"type": "websocket.receive", "text": envelope.encode()})

    def leave(self) -> None:
        self._incoming.put_nowait({"type": "websocket.disconnect", "code": 1000})

    @property
    def answers(self) -> list[WsxEnvelope]:
        return [
            WsxEnvelope(m["text"])
            for m in self.sent
            if m["type"] == "websocket.send" and m.get("text", "").startswith(PREFIX)
        ]


class XT_Machine:
    """The whole machine of one story: server, front, pool, and one browser."""

    def __init__(self, lane: Any) -> None:
        self.lane = lane
        self.commander = lane.commander
        self.front = XT_Front(lane.commander, code="site0", mount="")
        self.server = WsServer(applications=[self.front])
        self.browser = XT_Browser()
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> XT_Machine:
        scope: Scope = {
            "type": "websocket",
            "path": "/_wsx",
            "headers": [
                (b"host", b"site.example"),
                (b"cookie", f"{SPA_CONNECTION_ID_COOKIE}={CID}".encode()),
            ],
            "subprotocols": [],
        }
        connection = WsxConnection(self.server, scope, self.browser.receive, self.browser.send)
        self._task = asyncio.create_task(connection.serve())
        await self.settle()
        return self

    async def __aexit__(self, *failure: Any) -> None:
        self.browser.leave()
        if self._task is not None:
            await self._task

    async def settle(self) -> None:
        """Let the connection's task, the lane and the worker move on."""
        for _ in range(20):
            await asyncio.sleep(0)

    async def message(self, envelope: WsxEnvelope) -> WsxEnvelope | None:
        """Say one message and hand back the answer, when it asked for one."""
        before = len(self.browser.answers)
        self.browser.says(envelope)
        for _ in range(40):
            await asyncio.sleep(0)
            if len(self.browser.answers) > before:
                return self.browser.answers[-1]
        return None

    async def open_channel(self, page_id: str = PAGE, **parameters: Any) -> WsxEnvelope | None:
        return await self.message(
            WsxEnvelope(
                id=f"open-{page_id}",
                method="WSK",
                path="/_wsx/openchannel",
                page_id=page_id,
                data={"parameters": parameters} if parameters else None,
            )
        )


@pytest.fixture
async def machine(worker_commander_lane):
    """The machine with one page already born on the worker, as a site does."""
    lane = worker_commander_lane
    lane.commander.connection_user_map[CID] = USER
    lane.commander.resolve_user(CID)
    await lane.verb("new_connection", CID, user=USER)
    await lane.verb("new_page", USER, PAGE, connection_id=CID)
    async with XT_Machine(lane) as running:
        yield running


class TestTheHandshakeOfTheSpa:
    async def test_a_socket_with_no_connection_cookie_is_closed_1008(
        self, worker_commander_lane
    ) -> None:
        # What the first browser found (#70 A): the front declared no cookie,
        # so a socket opened without one stayed open for ever. The gate of
        # W-13 had been built and had no user.
        machine = XT_Machine(worker_commander_lane)
        scope: Scope = {
            "type": "websocket",
            "path": "/_wsx",
            "headers": [(b"host", b"site.example")],
            "subprotocols": [],
        }
        connection = WsxConnection(
            machine.server, scope, machine.browser.receive, machine.browser.send
        )
        # The browser leaves right away: without the gate the read loop would
        # wait for ever, which is exactly what the defect looked like.
        machine.browser.leave()
        await connection.serve()
        assert machine.browser.sent == [
            {"type": "websocket.accept"},
            {
                "type": "websocket.close",
                "code": 1008,
                "reason": "connection cookie required: spa_connection_id",
            },
        ]

    async def test_a_socket_with_the_cookie_is_let_in(self, machine) -> None:
        assert {"type": "websocket.accept"} in machine.browser.sent

    def test_the_front_names_the_cookie_its_handshake_must_carry(self) -> None:
        assert SpaApplication(code="site0", mount="").handshake_cookie == (
            SPA_CONNECTION_ID_COOKIE
        )


class TestOpeningTheChannelOfAPage:
    async def test_the_page_is_told_its_channel_is_open(self, machine) -> None:
        answer = await machine.open_channel()
        assert answer is not None
        assert (answer.id, answer.status) == (f"open-{PAGE}", 200)

    async def test_the_worker_wrote_the_channel_on_the_row(self, machine) -> None:
        await machine.open_channel()
        assert machine.lane.worker.page_register.get(PAGE)["wsx"] is True

    async def test_the_parameters_the_page_asked_for_are_on_the_row(self, machine) -> None:
        await machine.open_channel(sequential=True)
        assert machine.lane.worker.page_register.get(PAGE)["wsx"] == {"sequential": True}

    async def test_the_page_now_speaks_on_this_socket(self, machine) -> None:
        assert machine.server.websockets.get_page_socket(PAGE) is None
        await machine.open_channel()
        assert machine.server.websockets.get_page_socket(PAGE) is not None

    async def test_saying_it_twice_changes_nothing(self, machine) -> None:
        await machine.open_channel()
        first = machine.server.websockets.get_page_socket(PAGE)
        answer = await machine.open_channel()
        assert answer.status == 200
        assert machine.server.websockets.get_page_socket(PAGE) is first


class TestAPageThatIsNotThisConnections:
    async def test_a_page_of_another_connection_is_refused(self, machine) -> None:
        machine.commander.page_connection_map["page-elsewhere"] = "cid-b"
        answer = await machine.open_channel("page-elsewhere")
        assert answer.status == 403

    async def test_a_page_nobody_ever_created_is_refused(self, machine) -> None:
        answer = await machine.open_channel("page-nowhere")
        assert answer.status == 403

    async def test_neither_is_ever_bound_to_the_socket(self, machine) -> None:
        machine.commander.page_connection_map["page-elsewhere"] = "cid-b"
        await machine.open_channel("page-elsewhere")
        await machine.open_channel("page-nowhere")
        assert machine.server.websockets.get_page_socket("page-elsewhere") is None
        assert machine.server.websockets.get_page_socket("page-nowhere") is None

    async def test_a_message_that_names_no_page_is_refused(self, machine) -> None:
        answer = await machine.message(
            WsxEnvelope(id="m1", method="WSK", path="/_wsx/openchannel")
        )
        assert answer.status == 400


class TestWhatTheWorkerRefuses:
    async def test_a_channel_for_a_page_the_worker_never_saw_is_an_error(
        self, machine
    ) -> None:
        # The vertex believes the page is this connection's — the front lets it
        # through — but the worker has no such row: the command fails there,
        # loudly, and nothing is bound.
        machine.commander.page_connection_map["ghost"] = CID
        answer = await machine.open_channel("ghost")
        assert answer.status == 500 and "never born here" in answer.data
        assert machine.server.websockets.get_page_socket("ghost") is None


class TestACallerWithNoCookie:
    async def test_the_command_refuses_a_request_that_carries_no_connection(
        self, worker_commander_lane
    ) -> None:
        # No websocket can reach this any more — the handshake gate closes such
        # a socket first (#70 A) — but the route is a route: an HTTP caller can
        # knock on it with no cookie at all, and the answer must never be «some
        # connection».
        front = XT_Front(worker_commander_lane.commander, code="site0", mount="")
        control = front.route.node("/_wsx/openchannel")
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/_wsx/openchannel",
                "headers": [],
                "genro.page_id": PAGE,
            },
            no_body,
        )
        await request.init()
        with pytest.raises(HTTPBadRequest, match="no cookie"):
            await control(parameters=None, _request=request)


class TestAMessageBeforeTheChannelIsOpen:
    async def test_the_browser_is_told_what_is_wrong_and_not_that_the_site_broke(
        self, machine
    ) -> None:
        # What the first browser found (#70 C): the refusal was normalised into
        # the 502 of an upstream that broke, and the reason lived only in the
        # worker's log. It is a refusal of the CLIENT, not a failure of the
        # site, and it carries its own status and its own words.
        answer = await machine.message(
            WsxEnvelope(id="m1", method="WSK", path="/main/rpc", page_id=PAGE)
        )
        assert answer.status == 409
        assert "no open channel" in answer.data

    async def test_the_page_is_served_once_its_channel_is_open(self, machine) -> None:
        await machine.open_channel()
        answer = await machine.message(
            WsxEnvelope(id="m2", method="WSK", path="/main/rpc", page_id=PAGE)
        )
        # The lane's worker hosts no application, so what comes back now is the
        # site's own failure — a 502 with the fixed text — and no longer the
        # refusal: the channel is open and the message was passed on.
        assert answer.status == 502


class TestTheChannelSurvivesTheDeposit:
    async def test_a_page_woken_from_the_deposit_still_has_its_channel(
        self, machine
    ) -> None:
        # W-5: a freeze does not touch the websocket, and the browser notices
        # nothing. So the row must come back with its channel — otherwise the
        # very next message of a page that is still connected would be refused.
        await machine.open_channel(sequential=True)
        worker = machine.lane.worker
        await worker.freeze_designated_user(USER)
        assert worker.page_register.get(PAGE) is None

        # The road a request takes when it finds him frozen: the store comes
        # home, then the connection with the pages hanging under it.
        await worker.adopt_user(USER)
        await worker.adopt_connection(USER, CID)

        assert worker.page_register.get(PAGE)["wsx"] == {"sequential": True}

    async def test_the_queue_of_a_woken_page_is_a_fresh_one(self, machine) -> None:
        # The lock itself never travels: it is an object, and the page that
        # comes back is served by a loop that never saw the old one.
        await machine.open_channel(sequential=True)
        worker = machine.lane.worker
        before = worker.page_register.get(PAGE)["call_lock"]
        await worker.freeze_designated_user(USER)
        await worker.adopt_user(USER)
        await worker.adopt_connection(USER, CID)
        after = worker.page_register.get(PAGE)["call_lock"]
        assert isinstance(after, asyncio.Lock) and after is not before


class TestTheServerSpeaksToTheOpenedPage:
    async def test_the_worker_reaches_the_browser_through_the_vertex(self, machine) -> None:
        # The whole road back: the site's own code calls `send_message` on a
        # pool thread, the CALL climbs the lane, the front finds the socket.
        await machine.open_channel()
        delivered = await machine.lane.verb("send_message", PAGE, "/main/refresh", {"n": 1})
        await machine.settle()
        assert delivered is True
        pushed = [m for m in machine.browser.answers if m.id is None]
        assert [(m.path, m.data, m.page_id) for m in pushed] == [
            ("/main/refresh", {"n": 1}, PAGE)
        ]

    async def test_bytes_reach_the_browser_through_the_codec(self, machine) -> None:
        # What the first browser found (#70 B): the lane is JSON, so `data`
        # went into `json.dumps` as it was and a bytes payload died there with
        # a TypeError — the browser saw a 502 and no push. The codec that
        # carries bytes is TYTX, and the worker speaks it before the CALL.
        await machine.open_channel()
        delivered = await machine.lane.verb(
            "send_message", PAGE, "/main/blob", {"blob": b"\x00\x01\xff", "n": 1}
        )
        await machine.settle()
        assert delivered is True
        pushed = [m for m in machine.browser.answers if m.id is None]
        assert pushed[0].data == {"blob": b"\x00\x01\xff", "n": 1}

    async def test_what_the_codec_carries_survives_the_whole_road(self, machine) -> None:
        await machine.open_channel()
        sent = {"day": date(2026, 9, 7), "total": Decimal("9.99"), "nothing": None}
        await machine.lane.verb("send_message", PAGE, "/main/typed", sent)
        await machine.settle()
        assert [m for m in machine.browser.answers if m.id is None][0].data == sent

    async def test_a_page_that_never_opened_its_channel_is_reachable_by_nobody(
        self, machine
    ) -> None:
        delivered = await machine.lane.verb("send_message", PAGE, "/main/refresh")
        assert delivered is False

    async def test_a_page_the_vertex_no_longer_knows_is_reachable_by_nobody(
        self, machine
    ) -> None:
        # The fold dropped the page from the map: the socket may still hold a
        # stale binding, and the branch validates before it writes.
        await machine.open_channel()
        machine.commander.page_connection_map.pop(PAGE)
        delivered = await machine.lane.verb("send_message", PAGE, "/main/refresh")
        assert delivered is False
