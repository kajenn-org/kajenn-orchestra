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

"""The worker hosts an ASGI application, and the legacy reaches it (#68 phase 4a).

Contract tests. There is ONE seam on the worker, ``asgi_app``, and one road out
of ``_serve_request``: ``hosted_app_seam``. A worker that hosts only a WSGI site
takes the shortcut — it assigns ``wsgi_app`` and the core wraps it in a
``WsgiSeam`` — and a consumer whose own ASGI application must delegate some
paths to a legacy site builds that same ``WsgiSeam`` and calls it from its own
router. The core knows no path prefixes (owner, 2026-09-06, form B).

The shortcut is covered by the whole existing rig, which passes unchanged:
``test_orchestration_m2_e2e.py`` drives a real process whose site is assigned to
``wsgi_app``, and every story it tells now travels through the adapter.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from kajenn.exceptions import HTTPException
from kajenn_orchestra.environ import WsgiSeam
from kajenn_orchestra.orchestration import FreezeHandler, SpaWorker

from .conftest import attach_wire

CID = "cid-a"
USER = "mario"


def http_call(path: str = "/main", body: bytes = b"", **http: Any) -> dict[str, Any]:
    """The http CALL form as the front packs it."""
    call: dict[str, Any] = {
        "method": "GET",
        "path": path,
        "query_string": "who=mario",
        "headers": [["host", "site.example:8080"], ["cookie", f"spa_connection_id={CID}"]],
        "body": body,
        "client": ["10.0.0.9", 51234],
        "scheme": "https",
        "cid": CID,
    }
    call.update(http)
    return {"http": call, "identity": USER}


def body_of(served: dict[str, Any]) -> bytes:
    """The answer's body, out of the wire form."""
    return served["body"]


def headers_of(served: dict[str, Any]) -> dict[str, str]:
    return {name.lower(): value for name, value in served["headers"]}


class XT_AsgiWorker(SpaWorker):
    """A worker hosting an ASGI application, the way a consumer builds one.

    The application is built HERE and given this worker (N22: the core writes no
    live object into a scope), so it can call the worker's own verbs while it
    serves — which is what a real consumer does with `new_connection`.
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        self.seen: list[dict[str, Any]] = []
        self.asgi_app = self.application

    async def application(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        """Say back what arrived, and register the connection while serving."""
        message = await receive()
        self.seen.append(dict(scope))
        if self.connection_register.get(CID) is None:
            self.new_connection(CID, user=scope["genro.identity"])
        answer = (
            f"{scope['method']} {scope['path']}?{scope['query_string'].decode()} "
            f"for {scope['genro.identity']} body={message['body'].decode()}"
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 201,
                "headers": [(b"content-type", b"text/plain"), (b"x-worker", self.name.encode())],
            }
        )
        await send({"type": "http.response.body", "body": answer})


class XT_MixedWorker(SpaWorker):
    """A worker whose ASGI application delegates some paths to a legacy site.

    This is form B seen from the consumer's side: the router is the
    application's own, and `/legacy/...` goes to the WSGI callable through the
    core's adapter, built once with this worker in hand.
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        self.legacy = WsgiSeam(self.tiny_site, self)
        self.environs: list[dict[str, Any]] = []
        self.asgi_app = self.application

    def tiny_site(self, environ: dict[str, Any], start_response: Any) -> list[bytes]:
        """The legacy callable: it sets a cookie and redirects, like a real one."""
        self.environs.append(dict(environ))
        start_response(
            "302 Found",
            [
                ("Content-Type", "text/plain"),
                ("Set-Cookie", "legacy_session=abc; Path=/"),
                ("Location", "/legacy/next"),
            ],
        )
        return [f"legacy saw {environ['SCRIPT_NAME']}|{environ['PATH_INFO']}".encode()]

    async def application(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["path"].startswith("/legacy"):
            # The consumer's own choice: move the prefix into `root_path`, so
            # the legacy site keeps the view of its URLs it always had.
            delegated = {**scope, "root_path": "/legacy"}
            await self.legacy(delegated, receive, send)
            return
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"served by the new side"})


class XT_WholePathWorker(XT_MixedWorker):
    """The same delegation, with the path left whole and no ``root_path``."""

    async def application(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        await self.legacy(scope, receive, send)


def with_open_page(worker: SpaWorker, page_id: str = "p1") -> None:
    """Give the worker a page that already opened its channel.

    A message that names a page is refused unless that page said
    ``openchannel`` first (#68 phase 4), so a test that passes the page keys
    has to have a page.
    """
    worker.open_request_slot()
    worker.new_page(USER, page_id, connection_id=CID)
    worker.page_register.get(page_id)["wsx"] = True


def worker_of(worker_class: type, tmp_path: Any) -> SpaWorker:
    worker = worker_class(
        "standard_0001", freeze_handler=FreezeHandler(tmp_path / "frozen_users")
    )
    attach_wire(worker)
    return worker


@pytest.fixture
def asgi_worker(tmp_path):
    worker = worker_of(XT_AsgiWorker, tmp_path)
    yield worker
    worker.exit_process()


@pytest.fixture
def mixed_worker(tmp_path):
    worker = worker_of(XT_MixedWorker, tmp_path)
    yield worker
    worker.exit_process()


class TestAnAsgiApplicationAlone:
    async def test_the_request_reaches_it_as_a_scope(self, asgi_worker) -> None:
        served = await asgi_worker._serve_request(http_call(body=b"payload"))
        assert body_of(served) == b"GET /main?who=mario for mario body=payload"

    async def test_the_answer_carries_status_and_headers(self, asgi_worker) -> None:
        served = await asgi_worker._serve_request(http_call())
        assert served["status"] == 201
        assert headers_of(served)["x-worker"] == "standard_0001"

    async def test_the_scope_is_a_plausible_asgi_one(self, asgi_worker) -> None:
        await asgi_worker._serve_request(http_call())
        scope = asgi_worker.seen[0]
        assert scope["type"] == "http" and scope["asgi"]["version"] == "3.0"
        assert scope["scheme"] == "https" and scope["root_path"] == ""
        assert scope["server"] == ("site.example", 8080)
        assert scope["client"] == ("10.0.0.9", 51234)
        assert (b"cookie", f"spa_connection_id={CID}".encode()) in scope["headers"]

    async def test_the_identity_of_the_call_is_the_one_the_pool_routed_on(
        self, asgi_worker
    ) -> None:
        await asgi_worker._serve_request(http_call())
        assert asgi_worker.seen[0]["genro.identity"] == USER

    async def test_the_page_keys_are_absent_when_the_call_did_not_carry_them(
        self, asgi_worker
    ) -> None:
        await asgi_worker._serve_request(http_call())
        scope = asgi_worker.seen[0]
        assert "genro.page_id" not in scope and "genro.reply_path" not in scope

    async def test_the_page_keys_travel_when_the_call_carries_them(self, asgi_worker) -> None:
        with_open_page(asgi_worker)
        await asgi_worker._serve_request(
            http_call(page_id="p1", reply_path="/main/done")
        )
        scope = asgi_worker.seen[0]
        assert (scope["genro.page_id"], scope["genro.reply_path"]) == ("p1", "/main/done")

    async def test_the_application_registers_its_connection_while_serving(
        self, asgi_worker
    ) -> None:
        # The rows are the site's: the application reaches the worker because
        # whoever built it gave it the worker, not because a scope key did.
        await asgi_worker._serve_request(http_call())
        assert asgi_worker.connection_register.get(CID) is not None
        assert asgi_worker.request_slot.connection_id == CID


class TestAnAsgiApplicationDelegatingToTheLegacy:
    async def test_the_legacy_answer_comes_back_whole(self, mixed_worker) -> None:
        served = await mixed_worker._serve_request(http_call(path="/legacy/invoices"))
        assert served["status"] == 302
        assert body_of(served) == b"legacy saw /legacy|/invoices"

    async def test_set_cookie_and_location_travel_untouched(self, mixed_worker) -> None:
        served = await mixed_worker._serve_request(http_call(path="/legacy/invoices"))
        headers = headers_of(served)
        assert headers["set-cookie"] == "legacy_session=abc; Path=/"
        assert headers["location"] == "/legacy/next"

    async def test_the_legacy_sees_the_urls_it_always_saw(self, mixed_worker) -> None:
        await mixed_worker._serve_request(http_call(path="/legacy/invoices"))
        environ = mixed_worker.environs[0]
        assert (environ["SCRIPT_NAME"], environ["PATH_INFO"]) == ("/legacy", "/invoices")

    async def test_the_legacy_gets_the_facts_of_the_request(self, mixed_worker) -> None:
        await mixed_worker._serve_request(http_call(path="/legacy/x", body=b"sent"))
        environ = mixed_worker.environs[0]
        assert environ["REQUEST_METHOD"] == "GET"
        assert environ["QUERY_STRING"] == "who=mario"
        assert environ["wsgi.input"].read() == b"sent"
        assert environ["HTTP_COOKIE"] == f"spa_connection_id={CID}"
        assert environ["genro.identity"] == USER
        assert (environ["SERVER_NAME"], environ["SERVER_PORT"]) == ("site.example", "8080")

    async def test_the_new_side_is_served_by_the_application_itself(self, mixed_worker) -> None:
        served = await mixed_worker._serve_request(http_call(path="/v2/orders"))
        assert body_of(served) == b"served by the new side"
        assert mixed_worker.environs == []

    async def test_a_router_that_leaves_the_path_whole_is_served_too(self, tmp_path) -> None:
        worker = worker_of(XT_WholePathWorker, tmp_path)
        try:
            await worker._serve_request(http_call(path="/legacy/invoices"))
            environ = worker.environs[0]
            assert (environ["SCRIPT_NAME"], environ["PATH_INFO"]) == ("", "/legacy/invoices")
        finally:
            worker.exit_process()


class TestTheSeamAWorkerDeclares:
    def test_both_seams_assigned_is_refused_by_name(self, asgi_worker) -> None:
        asgi_worker.wsgi_app = lambda environ, start_response: [b""]
        with pytest.raises(RuntimeError, match="both assigned"):
            asgi_worker.hosted_app_seam

    def test_a_worker_that_hosts_nothing_says_so(self, tmp_path) -> None:
        worker = worker_of(SpaWorker, tmp_path)
        try:
            with pytest.raises(RuntimeError, match="hosts no application"):
                worker.hosted_app_seam
        finally:
            worker.exit_process()

    def test_the_shortcut_is_served_through_the_adapter(self, tmp_path) -> None:
        worker = worker_of(SpaWorker, tmp_path)
        try:
            worker.wsgi_app = lambda environ, start_response: [b""]
            seam = worker.hosted_app_seam
            assert isinstance(seam.asgi_app, WsgiSeam)
            assert seam.asgi_app.worker is worker
        finally:
            worker.exit_process()


class TestSynchronousWorkInsideARequest:
    async def test_run_sync_runs_in_the_slot_of_its_call(self, tmp_path) -> None:
        # What the application announces from a pool thread rides the reply of
        # the CALL it is serving: the slot follows the work onto the thread.
        class XT_SyncWorker(XT_AsgiWorker):
            async def application(self, scope, receive, send) -> None:
                await receive()
                slot_name = await self.run_sync(self.on_the_thread)
                await send({"type": "http.response.start", "status": 200, "headers": []})
                await send({"type": "http.response.body", "body": slot_name.encode()})

            def on_the_thread(self) -> str:
                self.new_connection(CID, user=USER)
                return type(self.request_slot).__name__

        worker = worker_of(XT_SyncWorker, tmp_path)
        try:
            worker.open_request_slot()
            served = await worker._serve_request(http_call())
            assert body_of(served) == b"RequestSlot"
            # The birth of a connection under an unseen user announces the
            # user first: both rode the slot the pool thread found.
            assert [event["op"] for event in worker.worker_events] == [
                "new_user",
                "new_connection",
            ]
        finally:
            worker.exit_process()


class TestTheFrozenUserComesBack:
    async def test_a_frozen_user_is_adopted_and_the_application_serves_him(
        self, worker_commander_lane, tmp_path
    ) -> None:
        # The whole road: the user is parked in the deposit, and the next
        # request wakes him — the ASGI application serves it like any other.
        lane = worker_commander_lane
        await lane.verb("new_connection", CID, user=USER)
        await lane.worker.freeze_designated_user(USER)
        assert lane.worker.user_register.get(USER) is None

        served_by = XT_AsgiWorker(
            "standard_0001", freeze_handler=lane.worker.freeze_handler
        )
        attach_wire(served_by)
        try:
            served = await served_by._serve_request(
                {**http_call(), "user_frozen": True}
            )
            assert body_of(served).startswith(b"GET /main")
            assert served_by.user_register.get(USER) is not None
        finally:
            served_by.exit_process()


class TestTheWorkerThatHostsOnlyWsgi:
    async def test_the_shortcut_serves_the_request_end_to_end(self, tmp_path) -> None:
        # The bridge's own shape, unchanged: assign `wsgi_app`, and the core
        # takes it through the adapter without the consumer knowing.
        seen: list[dict[str, Any]] = []

        def site(environ: dict[str, Any], start_response: Any) -> list[bytes]:
            seen.append(dict(environ))
            start_response("200 OK", [("Content-Type", "text/plain")])
            return [b"the legacy answered"]

        worker = worker_of(SpaWorker, tmp_path)
        try:
            worker.wsgi_app = site
            served = await worker._serve_request(http_call())
            assert body_of(served) == b"the legacy answered"
            assert seen[0]["PATH_INFO"] == "/main"
            assert seen[0]["genro.identity"] == USER
        finally:
            worker.exit_process()

    async def test_the_body_of_a_post_reaches_the_legacy(self, tmp_path) -> None:
        def site(environ: dict[str, Any], start_response: Any) -> list[bytes]:
            sent = environ["wsgi.input"].read()
            start_response("200 OK", [("Content-Type", "text/plain")])
            return [sent]

        worker = worker_of(SpaWorker, tmp_path)
        try:
            worker.wsgi_app = site
            served = await worker._serve_request(http_call(method="POST", body=b"a=1&b=2"))
            assert body_of(served) == b"a=1&b=2"
        finally:
            worker.exit_process()


class TestTheContractOfTheTwoSeams:
    async def test_a_second_read_of_the_body_says_the_client_is_gone(self, tmp_path) -> None:
        # The ASGI contract after the body: an application that reads twice
        # must not hang waiting for a chunk that will never come.
        read: list[str] = []

        class XT_TwiceWorker(SpaWorker):
            def __init__(self, name: str, **kwargs: Any) -> None:
                super().__init__(name, **kwargs)
                self.asgi_app = self.application

            async def application(self, scope, receive, send) -> None:
                read.append((await receive())["type"])
                read.append((await receive())["type"])
                await send({"type": "http.response.start", "status": 200, "headers": []})
                await send({"type": "http.response.body", "body": b""})

        worker = worker_of(XT_TwiceWorker, tmp_path)
        try:
            await worker._serve_request(http_call())
            assert read == ["http.request", "http.disconnect"]
        finally:
            worker.exit_process()

    async def test_the_legacy_iterable_is_closed_as_the_spec_requires(self, tmp_path) -> None:
        closed: list[bool] = []

        class XT_ClosingBody:
            def __iter__(self):
                return iter([b"done"])

            def close(self) -> None:
                closed.append(True)

        def site(environ: dict[str, Any], start_response: Any) -> Any:
            start_response("200 OK", [])
            return XT_ClosingBody()

        worker = worker_of(SpaWorker, tmp_path)
        try:
            worker.wsgi_app = site
            served = await worker._serve_request(http_call())
            assert body_of(served) == b"done" and closed == [True]
        finally:
            worker.exit_process()

    async def test_the_deprecated_write_leads_the_body(self, tmp_path) -> None:
        # PEP 3333: what `write` put down comes before what the iterable yields.
        def site(environ: dict[str, Any], start_response: Any) -> list[bytes]:
            write = start_response("200 OK", [])
            write(b"first ")
            return [b"then"]

        worker = worker_of(SpaWorker, tmp_path)
        try:
            worker.wsgi_app = site
            served = await worker._serve_request(http_call())
            assert body_of(served) == b"first then"
        finally:
            worker.exit_process()

    async def test_the_client_address_reaches_the_legacy(self, tmp_path) -> None:
        def site(environ: dict[str, Any], start_response: Any) -> list[bytes]:
            start_response("200 OK", [])
            return [f"{environ['REMOTE_ADDR']}:{environ['REMOTE_PORT']}".encode()]

        worker = worker_of(SpaWorker, tmp_path)
        try:
            worker.wsgi_app = site
            served = await worker._serve_request(http_call())
            assert body_of(served) == b"10.0.0.9:51234"
        finally:
            worker.exit_process()

    async def test_repeated_headers_reach_the_legacy_the_way_pep_3333_wants(
        self, tmp_path
    ) -> None:
        # Duplicates are joined by a comma, cookies by "; " — a comma there
        # would fuse two cookies into one mangled value.
        def site(environ: dict[str, Any], start_response: Any) -> list[bytes]:
            start_response("200 OK", [])
            return [f"{environ['HTTP_X_TWICE']}|{environ['HTTP_COOKIE']}".encode()]

        worker = worker_of(SpaWorker, tmp_path)
        try:
            worker.wsgi_app = site
            served = await worker._serve_request(
                http_call(
                    headers=[
                        ["x-twice", "one"],
                        ["x-twice", "two"],
                        ["cookie", "a=1"],
                        ["cookie", "b=2"],
                    ]
                )
            )
            assert body_of(served) == b"one,two|a=1; b=2"
        finally:
            worker.exit_process()

    async def test_a_delegation_after_the_body_was_read_sends_an_empty_one(
        self, tmp_path
    ) -> None:
        # The application read the body itself and only then delegated: the
        # adapter finds the disconnect, and the legacy gets no body rather than
        # waiting for one.
        class XT_LateWorker(SpaWorker):
            def __init__(self, name: str, **kwargs: Any) -> None:
                super().__init__(name, **kwargs)
                self.legacy = WsgiSeam(self.site, self)
                self.asgi_app = self.application
                self.seen_body: list[bytes] = []

            def site(self, environ: dict[str, Any], start_response: Any) -> list[bytes]:
                self.seen_body.append(environ["wsgi.input"].read())
                start_response("200 OK", [])
                return [b"ok"]

            async def application(self, scope, receive, send) -> None:
                await receive()
                await self.legacy(scope, receive, send)

        worker = worker_of(XT_LateWorker, tmp_path)
        try:
            await worker._serve_request(http_call(body=b"read by the app"))
            assert worker.seen_body == [b""]
        finally:
            worker.exit_process()

    async def test_the_page_keys_reach_the_legacy_too(self, mixed_worker) -> None:
        with_open_page(mixed_worker)
        await mixed_worker._serve_request(
            http_call(path="/legacy/x", page_id="p1", reply_path="/main/done")
        )
        environ = mixed_worker.environs[0]
        assert (environ["genro.page_id"], environ["genro.reply_path"]) == ("p1", "/main/done")


class TestAMessageOfAPage:
    async def test_a_page_that_never_opened_its_channel_is_refused(self, asgi_worker) -> None:
        # A refusal of the CLIENT, with a status of its own: the browser reads
        # 409 and these words, not the 502 of a site that broke (#70 C).
        asgi_worker.open_request_slot()
        asgi_worker.new_page(USER, "p1", connection_id=CID)
        with pytest.raises(HTTPException, match="no open channel") as refused:
            await asgi_worker._serve_request(http_call(page_id="p1"))
        assert refused.value.status == 409

    async def test_a_page_this_worker_never_saw_is_refused(self, asgi_worker) -> None:
        with pytest.raises(HTTPException, match="no open channel"):
            await asgi_worker._serve_request(http_call(page_id="never-born"))

    async def test_a_request_that_names_no_page_passes(self, asgi_worker) -> None:
        # An ordinary http request of the site names no page and is untouched.
        served = await asgi_worker._serve_request(http_call())
        assert served["status"] == 201


class TestWhatTheSeamRefuses:
    async def test_an_application_that_answers_nothing_is_an_explicit_error(
        self, tmp_path
    ) -> None:
        class XT_SilentWorker(SpaWorker):
            def __init__(self, name: str, **kwargs: Any) -> None:
                super().__init__(name, **kwargs)
                self.asgi_app = self.application

            async def application(self, scope, receive, send) -> None:
                await asyncio.sleep(0)

        worker = worker_of(XT_SilentWorker, tmp_path)
        try:
            with pytest.raises(RuntimeError, match="did not start a response"):
                await worker._serve_request(http_call())
        finally:
            worker.exit_process()
