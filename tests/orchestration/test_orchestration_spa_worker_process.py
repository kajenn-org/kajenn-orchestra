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

"""SpaWorker on the wire: the shell, the envelopes, the photo, the wire that falls.

Everything here is real except the level above the wire: a real Unix socket, the
package's own connector and frames, the package's own ``WorkerEntry`` running
the worker exactly as the spawned child does — it is only run on this same loop
instead of in another process, which is what makes the story watchable step by
step. What is doubled is the handler above the connector, because the
GroupHandler and the Commander are Macro 3's.

The last test spawns a REAL child through the M1 ``WorkerHandler``, and there the
entry is the process it is meant to be: the site answers over the wire, the wire
is then taken away, and the orphan ends its own process leaving the deposit
empty — a process that lost its wire saves nothing.

The sockets and the deposit live under a short ``mkdtemp`` root: the system caps
a UDS path at about a hundred characters and pytest's own directory is already
past it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any

import pytest


from kajenn_orchestra.spa_app import SPA_CONNECTION_ID_COOKIE
from tests.orchestration.frame_helpers import call_endpoint, control_frame
from kajenn.channel.frame import FrameStream
from kajenn_orchestra.orchestration import (
    FreezeHandler,
    SpaWorker,
    WorkerEntry,
    WorkerHandler,
)
from kajenn_orchestra.orchestration.worker_connector import (
    REPLY_METHOD,
    ENVELOPE_SLOT_WORKER_SNAPSHOT,
    WorkerConnector,
)
from kajenn_orchestra.orchestration.worker_entry import DEFAULT_WORKER_CLASS

from .group_stub import GroupStub
from .conftest import kill_process, wait_for
from kajenn_orchestra.orchestration.worker_handler import (
    DROP_CONNECTION_OP_PATH,
    DROP_USER_OP_PATH,
    FREEZE_USER_OP_PATH,
    PING_OP_PATH,
    QUIT_OP_PATH,
    WORKER_ENV_VAR,
)

WORKER_NAME = "standard_0001"
GROUP = "standard"
ENTRY_MODULE = "kajenn_orchestra.orchestration.worker_entry"
ECHO_WORKER = f"{__name__}:EchoWorker"
ENGINE_WORKER = f"{__name__}:EngineWorker"
LONG_TTL = 30.0
BROKEN_PATH = "/falls_over"


class EchoWorker(SpaWorker):
    """A worker hosting one tiny WSGI site: the seam a subclass fills, played here.

    The real filler is the genropy-asgi bridge, whose worker mounts a whole
    site. This one answers with the facts of the request, which is all a test
    needs to know the environ was synthesized and the answer came back whole.
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        self.wsgi_app = self.echo_site

    def echo_site(self, environ: dict[str, Any], start_response: Any) -> list[bytes]:
        """The WSGI callable: say back who asked, and for what — or break on demand.

        Like a real site, it BAPTISES while serving (the rows are the site's to
        bear): a named identity whose connection this worker does not hold
        registers one, under the id the cookie carries — the core-only world,
        where the site is content with the id it was given.
        """
        if environ["PATH_INFO"] == BROKEN_PATH:
            raise RuntimeError("the site fell over")
        identity = environ["genro.identity"]
        cid = cid_of(environ)
        if identity is not None and self.connection_register.get(cid) is None:
            self.new_connection(cid, user=identity)
        start_response("200 OK", [("Content-Type", "text/plain"), ("X-Worker", self.name)])
        return [f"{environ['REQUEST_METHOD']} {environ['PATH_INFO']} for {identity}".encode()]


class EngineWorker(SpaWorker):
    """A worker that declares a ``group_engine``: the seam the bridge's own fills.

    The real one receives a built genropy site. This one only keeps what it was
    given, which is all a test needs to know the object crossed the constructor.
    """

    def __init__(self, name: str, *, group_engine: Any, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        self.group_engine = group_engine


class HandlerStub:
    """The WorkerHandler seen from the wire: the envelopes it takes, what it is told.

    The wire hands every envelope over whole and writes back down whatever comes
    out, so this stub plays the fold as well: it files the photo an envelope
    carried, keeps the worker events for the assertions, and answers with
    nothing — which is what the real chain composes.
    It doubles as the level above the handler in the real-child test at the end,
    where the only thing asked of a group is the wake — so it answers for that
    too, and counts it.
    """

    def __init__(self) -> None:
        self.worker_snapshot: dict[str, Any] | None = None
        self.announced: list[dict[str, Any]] = []
        self.lost = 0
        self.wakes = 0

    def read_envelope(self, envelope: dict[str, Any]) -> dict[str, Any]:
        """Read the envelope as the three layers would; nothing travels back down."""
        if ENVELOPE_SLOT_WORKER_SNAPSHOT in envelope:
            self.worker_snapshot = envelope[ENVELOPE_SLOT_WORKER_SNAPSHOT]
        self.announced.extend(envelope.get("worker_events") or ())
        return {}

    def on_child_lost(self) -> None:
        self.lost += 1

    def ping_now(self) -> None:
        self.wakes += 1


class Wire:
    """One worker's socket with its handler above it, both on this same loop.

    ``take`` runs the package's own ``WorkerEntry`` against this socket, so what
    is under test is the very shell the spawned child runs — its worker is
    reachable afterwards for the assertions the wire alone cannot make.
    """

    def __init__(self, socket_path: Path, deposit_path: Path) -> None:
        self.socket_path = socket_path
        self.deposit_path = deposit_path
        self.handler = HandlerStub()
        self.connector = WorkerConnector(self.handler, socket_path)
        self.entries: list[WorkerEntry] = []
        self.lives: list[asyncio.Task[None]] = []

    def spawn_config(self, worker_class: str, kwargs: dict[str, Any]) -> dict[str, Any]:
        """The seven-key payload, exactly as a WorkerHandler writes it."""
        return {
            "name": WORKER_NAME,
            "uds_url": self.connector.address,
            "frozen_users_path": str(self.deposit_path),
            "main_threadpool_size": 4,
            "aux_threadpool_size": 1,
            "worker_class": worker_class,
            "kwargs": {"group": GROUP, "worker_snapshot_ttl": LONG_TTL, **kwargs},
        }

    async def start(self) -> None:
        await self.connector.start()

    async def take(self, worker_class: str = ECHO_WORKER, **kwargs: Any) -> SpaWorker:
        """Run one worker's whole life against this wire, and hand it over."""
        entry = WorkerEntry(config=self.spawn_config(worker_class, kwargs))
        self.entries.append(entry)
        self.lives.append(asyncio.create_task(entry.serve()))
        await wait_for(lambda: self.connector.connected)
        return entry.worker

    async def stop(self) -> None:
        """Close the wire and let every worker on it finish its own end."""
        await self.connector.stop()
        for life in self.lives:
            life.cancel()
            try:
                await life
            except asyncio.CancelledError:
                pass


class ParentWire:
    """A bare parent: it answers the presentation, and it can break the protocol.

    No connector above it, so what it proves is what a worker does when the thing
    on the other end stops speaking the language — a picture the package's own
    wire, which never writes nonsense, cannot produce.
    """

    def __init__(self, socket_path: Path, deposit_path: Path) -> None:
        self.socket_path = socket_path
        self.deposit_path = deposit_path
        self.server: asyncio.Server | None = None
        self.stream: FrameStream | None = None
        self.entries: list[WorkerEntry] = []
        self.lives: list[asyncio.Task[None]] = []

    @property
    def address(self) -> str:
        return f"uds:{self.socket_path}"

    async def start(self) -> None:
        self.server = await asyncio.start_unix_server(self.serve, path=str(self.socket_path))

    async def take(self) -> SpaWorker:
        """Run one worker's life against this parent, and hand it over presented."""
        entry = WorkerEntry(
            config={
                "name": WORKER_NAME,
                "uds_url": self.address,
                "frozen_users_path": str(self.deposit_path),
                "main_threadpool_size": 2,
                "aux_threadpool_size": 1,
                "worker_class": ECHO_WORKER,
                "kwargs": {"group": GROUP, "worker_snapshot_ttl": 0},
            }
        )
        self.entries.append(entry)
        self.lives.append(asyncio.create_task(entry.serve()))
        await wait_for(lambda: entry.worker is not None and entry.worker.stream is not None)
        return entry.worker

    async def stop(self) -> None:
        """Let the workers go first: a server waits for the connections it holds."""
        for life in self.lives:
            life.cancel()
            try:
                await life
            except asyncio.CancelledError:
                pass
        if self.stream is not None:
            await self.stream.close()
        self.server.close()
        await self.server.wait_closed()

    async def send_nonsense(self) -> None:
        """Write something that is not a wsx envelope: the protocol violation drill."""
        payload = b"this is not a wsx envelope"
        self.stream.writer.write(len(payload).to_bytes(4, "big") + payload)
        await self.stream.writer.drain()

    async def serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        stream = self.stream = FrameStream(reader, writer)
        while True:
            frame = await stream.read()
            if frame is None:
                return
            await stream.write(
                control_frame(
                    id=frame.id,
                    method=REPLY_METHOD,
                    path=frame.path,
                    data={},
                )
            )


def http_call(
    cid: str, identity: str | None = None, *, path: str = "/invoices", **payload: Any
) -> dict[str, Any]:
    """The http CALL form as the front packs it: the request, and who it is for.

    ``identity`` travels verbatim: None IS the anonymous first visit, never a
    name derived from the cookie — the vertex mints nobody.
    """
    return {
        "http": {
            "method": "GET",
            "path": path,
            "query_string": "",
            "headers": [
                ["host", "site.example:8080"],
                ["cookie", f"{SPA_CONNECTION_ID_COOKIE}={cid}"],
            ],
            "body": "",
            "cid": cid,
        },
        "identity": identity,
        **payload,
    }


def cid_of(environ: dict[str, Any]) -> str | None:
    """The connection a request carries, read where a real site reads it."""
    morsel = SimpleCookie(environ.get("HTTP_COOKIE", "")).get(SPA_CONNECTION_ID_COOKIE)
    return None if morsel is None else morsel.value


def body_of(reply: dict[str, Any]) -> str:
    """The WSGI answer's body, decoded out of the wire form."""
    return reply["result"]["body"].decode()


def announced(reply: dict[str, Any]) -> list[str]:
    """The protocol names the reply carried up, in order."""
    return [event["op"] for event in reply["worker_events"]]


def age_user(worker: SpaWorker, user: str, seconds: float) -> None:
    """Push a user and his connections that many seconds into the past.

    The forced step is what makes a restamp visible: a clock that was not
    written again stays where this put it.
    """
    item = worker.user_register.get(user)
    for one in [item] + [worker.connection_register.get(cid) for cid in item["connections"]]:
        for clock in ("last_refresh_ts", "last_user_ts", "last_rpc_ts"):
            one[clock] -= seconds


@pytest.fixture
def deposit(short_root):
    """The deposit as the parent side reads it — the same root the worker is given."""
    return FreezeHandler(short_root / "frozen_users")


@pytest.fixture
async def wire(short_root, deposit):
    one = Wire(short_root / "w.sock", short_root / "frozen_users")
    await one.start()
    yield one
    await one.stop()


@pytest.fixture
async def parent_wire(short_root, deposit):
    parent = ParentWire(short_root / "p.sock", short_root / "frozen_users")
    await parent.start()
    yield parent
    await parent.stop()


# ----------------------------------------------------------------------
# The presentation: a live process has a photo, and the store on board
# ----------------------------------------------------------------------


async def test_the_presentation_carries_the_first_photo_and_brings_the_store_home(wire):
    await wire.take()

    photo = dict(wire.handler.worker_snapshot)
    pss_bytes = photo.pop("pss_bytes")
    assert pss_bytes is None or pss_bytes >= 0
    rss_bytes = photo.pop("rss_bytes")
    assert rss_bytes is None or rss_bytes >= 0
    assert photo == {
        "pid": os.getpid(),
        "name": WORKER_NAME,
        "group": GROUP,
        "user_count": 0,
        "connection_count": 0,
        "page_count": 0,
        "connections": {},
        "users": {},
    }


async def test_the_beat_is_answered_and_asks_for_nothing_else(wire):
    await wire.take()

    reply = await wire.connector.call(PING_OP_PATH, timeout=5.0)

    assert reply["result"] == {}
    assert reply["worker_events"] == []


async def test_an_op_nobody_here_knows_is_refused_by_name(wire):
    await wire.take()

    reply = await wire.connector.call("/group/nothing_of_the_kind", timeout=5.0)

    assert reply["error"].startswith("NotFound")


# ----------------------------------------------------------------------
# The http form: the row first, the site after
# ----------------------------------------------------------------------


async def test_an_http_call_is_served_by_the_site_the_subclass_assigned(wire):
    await wire.take()

    reply = await call_endpoint(
        wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0
    )

    assert reply["result"]["status"] == 200
    assert ["X-Worker", WORKER_NAME] in reply["result"]["headers"]
    assert body_of(reply) == "GET /invoices for mario"


async def test_the_request_finds_its_row_born_and_its_clocks_stamped(wire):
    worker = await wire.take()
    before = time.time()

    reply = await call_endpoint(
        wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0
    )

    assert announced(reply) == ["new_user", "new_connection"]
    assert worker.connection_register.get("cid-a")["user"] == "mario"
    assert worker.user_register.get("mario")["state"] == "active"
    assert worker.user_register.get("mario")["last_rpc_ts"] >= before
    assert worker.connection_register.get("cid-a")["last_rpc_ts"] >= before


async def test_every_served_call_writes_the_real_clock_again(wire):
    worker = await wire.take()
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)
    first = worker.user_register.get("mario")["last_rpc_ts"]
    age_user(worker, "mario", 60)

    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)

    assert worker.user_register.get("mario")["last_rpc_ts"] > first
    assert worker.connection_register.get("cid-a")["last_rpc_ts"] > first


async def test_an_anonymous_request_touches_no_register(wire):
    """The doctrine of 2026-08-21: the vertex mints nobody, and a site that
    baptises nobody leaves nothing — no rows, no announcements, no leak."""
    worker = await wire.take()

    reply = await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a"), timeout=5.0)

    assert body_of(reply) == "GET /invoices for None"
    assert worker.connection_register.keys() == []
    assert worker.user_register.keys() == []
    assert announced(reply) == []


async def test_the_second_request_of_a_connection_costs_no_new_row(wire):
    worker = await wire.take()
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)

    reply = await call_endpoint(
        wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0
    )

    assert announced(reply) == []
    assert worker.user_register.keys() == ["mario"]


async def test_a_frozen_user_comes_home_when_the_envelope_says_so(wire, deposit):
    worker = await wire.take()
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)
    await wire.connector.call(FREEZE_USER_OP_PATH, {"user": "mario"}, timeout=5.0)
    assert deposit.read_user_register_item("mario") is not None

    reply = await call_endpoint(
        wire.connector, "/site/invoices", http_call("cid-a", "mario", user_frozen=True), timeout=5.0
    )

    assert "user_adopted" in announced(reply)
    assert worker.user_register.get("mario")["state"] == "active"
    assert deposit.read_user_register_item("mario") is None


async def test_a_deposit_that_never_frees_the_folder_answers_the_call_with_its_failure(
    wire, deposit
):
    await wire.take(deposit_lock_wait_limit=0.05)
    deposit.take_lock("mario", "standard_0002")

    reply = await call_endpoint(
        wire.connector, "/site/invoices", http_call("cid-a", "mario", user_frozen=True), timeout=5.0
    )

    assert "TimeoutError" in reply["error"]
    assert "result" not in reply


async def test_a_site_that_falls_over_answers_with_its_failure_and_frees_the_user(wire):
    await wire.take()
    # A first request baptises him: the broken one below must not hold him.
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)

    reply = await call_endpoint(
        wire.connector,
        "/site/falls_over",
        http_call("cid-a", "mario", path=BROKEN_PATH),
        timeout=5.0,
    )

    assert reply["error"] == "RuntimeError: the site fell over"
    frozen = await wire.connector.call(FREEZE_USER_OP_PATH, {"user": "mario"}, timeout=5.0)
    assert frozen["result"] == {"frozen": "mario"}


async def test_a_request_that_names_no_connection_is_answered_with_its_failure(wire):
    await wire.take()
    payload = http_call("cid-a", "mario")
    del payload["http"]["cid"]

    reply = await call_endpoint(wire.connector, "/site/invoices", payload, timeout=5.0)

    assert reply["error"] == "ValueError: HTTP routing metadata is missing cid"


async def test_a_worker_with_no_site_refuses_the_form_and_registers_nobody(wire):
    worker = await wire.take(f"{SpaWorker.__module__}:SpaWorker")

    reply = await call_endpoint(
        wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0
    )

    # The base worker lives and serves its orders; what it cannot do is serve a
    # request, and the seam property is what says so (#68 N29).
    assert "hosts no application" in reply["error"]
    assert worker.user_register.keys() == []


# ----------------------------------------------------------------------
# The photo: from birth, on a change, and never twice inside the ttl
# ----------------------------------------------------------------------


async def test_two_replies_inside_the_ttl_carry_one_photo(wire):
    await wire.take(worker_snapshot_ttl=0.05)
    await asyncio.sleep(0.06)

    first = await wire.connector.call(PING_OP_PATH, timeout=5.0)
    second = await wire.connector.call(PING_OP_PATH, timeout=5.0)
    await asyncio.sleep(0.06)
    third = await wire.connector.call(PING_OP_PATH, timeout=5.0)

    assert ENVELOPE_SLOT_WORKER_SNAPSHOT in first
    assert ENVELOPE_SLOT_WORKER_SNAPSHOT not in second
    assert (
        third[ENVELOPE_SLOT_WORKER_SNAPSHOT]["pid"] == first[ENVELOPE_SLOT_WORKER_SNAPSHOT]["pid"]
    )


async def test_a_user_arriving_puts_the_photo_on_the_envelope_whatever_the_ttl(wire):
    await wire.take()
    await wire.connector.call(PING_OP_PATH, timeout=5.0)

    arrival = await call_endpoint(
        wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0
    )
    quiet = await wire.connector.call(PING_OP_PATH, timeout=5.0)

    assert arrival[ENVELOPE_SLOT_WORKER_SNAPSHOT]["users"]["mario"] == {
        "item": arrival[ENVELOPE_SLOT_WORKER_SNAPSHOT]["users"]["mario"]["item"],
        "transfer_flag": None,
    }
    assert arrival[ENVELOPE_SLOT_WORKER_SNAPSHOT]["user_count"] == 1
    assert ENVELOPE_SLOT_WORKER_SNAPSHOT not in quiet


async def test_a_user_leaving_puts_the_photo_on_the_envelope_too(wire):
    await wire.take()
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)
    await wire.connector.call(PING_OP_PATH, timeout=5.0)

    frozen = await wire.connector.call(FREEZE_USER_OP_PATH, {"user": "mario"}, timeout=5.0)

    assert announced(frozen) == ["user_frozen"]
    assert frozen[ENVELOPE_SLOT_WORKER_SNAPSHOT]["user_count"] == 0


async def test_a_user_waking_puts_the_photo_on_the_envelope_too(wire, deposit):
    await wire.take()
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)
    await wire.connector.call(FREEZE_USER_OP_PATH, {"user": "mario"}, timeout=5.0)
    await wire.connector.call(PING_OP_PATH, timeout=5.0)

    woken = await call_endpoint(
        wire.connector, "/site/invoices", http_call("cid-a", "mario", user_frozen=True), timeout=5.0
    )
    quiet = await wire.connector.call(PING_OP_PATH, timeout=5.0)

    assert announced(woken) == ["user_adopted", "new_connection"]
    assert woken[ENVELOPE_SLOT_WORKER_SNAPSHOT]["users"]["mario"]["item"]["state"] == "active"
    assert woken[ENVELOPE_SLOT_WORKER_SNAPSHOT]["user_count"] == 1
    assert ENVELOPE_SLOT_WORKER_SNAPSHOT not in quiet


async def test_the_photo_carries_the_flag_the_transfers_decided(wire):
    worker = await wire.take(worker_snapshot_ttl=0)
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)
    worker.plan_transfers(transfer_users=["mario"])

    reply = await wire.connector.call(PING_OP_PATH, timeout=5.0)

    photo = reply[ENVELOPE_SLOT_WORKER_SNAPSHOT]
    assert photo["users"]["mario"]["transfer_flag"] == "T"
    assert photo["users"]["mario"]["item"]["state"] == "active"
    assert photo["users"]["mario"]["item"]["connection_count"] == 1
    assert photo["connections"]["cid-a"]["user"] == "mario"


# ----------------------------------------------------------------------
# The three ops: everybody out, one user out, one connection out
# ----------------------------------------------------------------------


async def test_the_order_to_leave_is_answered_with_everybody_flagged_and_then_taken(wire, deposit):
    worker = await wire.take(worker_snapshot_ttl=0, transfer_start_delay=0.1)
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)
    await call_endpoint(wire.connector, "/site/orders", http_call("cid-b", "anna"), timeout=5.0)

    leaving = await wire.connector.call(QUIT_OP_PATH, timeout=5.0)

    flags = leaving[ENVELOPE_SLOT_WORKER_SNAPSHOT]["users"]
    assert {user: pair["transfer_flag"] for user, pair in flags.items()} == {
        "mario": "T",
        "anna": "T",
    }
    await wait_for(lambda: worker.exited)
    assert deposit.read_user_register_item("mario") is not None
    assert deposit.read_user_register_item("anna") is not None


async def test_the_order_to_drop_a_user_answers_with_what_it_announced(wire):
    worker = await wire.take()
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)

    reply = await wire.connector.call(DROP_USER_OP_PATH, {"user": "mario"}, timeout=5.0)

    assert announced(reply) == ["drop_connections", "drop_user"]
    assert worker.user_register.keys() == []
    assert worker.connection_register.keys() == []


async def test_the_order_to_drop_a_connection_answers_with_what_it_announced(wire):
    worker = await wire.take()
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)

    reply = await wire.connector.call(DROP_CONNECTION_OP_PATH, {"cid": "cid-a"}, timeout=5.0)

    assert announced(reply) == ["drop_connection", "drop_user"]
    assert worker.connection_register.keys() == []


async def test_the_freeze_order_is_answered_once_the_user_is_parked(wire, deposit):
    worker = await wire.take()
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)

    reply = await wire.connector.call(FREEZE_USER_OP_PATH, {"user": "mario"}, timeout=5.0)

    assert reply["result"] == {"frozen": "mario"}
    assert "user_frozen" in announced(reply)
    assert worker.user_register.keys() == []
    assert deposit.read_user_register_item("mario") is not None


async def test_the_freeze_order_for_a_stranger_is_refused_in_the_reply(wire, deposit):
    await wire.take()

    reply = await wire.connector.call(FREEZE_USER_OP_PATH, {"user": "nobody"}, timeout=5.0)

    assert "no user 'nobody' here" in reply["error"]
    assert "result" not in reply
    assert deposit.read_user_register_item("nobody") is None


# ----------------------------------------------------------------------
# The envelope that has no lane
# ----------------------------------------------------------------------


async def test_an_envelope_that_is_not_an_order_is_denounced_and_nothing_else(wire, caplog):
    caplog.set_level(logging.WARNING)
    worker = await wire.take()

    # A REPLY has a lane of its own now — the answer to a call this worker
    # placed upward — so what is denounced is whatever is neither of the two.
    worker.handle_frame(control_frame(method="POST", path="/op/anything"))

    assert "unexpected envelope POST" in caplog.text
    assert worker.exited is False


async def test_a_violation_of_the_protocol_ends_the_wire_like_a_death(parent_wire):
    worker = await parent_wire.take()

    await parent_wire.send_nonsense()

    await wait_for(lambda: worker.exited)


# ----------------------------------------------------------------------
# The wire that falls: the process ends and saves nothing
# ----------------------------------------------------------------------


async def test_a_dead_wire_ends_the_worker_and_writes_nothing_to_the_deposit(wire, deposit):
    worker = await wire.take()
    await call_endpoint(wire.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=5.0)

    await wire.connector.stop()

    await wait_for(lambda: worker.exited)
    assert deposit.user_folders == set()
    assert worker.user_register.keys() == ["mario"]


# ----------------------------------------------------------------------
# The spawn contract: what the shell refuses to guess
# ----------------------------------------------------------------------


def test_a_spawn_without_its_payload_is_a_contract_violation(monkeypatch):
    monkeypatch.delenv(WORKER_ENV_VAR, raising=False)

    with pytest.raises(SystemExit, match="is not set"):
        WorkerEntry()


def test_a_payload_that_is_not_an_object_is_refused_by_what_it_is(monkeypatch):
    monkeypatch.setenv(WORKER_ENV_VAR, "not json at all")
    with pytest.raises(SystemExit, match="not valid JSON"):
        WorkerEntry()

    monkeypatch.setenv(WORKER_ENV_VAR, '["a", "list"]')
    with pytest.raises(SystemExit, match="must be a JSON object"):
        WorkerEntry()


def test_a_payload_short_of_a_key_says_which_one(monkeypatch):
    monkeypatch.setenv(WORKER_ENV_VAR, '{"name": "standard_0001"}')

    with pytest.raises(SystemExit, match="missing uds_url, frozen_users_path"):
        WorkerEntry()


def test_the_payload_is_read_from_the_environment_the_handler_wrote(monkeypatch, short_root):
    monkeypatch.setenv(
        WORKER_ENV_VAR,
        json.dumps(
            {
                "name": WORKER_NAME,
                "uds_url": "uds:/nowhere.sock",
                "frozen_users_path": str(short_root / "frozen_users"),
                "main_threadpool_size": 8,
                "aux_threadpool_size": 2,
                "worker_class": None,
                "kwargs": {"group": GROUP},
            }
        ),
    )

    entry = WorkerEntry()

    assert entry.name == WORKER_NAME
    assert entry.main_threadpool_size == 8
    assert entry.aux_threadpool_size == 2
    assert entry.worker_class == DEFAULT_WORKER_CLASS


def test_a_worker_class_that_is_not_a_reference_is_refused(short_root):
    entry = WorkerEntry(
        config={
            "name": WORKER_NAME,
            "uds_url": "uds:/nowhere.sock",
            "frozen_users_path": str(short_root / "frozen_users"),
            "worker_class": "kajenn_orchestra.orchestration.spa_worker.SpaWorker",
        }
    )

    with pytest.raises(SystemExit, match="module.path:ClassName"):
        entry.build_worker()


def test_the_payload_naming_no_class_builds_the_worker_of_the_house(short_root):
    entry = WorkerEntry(
        config={
            "name": WORKER_NAME,
            "uds_url": "uds:/nowhere.sock",
            "frozen_users_path": str(short_root / "frozen_users"),
            "kwargs": {"group": GROUP},
        }
    )

    worker = entry.build_worker()

    assert type(worker) is SpaWorker
    assert worker.group == GROUP
    assert worker.freeze_handler.root_path == short_root / "frozen_users"


def test_the_group_engine_handed_to_the_entry_reaches_the_worker(short_root):
    engine = object()
    entry = WorkerEntry(
        config={
            "name": WORKER_NAME,
            "uds_url": "uds:/nowhere.sock",
            "frozen_users_path": str(short_root / "frozen_users"),
            "worker_class": ENGINE_WORKER,
            "kwargs": {"group": GROUP},
        },
        group_engine=engine,
    )

    worker = entry.build_worker()

    assert worker.group_engine is engine


def test_no_group_engine_is_passed_to_a_worker_that_was_handed_none(short_root):
    entry = WorkerEntry(
        config={
            "name": WORKER_NAME,
            "uds_url": "uds:/nowhere.sock",
            "frozen_users_path": str(short_root / "frozen_users"),
            "worker_class": ENGINE_WORKER,
            "kwargs": {"group": GROUP},
        }
    )

    # EngineWorker demands the keyword: its absence is what the failure says.
    with pytest.raises(TypeError, match="group_engine"):
        entry.build_worker()


# ----------------------------------------------------------------------
# The same story in a real child process
# ----------------------------------------------------------------------


@pytest.fixture
async def handler(short_root, repo_on_pythonpath):
    """A real WorkerHandler spawning the real entry; nothing of it outlives the test."""
    worker_handler = WorkerHandler(
        GroupStub(short_root / "frozen_users"),
        WORKER_NAME,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module=ENTRY_MODULE,
        main_threadpool_size=4,
        aux_threadpool_size=1,
        worker_class=ECHO_WORKER,
        worker_kwargs={"group": GROUP},
        process_ping_timeout=10.0,
    )
    yield worker_handler
    if worker_handler.process is not None:
        kill_process(worker_handler.process)
        await wait_for(lambda: not worker_handler.process.alive)
    await worker_handler.connector.stop()


async def test_a_real_child_serves_its_site_and_ends_alone_when_the_wire_goes(handler, deposit):
    # It is born in a process of its own, and presents itself with a photo that
    # already knows it is that process: the pid is the one the handler spawned.
    await handler.launch_process()
    assert handler.worker_snapshot["pid"] == handler.process.pid
    assert handler.worker_snapshot["user_count"] == 0

    # It is alive, and it serves: the request crosses the process boundary
    # emulated in the envelope and comes back as the site answered it.
    await handler.ping_process()
    reply = await call_endpoint(
        handler.connector, "/site/invoices", http_call("cid-a", "mario"), timeout=10.0
    )
    assert body_of(reply) == "GET /invoices for mario"
    assert announced(reply) == ["new_user", "new_connection"]

    # The wire is taken away under it. A process nobody can speak to any more is
    # not vouched for, so it saves nothing and ends its process by itself; its
    # user is lost at the vertex.
    await handler.connector.stop()

    await wait_for(lambda: not handler.process.alive, timeout=15.0)
    assert handler.process.exit_code == 0
    assert deposit.user_folders == set()
