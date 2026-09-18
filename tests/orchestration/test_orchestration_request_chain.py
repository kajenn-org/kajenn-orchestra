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

"""The chain of one request: from the cookie to the worker, and the refusals.

The subject is the WALK — who is minted, which group answers for him, which
worker is picked, what travels down the wire and which class comes back up. The
wire itself is a double: what a request puts on it and what it does with the
answer is this file's business, and how a frame crosses a socket has its own
tests, two files over. The workers are real ``WorkerHandler`` with no process
under them, exactly as the placement tests build them — a placement reads the
state and the last photo, and both are written straight in.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.orchestration.frame_helpers import control_frame, read_http_request, http_reply
from kajenn.http_record import HttpRecord

from kajenn_orchestra.orchestration import (
    AssignmentRefused,
    GroupHandler,
    SiteFailedRequest,
    SpaCommander,
    WorkerHandler,
)
from kajenn_orchestra.orchestration.spa_commander import SHAPE_REVIEW_SECONDS, SITE_PATH_PREFIX

#: The concession of these groups, so a photo of 780_000 bytes is a worker at 78%.
MEMORY_CEILING = 1_000_000

HOLD_TIMEOUT = 5.0



def minted(commander, cid: str) -> str:
    """The identity the site would baptise for this cookie, learned by the vertex.

    The old mint died with the doctrine (the cookie routes, the site names):
    tests stage the junction the fold of ``new_connection`` would have written.
    """
    user = f"guest_{cid}"
    commander.record_connection_user(cid, user)
    return user


class ConnectorDouble:
    """The wire seen from the chain: what it was called with, and what it answers.

    Args:
        reply: the REPLY payload to give back.
        failure: raised instead of answering, for the wire that is gone.
    """

    def __init__(self, reply: dict[str, Any] | None = None, failure: Exception | None = None):
        self.reply = reply if reply is not None else {"result": {"status": 200}}
        self.failure = failure
        #: One entry per call, as (path, payload) — what really went down.
        self.calls: list[tuple[str, Any]] = []

    async def call(self, path: str, data: Any = None, timeout: float | None = None) -> Any:
        self.calls.append((path, data))
        if self.failure is not None:
            raise self.failure
        return self.reply

    async def call_frame(self, frame, timeout=None):
        data = {**{k: v for k, v in frame.info.items() if k not in ("format", "cid")},
                "http": read_http_request(frame)}
        return http_reply(frame, await self.call(frame.path, data, timeout))


@pytest.fixture
def commander(short_root):
    return SpaCommander(short_root / "frozen_users")


@pytest.fixture
def group(commander, short_root):
    """A group nobody has launched anything in: its policies are the defaults."""
    return GroupHandler(
        commander,
        "standard",
        memory_concession_bytes=MEMORY_CEILING,
        worker_memory_max_percent=100.0,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module="never.launched",
    )


def worker_at(group, name: str, occupancy_percent: float = 10.0, **wire: Any) -> WorkerHandler:
    """One worker of the group at that occupancy, with a double for its wire."""
    worker_handler = WorkerHandler(group, name, **group.worker_settings)
    worker_handler.state = "running"
    worker_handler.worker_snapshot = {"rss_bytes": int(MEMORY_CEILING * occupancy_percent / 100)}
    worker_handler.connector = ConnectorDouble(**wire)
    group.worker_handler_map[name] = worker_handler
    return worker_handler


def request(path: str = "/invoices") -> dict[str, Any]:
    """The http form as the front packs it, minus the cid the chain adds."""
    return control_frame(method="CALL", path=SITE_PATH_PREFIX + path, data={"http": {
        "method": "GET",
        "path": path,
        "query_string": "",
        "headers": [["host", "site.example:8080"]],
        "body": "",
    }})



async def test_a_newcomer_travels_anonymous_and_the_site_baptises(commander, group):
    """The vertex mints nobody: a request with no connection goes to the
    reception, the site names one, and the fold writes the indexes at the fact."""
    worker_handler = worker_at(group, "standard_0001")

    reply = await commander.serve_request(None, request(), hold_timeout=HOLD_TIMEOUT)

    assert HttpRecord().decode_response(reply.payload)["status"] == 200
    # Nothing was minted: the request travelled anonymous, to the reception.
    assert commander.connection_user_map == {}
    assert commander.user_map == {}
    path, payload = worker_handler.connector.calls[0]
    assert path == f"{SITE_PATH_PREFIX}/invoices"
    assert payload == {
        "http": {**read_http_request(request()), "cid": None},
        "identity": None,
        "user_frozen": False,
    }
    # The site baptised while serving; its announcement is what writes the rungs.
    worker_handler.read_envelope(
        {
            "worker_events": [
                {
                    "op": "new_connection",
                    "worker": "standard_0001",
                    "user": "guest_legacy1",
                    "connection_id": "legacy-conn-1",
                }
            ]
        }
    )
    assert commander.connection_user_map == {"legacy-conn-1": "guest_legacy1"}
    assert "guest_legacy1" in commander.user_map
    assert group.user_worker_map["guest_legacy1"] == "standard_0001"


async def test_a_resident_goes_to_his_own_worker_and_is_placed_again_by_nobody(commander, group):
    first = worker_at(group, "standard_0001", occupancy_percent=70.0)
    second = worker_at(group, "standard_0002", occupancy_percent=10.0)
    commander.record_connection_user("cid-a", "mario")
    group.user_worker_map["mario"] = "standard_0002"
    commander.record_user_group("mario", "standard")

    await commander.serve_request("cid-a", request(), hold_timeout=HOLD_TIMEOUT)

    # The hottest-first walk would have chosen the first: a resident is not walked.
    assert first.connector.calls == []
    assert len(second.connector.calls) == 1


async def test_the_freezer_verdict_travels_with_the_request(commander, group):
    worker_handler = worker_at(group, "standard_0001")
    commander.record_connection_user("cid-a", "mario")
    commander.mark_user_frozen("mario")
    commander.record_user_group("mario", "standard")

    await commander.serve_request("cid-a", request(), hold_timeout=HOLD_TIMEOUT)

    _path, payload = worker_handler.connector.calls[0]
    assert payload["user_frozen"] is True


async def test_a_user_wakes_in_the_group_he_was_frozen_on(commander, short_root):
    """A user never changes group: his parcel was written by THAT group's code."""
    stable = GroupHandler(
        commander,
        "stable",
        memory_concession_bytes=MEMORY_CEILING,
        worker_memory_max_percent=100.0,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module="never.launched",
    )
    canary = GroupHandler(
        commander,
        "canary",
        memory_concession_bytes=MEMORY_CEILING,
        worker_memory_max_percent=100.0,
        instance_dir=short_root / "i",
        frozen_users_path=short_root / "frozen_users",
        entry_module="never.launched",
    )
    on_stable = worker_at(stable, "stable_0001")
    on_canary = worker_at(canary, "canary_0001")
    commander.record_connection_user("cid-a", "mario")
    commander.record_user_group("mario", "canary")
    commander.mark_user_frozen("mario")

    await commander.serve_request("cid-a", request(), hold_timeout=HOLD_TIMEOUT)

    assert commander.default_group == "stable"
    assert on_stable.connector.calls == []
    assert len(on_canary.connector.calls) == 1


async def test_a_pool_that_takes_nobody_refuses_and_says_when_to_come_back(commander, group):
    with pytest.raises(AssignmentRefused) as refused:
        await commander.serve_request("cid-a", request(), hold_timeout=HOLD_TIMEOUT)

    assert refused.value.retry_after == SHAPE_REVIEW_SECONDS
    assert commander.counters["requests_refused"] == 1


async def test_a_request_for_a_user_on_hold_leaves_the_moment_he_is_home(commander, group):
    worker_at(group, "standard_0001")
    commander.record_connection_user("cid-a", "mario")
    commander.record_user_group("mario", "standard")
    commander.hold_user("mario", "transfer_flag T")

    serving = asyncio.ensure_future(
        commander.serve_request("cid-a", request(), hold_timeout=HOLD_TIMEOUT)
    )
    await asyncio.sleep(0)
    assert not serving.done()

    commander.mark_user_adopted("mario")

    assert HttpRecord().decode_response((await serving).payload)["status"] == 200
    assert commander.counters["requests_refused"] == 0


async def test_a_hold_that_outlives_the_budget_is_a_refusal(commander, group):
    worker_at(group, "standard_0001")
    commander.record_connection_user("cid-a", "mario")
    commander.hold_user("mario", "transfer_flag T")

    with pytest.raises(AssignmentRefused) as refused:
        await commander.serve_request("cid-a", request(), hold_timeout=0.01)

    assert refused.value.user == "mario"
    assert refused.value.retry_after == SHAPE_REVIEW_SECONDS
    assert commander.counters["requests_refused"] == 1


async def test_the_budget_is_the_whole_wait_and_not_one_of_them(commander, group):
    """A user let go and held again spends the request's budget, he does not renew it.

    The keeper releases every hundredth of a second for a whole second, so each
    wait ends on its own: a budget spent per WAIT would let this request go round
    for as long as the keeper lives. The refusal has to arrive on the budget the
    request gave, and the clock is what says which of the two happened.
    """
    worker_at(group, "standard_0001")
    commander.record_connection_user("cid-a", "mario")
    commander.hold_user("mario", "transfer_flag T")

    async def let_go_and_hold_again() -> None:
        for _ in range(100):
            await asyncio.sleep(0.01)
            commander.mark_user_adopted("mario")
            commander.hold_user("mario", "transfer_flag T")

    keeper = asyncio.ensure_future(let_go_and_hold_again())
    started = asyncio.get_running_loop().time()
    with pytest.raises(AssignmentRefused):
        await commander.serve_request("cid-a", request(), hold_timeout=0.05)
    elapsed = asyncio.get_running_loop().time() - started
    keeper.cancel()

    assert elapsed < 0.5


async def test_a_site_that_failed_inside_its_process_is_not_a_refusal(commander, group):
    worker_at(group, "standard_0001", reply={"error": "ZeroDivisionError: division by zero"})

    with pytest.raises(SiteFailedRequest) as failed:
        await commander.serve_request("cid-a", request(), hold_timeout=HOLD_TIMEOUT)

    assert "ZeroDivisionError" in str(failed.value)
    # It is the upstream's failure: nobody was refused and nothing is owed a wait.
    assert commander.counters["requests_refused"] == 0


async def test_a_wire_that_is_gone_falls_through_to_the_caller(commander, group):
    """The window between a death and the round that buries it: loud, self-healing."""
    worker_at(group, "standard_0001", failure=ConnectionError("no child on the wire"))

    with pytest.raises(ConnectionError):
        await commander.serve_request("cid-a", request(), hold_timeout=HOLD_TIMEOUT)


async def test_two_requests_of_the_same_unknown_land_on_the_reception(commander, group):
    """Anonymous requests all go to ONE door by construction: the reception is
    the oldest living worker, so two unknowns cannot split before the site
    has baptised either of them."""
    first = worker_at(group, "standard_0001", occupancy_percent=70.0)
    second = worker_at(group, "standard_0002", occupancy_percent=10.0)

    await asyncio.gather(
        commander.serve_request("cid-a", request(), hold_timeout=HOLD_TIMEOUT),
        commander.serve_request("cid-a", request("/orders"), hold_timeout=HOLD_TIMEOUT),
    )

    assert len(first.connector.calls) == 2
    assert second.connector.calls == []
    # And no placement was written: the fold of the site's baptism owns that.
    assert group.user_worker_map == {}
