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

"""Contract: the orders a worker takes are a routing tree (#59, D59-9..14, block 2).

The CALLs that come DOWN the worker_commander_lane to a worker are resolved by name on the tree
the worker hosts, ``worker_dispatcher`` — not by a chain of ``if`` on the path.
The first segment names who ISSUES the order: ``group`` for the group's
(freeze_user, drop_user, drop_connection, quit, ping), ``commander`` for the
vertex's (observe, census, eval). ``answer_call`` resolves, calls, awaits when the
operation is a coroutine, and sends the one REPLY: the result, or the error —
``NotFound`` for a path nobody serves, ``TypeError`` for a payload that does
not fit the signature, both before any body runs. The http form is not an
order and stays out of the tree. A consumer attaches its own order class under
the issuer's branch, by a name of its own, and the vertex reaches it the same
way.

The tests place raw CALLs from a real ``WorkerHandler`` to a real ``SpaWorker``
over a real UDS. ``connector.call`` answers the REPLY data verbatim: ``result``
or ``error``.
"""

from __future__ import annotations

from typing import Any

from genro_routes import RoutingClass, route

from kajenn_orchestra.orchestration.worker_handler import (
    CENSUS_OP_PATH,
    DROP_CONNECTION_OP_PATH,
    EVAL_OP_PATH,
    OBSERVE_OP_PATH,
    PING_OP_PATH,
)

from .conftest import XT_WorkerCommanderLane


class XT_ExtraOrders(RoutingClass):
    """What a consumer attaches under the commander's orders: one order of its own."""

    def __init__(self, spa_worker: Any) -> None:
        self.spa_worker = spa_worker

    @route()
    def echo(self, **payload: Any) -> dict[str, Any]:
        return {"echo": payload, "worker": self.spa_worker.name}


async def order(
    worker_commander_lane: XT_WorkerCommanderLane,
    path: str,
    data: dict[str, Any] | None = None,
) -> Any:
    """Place one order on the worker the way the vertex does, and read the REPLY data."""
    return await worker_commander_lane.worker_handler.connector.call(path, data)


async def test_the_paths_name_who_issues_the_order():
    assert PING_OP_PATH == "/group/ping"
    assert CENSUS_OP_PATH == "/commander/census"


async def test_the_beat_is_answered(worker_commander_lane):
    assert (await order(worker_commander_lane, PING_OP_PATH))["result"] == {}


async def test_the_census_is_the_workers_own(worker_commander_lane):
    reply = await order(worker_commander_lane, CENSUS_OP_PATH, {})
    assert reply["result"]["name"] == worker_commander_lane.worker_name
    assert "user_register" in reply["result"]


async def test_eval_answers_the_repr_and_a_failing_expression_answers_the_error(
    worker_commander_lane,
):
    assert (await order(worker_commander_lane, EVAL_OP_PATH, {"expr": "1 + 1"}))["result"] == {
        "repr": "2"
    }
    failed = await order(worker_commander_lane, EVAL_OP_PATH, {"expr": "1 / 0"})
    assert "ZeroDivisionError" in failed["error"]


async def test_observe_switches_the_workers_observation(worker_commander_lane):
    assert worker_commander_lane.worker.observation_on is False
    assert (await order(worker_commander_lane, OBSERVE_OP_PATH, {"on": True}))["result"] == {}
    assert worker_commander_lane.worker.observation_on is True


async def test_dropping_a_connection_nobody_holds_is_answered_quietly(worker_commander_lane):
    assert (await order(worker_commander_lane, DROP_CONNECTION_OP_PATH, {"cid": "nobody"}))[
        "result"
    ] == {}


async def test_an_unknown_order_is_refused_by_name(worker_commander_lane):
    reply = await order(worker_commander_lane, "/group/nothing_here", {})
    assert "NotFound" in reply["error"]


async def test_an_order_that_does_not_fit_the_signature_is_refused_before_the_body_runs(
    worker_commander_lane,
):
    reply = await order(worker_commander_lane, EVAL_OP_PATH, {"nope": 1})
    assert "TypeError" in reply["error"]
    assert "expr" in reply["error"]


async def test_the_dispatcher_says_what_orders_the_worker_takes(worker_commander_lane):
    tree = worker_commander_lane.worker.worker_dispatcher.route.nodes()
    # Two branches name who ISSUES an order; the third names a matter — what a
    # page asks of its own channel (#68 phase 4).
    assert set(tree["routers"]) == {"group", "commander", "wsx"}
    assert set(tree["routers"]["group"]["entries"]) == {
        "ping", "quit", "drop_user", "drop_connection", "freeze_user"
    }
    assert set(tree["routers"]["commander"]["entries"]) == {"observe", "census", "eval"}
    assert set(tree["routers"]["wsx"]["entries"]) == {"openchannel"}


async def test_a_consumer_attaches_its_own_orders_and_the_vertex_reaches_them(
    worker_commander_lane,
):
    worker_commander_lane.worker.worker_dispatcher.commander_orders.add_branches(
        {"name": "xt_extra", "instance": XT_ExtraOrders(worker_commander_lane.worker)}
    )
    reply = await order(worker_commander_lane, "/commander/xt_extra/echo", {"x": 1})
    assert reply["result"] == {"echo": {"x": 1}, "worker": worker_commander_lane.worker_name}
