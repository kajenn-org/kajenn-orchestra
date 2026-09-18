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

"""The group's own life: it is born, it grows, it restarts, it closes, it breaks.

Every worker here is a REAL child process: a scripted one written to disk at test
time, started by the group itself through its own ``WorkerHandler``, configured by
the very payload the handler puts in ``KAJENN_WORKER``. The child answers
every order with a photo it was told to carry — how much memory it holds, and who
is flagged for the freezer — leaves when it is asked to, or never shows up at all,
which is the group's ``broken`` crisis.

The vertex above the group is real too, so the marks a departure leaves are read
where they are really written. What no test doubles is the group: it is the
subject.

The sockets live under a short ``mkdtemp`` root: the system caps a UDS path at
about a hundred characters and pytest's own directory is already past it, which is
the very reason worker names are short.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import time as real_time
from pathlib import Path
from typing import Any

import pytest

from kajenn_orchestra.orchestration import AssignmentRefused, GroupHandler, SpaCommander
from kajenn_orchestra.orchestration import group_handler as group_handler_module
from kajenn_orchestra.orchestration.worker_connector import (
    ENVELOPE_SLOT_WORKER_EVENTS,
    ENVELOPE_SLOT_WORKER_SNAPSHOT,
)
from kajenn_orchestra.orchestration.worker_handler import (
    DROP_USER_OP_PATH,
    FREEZE_USER_OP_PATH,
    QUIT_OP_PATH,
    WORKER_ENV_VAR,
    WorkerHandler,
)
from tests.orchestration.frame_helpers import control_frame

from .conftest import kill_process, wait_for


def warm(worker_handler, cpu_temperature_percent: float) -> None:
    """Declare a fresh filtered temperature, as the meter would have measured it."""
    worker_handler.cpu_temperature_percent = cpu_temperature_percent
    worker_handler.cpu_temperature_sampled_at = real_time.monotonic()
    worker_handler.cpu_temperature_interval_seconds = 0.1


CHILD_SCRIPT = '''
"""A scripted worker of a group: one photo, one answer, one departure."""

import asyncio
import json
import os
import time

from tests.orchestration.frame_helpers import call_endpoint, control_frame, read_control

from kajenn.channel.frame import REGISTER_METHOD, REGISTER_PATH, Frame, FrameStream


async def live() -> None:
    payload = json.loads(os.environ["{env_var}"])
    kwargs = payload["kwargs"]
    if kwargs["behaviour"] == "absent":
        await asyncio.sleep(60)
        return
    # The row of a user as a real photo carries it: his flag, his state, and his
    # three clocks. The two REAL ones are pushed into the past by the silence the
    # story declares for him; ``last_refresh_ts`` is always NOW, which is a beat
    # keeping the row warm and proving nobody. A story whose users are RESIDENTS
    # and not departing declares no flag: a flag is read at the vertex as a hold.
    now = time.time()
    photo = {{
        "pid": os.getpid(),
        "rss_bytes": kwargs["rss_bytes"],
        "users": {{
            user: {{
                "transfer_flag": kwargs["transfer_flag"],
                "item": {{
                    "state": "active",
                    "connection_count": 1,
                    "last_refresh_ts": now,
                    "last_user_ts": now - kwargs["user_silence"].get(user, 0),
                    "last_rpc_ts": now - kwargs["user_silence"].get(user, 0),
                }},
            }}
            for user in kwargs["users"]
        }},
    }}
    reader, writer = await asyncio.open_unix_connection(payload["uds_url"].removeprefix("uds:"))
    stream = FrameStream(reader, writer)
    await stream.write(
        control_frame(
            method=REGISTER_METHOD,
            path=REGISTER_PATH,
            data={{"pid": os.getpid(), "{snapshot_key}": photo}},
        )
    )
    await stream.read()
    while True:
        frame = await stream.read()
        if frame is None:
            return
        if frame.method == "CALL":
            data = {{"result": {{}}}}
            # The ordered freeze: the REPLY IS the confirmation, and the worker
            # event of the departure rides it. Refused on demand, which is the
            # departure that did not happen.
            if frame.path == "{freeze_path}":
                user = (read_control(frame) or {{}})["user"]
                # Taken and never answered: the order that outlives the caller's
                # own deadline, which is the wait the group puts a ceiling on.
                if kwargs["freeze_unanswered"]:
                    continue
                if kwargs["freeze_refused"]:
                    data = {{"error": "the deposit refused the parcels of " + user}}
                else:
                    photo["users"].pop(user, None)
                    data = {{
                        "result": {{"frozen": user}},
                        "{events_key}": [
                            {{
                                "op": "user_frozen",
                                "worker": payload["name"],
                                "user": user,
                                "placement": None,
                            }}
                        ],
                    }}
                await stream.write(
                    control_frame(id=frame.id, method="REPLY", path=frame.path, data=data)
                )
                continue
            # The order to forget somebody: the row goes and the departure is
            # ANNOUNCED, which is what prunes the indexes above.
            if frame.path == "{drop_path}":
                user = (read_control(frame) or {{}})["user"]
                # Taken and never answered, like the freeze above: the drop order
                # has a ceiling of its own and the group must not sit on it.
                if kwargs["drop_unanswered"]:
                    continue
                photo["users"].pop(user, None)
                await stream.write(
                    control_frame(
                        id=frame.id,
                        method="REPLY",
                        path=frame.path,
                        data={{
                            "result": {{}},
                            "{events_key}": [
                                {{"op": "drop_user", "worker": payload["name"], "user": user}}
                            ],
                        }},
                    )
                )
                continue
            # A real worker attaches its photo only when one is DUE, so an
            # answer — the one to the order to leave included — may carry none.
            if frame.path != "{quit_path}" or kwargs["photo_on_quit"]:
                data["{snapshot_key}"] = photo
            await stream.write(
                control_frame(id=frame.id, method="REPLY", path=frame.path, data=data)
            )
            # Asked to leave it leaves, after answering: the answer is what
            # carries the photo of everybody it is about to park.
            if frame.path == "{quit_path}":
                await stream.close()
                return


asyncio.run(live())
'''.format(
    env_var=WORKER_ENV_VAR,
    snapshot_key=ENVELOPE_SLOT_WORKER_SNAPSHOT,
    events_key=ENVELOPE_SLOT_WORKER_EVENTS,
    quit_path=QUIT_OP_PATH,
    freeze_path=FREEZE_USER_OP_PATH,
    drop_path=DROP_USER_OP_PATH,
)

CHILD_MODULE = "scripted_group_child"

#: What the machine concedes these groups. Their quota is the whole of it by
#: default; what ONE worker of theirs may hold is half, so that a group of this
#: file can hold two of them and still have room for a third to be born.
MEMORY_CEILING = 1_000_000

#: The ceiling every photo below is read against: an rss written as a fraction
#: of THIS is that fraction of one worker's occupancy.
WORKER_CEILING = MEMORY_CEILING // 2


@pytest.fixture
def instance_root(monkeypatch):
    """A short root holding the sockets, the deposit and the scripted child."""
    root = Path(tempfile.mkdtemp(prefix="gnrgh_"))
    (root / f"{CHILD_MODULE}.py").write_text(CHILD_SCRIPT)
    inherited = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(root), inherited]).rstrip(os.pathsep))
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def commander(instance_root):
    return SpaCommander(instance_root / "frozen_users")


@pytest.fixture
def group_settings(instance_root):
    """What a group of this file is built with: the child's identity and the paths."""
    return {
        "instance_dir": instance_root / "i",
        "frozen_users_path": instance_root / "frozen_users",
        "entry_module": CHILD_MODULE,
        # The scenarios of this file size ONE worker over half the quota, so two
        # of them fit it and a third may still be born; the core default sizes a
        # group for worker_max_number workers instead.
        "worker_memory_max_percent": 50.0,
        # Every worker here is newborn: the minimum life would exempt them all
        # from closure. The one test about the minimum life sets its own.
        "worker_min_life_seconds": 0.0,
        "worker_kwargs": {
            "behaviour": "answer",
            "users": [],
            "user_silence": {},
            "transfer_flag": "T",
            "rss_bytes": 0,
            "photo_on_quit": True,
            "freeze_refused": False,
            "freeze_unanswered": False,
            "drop_unanswered": False,
        },
        # Wide: the same value bounds the wait for the presentation, and a
        # fresh interpreter on a loaded machine can take seconds to get there.
        # The one scenario about a process that never shows up sets its own.
        "process_ping_timeout": 10.0,
    }


@pytest.fixture
async def make_group(commander, group_settings):
    """Build groups, and let no process or socket of theirs outlive the test."""
    groups: list[GroupHandler] = []

    def build(
        *,
        behaviour: str = "answer",
        users: list[str] | None = None,
        user_silence: dict[str, float] | None = None,
        transfer_flag: str | None = "T",
        rss_bytes: int = 0,
        photo_on_quit: bool = True,
        freeze_refused: bool = False,
        freeze_unanswered: bool = False,
        drop_unanswered: bool = False,
        **policies: Any,
    ) -> GroupHandler:
        settings = dict(group_settings)
        settings["worker_kwargs"] = {
            "behaviour": behaviour,
            "users": users or [],
            "user_silence": user_silence or {},
            "transfer_flag": transfer_flag,
            "rss_bytes": rss_bytes,
            "photo_on_quit": photo_on_quit,
            "freeze_refused": freeze_refused,
            "freeze_unanswered": freeze_unanswered,
            "drop_unanswered": drop_unanswered,
        }
        settings.update(policies)
        group = GroupHandler(
            commander,
            "standard",
            memory_concession_bytes=MEMORY_CEILING,
            **settings,
        )
        groups.append(group)
        return group

    yield build
    for group in groups:
        for worker_handler in list(group.worker_handler_map.values()):
            if worker_handler.process is not None:
                kill_process(worker_handler.process)
                await wait_for(lambda: not worker_handler.process.alive)
            await worker_handler.connector.stop()


def known_at_the_vertex(commander, cid: str, user: str) -> None:
    """What the login will do in Macro 4: this cid is that person's, and he has a row."""
    commander.connection_user_map[cid] = user
    commander.resolve_user(cid)


async def test_the_first_worker_of_a_group_is_its_reception(make_group):
    group = make_group()
    assert group.reception is None

    worker_handler = await group.start_worker()

    assert group.worker_handler_map == {"standard_0001": worker_handler}
    assert group.reception is worker_handler
    assert worker_handler.state == "running"
    assert worker_handler.worker_snapshot["pid"] == worker_handler.process.pid
    assert group.state == "running"


async def test_a_group_of_one_closes_nobody(make_group):
    group = make_group()
    reception = await group.start_worker()

    await group.check_occupancy(now=True)

    # Empty as it is, the reception is still the one that receives whoever
    # arrives unplaced: it is nobody's spare capacity.
    assert list(group.worker_handler_map) == ["standard_0001"]
    assert reception.state == "running"


async def test_a_growth_the_quota_refuses_saturates_the_group_until_there_is_room(
    make_group, commander
):
    # Half the concession is this group's, and one worker of it may hold half of
    # that: two of them at 85% of what they may hold — past the memory veto —
    # stand together at 42.5% of the concession, and a third one's ceiling would
    # not fit the group's share.
    quota = MEMORY_CEILING // 2
    ceiling = quota // 2
    group = make_group(rss_bytes=int(0.85 * ceiling), memory_max_percent=50.0)
    reception = await group.start_worker()
    spare = await group.start_worker()

    # The refusal writes the saturation: nobody admits him, and the quota
    # refuses the birth that would.
    known_at_the_vertex(commander, "cid-a", "mario")
    with pytest.raises(AssignmentRefused):
        await group.assign_user("mario")

    assert sorted(group.worker_handler_map) == ["standard_0001", "standard_0002"]
    assert group.memory_occupied_percent == 42.5
    assert group.state == "saturated"

    # Somebody left: the next check finds the quota affords a birth again, and
    # the crisis is over without anybody having to say so.
    reception.worker_snapshot = {"rss_bytes": ceiling // 5}
    spare.worker_snapshot = {"rss_bytes": 3 * ceiling // 5}
    await group.check_occupancy(now=True)

    assert group.memory_occupied_percent == 20.0
    assert group.state == "running"
    assert sorted(group.worker_handler_map) == ["standard_0001", "standard_0002"]


async def test_a_process_that_never_starts_breaks_the_group_until_one_does(make_group, caplog):
    # The short window here bounds the wait for the absent one, and nothing
    # else: the test would otherwise sit the whole file default out.
    group = make_group(behaviour="absent", process_ping_timeout=2.0)

    with caplog.at_level("ERROR"):
        assert await group.start_worker() is None

    assert group.state == "broken"
    assert group.worker_handler_map == {}
    assert "could not be started" in caplog.text

    # The first process that starts closes the crisis — nothing else does.
    # It presents itself, so it gets the wide window back.
    group.worker_settings["worker_kwargs"]["behaviour"] = "answer"
    group.worker_settings["process_ping_timeout"] = 10.0
    assert await group.start_worker() is not None
    assert group.state == "running"


async def test_a_worker_past_the_restart_setpoint_is_replaced_by_a_fresh_one(make_group, commander):
    group = make_group(rss_bytes=int(0.99 * WORKER_CEILING), users=["mario"])
    known_at_the_vertex(commander, "cid-a", "mario")
    doomed = await group.start_worker()
    doomed.worker_snapshot["pss_bytes"] = int(0.99 * WORKER_CEILING)
    doomed.hosted_users.add("mario")
    group.user_worker_map["mario"] = doomed.name

    await group.check_occupancy(now=True)

    assert list(group.worker_handler_map) == ["standard_0002"]
    await wait_for(lambda: not doomed.process.alive)
    assert doomed.state == "quitted"
    # His state went to the freezer with the process that held it, and his
    # placement is to be assigned: his next request decides where he wakes.
    assert commander.user_is_frozen("mario") is True
    assert group.user_worker_map == {"mario": None}
    assert group.state == "running"


async def test_shared_rss_does_not_restart_a_worker_whose_pss_is_small(make_group):
    group = make_group(rss_bytes=int(0.99 * WORKER_CEILING))
    worker = await group.start_worker()
    worker.worker_snapshot["pss_bytes"] = int(0.20 * WORKER_CEILING)

    await group.check_occupancy(now=True)

    assert list(group.worker_handler_map) == ["standard_0001"]
    assert worker.state == "running"


async def test_a_death_that_outruns_the_close_order_leaves_no_zombie(make_group, commander):
    group = make_group()
    known_at_the_vertex(commander, "cid-a", "mario")
    doomed = await group.start_worker()
    doomed.hosted_users.add("mario")
    group.user_worker_map["mario"] = doomed.name
    await wait_for(lambda: doomed.worker_snapshot is not None)
    kill_process(doomed.process)
    await wait_for(lambda: doomed.state == "aborted")

    fresh = await group.restart_worker(doomed)

    # The order found the death already written and ordered nothing: the wild
    # death was not overwritten into a departure nobody would ever settle, the
    # dead one is buried with the users it held, and the fresh one serves.
    assert doomed.state == "aborted"
    assert list(group.worker_handler_map) == [fresh.name]
    assert group.reception is fresh
    assert group.user_worker_map == {}


async def test_a_dead_worker_never_photographed_is_ordered_nothing(make_group):
    group = make_group()
    doomed = await group.start_worker()
    kill_process(doomed.process)
    await wait_for(lambda: doomed.state == "aborted")
    doomed.worker_snapshot = None

    # The state is read BEFORE the photo beat: no beat is thrown at a dead wire.
    fresh = await group.restart_worker(doomed)

    assert doomed.state == "aborted"
    assert list(group.worker_handler_map) == [fresh.name]


async def test_a_worker_on_its_way_out_is_nobodys_spare(make_group):
    group = make_group()
    await group.start_worker()
    leaving = await group.start_worker()
    leaving.state = "quitting"

    await group.check_occupancy(now=True)

    # Its closure is already somebody's order: it is not a candidate for a
    # second one, and its room counts for nobody.
    assert leaving.state == "quitting"


async def test_the_closure_of_a_spare_worker_goes_through_its_six_steps(make_group, commander):
    group = make_group(users=["mario"])
    known_at_the_vertex(commander, "cid-a", "mario")
    reception = await group.start_worker()
    spare = await group.start_worker()
    spare.hosted_users.add("mario")
    group.user_worker_map["mario"] = spare.name
    warm(reception, 1.0)
    warm(spare, 1.0)

    # 1. the group decides on one reading: what the spare one holds, the others
    # can hold and still admit.
    await group.check_occupancy(now=True)

    # 2-4. it answered the order at once, with the photo of everybody flagged for
    # the freezer; then it drained and ENDED BY ITSELF, and that end was awaited.
    assert spare.worker_snapshot["users"]["mario"]["transfer_flag"] == "T"
    assert spare.state == "quitted"
    await wait_for(lambda: not spare.process.alive)
    assert reception.state == "running"

    # 5. at the round that reads the ended state, the group takes it out: out of
    # the list, its wire away, its placements released — and the vertex marks the
    # user whose own worker event died with the wire.
    spare.envelope_handler.report_death()

    assert list(group.worker_handler_map) == ["standard_0001"]
    assert group.reception is reception
    assert commander.user_is_frozen("mario") is True
    assert group.user_worker_map == {"mario": None}
    await wait_for(lambda: not spare.connector.socket_path.exists())


async def test_a_closure_the_memory_veto_refuses_is_not_made(make_group):
    group = make_group()
    reception = await group.start_worker()
    spare = await group.start_worker()
    reception.worker_snapshot = {"rss_bytes": int(0.78 * WORKER_CEILING)}
    spare.worker_snapshot = {"rss_bytes": int(0.05 * WORKER_CEILING)}
    warm(reception, 1.0)
    warm(spare, 1.0)

    await group.check_occupancy(now=True)

    # Cool as both are, the reception would stand at 83 of memory with the
    # spare's share: the veto refuses a closure the CPU would have allowed.
    assert sorted(group.worker_handler_map) == ["standard_0001", "standard_0002"]
    assert spare.state == "running"


async def test_a_worker_in_its_first_seconds_is_no_closure_candidate(make_group):
    group = make_group(worker_min_life_seconds=60.0)
    reception = await group.start_worker()
    young = await group.start_worker()
    warm(reception, 1.0)
    warm(young, 1.0)

    await group.check_occupancy(now=True)

    # Empty as it reads, its occupancy measures its own birth, not its work:
    # a newborn is otherwise both the proof that growth was needed and the
    # proof that closing is safe (#36).
    assert young.state == "running"


async def test_a_closure_leaving_a_survivor_warm_is_not_ordered(make_group):
    # 35 shared onto 35 puts the survivor at 70 of CPU: under the admission close
    # threshold —
    # one threshold for both decisions would close here and grow the round
    # after (#36) — but over cpu_close_percent, so the pool holds:
    # the band between the two thresholds is its normal state.
    group = make_group()
    reception = await group.start_worker()
    spare = await group.start_worker()
    warm(reception, 35.0)
    warm(spare, 35.0)

    await group.check_occupancy(now=True)

    assert spare.state == "running"


async def test_a_closure_the_survivors_cannot_take_by_heads_is_refused(make_group):
    # The occupancy says yes — everybody near zero — but the survivors' own
    # worker_max_users cannot seat the spare's placed users. The ceiling is
    # asked LAST, after the occupancy has spoken, the same order the placement
    # asks it in (#36).
    group = make_group(worker_max_users=2)
    reception = await group.start_worker()
    spare = await group.start_worker()
    group.user_worker_map.update(
        {"anna": reception.name, "bruno": reception.name, "carla": spare.name, "dario": spare.name}
    )
    warm(reception, 1.0)
    warm(spare, 1.0)

    await group.check_occupancy(now=True)

    assert spare.state == "running"


async def test_a_placement_pointing_at_a_worker_that_died_goes_with_it(make_group, commander):
    group = make_group()
    worker_handler = await group.start_worker()
    user = "guest_legacy1"
    commander.record_connection_user("cid-a", user)
    assert await group.assign_user(user) == worker_handler.name

    # The process dies before it ever said the user had arrived in it: nobody
    # names him at the death, so his placement goes with the worker holding it.
    kill_process(worker_handler.process)
    await wait_for(lambda: worker_handler.state == "aborted")
    worker_handler.envelope_handler.report_death()

    assert group.worker_handler_map == {}
    assert group.user_worker_map == {}


async def test_a_death_reported_for_a_worker_this_group_does_not_have_is_loud(make_group):
    group = make_group()
    await group.start_worker()

    with pytest.raises(KeyError):
        group.drop_worker("standard_9999")

    assert list(group.worker_handler_map) == ["standard_0001"]


async def test_an_ordered_quit_photographs_the_worker_first_so_nobody_is_lost(
    make_group, commander
):
    # A worker whose answer to the order carries no photo — which is what the
    # throttle of a real one does when none is due.
    group = make_group(users=["mario"], photo_on_quit=False)
    known_at_the_vertex(commander, "cid-a", "mario")
    worker_handler = await group.start_worker()
    worker_handler.hosted_users.add("mario")
    # And of which there is no photo at all: a departure is settled on the last
    # one, so without the beat the order takes first, this user would be purged
    # as lost instead of parked in the freezer.
    worker_handler.worker_snapshot = None

    await group.restart_worker(worker_handler)

    assert commander.user_is_frozen("mario") is True
    assert "mario" in commander.user_map
    assert list(group.worker_handler_map) == ["standard_0002"]


async def test_a_photo_past_the_restart_setpoint_brings_the_round_forward(make_group):
    group = make_group()
    worker_handler = await group.start_worker()
    group.ping_now_event.clear()

    worker_handler.read_envelope(
        {ENVELOPE_SLOT_WORKER_SNAPSHOT: {"rss_bytes": WORKER_CEILING // 2}}
    )
    assert group.ping_now_event.is_set() is False

    worker_handler.read_envelope({ENVELOPE_SLOT_WORKER_SNAPSHOT: {"rss_bytes": WORKER_CEILING}})

    assert group.ping_now_event.is_set() is True


async def test_every_order_of_the_group_leaves_its_row_in_the_orchestration_log(
    make_group, commander, caplog
):
    group = make_group()
    with caplog.at_level("INFO", logger="kajenn.orchestration.orders"):
        worker_handler = await group.start_worker()
        await group.restart_worker(worker_handler)

    rows = [record.getMessage() for record in caplog.records]
    assert any("order=start_worker subject=standard_0001" in row for row in rows)
    assert any("order=restart_worker subject=standard_0001" in row for row in rows)
    assert any(
        "order=drop_worker subject=standard_0001 numbers=None outcome=quitted" in row
        for row in rows
    )
    assert any("order=start_worker subject=standard_0002" in row for row in rows)


async def test_the_vertex_builds_its_groups_with_the_concession_already_inside(
    instance_root, group_settings
):
    vertex = SpaCommander(instance_root / "frozen_users", groups={"standard": group_settings})

    group = vertex.group_map["standard"]
    assert isinstance(group, GroupHandler)
    assert group.spa_commander is vertex
    assert group.memory_concession_bytes == vertex.memory_concession_bytes
    assert vertex.default_group == "standard"


async def test_a_group_built_without_the_concession_says_so_at_once(commander, group_settings):
    with pytest.raises(TypeError):
        GroupHandler(commander, "standard", **group_settings)


async def test_start_brings_the_reception_up_and_stop_leaves_no_child_alive(
    instance_root, group_settings
):
    vertex = SpaCommander(instance_root / "frozen_users", groups={"standard": group_settings})
    group = vertex.group_map["standard"]
    try:
        await vertex.start()

        # READY is the reception having presented itself: start returns there.
        reception = group.reception
        assert reception is not None
        assert reception.state == "running"
        process = reception.process
        assert process.alive
    finally:
        await vertex.stop()

    assert not process.alive
    assert not reception.connector.connected
    # The death of a shutdown is ORDERED: no alarm is owed for it, and the log
    # of a clean stop must not read like N processes died on their own.
    assert reception.state == "quitted"


async def test_the_group_of_a_user_is_written_where_he_is_placed(make_group, commander):
    group = make_group()
    await group.start_worker()
    known_at_the_vertex(commander, "cid-a", "mario")

    await group.assign_user("mario")

    assert group.user_worker_map["mario"] == "standard_0001"
    assert commander.user_map["mario"]["group"] == "standard"


async def test_a_placement_nobody_took_writes_no_group(make_group, commander):
    # The vertex is saturated, so the group may not grow: the surrender path.
    group = make_group()
    commander.state = "saturated"
    known_at_the_vertex(commander, "cid-a", "mario")

    with pytest.raises(AssignmentRefused):
        await group.assign_user("mario")

    assert "mario" not in group.user_worker_map
    assert commander.user_map["mario"]["group"] is None


async def test_the_group_orders_every_worker_into_the_reboot_directory(make_group, monkeypatch):
    """One order per worker, each carrying where its parcels go."""
    group = make_group()
    await group.start_worker()
    await group.start_worker()
    ordered = []

    async def record(self, freezer_path=None):
        ordered.append((self.name, freezer_path))

    monkeypatch.setattr(WorkerHandler, "quit_process", record)

    await group.quit_all("/tmp/reboot_temp")

    assert ordered == [
        ("standard_0001", "/tmp/reboot_temp"),
        ("standard_0002", "/tmp/reboot_temp"),
    ]


async def test_the_quit_blocks_a_worker_s_users_before_its_order_leaves(
    make_group, commander, monkeypatch
):
    """The block is up when the order goes out, and only for that worker's users."""
    group = make_group()
    first = await group.start_worker()
    second = await group.start_worker()
    known_at_the_vertex(commander, "cid-a", "mario")
    known_at_the_vertex(commander, "cid-b", "lucia")
    group.user_worker_map["mario"] = first.name
    group.user_worker_map["lucia"] = second.name
    held_when_ordered = []

    async def record(self, freezer_path=None):
        held_when_ordered.append(
            (self.name, sorted(u for u, r in commander.user_map.items() if r["on_hold"]))
        )

    monkeypatch.setattr(WorkerHandler, "quit_process", record)

    await group.quit_all("/tmp/reboot_temp")

    # The users of the worker being ordered are already blocked; the one placed
    # on the worker whose turn has not come is not — his own order raises his.
    assert held_when_ordered == [
        (first.name, ["mario"]),
        (second.name, ["lucia", "mario"]),
    ]
    assert commander.user_map["mario"]["on_hold"] == f"quit of {first.name}"


async def test_a_worker_already_dead_is_ordered_nothing_and_blocks_nobody(
    make_group, commander, monkeypatch
):
    """Its death is written: the round that read it already said what became of him."""
    group = make_group()
    worker_handler = await group.start_worker()
    known_at_the_vertex(commander, "cid-a", "mario")
    group.user_worker_map["mario"] = worker_handler.name
    worker_handler.state = "aborted"
    ordered = []
    monkeypatch.setattr(
        WorkerHandler, "quit_process", lambda self, freezer_path=None: ordered.append(self.name)
    )

    await group.quit_all("/tmp/reboot_temp")

    assert ordered == []
    assert commander.user_map["mario"]["on_hold"] is None


async def test_the_quit_gives_every_hold_back_as_the_freezes_confirm(make_group, commander):
    """The whole soft quit: blocked before the order, frozen and free after it."""
    # The child's photo carries mario flagged for the freezer, which is what a
    # real worker's answer to the order says of everybody on board.
    group = make_group(users=["mario"])
    # His row exists before the process does: the REGISTER photo already carries
    # his flag, and a flag is read at the vertex as a hold.
    known_at_the_vertex(commander, "cid-a", "mario")
    worker_handler = await group.start_worker()
    group.user_worker_map["mario"] = worker_handler.name
    worker_handler.hosted_users.add("mario")

    await group.quit_all("/tmp/reboot_temp")

    # Blocked he is; the cause reads `transfer_flag T` and not the quit's own,
    # because this scripted child carries the flag from birth and a hold keeps
    # its first cause. What the cause says is test 1's business.
    assert commander.user_map["mario"]["on_hold"] is not None
    assert worker_handler.state == "quitted"

    # The death is read at the group's round, and it says of every flagged user
    # what his own announcement would have said had the wire outlived it.
    worker_handler.envelope_handler.report_death()

    assert commander.user_is_frozen("mario") is True
    assert commander.user_map["mario"]["on_hold"] is None
    assert group.user_worker_map["mario"] is None


async def test_a_worker_at_its_user_ceiling_makes_the_placement_father_a_new_one(
    make_group, commander
):
    """worker_max_users: the policy the bench sets to 1 — and the birth lives
    INSIDE the placement (owner, 2026-08-25): the second user is never sent
    away with a 503, his own placement brings his worker into being."""
    group = make_group(worker_max_users=1)
    await group.start_worker()
    commander.record_connection_user("cid-a", "guest_first1")
    commander.record_connection_user("cid-b", "guest_second1")
    assert await group.assign_user("guest_first1") == "standard_0001"

    assert await group.assign_user("guest_second1") == "standard_0002"

    assert sorted(group.worker_handler_map) == ["standard_0001", "standard_0002"]
    assert sorted(group.user_worker_map.values()) == ["standard_0001", "standard_0002"]


async def test_at_the_ceiling_with_no_way_to_grow_the_placement_surrenders(make_group, commander):
    group = make_group(worker_max_users=1)
    await group.start_worker()
    commander.record_connection_user("cid-a", "guest_first1")
    commander.record_connection_user("cid-b", "guest_second1")
    assert await group.assign_user("guest_first1") == "standard_0001"
    commander.state = "saturated"
    group.ping_now_event.clear()

    with pytest.raises(AssignmentRefused):
        await group.assign_user("guest_second1")

    assert group.ping_now_event.is_set()
    assert group.user_worker_map == {"guest_first1": "standard_0001"}


async def test_without_the_ceiling_nothing_changes(make_group, commander):
    group = make_group()
    await group.start_worker()
    commander.record_connection_user("cid-a", "guest_first1")
    commander.record_connection_user("cid-b", "guest_second1")
    assert await group.assign_user("guest_first1") == "standard_0001"
    assert await group.assign_user("guest_second1") == "standard_0001"


async def test_the_group_orders_the_freeze_and_the_confirmation_settles_everything(
    make_group, commander
):
    """The whole sequence for one user: block, order, confirmation, block gone."""
    group = make_group()
    worker_handler = await group.start_worker()
    known_at_the_vertex(commander, "cid-a", "mario")
    assert await group.assign_user("mario") == worker_handler.name
    worker_handler.hosted_users.add("mario")

    assert await group.freeze_hosted_user("mario") is True

    # Nothing of this was written by the method: the worker event travelled in
    # the REPLY that confirms the order, and the fold read it before the caller
    # was answered.
    assert commander.user_is_frozen("mario") is True
    assert commander.user_map["mario"]["on_hold"] is None
    assert group.user_worker_map["mario"] is None
    assert worker_handler.hosted_users == set()


async def test_a_freeze_the_worker_refuses_leaves_the_user_unblocked_and_where_he_was(
    make_group, commander
):
    """A departure that did not happen gives the block back."""
    group = make_group(freeze_refused=True)
    worker_handler = await group.start_worker()
    known_at_the_vertex(commander, "cid-a", "mario")
    await group.assign_user("mario")
    worker_handler.hosted_users.add("mario")

    assert await group.freeze_hosted_user("mario") is False

    assert commander.user_is_frozen("mario") is False
    assert commander.user_map["mario"]["on_hold"] is None
    assert commander.resolve_user("cid-a") == "mario"
    assert group.user_worker_map["mario"] == worker_handler.name
    assert worker_handler.hosted_users == {"mario"}


async def test_a_request_arriving_under_the_order_waits_and_is_served_after_it(
    make_group, commander
):
    """#40: no request of his can reach the emptying process, and none is refused.

    The order is held ON THE WIRE while a request of his arrives, which is the
    window the block exists for: it parks on his barrier instead of walking into
    a worker that is writing his parcels, and it is served once the confirmation
    has dropped the block — at the destination the vertex assigns him again.
    """
    group = make_group()
    worker_handler = await group.start_worker()
    known_at_the_vertex(commander, "cid-a", "mario")
    await group.assign_user("mario")
    worker_handler.hosted_users.add("mario")
    ordered = asyncio.Event()
    confirm = asyncio.Event()
    on_the_wire = []
    placed_call = worker_handler.connector.call

    async def held_order(path, data=None, timeout=None):
        on_the_wire.append(path)
        if path == FREEZE_USER_OP_PATH:
            ordered.set()
            await confirm.wait()
        return await placed_call(path, data, timeout)

    worker_handler.connector.call = held_order

    order = asyncio.ensure_future(group.freeze_hosted_user("mario"))
    await ordered.wait()
    request = asyncio.ensure_future(
        commander.serve_request(
            "cid-a",
            control_frame(method="CALL", path="/invoices", data={"http": {"path": "/invoices"}}),
            hold_timeout=5.0,
        )
    )
    await asyncio.sleep(0.05)

    # He waits, and NOTHING of his went down that wire: no page of his can be
    # born on the worker that is writing his parcels, which is #40 itself.
    assert not request.done()
    assert on_the_wire == [FREEZE_USER_OP_PATH]

    confirm.set()

    assert await order is True
    assert "error" not in (await request).info
    assert group.user_worker_map["mario"] == worker_handler.name


async def test_the_group_parks_whoever_has_gone_quiet_and_spares_the_active(make_group, commander):
    """The valve is the GROUP's: it reads the silence off the photo and orders.

    Mario has been silent a minute past a valve set to half of one, and his
    ``last_refresh_ts`` is NOW: a beat keeps a row warm and proves nobody. Anna
    has just spoken, and nobody touches her.
    """
    group = make_group(
        users=["mario", "anna"],
        user_silence={"mario": 60},
        transfer_flag=None,
        user_idle_freeze_minutes=0.5,
    )
    worker_handler = await group.start_worker()
    for cid, user in (("cid-a", "mario"), ("cid-b", "anna")):
        known_at_the_vertex(commander, cid, user)
        await group.assign_user(user)
        worker_handler.hosted_users.add(user)

    await group.check_user_activity(now=True)

    assert commander.user_is_frozen("mario") is True
    assert group.user_worker_map["mario"] is None
    assert commander.user_map["mario"]["on_hold"] is None
    assert commander.user_is_frozen("anna") is False
    assert group.user_worker_map["anna"] == worker_handler.name
    assert worker_handler.hosted_users == {"anna"}


async def test_whoever_is_past_his_own_expiry_is_dropped_and_not_parked(make_group, commander):
    """The other verdict: he is forgotten whole, and nothing of his is written.

    The horizon is the vertex's own — the one it applies to a parcel in the
    deposit — asked of it per identity, so a guest's shorter life needs no second
    setting anywhere. Expiry wins over the valve on the same user.
    """
    commander.user_expiry_hours = 0.5
    group = make_group(
        users=["ugo"],
        user_silence={"ugo": 3600},
        transfer_flag=None,
        user_idle_freeze_minutes=0.1,
    )
    worker_handler = await group.start_worker()
    known_at_the_vertex(commander, "cid-c", "ugo")
    await group.assign_user("ugo")
    worker_handler.hosted_users.add("ugo")

    await group.check_user_activity(now=True)

    assert "ugo" not in commander.user_map
    assert "ugo" not in group.user_worker_map
    assert worker_handler.hosted_users == set()
    assert commander.freeze_handler.user_folders == set()


async def test_a_drop_order_nobody_answers_expires_and_gives_the_block_back(
    make_group, commander, monkeypatch
):
    """The same ceiling on the other order: the round gives up rather than hang.

    The worker takes the drop and never answers it. Without a deadline this
    round would wait as long as the child stays mute, and the vertex gathers the
    group turns, so its whole clock would stop with it. The expiry takes the road
    of a refusal: the block falls, the user stays where he was, and the next
    round judges him again.
    """
    monkeypatch.setattr(group_handler_module, "DEPARTURE_ORDER_WAIT_LIMIT", 0.2)
    commander.user_expiry_hours = 0.5
    group = make_group(
        users=["ugo"],
        user_silence={"ugo": 3600},
        transfer_flag=None,
        drop_unanswered=True,
    )
    worker_handler = await group.start_worker()
    known_at_the_vertex(commander, "cid-c", "ugo")
    await group.assign_user("ugo")
    worker_handler.hosted_users.add("ugo")

    await group.check_user_activity(now=True)

    assert "ugo" in commander.user_map
    assert commander.user_map["ugo"]["on_hold"] is None
    assert "ugo" not in commander.user_hold_event_map
    assert group.user_worker_map["ugo"] == worker_handler.name


async def test_a_drop_order_that_cannot_be_sent_frees_the_user_and_the_round(make_group, commander):
    """A wire gone under the order: the hold falls and the other users are judged.

    The expiry road raises the block before it orders, so an order that cannot
    even be sent must give it back — otherwise every request of his would park
    at the vertex and answer 503 for as long as the process lives. And the round
    must go on: anna, silent past the valve, is parked in the same turn.
    """
    commander.user_expiry_hours = 0.5
    group = make_group(
        users=["ugo", "anna"],
        user_silence={"ugo": 3600, "anna": 60},
        transfer_flag=None,
        user_idle_freeze_minutes=0.5,
    )
    worker_handler = await group.start_worker()
    for cid, user in (("cid-c", "ugo"), ("cid-b", "anna")):
        known_at_the_vertex(commander, cid, user)
        await group.assign_user(user)
        worker_handler.hosted_users.add(user)
    worker_handler.connector._stream = None

    await group.check_user_activity(now=True)

    assert "ugo" in commander.user_map
    assert commander.user_map["ugo"]["on_hold"] is None
    assert "ugo" not in commander.user_hold_event_map
    assert commander.user_map["anna"]["on_hold"] is None


async def test_an_order_nobody_answers_expires_and_leaves_the_user_where_he_was(
    make_group, commander, monkeypatch
):
    """The deadline on the order: the round gives up rather than stop beating.

    The worker takes the order and never answers it. Without a deadline this
    round — and with it every later beat of the group — would wait as long as
    the call it is stuck behind lasts. The expiry takes the road of a refusal:
    the block falls, the user stays on his worker, and the next round judges him
    again.
    """
    monkeypatch.setattr(group_handler_module, "DEPARTURE_ORDER_WAIT_LIMIT", 0.2)
    group = make_group(freeze_unanswered=True)
    worker_handler = await group.start_worker()
    known_at_the_vertex(commander, "cid-a", "mario")
    await group.assign_user("mario")
    worker_handler.hosted_users.add("mario")

    assert await group.freeze_hosted_user("mario") is False

    assert commander.user_is_frozen("mario") is False
    assert commander.user_map["mario"]["on_hold"] is None
    assert group.user_worker_map["mario"] == worker_handler.name


async def test_an_order_cancelled_under_the_await_gives_the_block_back(make_group, commander):
    """The quit cancels the beat: a user caught mid-order must not stay blocked."""
    group = make_group(freeze_unanswered=True)
    worker_handler = await group.start_worker()
    known_at_the_vertex(commander, "cid-a", "mario")
    await group.assign_user("mario")
    worker_handler.hosted_users.add("mario")

    order = asyncio.ensure_future(group.freeze_hosted_user("mario"))
    await asyncio.sleep(0.05)
    assert commander.user_map["mario"]["on_hold"] is not None

    order.cancel()
    with pytest.raises(asyncio.CancelledError):
        await order

    assert commander.user_map["mario"]["on_hold"] is None
    assert "mario" not in commander.user_hold_event_map
    assert group.user_worker_map["mario"] == worker_handler.name


async def test_a_request_arriving_under_the_expiry_order_waits_instead_of_routing(
    make_group, commander
):
    """#40 on the OTHER road: the drop blocks him at the vertex the freeze does.

    The order to forget him is held on the wire while a request of his arrives.
    Without the block it would be routed onto the very worker that is erasing his
    rows; with it, it parks on his barrier and is served after — as the newcomer
    he is once his identity is gone, since the fold that prunes the indexes is
    also what lets the barrier go.
    """
    commander.user_expiry_hours = 0.5
    group = make_group(
        users=["ugo"],
        user_silence={"ugo": 3600},
        transfer_flag=None,
        user_idle_freeze_minutes=0.1,
    )
    worker_handler = await group.start_worker()
    known_at_the_vertex(commander, "cid-c", "ugo")
    await group.assign_user("ugo")
    worker_handler.hosted_users.add("ugo")
    ordered = asyncio.Event()
    confirm = asyncio.Event()
    on_the_wire = []
    placed_call = worker_handler.connector.call

    async def held_order(path, data=None, timeout=None):
        on_the_wire.append(path)
        if path == DROP_USER_OP_PATH:
            ordered.set()
            await confirm.wait()
        return await placed_call(path, data, timeout)

    worker_handler.connector.call = held_order

    round_of_the_group = asyncio.ensure_future(group.check_user_activity(now=True))
    await ordered.wait()
    request = asyncio.ensure_future(
        commander.serve_request(
            "cid-c",
            control_frame(method="CALL", path="/invoices", data={"http": {"path": "/invoices"}}),
            hold_timeout=5.0,
        )
    )
    await asyncio.sleep(0.05)

    assert commander.user_map["ugo"]["on_hold"] == f"expiry on {worker_handler.name}"
    assert not request.done()
    assert on_the_wire == [DROP_USER_OP_PATH]

    confirm.set()
    await round_of_the_group

    assert "ugo" not in commander.user_map
    assert "ugo" not in commander.user_hold_event_map
    assert "error" not in (await request).info
