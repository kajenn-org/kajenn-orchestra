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

"""The one clock: the round, the wake that anticipates it, and the vertex's own tasks.

Two kinds of subject live here, because the clock has two kinds of claim to make.
What the round DOES is proved on real child processes — a group over
``child_stub``, its own socket, its own beat down the wire and the answer climbing
the chain back into the handler. What the clock DECIDES — who gets a turn, whose
turn is skipped, which task has come round, what a failing turn does to its
siblings — is proved on a group double that does nothing but record the turn it
was given: driving those with real processes would prove the same thing more
slowly and less exactly.

The sockets and the deposit live under a short ``mkdtemp`` root: the system caps a
UDS path at about a hundred characters, and pytest's own directory is already
past it.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import pytest

from kajenn_orchestra.orchestration import GroupHandler, SpaCommander, group_handler, spa_commander
from kajenn_orchestra.orchestration.beats import every
from kajenn_orchestra.orchestration.spa_commander import GUEST_PREFIX

from .child_stub import GO_MUTE_OP
from .conftest import kill_process, wait_for

CHILD_MODULE = "tests.orchestration.child_stub"
WORKER_NAME = "standard_0001"


@pytest.fixture
def make_commander(short_root):
    """Build a vertex with the policies a test wants, over its own deposit."""

    def build(commander_class: type[SpaCommander] = SpaCommander, **policies: Any) -> SpaCommander:
        return commander_class(short_root / "frozen_users", **policies)

    return build


@pytest.fixture
def commander(make_commander):
    return make_commander()


@pytest.fixture
async def make_group(short_root, commander, repo_on_pythonpath):
    """Build real groups over the scripted child; no process or socket outlives the test."""
    groups: list[GroupHandler] = []

    def build(name: str = "standard", **policies: Any) -> GroupHandler:
        policies.setdefault("memory_concession_bytes", 4 * 1024**3)
        group = GroupHandler(
            commander,
            name,
            instance_dir=short_root / "i",
            frozen_users_path=short_root / "frozen_users",
            entry_module=CHILD_MODULE,
            worker_kwargs={"group": name},
            process_ping_timeout=1.0,
            **policies,
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


@pytest.fixture
async def clock(commander):
    """Start the one clock of this vertex, and let it beat no longer than the test."""
    beating: list[asyncio.Task[None]] = []

    def start() -> asyncio.Task[None]:
        task = asyncio.get_running_loop().create_task(commander.heartbeat_loop())
        beating.append(task)
        return task

    yield start
    for task in beating:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


class GroupDouble:
    """A group seen from the clock alone: its wake, its turns, and how long each takes.

    Args:
        name: the group's name, which is what the vertex files its turn under.
        delay: how long a turn of it takes.
        failing: whether its turn ends by raising.
    """

    def __init__(self, name: str, *, delay: float = 0.0, failing: bool = False) -> None:
        self.name = name
        self.ping_now_event = asyncio.Event()
        self.delay = delay
        self.failing = failing
        #: One entry per turn taken, saying whether the wake was rung for it.
        self.turns: list[bool] = []

    async def ping(self) -> None:
        """The turn, taken as the real group takes it: consume the wake, then work."""
        self.turns.append(self.ping_now_event.is_set())
        self.ping_now_event.clear()
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.failing:
            raise RuntimeError(f"the turn of {self.name} blew up")


class CountingCommander(SpaCommander):
    """A vertex that keeps the count of the times it asked the world for more room."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.asked = 0

    def need_resources(self) -> None:
        """Count the ask: this is the seam a Kubernetes commander really overrides."""
        self.asked += 1


def group_double(commander: SpaCommander, name: str, **shape: Any) -> GroupDouble:
    """One group double, hung under the vertex the way a real group hangs itself."""
    double = GroupDouble(name, **shape)
    commander.group_map[name] = double
    return double


def parked_state(commander: SpaCommander, user: str) -> None:
    """What a freeze leaves on disk and in the indexes, written as the machine writes it."""
    commander.connection_user_map[f"cid-{user}"] = user
    commander.resolve_user(f"cid-{user}")
    commander.freeze_handler.take_lock(user, WORKER_NAME)
    commander.freeze_handler.write_user_register_item(
        user, {"store": "whatever"}, writer=WORKER_NAME, cause="freeze", group="standard"
    )
    commander.freeze_handler.release_lock(user, WORKER_NAME)
    commander.mark_user_frozen(user)


def counting_check(checks: list[int], beats: int):
    """A reading of the shape that only counts itself: the real one has its own file.

    It carries a cadence like the real one, because the cadence is what is under
    test — the group only gives the turn.
    """

    @every(beats)
    async def check_occupancy(self) -> None:
        checks.append(len(checks) + 1)

    return check_occupancy


# -- the round, over real processes --


async def test_one_round_beats_a_real_child_and_its_answer_climbs_the_chain(
    make_group, commander
):
    group = make_group(process_ping_interval=0.0)
    worker_handler = await group.start_worker()
    worker_handler.worker_snapshot = None

    await commander.ping_groups()

    assert worker_handler.worker_snapshot["pid"] == worker_handler.process.pid
    assert worker_handler.state == "running"


async def test_only_the_workers_nobody_has_heard_from_are_beaten(make_group):
    group = make_group()
    worker_handler = await group.start_worker()
    worker_handler.worker_snapshot = None

    await group.ping_workers()

    # Its presentation arrived a moment ago: beating it would ask a process what
    # it has just said.
    assert not worker_handler.requires_beat_ping
    assert worker_handler.worker_snapshot is None

    worker_handler.process_ping_interval = 0.0

    await group.ping_workers()

    assert worker_handler.requires_beat_ping
    assert worker_handler.worker_snapshot["pid"] == worker_handler.process.pid


async def test_a_beat_that_raises_takes_no_sibling_beat_with_it(make_group):
    group = make_group()
    broken = await group.start_worker()
    healthy = await group.start_worker()
    for worker_handler in (broken, healthy):
        worker_handler.process_ping_interval = 0.0
        worker_handler.worker_snapshot = None

    async def failing_beat() -> None:
        raise ConnectionError("the wire is a stump")

    broken.ping_process = failing_beat

    await group.ping_workers()

    # The broken one's beat is its own business and nobody else's: the sibling
    # was beaten all the same, and its photo arrived.
    assert healthy.worker_snapshot["pid"] == healthy.process.pid


async def test_a_mute_process_delays_its_own_group_and_not_the_others(make_group, commander):
    mute_group = make_group(name="mute", process_ping_interval=0.0)
    live_group = make_group(name="live", process_ping_interval=0.0)
    mute = await mute_group.start_worker()
    live = await live_group.start_worker()
    await mute.connector.call(GO_MUTE_OP, timeout=5.0)
    live.worker_snapshot = None

    round_task = asyncio.get_running_loop().create_task(commander.ping_groups())
    await wait_for(lambda: live.worker_snapshot is not None)

    # The live group has already been served while the mute one is still
    # spending its two timeouts: the round is one turn per group, in parallel.
    assert not round_task.done()

    await round_task

    assert mute.state == "aborted"
    assert live.state == "running"


# -- the clock's decisions --


async def test_a_wake_brings_the_round_forward_on_that_group_only(
    commander, monkeypatch, clock
):
    monkeypatch.setattr(spa_commander, "HEARTBEAT_SECONDS", 30.0)
    first = group_double(commander, "first")
    second = group_double(commander, "second")
    clock()

    first.ping_now_event.set()
    await wait_for(lambda: first.turns)
    await asyncio.sleep(0.05)

    assert first.turns == [True]
    assert second.turns == []


async def test_a_round_that_raises_leaves_its_line_and_the_clock_beats_on(
    commander, monkeypatch, clock, caplog
):
    monkeypatch.setattr(spa_commander, "HEARTBEAT_SECONDS", 0.001)
    rounds: list[Any] = []

    async def failing_round(group_handlers: Any = None) -> None:
        rounds.append(group_handlers)
        raise RuntimeError("the round blew up")

    monkeypatch.setattr(commander, "ping_groups", failing_round)

    with caplog.at_level("ERROR"):
        beating = clock()
        await wait_for(lambda: len(rounds) >= 3)

    assert not beating.done()
    assert "the round failed" in caplog.text


async def test_a_group_still_in_its_turn_is_not_given_a_second_one(commander, caplog):
    slow = group_double(commander, "slow", delay=0.2)

    with caplog.at_level("WARNING"):
        await asyncio.gather(commander.ping_groups(), commander.ping_groups())

    assert slow.turns == [False]
    assert "still in its turn" in caplog.text


async def test_the_turns_of_a_round_run_together_and_a_failing_one_takes_nobody_with_it(
    commander,
):
    first = group_double(commander, "first", delay=0.2)
    second = group_double(commander, "second", delay=0.2)
    failing = group_double(commander, "failing", failing=True)

    started = asyncio.get_running_loop().time()
    await commander.ping_groups()
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.4
    assert first.turns == [False]
    assert second.turns == [False]
    assert failing.turns == [False]


async def test_a_beat_that_expired_during_an_anticipated_round_is_owed(
    commander, monkeypatch
):
    monkeypatch.setattr(spa_commander, "HEARTBEAT_SECONDS", 0.01)
    noisy = group_double(commander, "noisy")
    noisy.ping_now_event.set()

    woken = await commander._wait_beat()
    assert [group.name for group in woken] == ["noisy"]

    # The anticipated round outlives the beat: the timer expires OUTSIDE the
    # wait, and the next pass owes a full round instead of re-arming in silence.
    await asyncio.sleep(0.05)
    assert await commander._wait_beat() == []


async def test_a_group_ringing_at_every_breath_cannot_postpone_the_full_round(
    commander, monkeypatch, clock
):
    monkeypatch.setattr(spa_commander, "HEARTBEAT_SECONDS", 0.05)
    monkeypatch.setattr(SpaCommander.check_resources, "every_beats", 1)
    noisy = group_double(commander, "noisy")
    quiet = group_double(commander, "quiet")

    async def ring() -> None:
        while True:
            noisy.ping_now_event.set()
            await asyncio.sleep(0.005)

    ringing = asyncio.get_running_loop().create_task(ring())
    clock()
    # The timer survives the wakes it loses to: the quiet group still gets its
    # full rounds, and the vertex's own tasks still get their turns.
    await wait_for(lambda: len(quiet.turns) >= 2)
    ringing.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await ringing

    assert commander.beat_counts["check_resources"]["runs"] >= 1
    assert len(noisy.turns) >= len(quiet.turns)


async def test_each_task_of_the_vertex_runs_on_its_own_count_of_beats(
    commander, monkeypatch, clock
):
    monkeypatch.setattr(spa_commander, "HEARTBEAT_SECONDS", 0.001)
    # The cadence is read off the method at every call, so moving it here is all a
    # test has to do — the clock knows nothing about who is due.
    monkeypatch.setattr(SpaCommander.drop_expired_users, "every_beats", 1)
    monkeypatch.setattr(SpaCommander.check_resources, "every_beats", 2)
    monkeypatch.setattr(SpaCommander.cleanup_frozen, "every_beats", 1000)

    def runs_of(task_name: str) -> int:
        return commander.beat_counts.get(task_name, {}).get("runs", 0)

    def turns_of(task_name: str) -> int:
        return commander.beat_counts.get(task_name, {}).get("turns", 0)

    def round_closed() -> bool:
        """The last task of the round has caught up with the first: no round is open."""
        return turns_of("check_resources") >= 4 and turns_of("check_resources") == turns_of(
            "drop_expired_users"
        )

    beating = clock()
    # The clock is stopped ON A ROUND BOUNDARY and never mid-round: the counts are
    # read where every task of the beat has had the same number of turns, so the
    # relations below are exact and not a tolerance.
    await wait_for(round_closed)
    beats = turns_of("drop_expired_users")
    beating.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await beating

    assert runs_of("drop_expired_users") == beats
    assert runs_of("check_resources") == beats // 2
    # Its turns are counted all the same: the sweep is given every beat and says no.
    assert runs_of("cleanup_frozen") == 0
    assert turns_of("cleanup_frozen") == beats


async def test_a_task_that_raises_leaves_the_others_of_its_beat_alone(
    commander, monkeypatch, clock, caplog
):
    monkeypatch.setattr(spa_commander, "HEARTBEAT_SECONDS", 0.001)

    # Named as the method it stands in for: the row of a periodic method is kept
    # under that method's own name.
    @every(1)
    async def drop_expired_users(self) -> None:
        raise RuntimeError("the freezer disk is on fire")

    monkeypatch.setattr(SpaCommander, "drop_expired_users", drop_expired_users)
    monkeypatch.setattr(SpaCommander.check_resources, "every_beats", 1)

    def runs_of(task_name: str) -> int:
        return commander.beat_counts.get(task_name, {}).get("runs", 0)

    with caplog.at_level("ERROR"):
        clock()
        await wait_for(lambda: runs_of("check_resources") >= 2)

    # The bad disk is loud in the log and in its own row, and it takes nothing
    # else of its beat down — least of all the check that reads that same disk.
    reaper = commander.beat_counts["drop_expired_users"]
    assert reaper["errors"] >= 2
    assert reaper["last_error"] == "RuntimeError: the freezer disk is on fire"
    assert "failed" in caplog.text


# -- the group's own count of turns --


async def test_the_group_reads_its_shape_on_its_own_count_of_turns(
    make_group, monkeypatch
):
    checks: list[int] = []
    monkeypatch.setattr(group_handler.GroupHandler, "check_occupancy", counting_check(checks, 3))
    group = make_group()

    await group.ping()
    await group.ping()

    assert checks == []

    await group.ping()

    assert checks == [1]


async def test_a_woken_group_reads_its_shape_at_once_and_the_wake_is_spent(
    make_group, monkeypatch
):
    checks: list[int] = []
    monkeypatch.setattr(group_handler.GroupHandler, "check_occupancy", counting_check(checks, 1000))
    group = make_group()
    group.ping_now()

    await group.ping()

    assert checks == [1]

    await group.ping()

    assert checks == [1]


# -- the tasks nobody below the vertex can do --


async def test_the_frozen_are_forgotten_each_on_the_clock_of_his_own_kind(make_commander):
    commander = make_commander(user_expiry_hours=0.0, guest_expiry_hours=1000.0)

    # Nobody frozen: the sweep does not so much as reach for a thread.
    await commander.drop_expired_users(now=True)

    parked_state(commander, "mario")
    parked_state(commander, f"{GUEST_PREFIX}cid-g")
    # A row marked frozen whose parcel never reached the disk has no age to
    # judge: the sweep of the deposit is what answers for him, not the reaper.
    commander.connection_user_map["cid-p"] = "paolo"
    commander.resolve_user("cid-p")
    commander.mark_user_frozen("paolo")

    await commander.drop_expired_users(now=True)

    assert "mario" not in commander.user_map
    assert commander.freeze_handler.get_item_header("mario") is None
    assert f"{GUEST_PREFIX}cid-g" in commander.user_map
    assert "paolo" in commander.user_map
    assert commander.counters["frozen_users_discarded"] == 1


async def test_the_freezer_gives_up_what_no_row_of_the_vertex_claims(commander):
    parked_state(commander, "mario")
    parked_state(commander, "nobody")
    # The row goes WITHOUT the disk being told: a server killed before its dump, a
    # restore from a dump older than the freezer. It is the only way a folder is
    # left unclaimed, since forgetting a user now takes his state with him.
    del commander.user_map["nobody"]

    await commander.cleanup_frozen(now=True)

    assert commander.freeze_handler.user_folders == {
        commander.freeze_handler.user_to_userkey("mario")
    }
    assert commander.counters["orphan_folders_discarded"] == 1


async def test_the_memory_past_its_line_saturates_the_machine_and_asks_for_more(
    make_commander, monkeypatch
):
    commander = make_commander(CountingCommander, machine_memory_alarm_percent=40.0)
    # The gauge is read from /proc, which this platform may not have at all: the
    # arithmetic under test is the line, not the reading.
    monkeypatch.setattr(commander, "_machine_memory_used_percent", lambda: 42.0)

    await commander.check_resources(now=True)

    assert commander.state == "saturated"
    assert commander.asked == 1

    # The line moved out of the way is the same as the memory freeing: nobody has
    # to say the crisis is over.
    commander.machine_memory_alarm_percent = 50.0

    await commander.check_resources(now=True)

    assert commander.state == "running"
    assert commander.asked == 1


async def test_the_storage_under_the_reserve_is_said_out_loud_but_saturates_nobody(
    short_root, monkeypatch
):
    log_path = short_root / "orders.log"
    commander = CountingCommander(
        short_root / "frozen_users", orchestration_log_path=log_path
    )
    # No real volume is 100% free, so the lamp is on whatever disk runs the test.
    monkeypatch.setattr(spa_commander, "STORAGE_RESERVE_PERCENT", 100.0)

    await commander.check_resources(now=True)

    # Room on disk is not something the pool can grow into: the sysop is told,
    # the machine asks for more, and nobody is refused a seat over it.
    assert commander.state == "running"
    assert commander.asked == 1
    assert "outcome=on_reserve" in log_path.read_text()
