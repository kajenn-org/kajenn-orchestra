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

"""WorkerHandler: the handler, which stays, and the process under it, which does not.

A handler belongs to its group, carries a short name (``standard_0001``, minted by
the group; short because the socket path has a hard system budget) and owns one
socket. The process under it is replaceable: it can be killed, it can die on its
own, it can be reborn on the same name and the same socket — and every placement
pointing at the handler is untouched by all of that.

**Four orders, and each verb carries its object.** ``launch_process`` opens the
wire, spawns the child and waits for it to present itself; ``terminate_process``
kills the process group and waits for the OS to bury it; ``quit_process`` asks
the process to leave and waits for it to be gone; ``ping_process`` is one health
beat, and it gives back what the child answered. Nothing here freezes a user,
closes a tap or reads a policy: those belong one level up, and a handler that
took them would be deciding for the pool.

**The refusal is the answer.** ``assign_user`` is no order: it is the judgement a
group asks for while it walks its workers looking for one that takes a user. The
handler judges ITSELF — its own last photo against the setpoint its group carries
— and says no by RAISING, the class of the refusal being the reason. It writes
nothing: where a user lives is the group's map, and who is inside a process is
what that process announces.

**Low tolerance, and never two processes.** A mute beat is repeated ONCE past
the timeout — against a lost packet, not against a sick worker — and then the
process group is killed and its OS death awaited: SIGKILL, no escalation and no
grace, because a grace period is the users waiting. Only after that death may a
successor be launched: the wire is one, so a handler is never two processes. The
declared price is that the slow-but-healthy dies and its users log in again,
which is seconds of error instead of minutes of spinner.

**The death is a STATE, not a mark posed from outside.** ``state`` carries one of
five values and nobody but this handler writes it: ``starting`` (spawned, not yet
presented), ``running``, ``quitting`` (asked to leave, draining, not coming
back), ``quitted`` (died as it was ordered to, and the group has yet to consume
the fact) and ``aborted`` (died with nobody waiting for it — the wild death).

**The classification is the PURE WAIT.** An order to die parks a wait; the end of
the wire resolves it. An end of wire WITH a live wait is the death somebody
asked for; an end of wire without one is wild — no mark to set in advance, and
none to give back. The wait is parked ONLY when a child is really on the wire: a
wire with nobody on it reports nothing, and a wait left behind for a report that
never comes would make the handler read its next wild death as ordered. Either
way the handler writes the state, rings its group's wake and stops there: the
group learns at that round, reading the state. This
handler owns the list of its users (``hosted_users``), not the indexes of anybody
else — the group unhooks it from the placement and the Commander, the single
writer of the maps, prunes the traces, discards the parcels and removes the
semaphores the dead one had announced. Which is why nothing in this module
touches the deposit.

**Every envelope goes to the chain, and the chain answers what goes down.** What
arrives from below is handed to ``envelope_handler`` — the handler's own layer of
the fold — which reads the photo, lets the levels above read the worker events,
and gives back the payload for the envelope going down. So this handler carries
no knowledge of what a worker event means, and the wire carries none either:
the wire writes what it is handed. The one thing that answer carries today is the
global store, whole, and only to a process presenting itself — which is the only
one holding none of it.

**No worker-owned counters here.** The handler holds ``worker_snapshot``, the last photo its
process sent — filed by its own layer of the chain from whatever envelope carried
it, so a live process has one from its very presentation: the gauges the judge
reads are all in there EXCEPT CPU: the commander reads this process's cumulative
CPU clock through psutil, by way of the handler, and keeps the two-reading anchor
here. That temperature travels on no envelope. Aggregate counters still belong to the
Commander, which is also the one that decides the orders worth counting.

**The beat has no clock of its own.** ``ping_process`` is one beat and
``process_ping_interval`` is the cadence it is meant to be called at; the clock
that calls it belongs to whoever governs the group. What the handler does say is
whether it is worth beating at all: every envelope stamps the instant it
arrived, so a process that has just answered traffic is not ``silent`` and its
group leaves it alone. The handler's burial — the
socket taken away — is ``WorkerConnector.stop()``, called by whoever closes the
handler for good.

Every order and every wild death leaves its line here through the module
logger; the dedicated ``orchestration.log`` file, with its path, size and
rotation, is grammar and is not built yet.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import psutil

from .envelope_handler import WorkerEnvelopeHandler
from .exceptions import (
    AssignmentRefused,
    NoRoomError,
    WorkerQuittingError,
)
from .worker_connector import ENVELOPE_SLOT_PRESENTATION, WorkerConnector
from .worker_process import ForkedProcess, SpawnedProcess, WorkerProcess

#: The environment variable the spawn payload travels in, as today.
WORKER_ENV_VAR = "KAJENN_WORKER"

#: The orders going DOWN are paths on the worker's own tree (#59, D59-14): the
#: first segment names who ISSUES the order — ``group`` for the group's
#: (orchestration), ``commander`` for the vertex's — and the last one the
#: operation, a ``@route`` method of ``GroupOrders`` or ``CommanderOrders``.
#:
#: The health beat, and nothing else: it asks whether the process is alive, it
#: does not ask for the photo, which rides every envelope on its own.
PING_OP_PATH = "/group/ping"

#: The debug door: evaluate one expression inside the child, repr back.
EVAL_OP_PATH = "/commander/eval"

#: The structured reading of a whole process: every register, JSON-safe, in
#: one answer. Unlike the photo it is not periodic and nobody acts on it — it
#: exists to be shown to a human.
CENSUS_OP_PATH = "/commander/census"

#: The switch of the observation: whether the process reports every register
#: mutation of its own up the lane, as it happens. Off unless somebody is
#: watching — an observer must not change what it observes.
OBSERVE_OP_PATH = "/commander/observe"

#: The routing key of the order to leave: the process drains and ends itself.
#: Its answer comes back at once, carrying the photo with every user flagged for
#: cession — the level above parks them all in one read.
QUIT_OP_PATH = "/group/quit"

#: The routing key that takes one user off the process, and the one that takes
#: off a single connection of his. Each names the verb of ``SpaWorker`` that
#: serves it, and carries that verb's own argument.
DROP_USER_OP_PATH = "/group/drop_user"
DROP_CONNECTION_OP_PATH = "/group/drop_connection"

#: The routing key of the ordered freeze of ONE user: the worker waits for
#: whatever holds him — a pull bringing him home, his calls in flight — parks
#: him, and only then answers, so the REPLY IS the confirmation. A user this
#: process does not host is refused out loud in that same REPLY.
FREEZE_USER_OP_PATH = "/group/freeze_user"

#: The routing key of the worker's OWN announcement: the envelope of what
#: happened in this process while no CALL was being served — the transfer cycle
#: of a quit — folded exactly as the envelope of a REPLY is. Every other worker
#: event rides the REPLY of the CALL that caused it (owner, 2026-09-04). It is
#: the group's operation: the worker announces to ITS group (#59, D59-14).
ANNOUNCE_OP_PATH = "/group/announce"

#: How long an ordered departure may take before this handler stops waiting for
#: it, in seconds. Past it the process is killed and the death that follows is an
#: abort like any other: whoever was leaving had its time.
QUIT_TIMEOUT_SECONDS = 30.0

#: Seconds between two beats of the same process — the cadence, not a clock.
PROCESS_PING_INTERVAL = 5.0

#: How long a process may stay mute before the beat counts as missed. Twice
#: this, and the process is killed.
PROCESS_PING_TIMEOUT = 10.0

# How often the wait for an OS death re-reads the process: nothing signals that,
# so it polls. The wait for the end of the wire does not — that one is a future.
WAIT_POLL_INTERVAL = 0.05

#: A temperature older than three intended samples is unavailable. One second
#: is the floor, so a brief event-loop delay cannot withdraw a healthy gauge.
CPU_TEMPERATURE_MIN_STALE_SECONDS = 1.0

__all__ = [
    "ANNOUNCE_OP_PATH",
    "CENSUS_OP_PATH",
    "DROP_CONNECTION_OP_PATH",
    "DROP_USER_OP_PATH",
    "EVAL_OP_PATH",
    "FREEZE_USER_OP_PATH",
    "OBSERVE_OP_PATH",
    "PING_OP_PATH",
    "PROCESS_PING_INTERVAL",
    "PROCESS_PING_TIMEOUT",
    "QUIT_OP_PATH",
    "QUIT_TIMEOUT_SECONDS",
    "WORKER_ENV_VAR",
    "WorkerHandler",
]


class WorkerHandler:
    """One handler of a group: its wire, its process, its users, its last photo.

    Args:
        group_handler: the group this handler belongs to; the end of a process is
            told to it as ``ping_now()``, it reads ``state`` at that round, and
            its ``envelope_handler`` is the way up for everything the process
            announces.
        name: the handler's name, minted by the group as ``<group>_<counter>``;
            it names the socket too, so it is short.
        instance_dir: the directory holding the sockets of this installation.
        frozen_users_path: the deposit root the child builds its own access to.
        entry_module: the module the child is started as (``python -m ...``).
        main_threadpool_size: the child's traffic pool size, None for its own
            default.
        aux_threadpool_size: the child's service pool size, much smaller.
        worker_class: the ``module:Class`` the child loads, None for its own.
        worker_kwargs: the grammar handed to that class; travels as ``kwargs``.
        executable: the interpreter to spawn with, this one by default.
        process_ping_interval: the cadence the beat is meant to be called at.
        process_ping_timeout: how long the process may stay mute per beat.
    """

    def __init__(
        self,
        group_handler: Any,
        name: str,
        *,
        instance_dir: str | Path,
        frozen_users_path: str | Path,
        entry_module: str,
        main_threadpool_size: int | None = None,
        aux_threadpool_size: int | None = None,
        worker_class: str | None = None,
        worker_kwargs: dict[str, Any] | None = None,
        executable: str | None = None,
        process_ping_interval: float = PROCESS_PING_INTERVAL,
        process_ping_timeout: float = PROCESS_PING_TIMEOUT,
    ) -> None:
        self.group_handler = group_handler
        self.name = name
        self.instance_dir = Path(instance_dir)
        self.frozen_users_path = Path(frozen_users_path)
        self.entry_module = entry_module
        self.main_threadpool_size = main_threadpool_size
        self.aux_threadpool_size = aux_threadpool_size
        self.worker_class = worker_class
        self.worker_kwargs = worker_kwargs or {}
        self.executable = executable or sys.executable
        self.process_ping_interval = process_ping_interval
        self.process_ping_timeout = process_ping_timeout
        self.process: WorkerProcess | None = None
        #: Where the process under this handler is in its life: ``starting``,
        #: ``running``, ``quitting``, ``quitted``, ``aborted``.
        #: Written only here; the group reads it at its round.
        self.state = "starting"
        #: The last photo the process sent, on whatever envelope carried it:
        #: memory, load, counts, per-connection clocks. Filed by this handler's
        #: own layer of the chain.
        self.worker_snapshot: dict[str, Any] | None = None
        #: The soft CPU admission (#43): True, this worker is a candidate for
        #: NEW users. The group's judge writes False when the smoothed
        #: ``cpu_temperature_percent`` crosses above ``cpu_admission_close_percent`` — the placement
        #: then skips this worker — and True again below
        #: ``cpu_admission_reopen_percent``. Over the threshold it stays closed;
        #: capacity is born only when concrete demand finds no open candidate.
        #: Sticky users are untouched, the memory veto in ``assign_user`` stands
        #: apart, and the state dies with the
        #: handler. State only — this handler decides nothing with it.
        self.cpu_admission_open = True
        #: When the placement last landed a user here (commander's monotonic
        #: clock), None before the first: the admission interval reads it.
        self.last_admission_monotonic: float | None = None
        #: The ``psutil.Process`` of the pid this handler owns, built once and
        #: rebuilt when the pid changes.
        self._process_probe: psutil.Process | None = None
        #: The last lightweight process reading as ``(process birth, cpu seconds,
        #: sample instant)``. The birth distinguishes a live worker from an
        #: unrelated process that later reused its pid.
        self._cpu_meter_reading: tuple[float, float, float] | None = None
        #: The worker's CPU share over the last meter interval, raw: telemetry
        #: only, no judge reads it. None until two readings of the same process
        #: exist: the first interval has no temperature yet.
        self.cpu_temperature_sample_percent: float | None = None
        #: The temperature the judges read: the raw samples through the group's
        #: asymmetric first-order filter (``cpu_heating_seconds`` up,
        #: ``cpu_cooling_seconds`` down), seeded by the first sample.
        self.cpu_temperature_percent: float | None = None
        #: When the temperature above was sampled, on the commander's monotonic
        #: clock, and the real width of the interval that produced it.
        self.cpu_temperature_sampled_at: float | None = None
        self.cpu_temperature_interval_seconds: float | None = None
        #: The offload condition last journaled for this worker, as
        #: ``(reason, subject)`` — ``single_user_overload`` and
        #: ``cpu_offload_no_active_candidate`` would otherwise repeat every
        #: beat for as long as they stand. Deduplication of the journal only,
        #: cleared when the worker leaves the offload picture; it dies with
        #: the handler and decides nothing.
        self.cpu_offload_condition: tuple[str, str | None] | None = None
        self.envelope_handler = WorkerEnvelopeHandler(self, group_handler.envelope_handler)
        self.connector = WorkerConnector(self, self.instance_dir / f"{name}.sock")
        self._logger = logging.getLogger(__name__)
        self._hosted_users: set[str] = set()
        self._death_wait: asyncio.Future[None] | None = None
        self._listening = False
        self._last_envelope_ts = 0.0
        self._running_since: float | None = None
        self._observation_switched = False
        self._observation_switch_tasks: set[asyncio.Task[Any]] = set()

    @property
    def life_seconds(self) -> float:
        """How long this worker has been serving, in seconds; 0.0 before it presented."""
        if self._running_since is None:
            return 0.0
        return time.monotonic() - self._running_since

    @property
    def requires_beat_ping(self) -> bool:
        """Whether nothing has been heard from this process for a whole cadence.

        Returns:
            True when the last envelope is older than ``process_ping_interval``.
        """
        return time.monotonic() - self._last_envelope_ts >= self.process_ping_interval

    @property
    def hosted_users(self) -> set[str]:
        """The users living on this handler's process; the fold is its single writer."""
        return self._hosted_users

    @property
    def spawn_payload(self) -> dict[str, Any]:
        """The child's whole configuration, as it travels JSON-encoded in ``KAJENN_WORKER``."""
        return {
            "name": self.name,
            "uds_url": self.connector.address,
            "frozen_users_path": str(self.frozen_users_path),
            "main_threadpool_size": self.main_threadpool_size,
            "aux_threadpool_size": self.aux_threadpool_size,
            "worker_class": self.worker_class,
            "kwargs": self.worker_kwargs,
        }

    def assign_user(self, user: str) -> None:
        """Judge whether this worker takes one more user, and refuse by raising.

        Args:
            user: the identity being placed here.

        Raises:
            WorkerQuittingError: its process is leaving or is gone.
            AssignmentRefused: it has not presented itself yet.
            NoRoomError: it already hosts ``worker_max_users`` placed users, or
                its memory is past ``worker_memory_admission_percent`` — the
                veto. The CPU admission is not judged here: the placement walks
                the open workers first and the CPU-closed ones only as its
                fallback, so this gate must let a closed worker take the user
                the memory allows.

        Nothing is written. The user count is read off ``user_worker_map``, the
        map the placement writes in the same breath — never ``hosted_users``,
        which the fold writes only when the worker has announced, and would let
        two rapid arrivals land on one worker before the first is on board.
        The admission interval is not judged here: it orders the placement's
        walk, it refuses nobody.
        """
        if self.state in ("quitting", "quitted", "aborted"):
            raise WorkerQuittingError(user, f"{self.name} is {self.state}")
        if self.state != "running":
            raise AssignmentRefused(user, f"{self.name} is {self.state}")
        policy = self.group_handler.policy
        placed = sum(
            1 for worker in self.group_handler.user_worker_map.values() if worker == self.name
        )
        if placed >= policy.worker_max_users:
            raise NoRoomError(user, f"{self.name} already hosts {placed} placed user(s)")
        memory_percent = self.group_handler.get_memory_occupancy_percent(self.worker_snapshot)
        if memory_percent > policy.worker_memory_admission_percent:
            raise NoRoomError(user, f"{self.name} stands at {memory_percent:.1f}% of memory")

    def get_process_cpu_reading(self) -> tuple[float, float] | None:
        """Read this worker's process birth and cumulative CPU seconds through psutil.

        Returns:
            ``(create time, cpu seconds)`` for the pid this handler owns, or
            None when there is no process or psutil cannot see it.

        The ``psutil.Process`` probe is built once per pid and reused. No
        command is spawned and no message is sent to the worker.
        """
        if self.process is None:
            return None
        try:
            if self._process_probe is None or self._process_probe.pid != self.process.pid:
                self._process_probe = psutil.Process(self.process.pid)
            times = self._process_probe.cpu_times()
            return self._process_probe.create_time(), times.user + times.system
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return None

    def record_cpu_reading(
        self, reading: tuple[float, float] | None, *, sampled_at: float
    ) -> float | None:
        """Turn two lightweight process readings into this worker's temperature.

        Returns:
            The filtered temperature after this reading, or None when the
            reading did not produce one (first of a process, cleared, no time
            elapsed).

        The raw share of one core over the interval is kept as
        ``cpu_temperature_sample_percent``; ``cpu_temperature_percent`` moves
        towards it by ``1 - exp(-elapsed / tau)``, with ``tau`` the group's
        ``cpu_heating_seconds`` when the sample is hotter than the filtered
        value and ``cpu_cooling_seconds`` when it is colder. The first sample of
        a process seeds the filter.

        A missing row or a changed process birth clears the unfinished measure;
        neither invents a zero. The result is separate commander-side telemetry:
        it never changes the full photo, while CPU orchestration reads this
        channel explicitly.
        """
        if reading is None:
            self._cpu_meter_reading = None
            self.cpu_temperature_sample_percent = None
            self.cpu_temperature_percent = None
            self.cpu_temperature_sampled_at = None
            self.cpu_temperature_interval_seconds = None
            return None
        created_at, cpu_seconds = reading
        previous = self._cpu_meter_reading
        self._cpu_meter_reading = (created_at, cpu_seconds, sampled_at)
        if previous is None or previous[0] != created_at:
            self.cpu_temperature_sample_percent = None
            self.cpu_temperature_percent = None
            self.cpu_temperature_sampled_at = None
            self.cpu_temperature_interval_seconds = None
            return None
        elapsed = sampled_at - previous[2]
        if elapsed <= 0.0:
            return None
        burned = max(0.0, cpu_seconds - previous[1])
        sample = 100.0 * min(burned / elapsed, 1.0)
        current = self.cpu_temperature_percent
        if current is None:
            temperature = sample
        else:
            policy = self.group_handler.policy
            tau = policy.cpu_heating_seconds if sample > current else policy.cpu_cooling_seconds
            temperature = current + (1.0 - math.exp(-elapsed / tau)) * (sample - current)
        self.cpu_temperature_sample_percent = sample
        self.cpu_temperature_percent = temperature
        self.cpu_temperature_sampled_at = sampled_at
        self.cpu_temperature_interval_seconds = elapsed
        return temperature

    def get_cpu_temperature_percent(self) -> float | None:
        """Return the fresh commander-side temperature, never a stale value."""
        temperature = self.cpu_temperature_percent
        sampled_at = self.cpu_temperature_sampled_at
        cadence = self.group_handler.spa_commander.cpu_temperature_sample_seconds
        if temperature is None or sampled_at is None or cadence is None:
            return None
        stale_after = max(CPU_TEMPERATURE_MIN_STALE_SECONDS, 3.0 * cadence)
        if time.monotonic() - sampled_at > stale_after:
            return None
        return temperature

    def read_envelope(self, envelope: dict[str, Any]) -> dict[str, Any]:
        """Hand an envelope that arrived from the process to the chain.

        Args:
            envelope: the payload as it came off the wire.

        Returns:
            What goes back down, as the chain composed it — nothing at all when
            there is no envelope going the other way.

        Stamps the instant this process was last heard from. The envelope that
        carries the presentation — the ``pid`` slot, said at birth and never
        again — is told to the vertex through ``on_worker_presented``, the seam
        a consumer overrides to speak to a newborn process.
        """
        self._last_envelope_ts = time.monotonic()
        spa_commander = self.group_handler.spa_commander
        if not self._observation_switched and spa_commander.observation_watched:
            self._observation_switched = True
            self._fire_observation_switch()
        if ENVELOPE_SLOT_PRESENTATION in envelope:
            spa_commander.on_worker_presented(self)
        return self.envelope_handler(envelope)

    def _fire_observation_switch(self) -> None:
        """Turn this process's observation on without holding up the envelope it presented with."""
        task = asyncio.create_task(self.connector.call(OBSERVE_OP_PATH, {"on": True}))
        self._observation_switch_tasks.add(task)
        task.add_done_callback(self._observation_switch_tasks.discard)

    def serve_child_call(self, path: str, data: dict[str, Any]) -> Any:
        """Resolve a CALL the child placed on the lane on the group's dispatcher, and call it.

        Args:
            path: the routing key the child chose — ``/group/…`` or ``/commander/…``.
            data: its payload, handed over as the operation's keyword arguments.

        Returns:
            Whatever the operation answers, sync or awaitable; the wire awaits
            it if needed and puts it in the REPLY.

        Raises:
            NotFound: no operation at that path, on either tree; the wire turns
                it into an error REPLY, so the child is answered either way.
            TypeError: the payload does not fit the operation's signature —
                refused before the body runs, the same way.

        This handler is only the rung the call climbs: nothing is written here.
        """
        return self.group_handler.group_dispatcher.route.node(path)(**data)

    async def launch_process(self) -> None:
        """Open the wire if it is closed, spawn the child, wait for it to present itself.

        Raises:
            RuntimeError: a process is already alive under this handler.
            TimeoutError: the child never presented itself within
                ``process_ping_timeout``; it is killed before the raise.

        Sets ``process`` and ``state`` — ``starting`` while the child is on its
        way, ``running`` once it has presented itself — and binds the socket on
        the first launch.
        """
        if self.process is not None and self.process.alive:
            raise RuntimeError(
                f"WorkerHandler {self.name}: its process (pid {self.process.pid}) is still alive"
            )
        if not self._listening:
            await self.connector.start()
            self._listening = True
        self.state = "starting"
        self.process = await self.start_process()
        self._logger.info(
            "Worker %s: launched its process (pid %s) on %s",
            self.name,
            self.process.pid,
            self.connector.address,
        )
        try:
            await asyncio.wait_for(self.connector.wait_connected(), self.process_ping_timeout)
        except TimeoutError:
            self._logger.warning(
                "Worker %s: its process never presented itself in %.1fs — killing",
                self.name,
                self.process_ping_timeout,
            )
            await self.terminate_process()
            raise
        self.state = "running"
        self._running_since = time.monotonic()

    async def start_process(self) -> WorkerProcess:
        """Bring the process into the world, by fork when the group has a template.

        Returns:
            The process, however it was born.

        Raises:
            TemplateRefused: the group has a template and it did not fork.
        """
        template = self.group_handler.template
        if template is None:
            return self.spawn_process()
        return ForkedProcess(await template.fork_worker(self.spawn_payload))

    def spawn_process(self) -> SpawnedProcess:
        """Start a brand new interpreter with the payload in its environment.

        Returns:
            The spawned process.

        A program that starts fresh carries nothing of this one, so the payload
        has to travel in something the exec preserves — which is why this birth
        uses an environment variable and the forked one does not.
        """
        env = dict(os.environ)
        env[WORKER_ENV_VAR] = json.dumps(self.spawn_payload)
        return SpawnedProcess(
            subprocess.Popen(
                [self.executable, "-m", self.entry_module], env=env, start_new_session=True
            )
        )

    async def terminate_process(self) -> None:
        """Kill the process group and wait until the OS has buried it; clears ``process``.

        The wait is bounded by ``QUIT_TIMEOUT_SECONDS``, the same bound an ordered
        death already has. A spawned process always ends it, because this handler
        is the parent and reading ``alive`` buries it. A forked one may not: it
        stays a zombie — alive, to a pid — until its template collects it, and a
        template that has stopped collecting would hold this wait forever. Past
        the bound the handler says so and lets go.
        """
        process = self.process
        self._logger.info("Worker %s: killing its process (pid %s)", self.name, process.pid)
        self._kill_process_group()
        try:
            await asyncio.wait_for(self._wait_for_death(process), QUIT_TIMEOUT_SECONDS)
        except TimeoutError:
            self._logger.warning(
                "Worker %s: its process (pid %s) was killed but is still not buried "
                "after %.0fs — letting go of it",
                self.name,
                process.pid,
                QUIT_TIMEOUT_SECONDS,
            )
        self.process = None

    async def _wait_for_death(self, process: WorkerProcess) -> None:
        """Poll until that process is gone."""
        while process.alive:
            await asyncio.sleep(WAIT_POLL_INTERVAL)

    async def quit_process(self, freezer_path: str | None = None) -> None:
        """Ask the process to leave, and wait until it is gone.

        Args:
            freezer_path: where the parcels of this departure go, when they must
                not go to the working deposit — the reboot directory of a soft
                quit. None leaves the child on its own deposit.

        Sets ``quitting`` and parks the wait its death resolves. Past
        ``QUIT_TIMEOUT_SECONDS`` on either leg the wait is dropped and the process
        is killed, so the death that follows is an abort.
        """
        self._logger.info("Worker %s: asked to leave", self.name)
        self.state = "quitting"
        death = self._park_death_wait()
        try:
            await self.connector.call(
                QUIT_OP_PATH, {"freezer_path": freezer_path}, timeout=QUIT_TIMEOUT_SECONDS
            )
            await asyncio.wait_for(death, QUIT_TIMEOUT_SECONDS)
        except TimeoutError:
            self._death_wait = None
            self._logger.warning(
                "Worker %s: still here %.1fs after being asked to leave — killing its process",
                self.name,
                QUIT_TIMEOUT_SECONDS,
            )
            await self.terminate_process()

    async def ping_process(self) -> dict[str, Any] | None:
        """One health beat: are you alive? Kill the process if it stays mute.

        Returns:
            The payload the child answered with, or None when it answered neither
            beat and its process was killed for it.

        Acts on the process when it stays mute: the end of the wire writes the state.
        """
        for beat in (1, 2):
            try:
                return await self.connector.call(
                    PING_OP_PATH, timeout=self.process_ping_timeout
                )
            except TimeoutError:
                self._logger.warning(
                    "Worker %s: beat %s of 2 unanswered after %.1fs",
                    self.name,
                    beat,
                    self.process_ping_timeout,
                )
        self._logger.warning("Worker %s: mute to both beats — killing its process", self.name)
        await self.terminate_process()
        return None

    def on_child_lost(self) -> None:
        """The wire died: the parked wait says whether anybody was expecting it.

        Sets ``state`` — ``quitted`` when a wait was live, ``aborted`` when the
        death was nobody's order — rings the group's wake, and gives the vertex
        back the store grant this process was holding, if it held one.
        """
        self.group_handler.spa_commander.commander_dispatcher.global_store.release_worker_lock(
            self.name
        )
        ordered = self._settle_death_wait()
        if ordered:
            self.state = "quitted"
            self._logger.info("Worker %s: its process left as it was asked to", self.name)
        else:
            self.state = "aborted"
            self._logger.warning(
                "Worker %s: WILD death of its process, %s users on board",
                self.name,
                len(self._hosted_users),
            )
        self.group_handler.ping_now()

    def _kill_process_group(self) -> None:
        """SIGKILL the child's whole process group; one already gone is the same outcome."""
        process = self.process
        if not process.alive:
            return
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass

    def expect_death(self) -> None:
        """Say that the death about to happen is awaited, so it is not a wild one.

        Acts on the parked wait, whose being live is what ``on_child_lost`` reads
        to tell an ordered death from a wild one: what follows is ``quitted`` and
        no alarm is owed for it. The killing is the caller's own next step.
        """
        self._park_death_wait()

    def _park_death_wait(self) -> asyncio.Future[None]:
        """Park the wait an ordered death resolves; its being live IS the order."""
        self._death_wait = asyncio.get_running_loop().create_future()
        return self._death_wait

    def _settle_death_wait(self) -> bool:
        """Resolve the parked wait if one is live, and say whether one was.

        Returns:
            True when somebody was waiting; a wait already given up counts for nobody.
        """
        death = self._death_wait
        self._death_wait = None
        if death is None or death.done():
            return False
        death.set_result(None)
        return True
