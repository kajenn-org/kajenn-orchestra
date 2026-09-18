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

"""The template process: build the group engine once, then fork workers off it.

``python -m kajenn_orchestra.orchestration.template_entry`` is what a GroupHandler
spawns, one per group, named ``template-<group>``. It builds the expensive thing
its group's workers all need — the ``group_engine`` — and from then on every
worker of that group is a ``fork`` of this process, so the engine is not built
again and not copied: the children find it already in their own memory.

**No asyncio here, on purpose.** A ``WorkerEntry`` runs ``asyncio.run``, and a
child forked out of a running loop inherits that loop as *running* — the call
would raise — over an epoll the parent shares. A synchronous template deletes
that whole class of trouble instead of working around it, and the measurements
the design rests on (one thread, no open channels, the macOS fork traps mute)
were taken on a process of exactly this shape.

**Everything travels as JSON lines on the pipes**, and nothing in an environment
variable. The first line ``stdin`` carries is this process's own configuration::

    {"name": "template-standard",
     "engine_factory": "some.module:SomeFactory",
     "kwargs": {"whatever the factory needs": "..."}}

``engine_factory`` is a ``module.path:ClassName`` reference to a class the
deployment provides; it is instantiated with ``kwargs`` and asked for the engine
by calling ``build_group_engine()`` on it. Both keys are mandatory: a template
with no factory has nothing to share, so a short line is a contract violation
and ends the process.

Every line after the first is one fork request, and it carries a worker's whole
spawn payload — the same object the handler already writes for a spawned child,
unchanged. The answer is one line back on ``stdout``: the child's pid, or an
error when the fork was refused.

**The child never receives the payload — it already has it.** ``fork`` copies
the memory, so the request the parent had just parsed is in the child's own
variable. That is also why the engine crosses at all: an object cannot travel in
an environment variable, and here it does not have to.

Three things happen in the child before anything else, in this order: it takes a
session of its own (``setsid``, or a kill of its process group would take the
template and every sibling with it); it closes the inherited pipes (a child
holding the write end of ``stdout`` open would mask the template's death, whose
clean signal is the EOF the GroupHandler reads); and only then does it live the
ordinary worker life, ``WorkerEntry`` with the payload and the engine.

``stderr`` is left alone — it is not a pipe — so the logs of the template and of
every child go where the GroupHandler's own go.

**The answer channel is nobody's stdout.** At birth (real pipes only, never the
injected ones of a test) the template duplicates its stdout descriptor and
keeps the duplicate as the channel, then points ``stdout`` itself at
``stderr``: the engine build runs arbitrary deployment code, and a ``print()``
in it must land in the logs — landing in the channel made the GroupHandler
read it as an answer, and the first forks of every container start failed with
«Extra data» until the build's lines were consumed. The forked child closes
the duplicate with the other pipes, and its own stdout — like the template's —
speaks to the logs.
"""

from __future__ import annotations

import gc
import importlib
import json
import logging
import os
import signal
import sys
import threading
from typing import Any

from genro_toolbox.smartasync import set_sync

from .worker_entry import WorkerEntry

#: The keys the first line cannot omit: without a factory there is nothing to share.
REQUIRED_KEYS = ("name", "engine_factory")

__all__ = ["REQUIRED_KEYS", "TemplateEntry"]


class TemplateEntry:
    """One template's whole life: read the first line, build the engine, fork on demand.

    Args:
        pipe_in: the line source; ``sys.stdin`` when omitted.
        pipe_out: where the answers go; ``sys.stdout`` when omitted.
    """

    def __init__(self, pipe_in: Any = None, pipe_out: Any = None) -> None:
        # Before any factory runs, and before any loop exists (D22): a storage
        # node reached under an unpinned loop hands back a coroutine, not a value.
        set_sync()
        self.pipe_in = sys.stdin if pipe_in is None else pipe_in
        if pipe_out is None:
            # The answer channel is a DUPLICATE of the real stdout, taken for
            # this process alone, and stdout itself is pointed at stderr —
            # BEFORE the engine is built, because the factory runs arbitrary
            # deployment code (the bridge's builds a whole legacy site) and
            # every print() of that build used to land IN the channel: the
            # GroupHandler read those lines as answers and the first forks of
            # every container start failed with «Extra data» (diagnosis of
            # 2026-08-28). From here on a print anywhere in this process — the
            # build, the frozen engine, a forked child before it closes its
            # copy — goes to the logs, and the channel carries answers only.
            # Only for the REAL stdout: a test that injects its pipes gets
            # them verbatim, and no descriptor of the test runner is touched.
            self.pipe_out = os.fdopen(os.dup(sys.stdout.fileno()), "w")
            os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
        else:
            self.pipe_out = pipe_out
        self.name = ""
        self.group_engine: Any = None
        self.logger = logging.getLogger(__name__)

    def read_launch(self) -> dict[str, Any]:
        """Read and validate the first line; a violation ends the process.

        Returns:
            The launch configuration as the GroupHandler wrote it.

        Raises:
            SystemExit: the pipe closed first, the line is not a JSON object, or
                a key nothing here may invent is missing.
        """
        raw = self.pipe_in.readline()
        if not raw:
            raise SystemExit("the pipe closed before the launch line arrived")
        try:
            config = json.loads(raw)
        except ValueError as exc:
            raise SystemExit(f"the launch line is not valid JSON: {exc}") from None
        if not isinstance(config, dict):
            raise SystemExit(
                f"the launch line must be a JSON object, got {type(config).__name__}"
            )
        missing = [key for key in REQUIRED_KEYS if not config.get(key)]
        if missing:
            raise SystemExit(f"the launch line is missing {', '.join(missing)}")
        return config

    def load_class(self, dotted: str) -> type:
        """Resolve a ``module.path:ClassName`` reference to the class object.

        Args:
            dotted: the reference as the launch line carries it.

        Returns:
            The class.

        Raises:
            SystemExit: the reference is not in that form.
        """
        module_path, _, class_name = dotted.partition(":")
        if not module_path or not class_name:
            raise SystemExit(f"engine_factory must be 'module.path:ClassName', got {dotted!r}")
        return getattr(importlib.import_module(module_path), class_name)

    def build_group_engine(self, config: dict[str, Any]) -> Any:
        """Ask the declared factory for this group's engine.

        Args:
            config: the launch configuration.

        Returns:
            Whatever the factory built. This process never looks inside it.
        """
        factory_class = self.load_class(config["engine_factory"])
        return factory_class(**(config.get("kwargs") or {})).build_group_engine()

    @property
    def live_thread_count(self) -> int:
        """How many threads are alive in this process; one is the fork invariant.

        A property of the PROCESS, not of this object: a template is a fresh
        process and answers 1, while the same code inside a test runner answers
        whatever that runner left running.
        """
        return threading.active_count()

    def freeze_heap(self) -> None:
        """Put everything alive out of the collector's reach, before the first fork.

        The engine is the biggest thing this process will ever hold, and the
        children get it for free — until the collector walks that inherited graph
        looking for cycles. Walking it writes to every object it touches, and a
        written page stops being shared: the child pays for the engine one page at
        a time, for a walk that can find nothing, because nothing here is garbage.
        ``gc.freeze`` moves what exists now into the permanent generation, which
        the collector never visits, and the children inherit that with the fork.

        Measured on the bridge's own site, 2026-08-24: 98 MB per worker after 200
        requests instead of 153. What a child allocates while serving is tracked
        and collected as always — only what the template built is frozen.

        The price, declared: among frozen objects no cycle is ever collected
        again, so two of them that point at each other and become unreachable stay.
        Reference counting still frees what it can, and the engine lives as long as
        the process does.
        """
        gc.freeze()
        self.logger.info(
            "Template %s: %s objects frozen, %s left for the collector",
            self.name,
            gc.get_freeze_count(),
            len(gc.get_objects()),
        )

    def reap_children(self, signum: int, frame: Any) -> None:
        """Collect the children that have died, and nothing else.

        The template is their parent only on paper: it buries them so they do not
        stay zombies, and never reads a status, decides, or reports. Installed as
        the ``SIGCHLD`` handler; a blocking read it interrupts resumes on its own.
        """
        while True:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if pid == 0:
                return

    def reply(self, answer: dict[str, Any]) -> None:
        """Write one answer line and push it out, so the buffer is empty at the next fork."""
        self.pipe_out.write(json.dumps(answer) + "\n")
        self.pipe_out.flush()

    def fork_worker(self, payload: dict[str, Any]) -> None:
        """Fork one worker for this payload, or refuse out loud; answers either way.

        Args:
            payload: the worker's spawn payload, as the handler wrote it.

        The refusal is not a silent check: a second thread at fork time is what
        arms the macOS traps this design is only safe without, so a template that
        has grown one says so and forks nothing.
        """
        alive = self.live_thread_count
        if alive != 1:
            self.reply({"error": f"{self.name}: {alive} threads alive, refusing to fork"})
            return
        pid = os.fork()
        if pid == 0:
            self.live_as_worker(payload)
        self.logger.info("Template %s: forked %s (pid %s)", self.name, payload["name"], pid)
        self.reply({"pid": pid})

    def live_as_worker(self, payload: dict[str, Any]) -> None:
        """Leave the template behind, then become the worker this payload names.

        Args:
            payload: the worker's spawn payload, already in this process's memory.

        Never returns. Two steps in this order: a session of its own, so a kill of
        this worker's process group cannot reach the template or a sibling; then
        the inherited pipes closed, so this child cannot mask the template's death
        by holding the write end open.
        """
        os.setsid()
        self.pipe_in.close()
        self.pipe_out.close()
        self.become_worker(payload)

    def become_worker(self, payload: dict[str, Any]) -> None:
        """Run the ordinary worker life and exit with its code.

        Args:
            payload: the worker's spawn payload.

        Never returns. The exit is the ordinary one, not ``os._exit``, so whatever
        the engine registered to run at exit still runs.
        """
        entry = WorkerEntry(config=payload, group_engine=self.group_engine)
        sys.exit(entry.run())

    def serve(self) -> None:
        """Build the engine, then fork on every line until the pipe ends.

        Sets ``name`` and ``group_engine``. Returns when the pipe closed: the
        GroupHandler is gone, and a template with nobody to serve has nothing to
        do — the workers already forked keep serving without it.
        """
        config = self.read_launch()
        self.name = config["name"]
        self.group_engine = self.build_group_engine(config)
        self.freeze_heap()
        signal.signal(signal.SIGCHLD, self.reap_children)
        self.logger.info("Template %s: engine built, waiting (pid %s)", self.name, os.getpid())
        while True:
            raw = self.pipe_in.readline()
            if not raw:
                self.logger.info("Template %s: its pipe ended, leaving", self.name)
                return
            self.fork_worker(json.loads(raw))

    def run(self) -> int:
        """Run the whole life; 0 when the pipe ended."""
        self.serve()
        return 0


def main() -> int:
    """``python -m kajenn_orchestra.orchestration.template_entry``: the launch shell."""
    logging.basicConfig(level=os.environ.get("KAJENN_LOG_LEVEL", "INFO"))
    return TemplateEntry().run()


if __name__ == "__main__":
    sys.exit(main())
