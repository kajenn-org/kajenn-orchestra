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

"""The group's wire to its template: launch it, ask it to fork, close it.

One per group, owned by the ``GroupHandler`` — the template is one per group
because the engine is, so a per-worker owner would be the wrong place. What
travels on it is JSON lines over the template's own ``stdin`` and ``stdout``: the
first line is the template's configuration, every line after it is one worker's
spawn payload, and each of those is answered with the forked child's pid.

**The launch is lazy and the relaunch is the same code.** Nothing starts a
template on a schedule: the first ``fork_worker`` finds none alive and starts one,
and so does the first one after a death. A group whose template died therefore
pays the engine's build once more and keeps going, while the workers already
forked serve on without it — their wire is intact and the template was never
watching them anyway.

**The first answer costs the engine.** The template reads its first line, builds
the engine, and only then looks for a fork request, so the answer to the first
request arrives after the whole build. One timeout covers both, which is why it
is generous.

A refusal comes back as a line with ``error`` in it, and becomes
``TemplateRefused`` here: the caller is ``start_worker``, which already knows what
to do with a launch that did not happen.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import Any

#: The module a template process runs.
TEMPLATE_ENTRY_MODULE = "kajenn_orchestra.orchestration.template_entry"

#: How long one fork request may take to be answered. Generous on purpose: the
#: first answer of a template's life waits for the engine to be built.
TEMPLATE_FORK_TIMEOUT = 60.0

#: How long a template may take to leave after its pipe was closed.
TEMPLATE_STOP_TIMEOUT = 10.0

__all__ = [
    "TEMPLATE_ENTRY_MODULE",
    "TEMPLATE_FORK_TIMEOUT",
    "TEMPLATE_STOP_TIMEOUT",
    "TemplateConnector",
    "TemplateRefused",
]


class TemplateRefused(Exception):
    """The template did not fork: it said no, or it was not there to say anything."""

    def __init__(self, template_name: str, cause: str) -> None:
        super().__init__(f"{template_name}: {cause}")
        self.template_name = template_name
        self.cause = cause


class TemplateConnector:
    """One group's template, and the two-message conversation with it.

    Args:
        group_handler: the group this template serves; its name names the template.
        engine_factory: ``module.path:ClassName`` of the class that builds the engine.
        engine_kwargs: what that class is instantiated with.
        executable: the interpreter to run the template with; this one by default.
        entry_module: the module the template runs.
        fork_timeout: how long one request may take to be answered.
    """

    def __init__(
        self,
        group_handler: Any,
        *,
        engine_factory: str,
        engine_kwargs: dict[str, Any] | None = None,
        executable: str | None = None,
        entry_module: str = TEMPLATE_ENTRY_MODULE,
        fork_timeout: float = TEMPLATE_FORK_TIMEOUT,
    ) -> None:
        self.group_handler = group_handler
        self.engine_factory = engine_factory
        self.engine_kwargs = engine_kwargs or {}
        self.executable = executable or sys.executable
        self.entry_module = entry_module
        self.fork_timeout = fork_timeout
        self.process: asyncio.subprocess.Process | None = None
        self._logger = logging.getLogger(__name__)

    @property
    def name(self) -> str:
        """This template's name: its group's, said as a template."""
        return f"template-{self.group_handler.name}"

    @property
    def alive(self) -> bool:
        """Whether there is a template process to talk to."""
        return self.process is not None and self.process.returncode is None

    @property
    def launch_payload(self) -> dict[str, Any]:
        """The first line: who this template is, and what it must build."""
        return {
            "name": self.name,
            "engine_factory": self.engine_factory,
            "kwargs": self.engine_kwargs,
        }

    async def launch_process(self) -> None:
        """Start the template and hand it its first line.

        Sets ``process``. The engine is not waited for here: the template builds
        it while nobody watches, and the wait lands on the first fork request.
        """
        self.process = await asyncio.create_subprocess_exec(
            self.executable,
            "-m",
            self.entry_module,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        await self.write_line(self.launch_payload)
        self._logger.info("Template %s: launched (pid %s)", self.name, self.process.pid)

    async def write_line(self, message: dict[str, Any]) -> None:
        """Put one JSON line on the template's stdin and push it out."""
        stdin = self.process.stdin
        stdin.write((json.dumps(message) + "\n").encode())
        await stdin.drain()

    async def read_answer(self) -> dict[str, Any]:
        """Read the one line the template answers with.

        Returns:
            The answer, parsed.

        Raises:
            TemplateRefused: the pipe ended instead — the template is gone.
        """
        raw = await self.process.stdout.readline()
        if not raw:
            raise TemplateRefused(self.name, "its pipe ended before it answered")
        return json.loads(raw)

    async def fork_worker(self, spawn_payload: dict[str, Any]) -> int:
        """Ask for one worker and give back the pid of the child that was forked.

        Args:
            spawn_payload: the worker's whole configuration, unchanged.

        Returns:
            The forked child's pid.

        Raises:
            TemplateRefused: it refused, it died, or it did not answer in time.

        Launches the template first when there is none alive, which is both the
        first launch of a group and the relaunch after a death.
        """
        if not self.alive:
            await self.launch_process()
        try:
            await self.write_line(spawn_payload)
            answer = await asyncio.wait_for(self.read_answer(), self.fork_timeout)
        except TimeoutError:
            raise TemplateRefused(
                self.name, f"no answer in {self.fork_timeout:.0f}s"
            ) from None
        if "pid" not in answer:
            raise TemplateRefused(self.name, answer.get("error", "no pid in its answer"))
        return int(answer["pid"])

    async def stop(self) -> None:
        """Close the pipe and wait for the template to leave; kill it if it stays.

        Clears ``process``. The workers it forked are not touched: they are not its
        children in any sense that matters, and they keep serving.
        """
        process = self.process
        if process is None:
            return
        self.process = None
        if process.returncode is not None:
            return
        process.stdin.close()
        try:
            await asyncio.wait_for(process.wait(), TEMPLATE_STOP_TIMEOUT)
        except TimeoutError:
            self._logger.warning(
                "Template %s: still here %.0fs after its pipe was closed — killing",
                self.name,
                TEMPLATE_STOP_TIMEOUT,
            )
            process.kill()
            await process.wait()
