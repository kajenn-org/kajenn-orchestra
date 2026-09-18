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

"""The worker process: one child, one wire, one worker, nothing else.

``python -m kajenn_orchestra.orchestration.worker_entry`` is what a WorkerHandler
spawns. The child is **monolingual**: it speaks only its handler's socket — no
HTTP port, no uvicorn, no second door. Its whole configuration arrives in the
``KAJENN_WORKER`` environment variable as one JSON object, the seven keys
the handler writes::

    {"name": "standard_0001",
     "uds_url": "uds:/tmp/gnr_x/standard_0001.sock",
     "frozen_users_path": "/var/lib/gnr/frozen_users",
     "main_threadpool_size": 8,
     "aux_threadpool_size": 2,
     "worker_class": "kajenn_orchestra.orchestration.spa_worker:SpaWorker",
     "kwargs": {"group": "standard"}}

``name``, ``uds_url`` and ``frozen_users_path`` are mandatory — the handler
always knows all three — and a missing or malformed variable is a spawn
contract violation: the process says so and exits, never guesses a default.
``worker_class`` is a ``module.path:ClassName`` reference and ``kwargs`` is the
grammar that class was configured with; the real deployment names a subclass,
because the base worker hosts no site.

One thing never travels in that payload: the ``group_engine``. It is an object,
the payload is JSON, and it is built once per group by the template process this
child may have been forked from — so it arrives as a constructor argument
instead, and ``build_worker`` puts it in the worker's own call only when there is
one. A child spawned the ordinary way gets none, and the base worker does not
declare it: it hosts no engine.

**Storage is declared synchronous first thing.** The genro-storage nodes are
``smartasync``: under a running loop they hand back a coroutine instead of a
value. The server pins the sync dispatch when it is built, and so does this
child — before its loop exists, so every task it spawns inherits the pin.

The life is one ``asyncio.run``: build the worker, connect to the socket it was
given, present it (pid and config echo, the answer bringing the whole global
store), then read envelopes until the wire ends. Two deaths reach that end. The
worker leaves on its own — ``quit`` closed the wire from this side — and there
is nothing left to do. Or the wire dies under it: the parent is gone, this
process is an orphan, and the D8 self-defense runs — everybody into the deposit,
then out. Either way the exit code is 0: whoever is watching reads a clean exit
as "this child is gone", not as a crash to throttle.

``WorkerEntry`` is importable and testable on its own; the ``main()`` at the
bottom is the thin ``python -m`` shell around it.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import sys
from typing import Any

from genro_toolbox.smartasync import set_sync

from kajenn.channel.frame import FrameStream
from .freeze_handler import FreezeHandler
from .spa_worker import SpaWorker
from .worker_handler import WORKER_ENV_VAR

#: The class the child builds when the payload names none: the base worker,
#: which serves the protocol and hosts no site of its own.
DEFAULT_WORKER_CLASS = "kajenn_orchestra.orchestration.spa_worker:SpaWorker"

#: The keys the child cannot invent for itself if they are missing.
REQUIRED_KEYS = ("name", "uds_url", "frozen_users_path")

__all__ = ["DEFAULT_WORKER_CLASS", "REQUIRED_KEYS", "WorkerEntry"]


class WorkerEntry:
    """One spawned worker's whole life, driven by the ``KAJENN_WORKER`` payload.

    Args:
        config: the spawn payload; read from the environment when omitted.
        group_engine: the object the template built once for the whole group and
            this child inherited by fork; None when this child was spawned, and
            then nothing of the sort reaches the worker.
    """

    def __init__(
        self, config: dict[str, Any] | None = None, *, group_engine: Any = None
    ) -> None:
        # Before the loop exists, so every task of this process inherits it
        # (D22): a storage node reached under a loop that is not pinned hands
        # back a coroutine instead of a value.
        set_sync()
        self.config = self.read_config() if config is None else config
        self.name: str = self.config["name"]
        self.uds_url: str = self.config["uds_url"]
        self.frozen_users_path: str = self.config["frozen_users_path"]
        self.main_threadpool_size: int | None = self.config.get("main_threadpool_size")
        self.aux_threadpool_size: int | None = self.config.get("aux_threadpool_size")
        self.worker_class: str = self.config.get("worker_class") or DEFAULT_WORKER_CLASS
        self.kwargs: dict[str, Any] = self.config.get("kwargs") or {}
        self.group_engine = group_engine
        self.worker: SpaWorker | None = None
        self.logger = logging.getLogger(__name__)

    def read_config(self) -> dict[str, Any]:
        """Parse and validate the spawn payload; a violation ends the process.

        Returns:
            The payload as the handler wrote it.

        Raises:
            SystemExit: the variable is absent, unparsable, or short of a key
                nothing here may invent.
        """
        raw = os.environ.get(WORKER_ENV_VAR)
        if not raw:
            raise SystemExit(f"{WORKER_ENV_VAR} is not set: nothing to spawn")
        try:
            config = json.loads(raw)
        except ValueError as exc:
            raise SystemExit(f"{WORKER_ENV_VAR} is not valid JSON: {exc}") from None
        if not isinstance(config, dict):
            raise SystemExit(
                f"{WORKER_ENV_VAR} must be a JSON object, got {type(config).__name__}"
            )
        missing = [key for key in REQUIRED_KEYS if not config.get(key)]
        if missing:
            raise SystemExit(f"{WORKER_ENV_VAR} is missing {', '.join(missing)}")
        return config

    def load_class(self, dotted: str) -> type:
        """Resolve a ``module.path:ClassName`` reference to the class object.

        Args:
            dotted: the reference as the payload carries it.

        Returns:
            The class.

        Raises:
            SystemExit: the reference is not in that form.
        """
        module_path, _, class_name = dotted.partition(":")
        if not module_path or not class_name:
            raise SystemExit(f"worker_class must be 'module.path:ClassName', got {dotted!r}")
        return getattr(importlib.import_module(module_path), class_name)

    def build_worker(self) -> SpaWorker:
        """Build the configured worker with its deposit, its pools and its grammar.

        Returns:
            The worker, with no wire yet.

        The deposit is built HERE, on this side: nothing is handed a
        FreezeHandler over the channel, because the road to safety must not
        depend on the wire.

        ``group_engine`` joins the call only when this child has one, so a base
        worker — which does not declare it — is built by the same line.
        """
        worker_class = self.load_class(self.worker_class)
        kwargs = dict(self.kwargs)
        if self.group_engine is not None:
            kwargs["group_engine"] = self.group_engine
        return worker_class(
            self.name,
            freeze_handler=FreezeHandler(self.frozen_users_path),
            main_threadpool_size=self.main_threadpool_size,
            aux_threadpool_size=self.aux_threadpool_size,
            **kwargs,
        )

    async def connect(self) -> FrameStream:
        """Open the wire to the handler's socket.

        Returns:
            The frame codec over that connection.
        """
        reader, writer = await asyncio.open_unix_connection(self.uds_url.removeprefix("uds:"))
        return FrameStream(reader, writer)

    async def serve(self) -> None:
        """Build, present, serve — and leave when the wire dies first.

        Sets ``worker``. Returns when the worker has left: on its own decision,
        or because the wire under it was gone, which saves nothing. A worker that
        declared BOTH hosted seams dies before the wire exists: that is a
        contradiction, not a choice.
        """
        self.worker = self.build_worker()
        # The one configuration error the boot catches: two seams assigned at
        # once, which is a contradiction somebody declared. NO seam at all is
        # not an error — it is the base worker, which serves its orders and no
        # request, and whose http CALL is refused by the property when it comes.
        if self.worker.asgi_app is not None and self.worker.wsgi_app is not None:
            self.worker.hosted_app_seam
        self.worker.attach_stream(await self.connect())
        await self.worker.send_presentation(self.config)
        self.logger.info(
            "Worker %s: serving on %s (pid %s)", self.name, self.uds_url, os.getpid()
        )
        await self.worker.receive_frames()
        if not self.worker.exited:
            await self.worker.on_wire_lost()
        self.logger.info("Worker %s: exited cleanly", self.name)

    def run(self) -> int:
        """Run the whole life; 0 on either death."""
        asyncio.run(self.serve())
        return 0


def main() -> int:
    """``python -m kajenn_orchestra.orchestration.worker_entry``: the spawn shell."""
    logging.basicConfig(level=os.environ.get("KAJENN_LOG_LEVEL", "INFO"))
    return WorkerEntry().run()


if __name__ == "__main__":
    sys.exit(main())
