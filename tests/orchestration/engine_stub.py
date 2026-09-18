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

"""What a deployment provides to the fork path, played small enough to test.

The bridge's own factory builds a whole genropy site; this one builds a string.
The worker beside it is the other half of the same seam: a worker that declares
``group_engine``, as the bridge's does and as the base worker does not.

Both are importable by name, because both are resolved from a payload — the
factory in the template, the worker in the child it forks.
"""

from __future__ import annotations

import json
import os
from typing import Any

from kajenn_orchestra.orchestration import SpaWorker


class EngineFactory:
    """Asked once, in the template, for the thing every worker of a group shares.

    Args:
        mark: what to build the engine out of, so a test can recognise it.
    """

    def __init__(self, mark: str = "plain") -> None:
        self.mark = mark

    def build_group_engine(self) -> str:
        """The engine, which here is a string and in the bridge is a site."""
        return f"engine-{self.mark}"


class BrokenFactory:
    """A factory that cannot build, so its template dies while trying."""

    def build_group_engine(self) -> str:
        """Never returns anything: this is how a deployment's own failure looks."""
        raise RuntimeError("this factory cannot build an engine")


class NoisyFactory(EngineFactory):
    """A factory whose build PRINTS, as the bridge's site build really does.

    The prints must reach the logs and never the answer channel: read as
    answers they failed the first forks of every container start
    («Extra data», diagnosed 2026-08-28).
    """

    def build_group_engine(self) -> str:
        """The same engine, announced out loud on stdout first."""
        print("noise: the engine build talks on stdout")
        print("noise: and more than once")
        return super().build_group_engine()


class EngineWorker(SpaWorker):
    """A worker that takes the engine, and writes down what it was given.

    Args:
        group_engine: the engine its group's template built.
        report_path: where to write what arrived, for a test in another process
            to read; nothing is written when it is not given.
    """

    def __init__(
        self,
        name: str,
        *,
        group_engine: Any,
        report_path: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, **kwargs)
        self.group_engine = group_engine
        if report_path is not None:
            with open(report_path, "w") as report:
                json.dump(
                    {"worker": name, "engine": group_engine, "pid": os.getpid()}, report
                )
