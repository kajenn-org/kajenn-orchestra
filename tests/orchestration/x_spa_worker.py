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

"""The instrumentation every end-to-end story needs from its child, and no story.

One thing, and it is apparatus rather than subject: the MEMORY a process declares
replaces the reading of ``/proc/self/status``, which macOS has not, so a story
whose subject is occupancy stays readable on every platform. The number travels
in the child's own ``worker_kwargs``, which is how the grammar configures the
class it names.

A story that needs a frozen user needs no door of its own any more: the group is
the rung that judges who sleeps, and it is asked for its round by hand.

What the site IS belongs to the story: this class wires ``wsgi_app`` to ``site``
and each story writes that method.
"""

from __future__ import annotations

from typing import Any

from kajenn_orchestra.orchestration import SpaWorker


class X_SpaWorker(SpaWorker):
    """A worker that declares its memory."""

    def __init__(self, name: str, *, declared_rss_bytes: int = 0, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        self.declared_rss_bytes = declared_rss_bytes
        self.wsgi_app = self.site

    @property
    def rss_bytes(self) -> int:
        """What this process declares it holds, in bytes."""
        return self.declared_rss_bytes

    @property
    def pss_bytes(self) -> None:
        """Leave PSS unavailable so the portable RSS fallback is exercised."""
        return None

    def site(self, environ: dict[str, Any], start_response: Any) -> list[bytes]:
        """The WSGI callable of the story this child belongs to.

        Args:
            environ: the PEP 3333 environ the seam built.
            start_response: the PEP 3333 callable.

        Returns:
            The body, in one chunk.
        """
        raise NotImplementedError(f"{type(self).__name__} has no site")
