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

"""The ``_server/inspector`` section: the SPA pool shown to a human.

Three addresses, one purpose — watching a pool while it works:

    ``/_server/inspector/page``     the page (HTML, from ``resources/``)
    ``/_server/inspector/census``   the whole pool as JSON, under the front's code
    ``/_server/inspector/stream``   the observation stream, as SSE

**Mounting IS the gate.** The SPA front attaches this section on its own
startup, only when ``KAJENN_INSPECTOR`` is set, and no route carries an
``auth_rule``: the inspector is a collaudo instrument, so it exists where
somebody asked for it and nowhere else. It lives here, in the SPA world, and
not among the server sections, because it reads a pool and nothing else — a
core that carries no orchestration must not import one to look at it.

**It never traverses the hosted site.** No cookie is minted, no connection is
opened, no site path is called: the section reads the commander's own surfaces
(``get_pool_census``) and its observation stream. An observer that changes
what it observes is useless.

Parent (dual relationship): the SpaApplication that attached it, stored as
``self.application``. A server has ONE (owner, 2026-09-07), so the census is
keyed by that front's code and carries the one entry the page already reads.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

from genro_routes import RoutingClass, route

from kajenn.sse import SseStream
from kajenn.streaming import StreamingResponse

if TYPE_CHECKING:
    from .spa_app import SpaApplication

__all__ = ["InspectorSection", "INSPECTOR_ENV_VAR"]

#: The environment variable whose presence mounts the inspector at all.
INSPECTOR_ENV_VAR = "KAJENN_INSPECTOR"


class InspectorSection(RoutingClass):
    """The ``_server/inspector`` mount: the page, the census, the stream.

    Args:
        application: the SpaApplication that attached this section — the front
            whose pool is being watched.
    """

    def __init__(self, application: SpaApplication) -> None:
        self.application = application

    @route(media_type="text/html")
    def page(self) -> str:
        """The inspector page: the commander above, one row per worker below.

        Note:
            Route: GET /_server/inspector/page
        """
        return (Path(__file__).parent / "resources" / "inspector.html").read_text()

    @route(media_type="application/json")
    async def census(self) -> dict[str, Any]:
        """This front's whole pool, keyed by its application code.

        Note:
            Route: GET /_server/inspector/census
        """
        return {self.application.code: await self.application.commander.get_pool_census()}

    @route()
    async def stream(self) -> StreamingResponse:
        """The observation stream: one ``census`` event, then every mutation.

        Note:
            Route: GET /_server/inspector/stream
        """
        return SseStream(self.observation_events(), retry_ms=2000).response()

    async def observation_events(self) -> AsyncIterator[dict[str, Any]]:
        """The events the page reads: the opening census, then what the pool reports.

        Subscribes the front's commander on open and unsubscribes it when the
        reader goes away — which is what switches the workers' reporting off
        again. The queue is read against the server's ``leaving``: a server that
        starts leaving ends the feed here, so the stream closes by itself
        instead of being cancelled when the graceful wait runs out.
        """
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        commander = self.application.commander
        server = self.application.server
        await commander.subscribe_observation(queue)
        try:
            yield {"event": "census", "data": await self.census()}
            while True:
                observation = await server.get_until_leaving(queue)
                if observation is None:
                    return
                yield {"event": "observation", "data": observation}
        finally:
            await commander.unsubscribe_observation(queue)
