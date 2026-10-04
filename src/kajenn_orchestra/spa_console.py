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

"""The pool's debug door, served as MCP tools: ask a live server anything.

One mechanism, no predicted questions: ``eval`` evaluates a Python
expression in one process of an SPA pool — ``commander`` in the server
process, or any worker by name, reached over the lane the commander already
holds to every child — and answers the value's ``repr``. Whatever was not
foreseen is readable by composing an expression; what a target's namespace
holds is on :meth:`SpaConsole.eval`.

**Mounting IS the gate.** The door is full eval by construction — there is no
read-only eval in Python — so it exists only where the recipe mounts
:class:`SpaConsoleMcpApplication` on purpose, and must never be mounted in
production. An MCP client connects to the app's endpoint
and asks in natural language; ``targets`` lists what can be looked into.
"""

from __future__ import annotations

from typing import Any

from genro_routes import RoutingClass, route

from kajenn.applications.mcp import McpApplication
from .spa_app import SpaApplication

__all__ = ["SpaConsole", "SpaConsoleMcpApplication"]


class SpaConsole(RoutingClass):
    """The tool surface: every route is an MCP tool.

    Args:
        application: the MCP app this surface belongs to — its server is where
            the SPA fronts are found, at call time and never before (the tools
            run on a mounted, started server; the surface is built earlier).
    """

    def __init__(self, application: Any) -> None:
        self.route.plug("pydantic")
        self.application = application

    @route()
    async def targets(self) -> dict:
        """Every process the door can look into, by SPA application code."""
        return {
            code: front.commander.console_targets for code, front in self.spa_fronts.items()
        }

    @route()
    async def eval(self, expr: str, target: str = "commander", app: str = "") -> dict:
        """Evaluate a Python expression in one process of the pool.

        Args:
            expr: the expression; the namespace holds ``commander`` on the
                vertex, ``worker`` inside a child.
            target: ``commander`` (default), or a worker's name from ``targets``.
            app: the SPA application code — needed only when the server mounts
                more than one SPA front.

        Returns:
            The target and the value's ``repr``.
        """
        front = self.spa_front(app)
        return {"target": target, "repr": await front.commander.eval_in_target(target, expr)}

    @property
    def spa_fronts(self) -> dict[str, SpaApplication]:
        """The SPA fronts mounted on this server, by code."""
        return {
            code: mounted
            for code, mounted in self.application.server.applications.items()
            if isinstance(mounted, SpaApplication)
        }

    def spa_front(self, app: str) -> SpaApplication:
        """The front ``app`` names — or the only one, when the server has one.

        Raises:
            ValueError: no SPA front here, or several and ``app`` named none.
        """
        fronts = self.spa_fronts
        if app:
            if app not in fronts:
                raise ValueError(f"no SPA front {app!r} — have: {', '.join(fronts) or 'none'}")
            return fronts[app]
        if len(fronts) == 1:
            return next(iter(fronts.values()))
        raise ValueError(
            f"several SPA fronts ({', '.join(fronts) or 'none'}): name one with app="
        )


class SpaConsoleMcpApplication(McpApplication):
    """The MCP app whose whole tool surface is the pool's debug door.

    Recipe-friendly: mount it and the door exists, leave it out and it does
    not — mounting is the gate, and a production recipe never mounts it.
    """

    mcp_name = "kajenn-spa-console"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(routing_class=SpaConsole(self), **kwargs)
