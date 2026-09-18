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

"""The mountable SPA front: one door, and no state at all.

**The pool is born with the server, not with the object.** An application reaches
its own configuration only once a server holds it, so the vertex is built at
startup out of what the recipe wrote under this front's code. There is ONE way to
build it, and the tests come through the same door as production.
``commander_class`` is a class attribute and not a kwarg: a Python type does not
travel in a recipe, and whoever wants another pool subclasses this front, which is
what the recipe already names.

**A pool belongs to the application that owns it.** The chain is server →
applications → this front → its ORCHESTRATION → its commander → its groups →
their workers, and the recipe says it in that shape: the pool's words are this
class's own grammar and are written under
``applications.<code>.orchestration.commander``. That node is REQUIRED — a spa
front declared without it, or with it and no commander under it, does not boot:
a front with no pool answers every request with a raise, and the recipe is asked
for the node instead. Several fronts on one server are legitimate,
each with its own orchestration. ``memory_max_percent`` stays a share of the
MACHINE, so apportioning it between two pools is the installation's own business;
an installation that over-declares is caught by the machine's alarm line, past
which nothing grows.

**Serving is a two-stage demux.**
Stage 1 reads the FIRST segment of the (already mount-relative) path: not one of
``internal_roots`` — the app's own first-level roots — and the path belongs to the
hosted site. Stage 2 resolves the FULL path in the app's own router: the node
exists, so the request is served natively; a structural miss under a claimed root
belongs to the site after all and falls through. ``resolves_natively`` asks the
router WITHOUT auth filters, so a route that exists but would be denied still
answers its own 403 natively and never leaks the site behind it.

**The forward is one line.** Everything the pool does — the identity, the wait of
a user between two homes, the placement, the wire — happens inside
``SpaCommander.serve_request``. This front never names a group, a worker or a
wire, and keeps no state of its own. It mints nothing either: the cookie carries
the hosted site's own connection id, read off the answer and written back on the
way out, so the identity the browser holds and the identity the site keeps are
one and the same.

**The pool is configurable while it runs.** ``orchestration.profile_name`` names
a stored profile the boot must find, ``env_settings`` is what the installation
fixed by environment — a constructor kwarg and no word of any grammar — and the
effective configuration of the one group is composed as
defaults ⊕ recipe ⊕ profile ⊕ env, the two immutable levels kept apart, so every
later apply recomposes instead of stacking. A profile governs exactly ONE group:
with zero or several the boot fails and a hot apply reads 409. With
``orchestration.control_enabled`` on, ``OrchestrationControl`` mounts under
``_orchestration`` — LAST, once the pool is actually up, so a boot that failed
leaves the router untouched — and the three routes reach the vertex's own apply;
with the gate off that path belongs to the hosted site like any other.

**What comes back out.** The site's own answer, rebuilt from the reply. A refusal
— nobody could take this user, or he stayed between two homes longer than this
front is willing to wait — is a polite **503** carrying the ``Retry-After`` the
vertex composed, because only the vertex knows when the machine will have decided
again. A failure — the site broke inside a healthy process, or the wire is gone —
is a **502**: the site is this gateway's upstream and its breakage is not the
client's fault. Both answer with a GENERIC line: the real text is written to the
log, where the sysop looks for it, and never handed to a browser, because an
exception's text carries the inside of the house.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING, Any, NoReturn

from genro_bag import BagResolver
from genro_builders.builder import element
from genro_routes import RoutingClass, route

from kajenn.asgi_endpoint import BufferedAsgiEndpoint
from kajenn.application import ApplicationGrammar
from kajenn.channel.frame import Frame
from kajenn.http_record import HttpRecord
from kajenn.transport_limits import FrameTooLarge, HttpBodyTooLarge, http_max_body_size
from kajenn.config.handler import ConfigError
from kajenn.exceptions import HTTPBadRequest, HTTPException, HTTPForbidden, HTTPNotFound
from kajenn.lifespan import FatalBootError
from kajenn.middleware.base import cookie_value
from kajenn_orchestra.orchestration_profile_store import (
    OrchestrationProfileContentError,
    OrchestrationProfileNameError,
    OrchestrationProfileNotFoundError,
    OrchestrationProfileStore,
)
from kajenn.response import Response
from kajenn.wsx_payload import SerializedWsxPayload
from kajenn.routed_application import RoutedApplication
from kajenn.server import QUITTING, REFUSED_RETRY_AFTER_SECONDS, RUNNING
from .inspector_section import INSPECTOR_ENV_VAR, InspectorSection
from .orchestration import AssignmentRefused, SiteFailedRequest, SpaCommander
from .orchestration.group_policy import GroupPolicy, GroupPolicyError
from .orchestration.spa_commander import SingleGroupRequired

if TYPE_CHECKING:
    from pathlib import Path

    from kajenn.types import Receive, Scope, Send

#: The routing cookie. Its value is the hosted site's OWN connection id, never a
#: number of ours: one identity space, so the chain from the cookie to the worker
#: translates nothing.
SPA_CONNECTION_ID_COOKIE = "spa_connection_id"

#: How long that cookie lives. The same 24 hours the site gives its own connection
#: cookie (``CONNECTION_TIMEOUT * 24``, gnrwebpage_proxy/connection.py): ours must
#: not die first, or a browser the site still recognises would come back with no
#: connection named and be routed anonymous for the rest of the day.
CONNECTION_COOKIE_MAX_AGE = 24 * 3600

#: How long a request may spend, in total, waiting for a user who is between two
#: homes. It is NOT derived from the beat: a move is an evict and an install,
#: milliseconds when things are well, and past a few seconds something is wrong
#: and the polite refusal is the honest answer.
REQUEST_HOLD_MAX_SECONDS = 5.0

#: The app's own root the runtime configuration answers under. It is claimed
#: ONLY when the gate is on: a first-level root of this front is a root the
#: hosted site loses, and a machine nobody reconfigures must not lose it.
ORCHESTRATION_ROOT = "_orchestration"

#: The front's internal root for what a page asks of its channel. The reserved
#: segment is the core's (the server answers ``/_wsx/ping`` itself); under an
#: application's mount it is this front's own control surface.
WSX_ROOT = "_wsx"

#: The words that used to hang on the application element and now live on the
#: ``orchestration`` node. A recipe still writing one of them is refused by name.
MOVED_APPLICATION_WORDS = ("profiles_path", "profile_name", "orchestration_control")

#: What the two refusals say out loud. The inside of the house — which user, which
#: worker, what the site raised — goes to the log and not through the wire.
ERR_503_TEXT = "server busy"
ERR_502_TEXT = "the site could not answer this request"


class SpaApplicationGrammar(ApplicationGrammar):
    """The words this front adds to a recipe, all under one node: ``orchestration``.

    They live HERE and not in the site dialect because a pool belongs to the
    application that owns it: the chain is server → applications → this front →
    ITS ORCHESTRATION → its commander → its groups → their workers, and the
    recipe says it in that shape::

        front = applications.application(app_class=SpaApplication, code="spa",
                                         mount="")
        orchestration = front.orchestration(profiles_path="/var/spa/profiles",
                                            profile_name="busy_hours",
                                            control_enabled=True)
        commander = orchestration.commander(frozen_users_path="/var/spa/frozen",
                                            instance_dir="/run/kajenn")
        commander.groups(default="standard").group(name="standard")

    ``orchestration`` is REQUIRED: a spa front IS its pool, so one declared
    without the node is an incomplete configuration and the server does not
    start. Wanting no pool is declaring no spa front. Nothing of the pool hangs
    on the application element any more — a recipe writing ``profiles_path``,
    ``profile_name`` or ``orchestration_control`` there is refused by name, with
    the new path in the message.

    ``env_settings`` is no word of any grammar: it is a dict the Python recipe
    composes at runtime out of the environment it has already read, and it
    travels as a plain constructor kwarg of the application — the last level of
    the overlay, above anything a recipe or a profile may say.
    """

    @element(sub_tags="commander[0:1]", node_label="orchestration")
    def orchestration(
        self,
        profiles_path: str | BagResolver | None = None,
        profile_name: str | None = None,
        control_enabled: bool = False,
    ) -> None:
        """The whole orchestration of this front: its own three words, then the pool.

        ``profiles_path`` is the folder the stored profiles are read from — the
        same one the ``_sysop`` archive writes — and ``profile_name`` the profile
        the boot must find and put in force. Named without a folder, or named and
        not there, or there and invalid: the server does not start.

        ``control_enabled`` opens the runtime configuration under the front's
        ``_orchestration`` root — apply, reload, status. Off, that root is never
        claimed and the path belongs to the hosted site.

        The node MUST carry a ``commander``: a profile and a control surface
        with no pool to act on address nothing, and the boot says so instead of
        starting half-configured. The node itself is not optional either — a spa
        front declared without it does not start.
        """

    @element(parent_tags="commander", sub_tags="group", collection_key="name")
    def groups(self, default: str = None) -> None:
        """Collection of worker groups, each labelled by its ``name`` — stable paths
        ``applications.<code>.orchestration.commander.groups.<name>``. A group is
        the workers built from ONE grammar: the same child, the same policies.

        The optional ``default`` ELECTS the group that receives whoever arrives
        with no past; omitted, the first group declared is the one. Unlike
        ``applications.default``, which is a redirect destination and elects
        nothing, this one decides where a newcomer is born."""

    @element(parent_tags="groups", sub_tags="")
    def group(
        self,
        name: str = None,
        memory_max_percent: float | BagResolver = None,
        worker_max_number: int | BagResolver = None,
        worker_memory_max_percent: float | BagResolver = None,
        worker_memory_admission_percent: float | BagResolver = None,
        restart_occupancy_max_percent: float | BagResolver = None,
        cpu_close_percent: float | BagResolver | None = None,
        cpu_admission_close_percent: float | BagResolver = None,
        cpu_admission_reopen_percent: float | BagResolver = None,
        cpu_offload_percent: float | BagResolver | None = None,
        cpu_retirement_quiet_seconds: float | BagResolver | None = None,
        cpu_heating_seconds: float | BagResolver | None = None,
        cpu_cooling_seconds: float | BagResolver | None = None,
        worker_admission_interval_seconds: float | BagResolver | None = None,
        worker_min_life_seconds: float | BagResolver = None,
        worker_max_users: int | BagResolver = None,
        user_idle_freeze_minutes: float | BagResolver = None,
        entry_module: str = None,
        executable: str | BagResolver = None,
        worker_class: str = None,
        main_threadpool_size: int | BagResolver = None,
        aux_threadpool_size: int | BagResolver = None,
        worker_kwargs: dict = None,
        engine_factory: str = None,
        engine_kwargs: dict = None,
    ) -> None:
        """One group of workers: its own policies, and the identity of its child.

        ``name`` is the collection key and names the group's workers too
        (``<name>_0001``), so it is short — a worker's name is its socket's.

        **Nothing here says how many workers there are.** The group brings its
        reception into being at boot and then grows on demand and shrinks by
        waste, so the count is a reading and never a setting —
        ``worker_max_number`` included: it says how many workers the quota is
        SIZED FOR (the per-worker ceiling becomes quota / that number, 6 when
        nothing is declared), and caps nothing. It replaces the bridge-era
        RAM-share-over-workers derivation with one intuitive count of slots;
        an explicit ``worker_memory_max_percent`` wins over it.

        The POLICIES: ``memory_max_percent`` is this group's share of the
        server's concession and ``worker_memory_max_percent`` what ONE worker may
        hold of that share (the same word one rung down — the cascade is machine,
        concession, quota, worker); ``worker_memory_admission_percent`` is how full a worker
        gets before it stops admitting and ``restart_occupancy_max_percent`` where
        a process is replaced instead of kept; ``cpu_close_percent`` is the temperature, shared onto the survivors, under
        which a worker is a closure candidate (unset, the reopen threshold itself)
        and ``worker_min_life_seconds`` the age before which a worker is no
        closure candidate; ``worker_admission_interval_seconds`` is how long after
        admitting a user a worker is skipped by the placement, so its load shows
        in the temperature first; ``user_idle_freeze_minutes`` is the silence past
        which the group parks a user in the freezer. ``cpu_admission_close_percent`` (experimental,
        off when omitted) turns on soft CPU admission: a worker above it is
        closed to NEW users and reopens below ``cpu_admission_reopen_percent``. CPU
        samples do not fork processes. When a concrete arrival finds no open
        worker that can admit it, placement creates one worker and assigns that
        same user. ``cpu_offload_percent`` (off when omitted; requires
        ``cpu_admission_close_percent`` and sits above it) makes a CPU-closed worker past
        it slim itself: one active user per beat — the least busy — is parked
        in the freezer, and his next request lands on an open worker or births
        one. ``cpu_retirement_quiet_seconds`` is how long the CPU must
        stay silent — no blocking or reopening — before retirement judges
        again: the quiet of the GROUP, distinct from the age of one worker,
        restarted whole by every CPU admission transition. ``cpu_heating_seconds``
        (1 s) and ``cpu_cooling_seconds`` (5 s) are the two time constants of the
        filter every CPU judge reads the 100 ms temperature through: a worker
        heats up fast and cools down slowly, so one idle sample never reopens it.

        The IDENTITY of the child: ``entry_module`` (what ``python -m`` runs),
        ``executable`` (the interpreter — a group is how two versions of a site
        live side by side), ``worker_class`` (the ``module:Class`` the child
        loads), the two thread pool sizes, and ``worker_kwargs``, the grammar that
        class is built with. The two paths are the installation's and are declared
        once, on ``commander``.

        The BIRTH of the child: ``engine_factory`` is the ``module:Class`` of the
        class that builds the one expensive thing all this group's workers share,
        and ``engine_kwargs`` what that class is built with. Declared, the group
        runs a template process that builds it once and forks every worker out of
        it, so the cost is paid once instead of once per worker. Omitted, the group
        spawns its workers the ordinary way and has no template at all.
        """

    @element(parent_tags="orchestration", sub_tags="groups[0:1]", node_label="commander")
    def commander(
        self,
        frozen_users_path: str | BagResolver = None,
        instance_dir: str | BagResolver = None,
        memory_max_percent: float | BagResolver = None,
        machine_memory_alarm_percent: float | BagResolver = None,
        orchestration_log_path: str | BagResolver = None,
        orchestration_log_max_bytes: int | BagResolver = None,
        orchestration_log_backup_count: int | BagResolver = None,
        user_expiry_hours: float | BagResolver = None,
        guest_expiry_hours: float | BagResolver = None,
        cpu_temperature_sample_seconds: float | BagResolver | None = None,
    ) -> None:
        """The SPA pool: the vertex's own policies, and the groups under it.

        The two PATHS of the installation, declared once here and shared by every
        group: ``frozen_users_path`` is the freezer root — the vertex reads what a
        worker wrote there, so it is one root for the whole machine — and
        ``instance_dir`` holds the sockets.

        ``cpu_temperature_sample_seconds`` is the cadence of commander-side,
        traffic-independent worker CPU measurement. CPU admission, placement and
        offload read this channel through each group's filter; omit it for the
        100 ms default.

        ``memory_max_percent`` is what this server may hold OF THE MACHINE (the
        concession; omitted, all of it), and every percentage below is a share of
        it. ``machine_memory_alarm_percent`` is the health line of the whole
        machine, past which nothing grows. The freezer's own storage answers to no
        key: under a tenth free the log says so and the machine asks for more.

        ``orchestration_log_path`` (+ ``_max_bytes`` / ``_backup_count``) is the
        file every order lands on — who decided, what, on whom, with which numbers
        and how it ended; omitted, the rows stay on the logger.

        ``user_expiry_hours`` / ``guest_expiry_hours`` are the ages a FROZEN user
        is kept for before the machine forgets him whole. A guest is shorter: he
        is a browser, not a person the machine knows.

        Technical times — the beat, the patience of a departure, the cadences —
        are module constants and not grammar: an installation tunes policies, not
        clocks.
        """


class OrchestrationControl(RoutingClass):
    """The runtime configuration of one pool: apply, reload, read.

    Mounted under ``_orchestration`` only when the front's gate is on. The three
    routes carry no logic of their own: they name what the caller asked for and
    hand it to the front, which owns the translation of the vertex's refusals.
    """

    def __init__(self, application: SpaApplication) -> None:
        self.application = application

    @route(openapi_method="post")
    async def apply(
        self, body_data: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Put the body in force as the profile level: an inline configuration.

        Args:
            body_data: the setpoints, written the way a stored profile writes
                them. Nothing stored stays active afterwards.

        Returns:
            The payload of the apply, as the vertex composed it.
        """
        return await self.application.apply_settings(
            profile=self.application.body_profile(body_data), source="inline"
        )

    @route(openapi_method="post")
    async def reload(
        self, body_data: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Read a stored profile off the disk again and put it in force.

        Args:
            body_data: optionally ``{"name": ...}`` — the profile to read, which
                becomes the active one; without it the active profile is reread.

        Returns:
            The payload of the apply, as the vertex composed it.

        Raises:
            HTTPException: 400 — no name was given and no profile is active, so
                there is nothing to reload.
        """
        asked = self.application.body_profile(body_data).get("name")
        name = asked or self.application.orchestration_commander.active_profile
        if name is None:
            raise HTTPBadRequest(
                "nothing to reload: no name was given and no profile is active"
            )
        return await self.application.apply_settings(profile_name=name, source="profile")

    @route()
    async def status(self) -> dict[str, Any]:
        """What configuration is in force right now; no lock is taken to answer."""
        return self.application.settings_status


class WebsocketOperations(RoutingClass):
    """What a WORKER calls on the vertex to reach a browser: the push.

    Attached under the commander's own operations by the front, because the
    delivery needs both halves of the machine — the registry of live sockets,
    which is the server's, and ``page_connection_map``, which is the vertex's.

    Args:
        application: the front, which holds the server and the commander.
    """

    def __init__(self, application: SpaApplication) -> None:
        self.application = application

    @route()
    async def send(
        self, page_id: str, path: str, data: Any = None, cid: str | None = None
    ) -> dict[str, Any]:
        """Write one message of the site onto the socket that page speaks on.

        Args:
            page_id: the page to address.
            path: what the client routes the message on.
            data: the payload, as the TYTX string the worker put on the lane —
                the lane is JSON and carries no date, Decimal or bytes of its
                own. The frontend forwards this explicit serialized value.
            cid: the connection the worker believes that page belongs to.

        Returns:
            ``{"delivered": bool}`` — written to a socket, or nobody there.

        The page is validated against ``page_connection_map`` before anything
        is written: a page the fold has already dropped is not there any more,
        and a page whose connection is another one is not this caller's to
        write to. Fire and forget (W-12): delivered means written to the
        socket, never executed by the page.
        """
        application = self.application
        owner = application.commander.page_connection_map.get(page_id)
        if owner is None or (cid is not None and owner != cid):
            return {"delivered": False}
        server = application.server
        return {"delivered": await server.send_serialized_message(
            page_id, path, SerializedWsxPayload(data)
        )}


class WsxControl(RoutingClass):
    """What a page asks of its channel, mounted under ``_wsx`` on the front.

    Sibling of ``OrchestrationControl``: a routing class under an internal root
    of the front, whose routes carry no logic of their own. ``openchannel`` is
    the only command for now, and it is the one a page MUST send before any
    other message of its own reaches the worker.
    """

    def __init__(self, application: SpaApplication) -> None:
        self.application = application
        # The neutral ``fields`` block is what tells ``bind_kwargs`` that a
        # handler declared ``_request``; it is the pydantic plugin that fills
        # it, so this surface arms it on its own router, as the console does.
        self.route.plug("pydantic")

    @route()
    async def openchannel(
        self, parameters: dict[str, Any] | None = None, _request=None
    ) -> dict[str, Any]:
        """Open the channel of one page: validate it, then write it on its row.

        Args:
            parameters: how it wants to be served on it; ``sequential`` asks for
                one call at a time.
            _request: the live request, injected by ``bind_kwargs``. Left
                unannotated so it stays out of the schema; it is where the
                connection AND the page are read from — a client does not get
                to say which connection it is, and the page it names travels in
                the envelope's own field, never in the payload.

        Returns:
            What the client reads as the answer of the command.

        Raises:
            HTTPForbidden: this page is not this connection's. The vertex knows
                which connection every page belongs to — the birth of a page
                rode the reply of the request that created it — so the check
                costs nothing and never goes down to the worker.
            HTTPBadRequest: the message names no page, or the request carries
                no connection at all.

        The page is bound to the socket by the CONNECTION, not here, and only
        because this answered 200: whoever holds the socket does the binding,
        whoever holds the pool decides (owner, 2026-09-07).
        """
        page_id = _request.scope.get("genro.page_id")
        if not page_id:
            raise HTTPBadRequest("openchannel names no page: put it in the envelope's page_id")
        cid = self.application.request_cid(_request.scope)
        if cid is None:
            raise HTTPBadRequest("this connection carries no cookie")
        commander = self.application.commander
        if commander.page_connection_map.get(page_id) != cid:
            raise HTTPForbidden(f"page {page_id!r} is not this connection's")
        reply = await commander.serve_wsx_request(
            cid,
            {"wsx": {"cid": cid, "page_id": page_id, "parameters": parameters}},
            hold_timeout=REQUEST_HOLD_MAX_SECONDS,
        )
        return {"channel": reply.get("result")}


class SpaApplication(RoutedApplication):
    """A single-page-application front backed by the new user-sticky pool."""

    #: The words this front adds to a recipe, read back under its own code.
    forwards_payloads = True

    grammar = SpaApplicationGrammar

    @property
    def handshake_cookie(self) -> str | None:
        """The connection cookie a websocket handshake must carry to reach this front.

        Returns:
            The name of the SPA's own connection cookie.

        Every message on that socket is a request of the user the cookie names,
        so a socket opened without one could never be served: the handshake is
        accepted and closed 1008, and the browser reads why. The first live
        probe found this property unimplemented and the socket left open for
        ever (#70).
        """
        return SPA_CONNECTION_ID_COOKIE

    #: The pool this front builds. A subclass names another one — a vertex that
    #: can grow its own machine, say — and the recipe names the subclass.
    commander_class: type[SpaCommander] = SpaCommander

    def __init__(self, *, env_settings: dict[str, Any] | None = None, **kwargs: Any) -> None:
        """Build the front; the whole orchestration is read at startup.

        Args:
            env_settings: the setpoints the installation fixed by environment,
                the strongest level of the overlay. It is a runtime dict and no
                word of any grammar, so it travels HERE and nowhere else.

        Raises:
            ConfigError: the recipe wrote one of the three words that moved
                under ``orchestration`` on the application element.
        """
        self.refuse_moved_words(kwargs)
        self._commander: SpaCommander | None = None
        self._channel_mounted = False
        self._logger = logging.getLogger(__name__)
        #: Where the named profiles are read from at boot; the vertex is given
        #: the same folder and reads them itself from there on. Written by the
        #: boot out of the orchestration node, so it is None until then.
        self.profiles_path: str | Path | None = None
        #: Which profile the boot puts in force, and the front's own answer to
        #: "what was active" until the first apply moves it.
        self.profile_name: str | None = None
        #: Whether the three configuration routes exist at all. The boot reads
        #: it, and mounts them only once the pool is up.
        self.control_enabled = False
        #: Whether THIS front already put them on its router. It tells a second
        #: boot (a retry, a stop and start) from a root somebody else claimed.
        self._control_mounted = False
        #: The environment's own level, kept as its own dict for good: every
        #: apply recomposes recipe ⊕ profile ⊕ env instead of stacking.
        self.env_settings = dict(env_settings or {})
        super().__init__(**kwargs)
        self._forward_slots = asyncio.Semaphore(16)

    def refuse_moved_words(self, kwargs: dict[str, Any]) -> None:
        """Refuse, by name, the words that moved under ``orchestration``.

        Args:
            kwargs: the constructor kwargs, which are the attributes the recipe
                wrote on the application element.

        Raises:
            ConfigError: naming every word found and the path it now lives at.
                The bare ``TypeError`` of an unexpected kwarg would say the word
                is unknown, when it is known and has moved.
        """
        moved = [word for word in MOVED_APPLICATION_WORDS if word in kwargs]
        if moved:
            raise ConfigError(
                f"{', '.join(moved)}: no longer written on the application element — "
                f"the orchestration of a spa front is one subtree, so these live on "
                f"applications.<code>.orchestration "
                f"(orchestration_control is now control_enabled)"
            )

    @property
    def commander(self) -> SpaCommander:
        """The pool this front owns.

        Raises:
            RuntimeError: it is not built yet — the vertex is born at startup,
                out of a configuration that only a mounted application can read.
        """
        if self._commander is None:
            raise RuntimeError(
                f"{type(self).__name__} has no pool yet: it is built when the server starts"
            )
        return self._commander

    @property
    def internal_roots(self) -> set[str]:
        """The app's OWN first-level roots, minus ``index``.

        Recomputed per access from the STRUCTURAL router view
        (``forbidden=True``): a route hidden by a plugin filter is still a
        claimed root, so it can never fall through to the hosted site.
        """
        nodes = self.route.nodes(lazy=True, forbidden=True)
        return (set(nodes.get("entries", {})) | set(nodes.get("routers", {}))) - {"index"}

    def resolves_natively(self, path: str) -> bool:
        """Whether ``path`` resolves to an EXISTING node in the app's router.

        Resolution runs with NO auth filters, so the answer is purely "does this
        node exist?". Only a genuine ``not_found`` is a miss; an existing node a
        filter would deny still answers True and stays native.
        """
        return bool(self.route.node(path).error != "not_found")

    async def on_startup(self) -> None:
        """Read the orchestration, build the pool, bring it up, then open the door.

        The vertex is born HERE and not in the constructor: its words live under
        ``applications.<code>.orchestration``, and an application reaches its own
        subtree only once a server has it. The three words of the node are read
        first and land on this front, so what follows composes on them.

        The order is the point. Everything that can refuse runs BEFORE anything
        is mounted — the node, the composition, the vertex, its start — and
        ``OrchestrationControl`` goes on the router last, only when the pool is
        actually up, with the inspector's own mount after it.
        A boot that fails leaves the router exactly as it was, and
        leaves this front holding NO vertex: a start that raises, and a mount that
        raises after it, both take the half-built pool back down first, so the
        next startup builds a new one instead of guarding a broken one.

        A front declared with no ``orchestration`` node is an INCOMPLETE
        configuration and does not boot: a spa front without a pool answers every
        request with a raise, so the recipe is asked for the node instead of the
        server pretending to serve. A front WITH the node and no commander under
        it is refused for the same reason: a profile and a control surface with
        nothing to act on address nothing. Wanting no pool at all is declaring no
        spa front, not declaring one and leaving it hollow.

        A startup on a front whose pool is already up does nothing: the same
        object is never given a second vertex while the first is running.

        Acts on this application — the vertex is born here — and, through
        ``SpaCommander.start``, on the machine: the base group's reception is
        launched and awaited, then the beat starts. A reception that would not
        start leaves its group broken and the front serves polite refusals until
        the group's own round brings one up: a process that fails to start can be
        a passing thing, and the machine knows how to heal it.

        A named profile or an environment level is composed onto the recipe
        BEFORE the vertex is built, so the pool is born already effective. What
        the composition refuses — a profile that is not there, one the schema
        rejects, a machine with no single group to give the setpoints to — is
        said once on this module's logger and raised as ``FatalBootError``, the
        one exception the lifespan does not swallow: the server does not start,
        because a pool nobody could configure as asked is not the pool that was
        asked for.
        """
        if self._commander is not None:
            return
        handler = self.server.config
        orchestration = handler.orchestration_kwargs(self.code)
        if orchestration is None:
            self.refuse_boot(
                "it declares no 'orchestration' node — a spa front is its pool, so "
                "the recipe must write applications.<code>.orchestration with a "
                "commander under it"
            )
        self.profiles_path = orchestration.get("profiles_path")
        self.profile_name = orchestration.get("profile_name")
        self.control_enabled = bool(orchestration.get("control_enabled", False))
        commander_kwargs = handler.commander_kwargs(self.code)
        if commander_kwargs is None:
            self.refuse_boot("its orchestration declares no commander")
        if (
            self.control_enabled
            and not self._control_mounted
            and ORCHESTRATION_ROOT in self.internal_roots
        ):
            self.refuse_boot(
                f"the {ORCHESTRATION_ROOT!r} root is already claimed by its own router, "
                f"so the runtime configuration has nowhere to mount"
            )
        try:
            groups, recipe_settings = self.boot_group_settings(handler.group_kwargs(self.code))
        except Exception as refused:
            self.refuse_boot(f"the pool cannot be configured: {refused}", refused)
        commander = self.commander_class(
            **commander_kwargs,
            groups=groups,
            profiles_path=self.profiles_path,
            recipe_settings=recipe_settings,
            env_settings=self.env_settings,
            active_profile=self.profile_name,
        )
        commander.server = self.server
        self._commander = commander
        try:
            await commander.start()
        except asyncio.CancelledError:
            # Whoever cancelled this boot gets its cancellation back: the pool is
            # taken down all the same, and nothing is turned into a boot failure.
            await self.take_pool_down(commander)
            raise
        except Exception as broken:
            await self.take_pool_down(commander)
            self.refuse_boot(f"the pool could not be brought up: {broken}", broken)
        try:
            self.mount_control()
            self.mount_channel_control()
        except Exception as broken:
            await self.take_pool_down(commander)
            self.refuse_boot(
                f"the runtime configuration could not be mounted and the pool was "
                f"taken back down: {broken}",
                broken,
            )
        self.mount_inspector()

    async def take_pool_down(self, commander: SpaCommander) -> None:
        """Undo a boot that could not finish: stop the pool, and let go of it.

        Args:
            commander: the vertex the boot built and could not bring all the way
                up. It is passed rather than read back, because letting go is
                exactly what this does.

        A pool that refuses to stop is said on this module's logger and nothing
        more: it must never replace the reason the boot failed, which is what the
        caller raises next. Either way the front ends holding no vertex, so the
        next startup builds a new one instead of finding this one.
        """
        try:
            await commander.stop()
        except Exception:
            self._logger.exception(
                "Front %s: the pool refused to go back down after a failed boot", self.code
            )
        finally:
            self._commander = None

    def refuse_boot(self, reason: str, cause: Exception | None = None) -> NoReturn:
        """Say the boot failed once, on this module's logger, and make it fatal.

        Args:
            reason: what is wrong, in the front's own words.
            cause: the exception underneath, when there is one — the tests read
                it back off ``__cause__``.

        Raises:
            FatalBootError: always. It is the one exception the lifespan does not
                swallow on startup, so uvicorn receives ``lifespan.startup.failed``.
        """
        failure = f"Front {self.code}: {reason}, the server does not start"
        self._logger.error(failure)
        raise FatalBootError(failure) from cause

    def mount_control(self) -> None:
        """Put the runtime configuration on the router, if the recipe asked for it.

        The LAST mutation of the boot, when the pool is up: the root is claimed
        only by a front that can actually answer on it. That the root is FREE was
        established before anything was built, so what is left here cannot refuse
        — and a startup after a shutdown finds the branch this method already put
        there and does nothing.
        """
        if not self.control_enabled or self._control_mounted:
            return
        self.route.add_branches(
            {"name": ORCHESTRATION_ROOT, "instance": OrchestrationControl(self)}
        )
        self._control_mounted = True

    def mount_inspector(self) -> None:
        """Put the pool's own watching page on ``_server``, if the environment asked.

        The last thing the boot does, and outside the mount above on purpose: a
        second ``inspector`` is a deliberate refusal, and wrapping it in that
        ``except`` would tell it as a failure of the runtime configuration.

        The section reads THIS pool, so it is the front that attaches it and not
        the ``_server`` app that hosts it: a core carrying no orchestration must
        not import one to look at it. A server has ONE orchestrated application
        (owner, 2026-09-07), so a mount already there means a second front is
        booting on the same server — the state that decision declares
        impossible, and the boot says so instead of watching one pool of two.

        Raises:
            FatalBootError: ``inspector`` is already attached.
        """
        if not os.environ.get(INSPECTOR_ENV_VAR):
            return
        server_app = self.server.applications.get("_server")
        if server_app is None:
            self._logger.warning(
                "Front %s: %s is set but no '_server' application is mounted, "
                "so the inspector has nowhere to go",
                self.code,
                INSPECTOR_ENV_VAR,
            )
            return
        if "inspector" in server_app.sections:
            self.refuse_boot(
                "the 'inspector' section is already attached, so a second front is "
                "booting on this server and a server has one orchestrated application"
            )
        server_app.attach_section(InspectorSection(self), name="inspector")

    def mount_channel_control(self) -> None:
        """Claim ``_wsx`` on this front's router: the channel commands of a page.

        No gate: a front that hosts pages hosts their channel too, and the
        command is refused per page anyway — a page that is not this
        connection's gets a 403 whether the root is there or not. Called once,
        when the pool is up, beside the orchestration root.

        The other half goes the other way: ``websocket`` is attached under the
        commander's own operations, which is how a worker reaches a browser.
        Not lazily — a worker may call it as soon as it is up.
        """
        if self._channel_mounted:
            return
        self.route.add_branches({"name": WSX_ROOT, "instance": WsxControl(self)})
        self.commander.commander_dispatcher.add_branches(
            [{"name": "websocket", "instance": WebsocketOperations(self)}]
        )
        self._channel_mounted = True

    def boot_group_settings(
        self, groups: dict[str, dict[str, Any]]
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        """Compose the one group's effective setpoints, and keep the recipe's apart.

        Args:
            groups: the groups the recipe wrote, as ``{name: kwargs}`` — only the
                words it actually wrote, so the library defaults are not there.

        Returns:
            The same map with that group's setpoints replaced by the effective
            ones (defaults ⊕ recipe ⊕ profile ⊕ env, materialized by
            ``GroupPolicy``), and the recipe's own setpoint level on its own —
            the vertex recomposes every later apply from it. A machine with no
            single group and nothing to overlay is handed back untouched: it is
            the composition that has always been legitimate.

        Raises:
            SingleGroupRequired: there is an overlay to compose and this machine
                has zero or several groups, so nothing says whose setpoints
                these are.
            OrchestrationProfileNotFoundError: the named profile is not there, or no folder
                was declared to look for it in.
            OrchestrationProfileNameError, OrchestrationProfileContentError:
                the file could not be read as a profile.
            GroupPolicyError: the composed settings are invalid, carrying every
                violation found.
        """
        if len(groups) != 1:
            if self.profile_name is None and not self.env_settings:
                return groups, {}
            raise SingleGroupRequired(
                f"Front {self.code}: a profile governs exactly one group, this recipe "
                f"declares {len(groups)} ({sorted(groups)})"
            )
        name, group_kwargs = next(iter(groups.items()))
        recipe_settings = {
            key: value for key, value in group_kwargs.items() if key in GroupPolicy.SETPOINTS
        }
        profile: dict[str, Any] = {}
        if self.profile_name is not None:
            if self.profiles_path is None:
                raise OrchestrationProfileNotFoundError(
                    f"profile {self.profile_name!r} was named and no profiles folder "
                    "was declared"
                )
            profile = OrchestrationProfileStore(self.profiles_path).read(self.profile_name)
        policy = GroupPolicy.from_settings({**recipe_settings, **profile, **self.env_settings})
        structural = {
            key: value for key, value in group_kwargs.items() if key not in GroupPolicy.SETPOINTS
        }
        return {name: {**structural, **policy.to_settings()}}, recipe_settings

    @property
    def orchestration_commander(self) -> SpaCommander:
        """The pool, when it is in a state to be reconfigured.

        Raises:
            HTTPException: 503 — the pool is not built yet, or the server has
                left RUNNING: a machine on its way out takes no new
                configuration, and the caller is told to come back.
        """
        if self._commander is None or self.server.state != RUNNING:
            raise HTTPException(
                503,
                "the pool is not in a state to be reconfigured",
                headers=[(b"retry-after", str(REFUSED_RETRY_AFTER_SECONDS).encode())],
            )
        return self._commander

    def body_profile(self, body_data: Any) -> dict[str, Any]:
        """The request body as a JSON object, or the one 400 that leaves no audit.

        Args:
            body_data: what the request layer hydrated — a dict when the body
                was a JSON object, the raw text when it could not be parsed.

        Returns:
            The object, an absent body reading as an empty one.

        Raises:
            HTTPException: 400 — the body is not a JSON object. It is refused
                HERE, before the vertex is asked anything, which is what keeps a
                malformed body out of the orchestration log.
        """
        if body_data is None:
            return {}
        if not isinstance(body_data, dict):
            raise HTTPBadRequest("the body of a configuration must be a JSON object")
        return body_data

    async def apply_settings(
        self,
        *,
        profile: dict[str, Any] | None = None,
        profile_name: str | None = None,
        source: str,
    ) -> dict[str, Any]:
        """Ask the vertex for a new effective configuration, and answer its refusals.

        Args:
            profile: the profile level given inline.
            profile_name: the stored profile to read as that level instead.
            source: who asked — the word that reaches the audit and the answer.

        Returns:
            The payload the vertex composed: ``outcome``, ``source``,
            ``active_profile``, ``generation``, ``changed_settings`` and
            ``effective_settings``.

        Raises:
            HTTPException: 400 the settings or the stored file are invalid — the
                violations are the message —, 404 the named profile is not
                there, 409 this machine has no single group, 503 the pool is not
                in a state to be reconfigured.
        """
        commander = self.orchestration_commander
        try:
            return await commander.apply_group_settings(
                profile=profile, profile_name=profile_name, source=source
            )
        except SingleGroupRequired as several:
            raise HTTPException(409, str(several)) from several
        except OrchestrationProfileNotFoundError as missing:
            raise HTTPNotFound(str(missing)) from missing
        except (
            GroupPolicyError,
            OrchestrationProfileNameError,
            OrchestrationProfileContentError,
        ) as rejected:
            raise HTTPBadRequest(str(rejected)) from rejected

    @property
    def settings_status(self) -> dict[str, Any]:
        """What configuration is in force: the profile, the generation, the record.

        Returns:
            ``active_profile``, ``generation``, ``last_apply`` and the
            ``effective_settings`` the one group is running on. Nothing is
            locked to read it: the swap that writes those four never yields the
            loop, so what is read here is one apply's picture and never a mix.

        Raises:
            HTTPException: 409 this machine has no single group, 503 the pool is
                not in a state to answer.
        """
        commander = self.orchestration_commander
        try:
            group = commander.configured_group
        except SingleGroupRequired as several:
            raise HTTPException(409, str(several)) from several
        return {
            "active_profile": commander.active_profile,
            "generation": commander.configuration_generation,
            "last_apply": commander.last_apply,
            "effective_settings": group.policy.to_settings(),
        }

    async def on_shutdown(self) -> None:
        """Take the pool down with the server: with a photo, or dry.

        A server that is QUITTING gets the soft quit — every user parked in the
        reboot directory and the vertex's own item beside them. Any other way
        out is dry, which is what every start that is not the deliberate liturgy
        expects to find (F2).

        The front lets go of the vertex whatever it answered, so the pool it held
        is never handed to a second startup: that one reads the configuration
        again and builds a new one.
        """
        commander = self._commander
        if commander is None:
            return
        try:
            if self.server.state == QUITTING:
                await commander.quit()
            else:
                await commander.stop()
        finally:
            # The front lets go whatever the vertex answered: a pool that failed
            # to go down cleanly is still not this front's any more, and the next
            # startup builds a new one instead of finding a dead one.
            self._commander = None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Demultiplex: the app's own router, else the hosted site."""
        path = str(scope.get("path", "/"))
        first_segment = path.strip("/").split("/")[0]
        if first_segment in self.internal_roots and self.resolves_natively(path):
            await super().__call__(scope, receive, send)
        else:
            await self.forward_request(scope, receive, send)

    async def forward_request(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Admit before buffering: at most sixteen bounded HTTP bodies per front."""
        async with self._forward_slots:
            await self._forward_request(scope, receive, send)

    async def _forward_request(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Serve a site path through the pool: pack, hand over, translate, answer.

        The cookie is read ONCE here and travels down as it came — None included,
        which is a browser the site has never named. Nothing is minted: the
        identity is the site's to give, and it gives it while serving. What comes
        back names the connection the request settled on, and that is what the
        cookie is written with.
        """
        carried = self.request_cid(scope)
        local_response = True
        try:
            http = await self.pack_http(scope, receive, carried)
            reply = await self.commander.serve_request(
                carried, http, hold_timeout=REQUEST_HOLD_MAX_SECONDS
            )
        except (FrameTooLarge, HttpBodyTooLarge):
            response = Response(content="Request too large", status_code=413)
        except AssignmentRefused as refusal:
            self._logger.warning("Front %s: %s", self.code, refusal)
            response = self.busy_response(refusal)
        except ConnectionError as gone:
            self._logger.warning("Front %s: %s", self.code, gone)
            response = self.wire_lost_response(gone)
        except SiteFailedRequest as failure:
            self._logger.error("Front %s: %s", self.code, failure)
            response = self.gateway_response(failure)
        else:
            response = self.build_response(reply, carried)
            local_response = False
        if local_response and scope.get("method") == "WSK":
            result = await BufferedAsgiEndpoint(response).serve(scope, b"")
            response = Response(content=result["body"], status_code=result["status"],
                                headers=result["headers"])
        await response(scope, receive, send)

    def busy_response(self, refusal: AssignmentRefused) -> Response:
        """The polite 503: come back when the machine will have decided again.

        No cookie goes out with it: the site never served this request, so there
        is no connection to name — and a refusal must not overwrite the one the
        browser already holds.
        """
        headers = [("content-type", "text/plain; charset=utf-8")]
        if refusal.retry_after is not None:
            headers.append(("retry-after", str(int(refusal.retry_after))))
        return Response(content=ERR_503_TEXT, status_code=503, headers=headers)

    def wire_lost_response(self, gone: ConnectionError) -> Response:
        """What a dead wire means depends on why the process on the other end left.

        A server that is quitting killed that wire on purpose, so the answer is
        the polite 503 a refusal gets — the browser is told to come back, not
        that something upstream broke. A wire that died while the server was
        running IS a breakage, and reads 502.
        """
        if self.server.state == RUNNING:
            return self.gateway_response()
        return Response(
            content=ERR_503_TEXT,
            status_code=503,
            headers=[
                ("content-type", "text/plain; charset=utf-8"),
                ("retry-after", str(REFUSED_RETRY_AFTER_SECONDS)),
            ],
        )

    def gateway_response(self, failure: SiteFailedRequest | None = None) -> Response:
        """What the caller reads when the worker answered a failure.

        Args:
            failure: what the worker said, when the caller is to be told; the
                502 with the fixed text is the answer without it.

        Returns:
            The refusal the worker named, with ITS words, when it named a
            status — a page that never opened its channel is a 409 and the
            browser must read why (#70). Everything else is the 502 of an
            upstream that broke, whose reason stays in the log alone: what
            failed inside the site is nobody's business out here.
        """
        if failure is not None and failure.status is not None:
            return Response(
                content=failure.cause,
                status_code=failure.status,
                headers=[("content-type", "text/plain; charset=utf-8")],
            )
        return Response(
            content=ERR_502_TEXT,
            status_code=502,
            headers=[("content-type", "text/plain; charset=utf-8")],
        )

    def build_response(self, reply: Frame, carried: str | None) -> Response:
        """The outer response, rebuilt from the site's own answer.

        The cookie is written when the connection the site settled on is not the
        one the browser sent: a first visit, and the replacement the site makes
        whenever the connection its own cookie names does not validate. A request
        that reused the connection it arrived with names none, and nothing is
        written.
        """
        result = HttpRecord().decode_response(reply.payload)
        response = Response(
            content=result["body"],
            status_code=int(result.get("status", 200)),
            headers=[(str(name), str(value)) for name, value in result.get("headers") or []],
        )
        settled = reply.info.get("connection_id")
        if settled is not None and settled != carried:
            response.set_cookie(
                SPA_CONNECTION_ID_COOKIE,
                settled,
                max_age=CONNECTION_COOKIE_MAX_AGE,
                path="/",
                httponly=True,
                samesite="lax",
            )
        return response

    def request_cid(self, scope: Scope) -> str | None:
        """The connection this request carries, or ``None`` when it carries none."""
        return cookie_value(scope, SPA_CONNECTION_ID_COOKIE)

    async def read_body(self, receive: Receive) -> bytes:
        """Drain the whole request body off the ASGI receive channel."""
        body = bytearray()
        while True:
            message = await receive()
            chunk = message.get("body", b"") or b""
            if len(body) + len(chunk) > http_max_body_size():
                raise HttpBodyTooLarge("request body exceeds buffered transport limit")
            body.extend(chunk)
            if not message.get("more_body", False):
                break
        return bytes(body)

    async def pack_http(
        self, scope: Scope, receive: Receive, cid: str | None
    ) -> Frame:
        """Pack routing facts and a bounded opaque HTTP record for the child.

        The headers are forwarded as they came: our cookie is ours to route on
        and the hosted site has no use for it — it reads its own, and the value
        in both is its own connection id anyway.

        ``page_id`` and ``reply_path`` join the routing info only when the scope carries
        them — a message born on a websocket does, a real HTTP request does not
        — and the endpoint writes them into the hosted scope under their genro names.
        """
        info = {"format": "http", "cid": cid}
        for key in ("genro.page_id", "genro.reply_path"):
            if key in scope:
                info[key.split(".")[1]] = scope[key]
        return Frame(method="CALL", path="/site" + str(scope.get("path", "/")),
                     info=info, payload=HttpRecord().encode_request(scope, await self.read_body(receive)))
