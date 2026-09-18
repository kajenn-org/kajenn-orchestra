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

"""Contract tests for the SpaApplication's profiles: the boot read and the router.

Design sections 1, 2, 7, 10; test matrix T1 T2 T3 T4 T5 T8 T9 T18 T19 T23. The
stage is the real thing — a real recipe, a real lifespan, a real vertex with one
real group — with only the PROCESSES left out: what these tests ask is what
configuration the pool is running on, and no child is needed to answer that.
"""

from __future__ import annotations

import base64
import json
import logging
from pathlib import Path
from typing import Any

import pytest

from tests.orchestration.frame_helpers import http_reply

from kajenn import AsgiServer
from kajenn_orchestra.spa_app import ORCHESTRATION_ROOT, SpaApplication
from kajenn.config.builder import AsgiConfigBuilder
from kajenn.lifespan import FatalBootError
from kajenn_orchestra.orchestration_profile_store import (
    OrchestrationProfileContentError,
    OrchestrationProfileNotFoundError,
    OrchestrationProfileStore,
)
from kajenn.server import STOPPING
from kajenn_orchestra.orchestration import SpaCommander
from kajenn_orchestra.orchestration.group_policy import GroupPolicyError
from kajenn_orchestra.orchestration.spa_commander import SingleGroupRequired

SITE_BODY = b"the hosted site answered"


class QuietCommander(SpaCommander):
    """The real vertex, minus the processes: nothing is launched, nothing forked."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def serve_request(
        self, cid: str | None, http: dict[str, Any], *, hold_timeout: float
    ) -> dict[str, Any]:
        """What the hosted site answers, so a fall-through is visible from outside."""
        return http_reply(http, {
            "result": {
                "status": 200,
                "headers": [],
                "body": base64.b64encode(SITE_BODY).decode(),
            }
        })



class ProfiledFront(SpaApplication):
    """The front under test, with a pool that costs nothing to build."""

    commander_class = QuietCommander


def pool_recipe(
    root: Path,
    groups: tuple[str, ...] = ("standard",),
    orchestration: dict[str, Any] | None = None,
    env_settings: dict[str, Any] | None = None,
) -> type[AsgiConfigBuilder]:
    """A recipe with the whole orchestration subtree and as many groups as asked for."""

    class PoolConfig(AsgiConfigBuilder):
        def main(self, configuration_root: Any) -> None:
            cfg = configuration_root.configuration()
            applications = cfg.applications()
            front_kwargs: dict[str, Any] = {}
            if env_settings is not None:
                front_kwargs["env_settings"] = env_settings
            front = applications.application(
                code="site0", mount="", app_class=ProfiledFront, **front_kwargs
            )
            commander = front.orchestration(**(orchestration or {})).commander(
                frozen_users_path=str(root / "frozen_users"),
                instance_dir=str(root / "i"),
            )
            if not groups:
                return
            collection = commander.groups(default=groups[0])
            for name in groups:
                collection.group(
                    name=name,
                    entry_module="never.launched",
                    worker_memory_admission_percent=70.0,
                    worker_max_number=3,
                )

    return PoolConfig


def pool_server(
    root: Path,
    groups: tuple[str, ...] = ("standard",),
    *,
    env_settings: dict[str, Any] | None = None,
    **orchestration: Any,
) -> AsgiServer:
    """A server built the way production builds one: everything through the recipe.

    The three words go on the orchestration node, ``env_settings`` on the
    application element — it is a runtime dict and no grammar declares it — and
    the front is instantiated by the server out of that recipe alone.
    """
    if "profiles_path" in orchestration:
        orchestration["profiles_path"] = str(orchestration["profiles_path"])
    return AsgiServer(config=pool_recipe(root, groups, orchestration, env_settings))


async def boot(server: AsgiServer) -> None:
    """Start the front directly, the refusal coming out unwrapped.

    ``on_startup`` declares any boot failure fatal by raising ``FatalBootError``
    around the refusal; what these tests assert is the refusal itself, so the
    cause comes back out as it was raised.
    """
    try:
        await server.applications["site0"].on_startup()
    except FatalBootError as fatal:
        assert fatal.__cause__ is not None
        raise fatal.__cause__ from None


async def lifespan_startup(server: AsgiServer) -> list[dict[str, Any]]:
    """Drive the ASGI lifespan startup and return what the server sent back."""
    sent: list[dict[str, Any]] = []
    inbox = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]

    async def receive() -> dict[str, Any]:
        return inbox.pop(0)

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await server.lifespan(  # type: ignore[operator]
        {"type": "lifespan"}, receive, send
    )
    return sent


async def ask(
    server: AsgiServer,
    path: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | bytes | None = None,
) -> tuple[int, Any]:
    """One request through the whole server; the status and the decoded answer."""
    headers = [(b"accept", b"application/json")]
    payload = b""
    if body is not None:
        payload = body if isinstance(body, bytes) else json.dumps(body).encode()
        headers.append((b"content-type", b"application/json"))
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": b"",
        "headers": headers,
    }
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": payload, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await server(scope, receive, send)
    status = next(m["status"] for m in sent if m["type"] == "http.response.start")
    raw = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    try:
        return status, json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return status, raw


def written(folder: Path, name: str, profile: dict[str, Any]) -> None:
    """Store one profile the way the archive stores it."""
    OrchestrationProfileStore(folder).write(name, profile)


# -- the boot read: four levels ----------------------------------------------


async def test_boot_precedence_four_levels(tmp_path):
    # wf:contract: T1 — at boot the effective configuration composes
    # wf:contract: defaults ⊕ recipe_settings ⊕ profile ⊕ env_settings: the
    # wf:contract: profile overrides the recipe and env_settings overrides the
    # wf:contract: profile, key by key.
    folder = tmp_path / "profiles"
    written(folder, "fast", {"worker_memory_admission_percent": 60.0, "worker_min_life_seconds": 5.0})
    server = pool_server(
        tmp_path,
        profiles_path=folder,
        profile_name="fast",
        env_settings={"worker_min_life_seconds": 9.0},
    )

    await boot(server)

    policy = server.applications["site0"].commander.configured_group.policy
    # The profile wins over the recipe...
    assert policy.worker_memory_admission_percent == 60.0
    # ...the environment wins over the profile...
    assert policy.worker_min_life_seconds == 9.0
    # ...the recipe still owns what nobody above it named...
    assert policy.worker_max_number == 3
    # ...and the dataclass default owns what nobody named at all.
    assert policy.cpu_close_percent is None


async def test_boot_without_named_profile_unchanged(tmp_path):
    # wf:contract: T2 — no profile_name means no profile level: behaviour
    # wf:contract: identical to today, generation 1, last_apply source "boot".
    server = pool_server(tmp_path)

    await boot(server)

    commander = server.applications["site0"].commander
    assert commander.active_profile is None
    assert commander.configuration_generation == 1
    assert commander.last_apply["source"] == "boot"
    assert commander.last_apply["digest"] is None
    # The recipe alone decided, and its own level is kept for the next apply.
    policy = commander.configured_group.policy
    assert policy.worker_memory_admission_percent == 70.0
    assert policy.worker_min_life_seconds == 60.0
    assert commander.recipe_settings == {
        "worker_memory_admission_percent": 70.0,
        "worker_max_number": 3,
    }


async def test_boot_failure_missing_named_profile(tmp_path):
    # wf:contract: T3 — a named profile that does not exist makes on_startup
    # wf:contract: raise: the lifespan fails and the server does not start.
    server = pool_server(tmp_path, profiles_path=tmp_path / "profiles", profile_name="nowhere")

    with pytest.raises(OrchestrationProfileNotFoundError):
        await boot(server)

    # Nothing was built: there is no pool to serve with.
    with pytest.raises(RuntimeError):
        server.applications["site0"].commander

    # And the lifespan itself fails, so the server does not start.
    fresh = pool_server(tmp_path, profiles_path=tmp_path / "profiles", profile_name="nowhere")
    sent = await lifespan_startup(fresh)
    assert not any(message["type"] == "lifespan.startup.complete" for message in sent)


async def test_boot_failure_invalid_profile(tmp_path, caplog):
    # wf:contract: T4 — corrupt JSON, non-object, oversize, symlink or schema
    # wf:contract: violation on the named profile: on_startup raises, the
    # wf:contract: violations are in the message on the spa app module logger.
    folder = tmp_path / "profiles"
    written(folder, "wrong", {"worker_memory_admission_percent": 200.0})
    server = pool_server(tmp_path, profiles_path=folder, profile_name="wrong")

    with caplog.at_level(logging.ERROR, logger="kajenn_orchestra.spa_app"):
        with pytest.raises(GroupPolicyError) as refused:
            await boot(server)

    assert len(refused.value.violations) == 1
    said = caplog.text
    for violation in refused.value.violations:
        assert violation in said

    # The same boot fails on a file that is not a JSON object at all.
    (folder / "text.json").write_text('"not an object"')
    broken = pool_server(tmp_path, profiles_path=folder, profile_name="text")
    with pytest.raises(OrchestrationProfileContentError):
        await boot(broken)


async def test_zero_or_multi_group_rejection(tmp_path):
    # wf:contract: T9 — a named profile with 0 or 2 groups fails the boot; a hot
    # wf:contract: apply on such a composition answers 409; without a profile and
    # wf:contract: without the gate a multi-group composition boots as today.
    folder = tmp_path / "profiles"
    written(folder, "fast", {"worker_memory_admission_percent": 60.0})

    for groups in ((), ("standard", "heavy")):
        server = pool_server(tmp_path, groups, profiles_path=folder, profile_name="fast")
        with pytest.raises(SingleGroupRequired):
            await boot(server)

    # Two groups, the gate on and nothing named: the boot is today's, and the
    # apply is what refuses — it is the one that needs a single group.
    gated = pool_server(tmp_path, ("standard", "heavy"), control_enabled=True)
    await boot(gated)
    assert set(gated.applications["site0"].commander.group_map) == {"standard", "heavy"}

    status, answer = await ask(
        gated, f"/{ORCHESTRATION_ROOT}/apply", method="POST", body={"worker_memory_admission_percent": 60.0}
    )
    assert status == 409
    assert "exactly one group" in answer["error"]

    # And with the gate off it boots as today, both groups on their own policy.
    plain = pool_server(tmp_path, ("standard", "heavy"))
    await boot(plain)
    assert set(plain.applications["site0"].commander.group_map) == {"standard", "heavy"}


# -- the router: the gate, the three routes, the answers ----------------------


async def test_router_gate_off_and_on(tmp_path):
    # wf:contract: T18 — gate off: _orchestration/* does not resolve natively
    # wf:contract: (the path goes to the hosted site); gate on: the three routes
    # wf:contract: resolve under _orchestration.
    closed = pool_server(tmp_path)
    await boot(closed)
    front = closed.applications["site0"]
    assert ORCHESTRATION_ROOT not in front.internal_roots

    status, answer = await ask(closed, f"/{ORCHESTRATION_ROOT}/status")
    assert status == 200
    assert answer == SITE_BODY

    opened = pool_server(tmp_path, control_enabled=True)
    await boot(opened)
    gated = opened.applications["site0"]
    assert ORCHESTRATION_ROOT in gated.internal_roots
    for route in ("apply", "reload", "status"):
        assert gated.resolves_natively(f"/{ORCHESTRATION_ROOT}/{route}") is True


async def test_http_contract_success_and_errors(tmp_path):
    # wf:contract: T19 — 200 carries the six fields (outcome, source,
    # wf:contract: active_profile, generation, changed_settings,
    # wf:contract: effective_settings); 400 invalid body/profile with violations;
    # wf:contract: 404 reload of a missing profile; 400 reload with no name and
    # wf:contract: no active profile ("nothing to reload"); 409 not exactly one
    # wf:contract: group; 503 commander not started or server not RUNNING.
    folder = tmp_path / "profiles"
    server = pool_server(tmp_path, profiles_path=folder, control_enabled=True)
    await boot(server)
    server_commander = server.applications["site0"].commander
    apply_path = f"/{ORCHESTRATION_ROOT}/apply"
    reload_path = f"/{ORCHESTRATION_ROOT}/reload"

    status, answer = await ask(
        server, apply_path, method="POST", body={"worker_memory_admission_percent": 65.0}
    )
    assert status == 200
    assert set(answer) == {
        "outcome",
        "source",
        "active_profile",
        "generation",
        "changed_settings",
        "effective_settings",
    }
    assert answer["outcome"] == "applied"
    assert answer["source"] == "inline"
    assert answer["active_profile"] is None
    assert answer["generation"] == 2
    assert answer["changed_settings"] == {"worker_memory_admission_percent": 65.0}
    assert answer["effective_settings"]["worker_memory_admission_percent"] == 65.0

    # 400 — the body is a JSON object the schema refuses, and every violation is said.
    status, answer = await ask(
        server,
        apply_path,
        method="POST",
        body={"worker_memory_admission_percent": 200.0, "unknown_setpoint": 1},
    )
    assert status == 400
    assert "worker_memory_admission_percent" in answer["error"]
    assert "unknown_setpoint" in answer["error"]

    # 404 — a reload of a name the folder does not hold.
    status, answer = await ask(server, reload_path, method="POST", body={"name": "nowhere"})
    assert status == 404

    # 400 — nothing to reload: no name, and the inline apply left no active profile.
    status, answer = await ask(server, reload_path, method="POST", body={})
    assert status == 400
    assert "nothing to reload" in answer["error"]

    # 409 — the machine has no single group the setpoints could govern.
    several = pool_server(tmp_path, ("standard", "heavy"), control_enabled=True)
    await boot(several)
    status, answer = await ask(several, apply_path, method="POST", body={})
    assert status == 409

    # Before the boot the root is not claimed at all: the gate is mounted last,
    # once the pool is up, so an unstarted front leaves the path to the site.
    unbooted = pool_server(tmp_path, control_enabled=True)
    assert ORCHESTRATION_ROOT not in unbooted.applications["site0"].internal_roots

    # 503 — the pool is gone under a mounted gate, and the server that left
    # RUNNING takes nothing.
    server.applications["site0"]._commander = None
    status, answer = await ask(server, apply_path, method="POST", body={})
    assert status == 503
    server.applications["site0"]._commander = server_commander
    server.state = STOPPING
    status, answer = await ask(server, apply_path, method="POST", body={})
    assert status == 503


async def test_profile_level_replacement(tmp_path):
    # wf:contract: T5 — apply of P1 then P2 missing one of P1's keys: that key
    # wf:contract: returns to the env_settings, recipe_settings or default value,
    # wf:contract: in that order of precedence.
    folder = tmp_path / "profiles"
    written(folder, "p1", {"worker_memory_admission_percent": 60.0, "worker_min_life_seconds": 5.0})
    written(folder, "p2", {"worker_min_life_seconds": 30.0})
    server = pool_server(
        tmp_path,
        profiles_path=folder,
        control_enabled=True,
        env_settings={"worker_max_users": 16},
    )
    await boot(server)
    reload_path = f"/{ORCHESTRATION_ROOT}/reload"

    status, first = await ask(server, reload_path, method="POST", body={"name": "p1"})
    assert status == 200
    assert first["effective_settings"]["worker_memory_admission_percent"] == 60.0

    status, second = await ask(server, reload_path, method="POST", body={"name": "p2"})
    assert status == 200
    # P1's key is not carried over: the recipe level owns it again...
    assert second["effective_settings"]["worker_memory_admission_percent"] == 70.0
    # ...P2's own key is in force...
    assert second["effective_settings"]["worker_min_life_seconds"] == 30.0
    # ...the environment still wins on its key...
    assert second["effective_settings"]["worker_max_users"] == 16
    # ...and a key nobody ever named is the default.
    assert second["effective_settings"]["cpu_close_percent"] is None
    assert second["active_profile"] == "p2"
    assert second["source"] == "profile"


async def test_invalid_apply_all_or_nothing(tmp_path):
    # wf:contract: T8 — one violation means the state is untouched, generation
    # wf:contract: does not move, and the response is 400 with the complete
    # wf:contract: violations list.
    server = pool_server(tmp_path, control_enabled=True)
    await boot(server)
    commander = server.applications["site0"].commander
    before = commander.configured_group.policy

    status, answer = await ask(
        server,
        f"/{ORCHESTRATION_ROOT}/apply",
        method="POST",
        body={"worker_min_life_seconds": 5.0, "worker_memory_admission_percent": 99.0},
    )

    assert status == 400
    # The one violation is the cross rule, and the valid key did not land either.
    assert "worker_memory_admission_percent" in answer["error"]
    assert commander.configured_group.policy is before
    assert commander.configuration_generation == 1
    assert commander.last_apply["outcome"].startswith("rejected: ")


async def test_status_introspection(tmp_path):
    # wf:contract: T23 — GET _orchestration/status renders active_profile,
    # wf:contract: generation, last_apply and effective_settings coherent with
    # wf:contract: the last apply, read-only, no lock taken.
    folder = tmp_path / "profiles"
    written(folder, "fast", {"worker_memory_admission_percent": 62.0})
    server = pool_server(tmp_path, profiles_path=folder, control_enabled=True)
    await boot(server)
    commander = server.applications["site0"].commander

    status, applied = await ask(
        server, f"/{ORCHESTRATION_ROOT}/reload", method="POST", body={"name": "fast"}
    )
    assert status == 200

    status, seen = await ask(server, f"/{ORCHESTRATION_ROOT}/status")

    assert status == 200
    assert seen["active_profile"] == "fast"
    assert seen["generation"] == applied["generation"]
    assert seen["last_apply"]["outcome"] == "applied"
    assert seen["last_apply"]["source"] == "profile"
    assert seen["last_apply"]["digest"] is not None
    assert seen["effective_settings"] == applied["effective_settings"]
    # Read-only: nothing moved, and the apply lock was never taken.
    assert commander.configuration_generation == applied["generation"]
    assert commander._configuration_lock.locked() is False


async def test_the_retirement_quiet_travels_the_four_levels(tmp_path):
    # wf:contract: cpu_retirement_quiet_seconds is a setpoint like any other:
    # wf:contract: the recipe writes it under the group, a stored profile
    # wf:contract: overrides it, env_settings overrides the profile, and what
    # wf:contract: nobody names falls back to the dataclass default.
    folder = tmp_path / "profiles"
    written(folder, "quiet", {"cpu_retirement_quiet_seconds": 30.0})

    # The recipe alone.
    plain = pool_server(tmp_path)
    await boot(plain)
    assert plain.applications["site0"].commander.configured_group.policy.\
        cpu_retirement_quiet_seconds == 60.0

    # The profile over the recipe.
    profiled = pool_server(tmp_path, profiles_path=folder, profile_name="quiet")
    await boot(profiled)
    assert profiled.applications["site0"].commander.configured_group.policy.\
        cpu_retirement_quiet_seconds == 30.0

    # And the environment over the profile.
    overridden = pool_server(
        tmp_path,
        profiles_path=folder,
        profile_name="quiet",
        env_settings={"cpu_retirement_quiet_seconds": 7.5},
    )
    await boot(overridden)
    commander = overridden.applications["site0"].commander
    assert commander.configured_group.policy.cpu_retirement_quiet_seconds == 7.5

    # It is readable from outside like every other setpoint.
    front = overridden.applications["site0"]
    assert front.settings_status["effective_settings"]["cpu_retirement_quiet_seconds"] == 7.5


async def test_a_profile_with_a_negative_quiet_fails_the_boot(tmp_path):
    # wf:contract: the quiet is validated with the rest of the policy: a stored
    # wf:contract: profile carrying a negative one does not start the server.
    folder = tmp_path / "profiles"
    written(folder, "wrong", {"cpu_retirement_quiet_seconds": -1.0})
    server = pool_server(tmp_path, profiles_path=folder, profile_name="wrong")

    with pytest.raises(GroupPolicyError) as refused:
        await boot(server)

    assert any("cpu_retirement_quiet_seconds" in v for v in refused.value.violations)
