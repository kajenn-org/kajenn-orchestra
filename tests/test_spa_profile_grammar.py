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

"""Contract tests for the ``orchestration`` subtree of the spa front.

The whole orchestration of a spa front hangs under ONE node: the recipe writes
``front.orchestration(...)``, the commander under it, the groups under that.
``profiles_path``, ``profile_name`` and ``control_enabled`` are words of that
node and of nothing else — written on the application element they are refused
by name, with the new path in the message. ``env_settings`` is no grammar word
at all: it is a dict a Python recipe builds at runtime and hands over as a plain
constructor kwarg of the application.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import pytest

from kajenn import AsgiServer
from kajenn_orchestra.spa_app import (
    ORCHESTRATION_ROOT,
    WSX_ROOT,
    OrchestrationControl,
    SpaApplication,
)
from kajenn.config.builder import AsgiConfigBuilder
from kajenn.config.handler import ConfigError, ConfigurationHandler
from kajenn.lifespan import FatalBootError
from kajenn_orchestra.orchestration import SpaCommander

from .test_spa_app_profiles import lifespan_startup

SPA_APP_LOGGER = "kajenn_orchestra.spa_app"


class QuietCommander(SpaCommander):
    """The real vertex, minus the processes: nothing is launched, nothing forked."""

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


class GrammarFront(SpaApplication):
    """The front under test, with a pool that costs nothing to build."""

    commander_class = QuietCommander


def recipe_with(
    root: Path,
    orchestration_words: dict[str, Any],
    with_commander: bool = True,
    app_class: type = GrammarFront,
) -> type[AsgiConfigBuilder]:
    """A recipe writing ``orchestration_words`` on the front's orchestration node."""

    class GrammarConfig(AsgiConfigBuilder):
        def main(self, configuration_root: Any) -> None:
            cfg = configuration_root.configuration()
            front = cfg.applications().application(
                code="site0", mount="", app_class=app_class
            )
            orchestration = front.orchestration(**orchestration_words)
            if not with_commander:
                return
            commander = orchestration.commander(
                frozen_users_path=str(root / "frozen_users"),
                instance_dir=str(root / "i"),
            )
            commander.groups(default="standard").group(
                name="standard", entry_module="never.launched"
            )

    return GrammarConfig


def application_recipe_with(root: Path, **front_words: Any) -> type[AsgiConfigBuilder]:
    """The same recipe, with the words written on the APPLICATION element instead."""

    class ApplicationConfig(AsgiConfigBuilder):
        def main(self, configuration_root: Any) -> None:
            cfg = configuration_root.configuration()
            front = cfg.applications().application(
                code="site0", mount="", app_class=GrammarFront, **front_words
            )
            commander = front.orchestration().commander(
                frozen_users_path=str(root / "frozen_users"),
                instance_dir=str(root / "i"),
            )
            commander.groups(default="standard").group(
                name="standard", entry_module="never.launched"
            )

    return ApplicationConfig


async def boot(server: AsgiServer) -> None:
    """Bring the front up the way the lifespan does."""
    await server.applications["site0"].on_startup()


def test_the_orchestration_node_carries_the_three_words(tmp_path: Path) -> None:
    # wf:contract: profiles_path, profile_name and control_enabled are words of
    # wf:contract: the orchestration node, read at applications.<code>.orchestration,
    # wf:contract: and they reach the front at boot — never the constructor.
    profiles = tmp_path / "profiles"
    recipe = recipe_with(
        tmp_path,
        {
            "profiles_path": str(profiles),
            "profile_name": "busy_hours",
            "control_enabled": True,
        },
    )

    handler = ConfigurationHandler(recipe)
    assert handler.orchestration_kwargs("site0") == {
        "profiles_path": str(profiles),
        "profile_name": "busy_hours",
        "control_enabled": True,
    }

    entries, _default = ConfigurationHandler(recipe).applications()
    app_class, app_kwargs = entries[0]
    assert app_class is GrammarFront
    # Nothing of the orchestration travels as a constructor kwarg any more.
    assert app_kwargs == {"code": "site0", "mount": ""}

    front = AsgiServer(config=recipe).applications["site0"]
    assert front.profiles_path is None
    assert front.profile_name is None
    assert front.control_enabled is False


async def test_the_boot_reads_the_node_onto_the_front(tmp_path: Path) -> None:
    # wf:contract: the boot lands the three words on the front and mounts the
    # wf:contract: control root — last, once the pool is up.
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "busy_hours.json").write_text(json.dumps({"worker_memory_admission_percent": 60.0}))
    server = AsgiServer(
        config=recipe_with(
            tmp_path,
            {
                "profiles_path": str(profiles),
                "profile_name": "busy_hours",
                "control_enabled": True,
            },
        )
    )
    front = server.applications["site0"]
    assert ORCHESTRATION_ROOT not in front.internal_roots

    await boot(server)

    assert front.profiles_path == str(profiles)
    assert front.profile_name == "busy_hours"
    assert front.control_enabled is True
    assert ORCHESTRATION_ROOT in front.internal_roots
    assert front.commander.active_profile == "busy_hours"


def test_the_commander_only_lives_under_the_orchestration(tmp_path: Path) -> None:
    # wf:contract: the old form front.commander(...) is refused by the grammar,
    # wf:contract: and the refusal names the node the commander now belongs to.
    class OldFormConfig(AsgiConfigBuilder):
        def main(self, configuration_root: Any) -> None:
            cfg = configuration_root.configuration()
            front = cfg.applications().application(
                code="site0", mount="", app_class=GrammarFront
            )
            front.commander(frozen_users_path=str(tmp_path / "frozen_users"))

    with pytest.raises(ValueError, match="orchestration") as refusal:
        ConfigurationHandler(OldFormConfig)
    assert "parent" in str(refusal.value)


@pytest.mark.parametrize(
    "word", ["profiles_path", "profile_name", "orchestration_control"]
)
def test_a_moved_word_on_the_application_element_is_refused_by_name(
    tmp_path: Path, word: str
) -> None:
    # wf:contract: the three words that moved are refused by NAME on the
    # wf:contract: application element, with the new path in the message — never
    # wf:contract: as a bare unexpected-kwarg TypeError.
    recipe = application_recipe_with(tmp_path, **{word: "x"})

    with pytest.raises(ConfigError) as refusal:
        AsgiServer(config=recipe)
    message = str(refusal.value)
    assert word in message
    assert "applications.<code>.orchestration" in message


def test_the_old_and_the_new_form_together_are_refused(tmp_path: Path) -> None:
    # wf:contract: a recipe writing a moved word on the application element AND
    # wf:contract: the orchestration node fails — no silent precedence.
    recipe = application_recipe_with(tmp_path, profile_name="busy_hours")

    with pytest.raises(ConfigError, match="profile_name"):
        AsgiServer(config=recipe)


def test_env_settings_is_not_grammar(tmp_path: Path) -> None:
    # wf:contract: env_settings is not writable from the grammar — neither on the
    # wf:contract: orchestration node nor anywhere else: it stays a runtime dict.
    grammar_file = tmp_path / "site_grammar.json"
    AsgiConfigBuilder.to_grammar(str(grammar_file))
    document = json.dumps(json.load(grammar_file.open()))
    assert "env_settings" not in document

    with pytest.raises(Exception) as refusal:
        ConfigurationHandler(
            recipe_with(tmp_path, {"env_settings": {"worker_max_users": 3}})
        ).applications()
    assert "env_settings" in str(refusal.value)


async def test_a_front_with_no_orchestration_does_not_boot(tmp_path: Path) -> None:
    # wf:contract: a spa front declared without the orchestration node is an
    # wf:contract: INCOMPLETE configuration: fatal boot, startup.failed, no
    # wf:contract: commander and no control root. Wanting no pool means
    # wf:contract: declaring no spa front.
    class NoPoolConfig(AsgiConfigBuilder):
        def main(self, configuration_root: Any) -> None:
            cfg = configuration_root.configuration()
            cfg.applications().application(code="site0", mount="", app_class=GrammarFront)

    server = AsgiServer(config=NoPoolConfig)
    with pytest.raises(FatalBootError, match="no 'orchestration' node"):
        await boot(server)

    front = server.applications["site0"]
    assert front._commander is None
    assert ORCHESTRATION_ROOT not in front.internal_roots

    sent = await lifespan_startup(AsgiServer(config=NoPoolConfig))
    assert [message["type"] for message in sent][0] == "lifespan.startup.failed"


async def test_a_server_with_no_spa_front_at_all_starts(tmp_path: Path) -> None:
    # wf:contract: it is the spa front WITHOUT its orchestration that is
    # wf:contract: incomplete, never a server that declares no spa front: one
    # wf:contract: without any starts the way it always did.
    class NoFrontConfig(AsgiConfigBuilder):
        def main(self, configuration_root: Any) -> None:
            configuration_root.configuration().server(host="127.0.0.1", port=8000)

    server = AsgiServer(config=NoFrontConfig)
    assert not [app for app in server.applications.values() if isinstance(app, SpaApplication)]

    sent = await lifespan_startup(server)
    assert [message["type"] for message in sent][0] == "lifespan.startup.complete"


async def test_an_orchestration_with_no_commander_does_not_boot(tmp_path: Path) -> None:
    # wf:contract: a declared orchestration node MUST carry a commander: profile
    # wf:contract: and control plane with no pool to act on address nothing, so
    # wf:contract: the boot is fatal and the lifespan answers startup.failed.
    server = AsgiServer(
        config=recipe_with(tmp_path, {"control_enabled": True}, with_commander=False)
    )

    with pytest.raises(FatalBootError, match="no commander"):
        await boot(server)

    front = server.applications["site0"]
    assert front._commander is None
    assert ORCHESTRATION_ROOT not in front.internal_roots

    fresh = AsgiServer(
        config=recipe_with(tmp_path, {"control_enabled": True}, with_commander=False)
    )
    sent = await lifespan_startup(fresh)
    assert [message["type"] for message in sent][0] == "lifespan.startup.failed"


def counting_front(
    built: list[Any],
    mount_breaks: bool = False,
    failing_starts: int = 0,
    stop_breaks: bool = False,
) -> type[SpaApplication]:
    """A front whose vertex counts what the lifecycle does to it.

    Args:
        built: every vertex ever constructed lands here, in order.
        mount_breaks: whether ``mount_control`` raises, to exercise the rollback.
        failing_starts: how many of the first vertices raise from ``start`` —
            AFTER arming their clock, the way a half-started pool leaves one.
        stop_breaks: whether ``stop`` raises once it has done its work, to check
            that a cleanup failure never replaces the reason the boot failed.
    """

    class CountingCommander(SpaCommander):
        """The real vertex, with a heartbeat that sleeps and no process at all."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.starts = 0
            self.stops = 0
            self.quits = 0
            built.append(self)

        async def start(self) -> None:
            self.starts += 1
            self._heartbeat_task = asyncio.ensure_future(asyncio.sleep(3600))
            if len(built) <= failing_starts:
                raise RuntimeError("the reception would not come up")

        async def stop(self) -> None:
            self.stops += 1
            if self._heartbeat_task is not None:
                self._heartbeat_task.cancel()
                self._heartbeat_task = None
            if stop_breaks:
                raise RuntimeError("the pool would not go down")

        async def quit(self) -> None:
            self.quits += 1
            await self.stop()

    class CountingFront(SpaApplication):
        commander_class = CountingCommander

        def mount_control(self) -> None:
            if mount_breaks:
                raise RuntimeError("the router refused the branch")
            super().mount_control()

    return CountingFront


def live_heartbeats(built: list[Any]) -> int:
    """How many of those vertices still hold a running clock."""
    return sum(
        1
        for commander in built
        if commander._heartbeat_task is not None and not commander._heartbeat_task.done()
    )


async def test_a_root_the_front_already_claims_does_not_boot(tmp_path: Path) -> None:
    # wf:contract: a front whose own router already answers on _orchestration
    # wf:contract: cannot also mount the runtime configuration there. The clash
    # wf:contract: is established BEFORE anything is built, so the boot fails
    # wf:contract: with no vertex constructed, no clock running and the router
    # wf:contract: exactly as the front left it.
    built: list[Any] = []

    class ClashingFront(counting_front(built)):  # type: ignore[misc]
        """A front that claims the control root for a page of its own."""

        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.route.add_branches(
                {"name": ORCHESTRATION_ROOT, "instance": OrchestrationControl(self)}
            )

    recipe = recipe_with(tmp_path, {"control_enabled": True}, app_class=ClashingFront)
    server = AsgiServer(config=recipe)
    front = server.applications["site0"]
    before = set(front.internal_roots)

    with pytest.raises(FatalBootError, match="already claimed"):
        await boot(server)

    assert built == []
    assert front._commander is None
    assert live_heartbeats(built) == 0
    assert set(front.internal_roots) == before

    sent = await lifespan_startup(AsgiServer(config=recipe))
    assert [message["type"] for message in sent][0] == "lifespan.startup.failed"


async def test_a_mount_that_breaks_takes_the_pool_back_down(tmp_path: Path) -> None:
    # wf:contract: the mount is the last mutation, and an unexpected failure of
    # wf:contract: it rolls the pool back: stop is called, the front holds no
    # wf:contract: vertex, no clock is left running, and the boot is fatal.
    built: list[Any] = []
    server = AsgiServer(
        config=recipe_with(
            tmp_path,
            {"control_enabled": True},
            app_class=counting_front(built, mount_breaks=True),
        )
    )
    front = server.applications["site0"]

    with pytest.raises(FatalBootError, match="taken back down"):
        await boot(server)

    assert len(built) == 1
    assert (built[0].starts, built[0].stops) == (1, 1)
    assert front._commander is None
    assert live_heartbeats(built) == 0
    assert ORCHESTRATION_ROOT not in front.internal_roots


async def test_a_second_startup_builds_no_second_pool(tmp_path: Path) -> None:
    # wf:contract: a startup on a front whose pool is already up does nothing —
    # wf:contract: one vertex, one start, one route, and no orphan clock.
    built: list[Any] = []
    server = AsgiServer(
        config=recipe_with(
            tmp_path, {"control_enabled": True}, app_class=counting_front(built)
        )
    )
    front = server.applications["site0"]

    await boot(server)
    await boot(server)

    assert len(built) == 1
    assert (built[0].starts, built[0].stops, built[0].quits) == (1, 0, 0)
    assert front.commander is built[0]
    assert live_heartbeats(built) == 1
    # The front's internal roots, each claimed ONCE: the orchestration control
    # and the channel commands of a page (#68 phase 4).
    assert set(front.route.nodes(lazy=True, forbidden=True)["routers"]) == {
        ORCHESTRATION_ROOT,
        WSX_ROOT,
    }


async def test_a_startup_after_a_shutdown_builds_a_new_pool_and_no_second_route(
    tmp_path: Path,
) -> None:
    # wf:contract: startup → shutdown → startup gives TWO vertices, never at the
    # wf:contract: same time: the first is stopped and let go, its clock is gone,
    # wf:contract: and the control root is not mounted a second time.
    built: list[Any] = []
    server = AsgiServer(
        config=recipe_with(
            tmp_path, {"control_enabled": True}, app_class=counting_front(built)
        )
    )
    front = server.applications["site0"]

    await boot(server)
    first = front.commander
    await front.on_shutdown()

    assert front._commander is None
    assert (first.starts, first.stops) == (1, 1)
    assert live_heartbeats(built) == 0

    await boot(server)

    assert len(built) == 2
    assert front.commander is built[1]
    assert front.commander is not first
    assert (built[1].starts, built[1].stops) == (1, 0)
    # Only the new one holds a clock: the first was let go for good.
    assert live_heartbeats(built) == 1
    assert first.stops == 1

    assert ORCHESTRATION_ROOT in front.internal_roots
    # The front's internal roots, each claimed ONCE: the orchestration control
    # and the channel commands of a page (#68 phase 4).
    assert set(front.route.nodes(lazy=True, forbidden=True)["routers"]) == {
        ORCHESTRATION_ROOT,
        WSX_ROOT,
    }
    for name in ("apply", "reload", "status"):
        assert front.resolves_natively(f"/{ORCHESTRATION_ROOT}/{name}") is True

    await front.on_shutdown()


async def test_a_boot_that_fails_leaves_the_router_untouched(tmp_path: Path) -> None:
    # wf:contract: nothing is mounted before the pool is up: a composition the
    # wf:contract: boot refuses claims no root at all, gate on or not.
    folder = tmp_path / "profiles"
    folder.mkdir()
    (folder / "wrong.json").write_text(json.dumps({"worker_memory_admission_percent": 200.0}))
    server = AsgiServer(
        config=recipe_with(
            tmp_path,
            {
                "profiles_path": str(folder),
                "profile_name": "wrong",
                "control_enabled": True,
            },
        )
    )

    with pytest.raises(FatalBootError):
        await boot(server)

    front = server.applications["site0"]
    assert front._commander is None
    assert ORCHESTRATION_ROOT not in front.internal_roots


async def test_a_start_that_raises_leaves_the_front_holding_nothing(tmp_path: Path) -> None:
    # wf:contract: a pool that arms something and then fails to come up is taken
    # wf:contract: back down: one vertex, one start, one stop, no clock left, the
    # wf:contract: front holding none, the router untouched and the boot fatal.
    built: list[Any] = []
    recipe = recipe_with(
        tmp_path,
        {"control_enabled": True},
        app_class=counting_front(built, failing_starts=1),
    )
    server = AsgiServer(config=recipe)
    front = server.applications["site0"]
    before = set(front.internal_roots)

    with pytest.raises(FatalBootError, match="could not be brought up") as refused:
        await boot(server)

    assert isinstance(refused.value.__cause__, RuntimeError)
    assert len(built) == 1
    assert (built[0].starts, built[0].stops) == (1, 1)
    assert front._commander is None
    assert live_heartbeats(built) == 0
    assert set(front.internal_roots) == before
    assert ORCHESTRATION_ROOT not in front.internal_roots

    # The same failure through the real lifespan: the server does not start.
    fresh: list[Any] = []
    sent = await lifespan_startup(
        AsgiServer(
            config=recipe_with(
                tmp_path,
                {"control_enabled": True},
                app_class=counting_front(fresh, failing_starts=1),
            )
        )
    )
    assert [message["type"] for message in sent][0] == "lifespan.startup.failed"
    assert live_heartbeats(fresh) == 0


async def test_a_second_attempt_after_a_failed_start_comes_up(tmp_path: Path) -> None:
    # wf:contract: the idempotent guard never protects a pool that failed to
    # wf:contract: start: the next startup builds a NEW vertex and brings it up.
    built: list[Any] = []
    server = AsgiServer(
        config=recipe_with(
            tmp_path,
            {"control_enabled": True},
            app_class=counting_front(built, failing_starts=1),
        )
    )
    front = server.applications["site0"]

    with pytest.raises(FatalBootError):
        await boot(server)
    await boot(server)

    assert len(built) == 2
    assert front.commander is built[1]
    assert (built[1].starts, built[1].stops) == (1, 0)
    assert live_heartbeats(built) == 1
    assert ORCHESTRATION_ROOT in front.internal_roots

    await front.on_shutdown()


async def test_a_cleanup_that_breaks_never_hides_the_mount_failure(
    tmp_path: Path, caplog: Any
) -> None:
    # wf:contract: when the rollback's own stop raises, the front still ends
    # wf:contract: holding no vertex and mounts nothing, the FatalBootError still
    # wf:contract: carries the MOUNT failure as its cause, and the cleanup
    # wf:contract: failure is readable on the module logger.
    built: list[Any] = []
    server = AsgiServer(
        config=recipe_with(
            tmp_path,
            {"control_enabled": True},
            app_class=counting_front(built, mount_breaks=True, stop_breaks=True),
        )
    )
    front = server.applications["site0"]

    with caplog.at_level(logging.ERROR, logger=SPA_APP_LOGGER):
        with pytest.raises(FatalBootError, match="taken back down") as refused:
            await boot(server)

    assert str(refused.value.__cause__) == "the router refused the branch"
    assert len(built) == 1
    assert (built[0].starts, built[0].stops) == (1, 1)
    assert front._commander is None
    assert ORCHESTRATION_ROOT not in front.internal_roots
    assert "refused to go back down" in caplog.text
    assert "the pool would not go down" in caplog.text
