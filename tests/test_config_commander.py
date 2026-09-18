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

"""The ``orchestration`` subtree of the config dialect, read and built.

The grammar is the core's (``kajenn/config/handler.py``): ``orchestration``
on its own path, ``commander`` one rung below it, one kwargs set per group. What
only the pool can answer for is here rather than in ``tests/core/test_config.py``
— two of these tests hand those kwargs to a real ``SpaCommander`` and
``GroupHandler``, and no core object stands in for them.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from kajenn import AsgiConfigBuilder, AsgiServer, BaseApplication, ConfigurationHandler
from kajenn.types import Scope, Receive, Send
from kajenn_orchestra.orchestration import GroupHandler, SpaCommander
from kajenn_orchestra.orchestration.spa_commander import ORDERS_LOGGER_NAME
from kajenn_orchestra.spa_app import SpaApplication


class ShopApp(BaseApplication):
    """Root app: answers ``shop``."""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"shop"})


class ApiApp(BaseApplication):
    """Secondary app: answers ``api``."""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"api"})


class TwoAppConfig(AsgiConfigBuilder):
    """Two apps (shop on the root, api secondary), cors + basic auth, host/port.

    The site with no pool at all, against which the read stack must answer
    ``None`` for every orchestration question. The same recipe drives the core's
    own config tests in kajenn; it is restated here so this suite reads no test
    module of another distribution.
    """

    def main(self, root: Any) -> None:
        cfg = root.configuration()
        cfg.server(host="0.0.0.0", port=9100, external_url="https://shop.example.com")
        cfg.middleware(cors=True)
        self.authentication_section(cfg)
        self.applications_section(cfg)

    def authentication_section(self, cfg: Any) -> None:
        """One Basic user, handed to ``AuthCore`` through ``credentials``."""
        creds = cfg.authentication().credentials()
        creds.basic_user(username="admin", password="secret", tags="admin")

    def applications_section(self, cfg: Any) -> None:
        """``shop`` claims the site root, ``api`` answers its own mount."""
        apps = cfg.applications(default="shop")
        apps.application(code="shop", mount="", app_class=ShopApp)
        apps.application(code="api", app_class=ApiApp)


class SpaPoolConfig(AsgiConfigBuilder):
    """The ``orchestration`` subtree with every key of both rungs, and two groups.

    The second group declares nothing but its child: what it leaves out is what
    the objects' own defaults answer for, which is the read stack's whole
    contract.
    """

    def main(self, root: Any) -> None:
        cfg = root.configuration()
        cfg.server(host="127.0.0.1", port=8000)
        front = cfg.applications().application(
            code="shop", mount="", app_class=SpaApplication
        )
        self.commander_section(front)

    def commander_section(self, front: Any) -> None:
        """The orchestration, the vertex under it, then its groups on two interpreters."""
        commander = front.orchestration().commander(
            frozen_users_path="/srv/shop/frozen_users",
            instance_dir="/srv/shop/instance",
            memory_max_percent=75.0,
            machine_memory_alarm_percent=85.0,
            orchestration_log_path="/srv/shop/logs/orchestration.log",
            orchestration_log_max_bytes=2_000_000,
            orchestration_log_backup_count=3,
            user_expiry_hours=480.0,
            guest_expiry_hours=12.0,
            cpu_temperature_sample_seconds=0.25,
        )
        groups = commander.groups()
        groups.group(
            name="stable",
            memory_max_percent=80.0,
            worker_memory_max_percent=40.0,
            worker_memory_admission_percent=70.0,
            restart_occupancy_max_percent=90.0,
            worker_min_life_seconds=120.0,
            worker_max_users=16,
            cpu_retirement_quiet_seconds=75.0,
            cpu_heating_seconds=2.0,
            cpu_cooling_seconds=8.0,
            user_idle_freeze_minutes=45.0,
            entry_module="kajenn_orchestra.orchestration.worker_entry",
            executable="/srv/shop/.venvs/stable/bin/python",
            worker_class="myshop.app:ShopWorker",
            main_threadpool_size=8,
            aux_threadpool_size=2,
            worker_kwargs={"site_path": "/srv/shop"},
            engine_factory="myshop.app:ShopEngineFactory",
            engine_kwargs={"source": "/srv/shop"},
        )
        groups.group(name="canary", entry_module="kajenn_orchestra.orchestration.worker_entry")


class ElectedGroupConfig(SpaPoolConfig):
    """The same pool, with the group that receives a newcomer elected by name."""

    def commander_section(self, front: Any) -> None:
        commander = front.orchestration().commander(
            frozen_users_path="/srv/shop/frozen_users"
        )
        groups = commander.groups(default="canary")
        groups.group(name="stable", entry_module="kajenn_orchestra.orchestration.worker_entry")
        groups.group(name="canary", entry_module="kajenn_orchestra.orchestration.worker_entry")


class TestCommanderSection:
    """``orchestration`` → its own three words, ``commander`` → the vertex's kwargs
    and one kwargs set per group."""

    def test_the_orchestration_node_is_read_on_its_own_path(self) -> None:
        """The three words of the node, and the commander one rung below it."""

        class ProfiledPoolConfig(SpaPoolConfig):
            def commander_section(self, front: Any) -> None:
                orchestration = front.orchestration(
                    profiles_path="/srv/shop/profiles",
                    profile_name="busy_hours",
                    control_enabled=True,
                )
                orchestration.commander(
                    frozen_users_path="/srv/shop/frozen_users"
                ).groups().group(name="stable")

        handler = ConfigurationHandler(ProfiledPoolConfig)

        assert handler.orchestration_kwargs("shop") == {
            "profiles_path": "/srv/shop/profiles",
            "profile_name": "busy_hours",
            "control_enabled": True,
        }
        assert handler.commander_kwargs("shop") == {
            "frozen_users_path": "/srv/shop/frozen_users"
        }
        assert set(handler.group_kwargs("shop")) == {"stable"}

    def test_an_orchestration_that_declares_no_commander_reads_as_none(self) -> None:
        """The node alone: its words are there, and there is no vertex to build."""

        class BareOrchestrationConfig(SpaPoolConfig):
            def commander_section(self, front: Any) -> None:
                front.orchestration(control_enabled=True)

        handler = ConfigurationHandler(BareOrchestrationConfig)

        assert handler.orchestration_kwargs("shop") == {"control_enabled": True}
        assert handler.commander_kwargs("shop") is None
        assert handler.group_kwargs("shop") == {}

    def test_the_recipe_elects_the_group_that_receives_a_newcomer(self) -> None:
        kwargs = ConfigurationHandler(ElectedGroupConfig).commander_kwargs("shop")

        assert kwargs["default_group"] == "canary"

    def test_the_vertex_reads_its_own_policies_and_not_the_workers_path(self) -> None:
        handler = ConfigurationHandler(SpaPoolConfig)

        assert handler.commander_kwargs("shop") == {
            "frozen_users_path": "/srv/shop/frozen_users",
            "memory_max_percent": 75.0,
            "machine_memory_alarm_percent": 85.0,
            "orchestration_log_path": "/srv/shop/logs/orchestration.log",
            "orchestration_log_max_bytes": 2_000_000,
            "orchestration_log_backup_count": 3,
            "user_expiry_hours": 480.0,
            "guest_expiry_hours": 12.0,
            "cpu_temperature_sample_seconds": 0.25,
        }

    def test_a_group_reads_its_policies_the_two_paths_and_its_childs_identity(self) -> None:
        groups = ConfigurationHandler(SpaPoolConfig).group_kwargs("shop")

        assert set(groups) == {"stable", "canary"}
        assert groups["stable"] == {
            "frozen_users_path": "/srv/shop/frozen_users",
            "instance_dir": "/srv/shop/instance",
            "memory_max_percent": 80.0,
            "worker_memory_max_percent": 40.0,
            "worker_memory_admission_percent": 70.0,
            "restart_occupancy_max_percent": 90.0,
            "worker_min_life_seconds": 120.0,
            "worker_max_users": 16,
            # The retirement's quiet is a policy of the GROUP (#43): how long
            # the CPU must stay silent before the closure judge resumes.
            "cpu_retirement_quiet_seconds": 75.0,
            "cpu_heating_seconds": 2.0,
            "cpu_cooling_seconds": 8.0,
            # The silence is the GROUP's own policy: it is the rung that judges
            # who has gone quiet, and the child measures nothing.
            "user_idle_freeze_minutes": 45.0,
            "entry_module": "kajenn_orchestra.orchestration.worker_entry",
            "executable": "/srv/shop/.venvs/stable/bin/python",
            "worker_class": "myshop.app:ShopWorker",
            "main_threadpool_size": 8,
            "aux_threadpool_size": 2,
            # The one key the CHILD reads is his: his group's name.
            "worker_kwargs": {"site_path": "/srv/shop", "group": "stable"},
            # And how its workers are born: a template builds the engine once and
            # forks every one of them out of it.
            "engine_factory": "myshop.app:ShopEngineFactory",
            "engine_kwargs": {"source": "/srv/shop"},
        }

    def test_a_group_that_declares_only_its_child_leaves_every_default_alone(self) -> None:
        canary = ConfigurationHandler(SpaPoolConfig).group_kwargs("shop")["canary"]

        assert canary == {
            "frozen_users_path": "/srv/shop/frozen_users",
            "instance_dir": "/srv/shop/instance",
            "entry_module": "kajenn_orchestra.orchestration.worker_entry",
            "worker_kwargs": {"group": "canary"},
        }

    def test_the_pool_keys_reach_the_vertex_and_the_group_that_read_them(self, tmp_path) -> None:
        groups = ConfigurationHandler(SpaPoolConfig).group_kwargs("shop")
        commander_kwargs = ConfigurationHandler(SpaPoolConfig).commander_kwargs("shop")
        commander_kwargs["frozen_users_path"] = tmp_path / "frozen_users"
        commander_kwargs["orchestration_log_path"] = tmp_path / "orchestration.log"

        commander = SpaCommander(**commander_kwargs)
        group = GroupHandler(
            commander,
            "stable",
            memory_concession_bytes=commander.memory_concession_bytes,
            **dict(
                groups["stable"],
                instance_dir=tmp_path / "instance",
                frozen_users_path=tmp_path / "frozen_users",
            ),
        )

        assert commander.memory_max_percent == 75.0
        assert commander.machine_memory_alarm_percent == 85.0
        assert commander.user_expiry_hours == 480.0
        assert commander.guest_expiry_hours == 12.0
        # The three log keys land on the rotating handler itself, not just in
        # the kwargs dict: the file, its size, how many are kept.
        attached, = logging.getLogger(ORDERS_LOGGER_NAME).handlers
        assert attached.maxBytes == 2_000_000
        assert attached.backupCount == 3
        assert group.worker_memory_admission_percent == 70.0
        assert group.restart_occupancy_max_percent == 90.0
        assert group.policy.worker_min_life_seconds == 120.0
        assert group.worker_max_users == 16
        assert group.memory_max_percent == 80.0
        assert group.worker_memory_max_percent == 40.0
        # The new word of #43 lands on the built group, exactly as declared.
        assert group.cpu_retirement_quiet_seconds == 75.0
        # The silence it judges is its own, and never travels down to the child.
        assert group.user_idle_freeze_minutes == 45.0
        # And what the group hands its workers is what a WorkerHandler is built
        # with: the identity of the child, the two paths, the child's own kwargs.
        assert group.worker_settings == {
            "frozen_users_path": tmp_path / "frozen_users",
            "instance_dir": tmp_path / "instance",
            "entry_module": "kajenn_orchestra.orchestration.worker_entry",
            "executable": "/srv/shop/.venvs/stable/bin/python",
            "worker_class": "myshop.app:ShopWorker",
            "main_threadpool_size": 8,
            "aux_threadpool_size": 2,
            "worker_kwargs": {"site_path": "/srv/shop", "group": "stable"},
        }
        # The two engine keys are NOT among those: they are the group's own, and
        # what they reach is the template it owns.
        assert group.template.launch_payload == {
            "name": "template-stable",
            "engine_factory": "myshop.app:ShopEngineFactory",
            "kwargs": {"source": "/srv/shop"},
        }

    def test_a_group_that_declares_no_engine_factory_owns_no_template(self, tmp_path) -> None:
        groups = ConfigurationHandler(SpaPoolConfig).group_kwargs("shop")
        commander = SpaCommander(tmp_path / "frozen_users")

        group = GroupHandler(
            commander,
            "canary",
            memory_concession_bytes=commander.memory_concession_bytes,
            **dict(
                groups["canary"],
                instance_dir=tmp_path / "instance",
                frozen_users_path=tmp_path / "frozen_users",
            ),
        )

        assert group.template is None

    def test_a_site_with_no_pool_declares_no_orchestration_at_all(self) -> None:
        handler = ConfigurationHandler(TwoAppConfig)
        assert handler.orchestration_kwargs("shop") is None
        assert handler.commander_kwargs("shop") is None
        assert handler.group_kwargs("shop") == {}

    def test_the_pool_section_travels_through_a_real_server(self) -> None:
        server = AsgiServer(config=SpaPoolConfig)
        assert server.config.commander_kwargs("shop")["memory_max_percent"] == 75.0
        assert set(server.config.group_kwargs("shop")) == {"stable", "canary"}

    def test_a_group_outside_its_collection_is_rejected_by_the_grammar(self) -> None:
        class StrayGroupConfig(AsgiConfigBuilder):
            def main(self, root: Any) -> None:
                cfg = root.configuration()
                front = cfg.applications().application(
                    code="shop", mount="", app_class=SpaApplication
                )
                front.orchestration().commander().group(name="stable")

        with pytest.raises(ValueError, match="parent"):
            ConfigurationHandler(StrayGroupConfig)

    def test_the_pool_is_not_a_section_of_the_site_dialect(self) -> None:
        """A pool belongs to the front that owns it, so the root has no words for it."""

        class TopLevelPoolConfig(AsgiConfigBuilder):
            def main(self, root: Any) -> None:
                root.configuration().orchestration()

        with pytest.raises(AttributeError, match="orchestration"):
            ConfigurationHandler(TopLevelPoolConfig)
