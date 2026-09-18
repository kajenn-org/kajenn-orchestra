"""Contract tests for SpaCommander.apply_group_settings — recomposition, generation.

Design sections 3, 5, 9; test matrix T6, T15, T27. The stage is a real vertex
with one real group and no worker started: what these tests ask is what the
effective configuration BECOMES, and no process is needed to answer that.
"""

import pytest

from kajenn_orchestra.orchestration import GroupHandler, SpaCommander

MEMORY_CEILING = 1_000_000


@pytest.fixture
def commander(tmp_path):
    """A vertex with one group, a profiles folder and the two immutable levels."""
    spa_commander = SpaCommander(
        tmp_path / "frozen_users",
        profiles_path=tmp_path / "profiles",
        recipe_settings={"worker_memory_admission_percent": 70.0, "worker_max_number": 3},
        env_settings={"worker_max_users": 16},
    )
    GroupHandler(
        spa_commander,
        "standard",
        memory_concession_bytes=MEMORY_CEILING,
        worker_memory_admission_percent=70.0,
        worker_max_number=3,
        worker_max_users=16,
        instance_dir=tmp_path / "i",
        frozen_users_path=tmp_path / "frozen_users",
    )
    return spa_commander


async def test_recompute_independent_of_previous_profile(commander):
    # wf:contract: T6 — apply(P2) after apply(P1) equals apply(P2) alone, key by
    # wf:contract: key: every apply recomposes defaults ⊕ recipe_settings ⊕
    # wf:contract: profile ⊕ env_settings; a key present in P1 and absent in P2
    # wf:contract: falls back to env, recipe or default, in that order (T5 sibling
    # wf:contract: at the HTTP level lives in phase 6).
    first = {"worker_memory_admission_percent": 60.0, "worker_min_life_seconds": 5.0}
    second = {"worker_min_life_seconds": 9.0}
    commander.profile_store.write("p1", first)
    commander.profile_store.write("p2", second)

    await commander.apply_group_settings(profile_name="p1", source="reload")
    after_both = await commander.apply_group_settings(profile_name="p2", source="reload")

    # The recipe level is back in charge of what P1 had overridden, and the env
    # level still wins over the recipe on its own key.
    assert after_both["effective_settings"]["worker_memory_admission_percent"] == 70.0
    assert after_both["effective_settings"]["worker_min_life_seconds"] == 9.0
    assert after_both["effective_settings"]["worker_max_users"] == 16
    # A key nobody names anywhere is the dataclass default.
    assert after_both["effective_settings"]["cpu_close_percent"] is None

    alone = SpaCommander(
        commander.freeze_handler.root_path,
        profiles_path=commander.profile_store.folder,
        recipe_settings=dict(commander.recipe_settings),
        env_settings=dict(commander.env_settings),
    )
    GroupHandler(
        alone,
        "standard",
        memory_concession_bytes=MEMORY_CEILING,
        instance_dir=commander.configured_group.worker_settings["instance_dir"],
        frozen_users_path=commander.freeze_handler.root_path,
    )
    fresh = await alone.apply_group_settings(profile_name="p2", source="reload")

    assert fresh["effective_settings"] == after_both["effective_settings"]
    assert fresh["active_profile"] == "p2"
    assert alone.active_profile == "p2"


async def test_derived_setpoint_worker_memory_max_percent(commander):
    # wf:contract: T15 — applying worker_max_number without an explicit
    # wf:contract: worker_memory_max_percent makes the next judgment read
    # wf:contract: quota/N; an explicit value wins; removing the explicit value
    # wf:contract: restores the derivation.
    group = commander.configured_group

    await commander.apply_group_settings(profile={"worker_max_number": 4})
    assert group.worker_memory_max_percent == 100.0 / 4

    await commander.apply_group_settings(
        profile={"worker_max_number": 4, "worker_memory_max_percent": 80.0}
    )
    assert group.worker_memory_max_percent == 80.0

    await commander.apply_group_settings(profile={"worker_max_number": 4})
    assert group.worker_memory_max_percent == 100.0 / 4


async def test_generation_advances_on_idempotent_apply(commander):
    # wf:contract: T27 — applying the same profile twice yields empty
    # wf:contract: changed_settings, outcome "applied", and a generation that
    # wf:contract: advances anyway: the audit counts successful attempts, not
    # wf:contract: differences.
    commander.profile_store.write("steady", {"cpu_close_percent": 30.0})

    first = await commander.apply_group_settings(profile_name="steady", source="reload")
    second = await commander.apply_group_settings(profile_name="steady", source="reload")

    assert first["changed_settings"] == {"cpu_close_percent": 30.0}
    assert second["changed_settings"] == {}
    assert second["outcome"] == "applied"
    assert (first["generation"], second["generation"]) == (2, 3)
    assert commander.configuration_generation == 3
    # Same settings, same fingerprint: the digest says what is in force, not
    # how many times it was asked for.
    assert commander.last_apply["generation"] == 3
    assert commander.last_apply["outcome"] == "applied"
    assert commander.last_apply["digest"] == commander._settings_digest(
        second["effective_settings"]
    )
