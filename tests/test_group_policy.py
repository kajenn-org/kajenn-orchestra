"""Contract tests for GroupPolicy — defaults, validation, null semantics.

Design sections 4, 6, 8 and the setpoint matrix; test matrix T7, T16, T17, T25.
"""

import json
import math

import pytest

from kajenn_orchestra.orchestration.group_policy import GroupPolicy, GroupPolicyError


def test_defaults_materialized_from_empty_settings():
    # wf:contract: T7 — GroupPolicy.from_settings({}) yields a COMPLETE policy
    # wf:contract: whose values equal today's GroupHandler constructor defaults;
    # wf:contract: a sparse settings dict fills the absent keys with those same
    # wf:contract: defaults, never a KeyError.
    policy = GroupPolicy.from_settings({})
    assert policy.worker_memory_admission_percent == 80.0
    assert policy.restart_occupancy_max_percent == 95.0
    assert policy.cpu_close_percent is None
    assert policy.cpu_admission_close_percent is None
    assert policy.cpu_admission_reopen_percent == 40.0
    assert policy.cpu_retirement_quiet_seconds == 60.0
    assert policy.cpu_heating_seconds == 1.0
    assert policy.cpu_cooling_seconds == 5.0
    assert policy.worker_min_life_seconds == 60.0
    assert policy.worker_max_users == math.inf
    assert policy.user_idle_freeze_minutes == math.inf
    assert policy.memory_max_percent == 100.0
    assert policy.worker_max_number == 6

    sparse = GroupPolicy.from_settings({"worker_memory_admission_percent": 70.0})
    assert sparse.worker_memory_admission_percent == 70.0
    assert sparse.cpu_close_percent is None
    assert sparse.worker_max_number == 6
    assert set(sparse.to_settings()) == set(GroupPolicy.SETPOINTS)


def test_validation_rejects_and_lists_all_violations():
    # wf:contract: T16 — from_settings collects EVERY violation into one error:
    # wf:contract: unknown keys; structural keys with the dedicated message
    # wf:contract: "structural, not a profile key"; bool passed as a number
    # wf:contract: (rejected before the numeric check); NaN/Infinity values;
    # wf:contract: percentages outside [0, 100]; memory_max_percent 0 and above
    # wf:contract: 100 both rejected (0 < v <= 100); negative times; non-integer
    # wf:contract: counts; worker_max_number <= 0; a broken CPU band
    # wf:contract: (reopen >= close); cross rules on the COMPLETE resulting policy:
    # wf:contract: memory admission >= restart_occupancy,
    # wf:contract: new_user_occupancy <= 0.
    # wf:contract: A single violation means NO policy object is produced.
    with pytest.raises(GroupPolicyError) as caught:
        GroupPolicy.from_settings(
            {
                "nonsense_percent": 1.0,
                "engine_factory": "module:factory",
                "worker_memory_admission_percent": True,
                "restart_occupancy_max_percent": float("nan"),
                "cpu_close_percent": float("inf"),
                "memory_max_percent": 0.0,
                "worker_min_life_seconds": -1.0,
                "worker_max_users": 1.5,
                "worker_max_number": 0,
            }
        )
    violations = caught.value.violations
    assert len(violations) == 9
    reported = " | ".join(violations)
    assert "nonsense_percent: unknown setpoint" in violations
    assert "engine_factory: structural, not a profile key" in violations
    for key in (
        "worker_memory_admission_percent",
        "restart_occupancy_max_percent",
        "cpu_close_percent",
        "memory_max_percent",
        "worker_min_life_seconds",
        "worker_max_users",
        "worker_max_number",
    ):
        assert any(v.startswith(f"{key}:") for v in violations), reported
    assert "worker_memory_admission_percent: expected a number, got bool" in violations
    assert "worker_max_users: expected an integer, got float" in violations

    with pytest.raises(GroupPolicyError) as caught:
        GroupPolicy.from_settings({"memory_max_percent": 100.5})
    assert caught.value.violations == [
        "memory_max_percent: 100.5 is out of range, must be > 0.0 and <= 100.0"
    ]
    assert GroupPolicy.from_settings({"memory_max_percent": 100.0}).memory_max_percent == 100.0

    with pytest.raises(GroupPolicyError) as caught:
        GroupPolicy.from_settings({"cpu_heating_seconds": 0.0})
    assert len(caught.value.violations) == 1

    with pytest.raises(GroupPolicyError) as caught:
        GroupPolicy.from_settings(
            {"cpu_admission_close_percent": 30.0, "cpu_admission_reopen_percent": 30.0}
        )
    assert "cpu_admission_reopen_percent" in caught.value.violations[0]


    with pytest.raises(GroupPolicyError) as caught:
        GroupPolicy.from_settings({"worker_memory_admission_percent": 96.0})
    assert len(caught.value.violations) == 1
    assert "restart_occupancy_max_percent" in caught.value.violations[0]

    # A per-key violation stops before the policy exists: no partial object.
    with pytest.raises(GroupPolicyError):
        GroupPolicy.from_settings({"worker_max_users": None, "worker_max_number": -3})


def test_null_means_unlimited_or_off():
    # wf:contract: T17 — worker_max_users null becomes math.inf internally and
    # wf:contract: null again in to_settings(); user_idle_freeze_minutes null the
    # wf:contract: same; cpu_admission_close_percent null becomes None (policy off);
    # wf:contract: to_settings() output is JSON-safe: json.dumps(...,
    # wf:contract: allow_nan=False) never raises on it.
    policy = GroupPolicy.from_settings(
        {
            "worker_max_users": None,
            "user_idle_freeze_minutes": None,
            "cpu_admission_close_percent": None,
        }
    )
    assert policy.worker_max_users == math.inf
    assert policy.user_idle_freeze_minutes == math.inf
    assert policy.cpu_admission_close_percent is None

    settings = policy.to_settings()
    assert settings["worker_max_users"] is None
    assert settings["user_idle_freeze_minutes"] is None
    assert settings["cpu_admission_close_percent"] is None
    assert json.loads(json.dumps(settings, allow_nan=False)) == settings

    bounded = GroupPolicy.from_settings({"worker_max_users": 12, "user_idle_freeze_minutes": 30.0})
    assert bounded.worker_max_users == 12
    assert bounded.to_settings()["worker_max_users"] == 12
    assert bounded.to_settings()["user_idle_freeze_minutes"] == 30.0
    assert json.dumps(GroupPolicy.from_settings({}).to_settings(), allow_nan=False)


def test_worker_memory_max_percent_explicit_or_derived():
    # wf:contract: the property renders the explicit value when set (it may
    # wf:contract: exceed 100), else 100.0 / worker_max_number; removing the
    # wf:contract: explicit key restores the derivation.
    derived = GroupPolicy.from_settings({"worker_max_number": 4})
    assert derived.worker_memory_max_percent == 25.0
    assert derived.to_settings()["worker_memory_max_percent"] is None

    explicit = GroupPolicy.from_settings(
        {"worker_max_number": 4, "worker_memory_max_percent": 400.0}
    )
    assert explicit.worker_memory_max_percent == 400.0
    assert explicit.to_settings()["worker_memory_max_percent"] == 400.0

    restored = GroupPolicy.from_settings(
        {"worker_max_number": 4, "worker_memory_max_percent": None}
    )
    assert restored.worker_memory_max_percent == 25.0
    assert restored.to_settings()["worker_memory_max_percent"] is None

    with pytest.raises(GroupPolicyError) as caught:
        GroupPolicy.from_settings({"worker_memory_max_percent": 0.0})
    assert "worker_memory_max_percent" in caught.value.violations[0]


def test_profile_version_reserved_key():
    # wf:contract: T25 — profile_version absent is accepted as 1; the value 1 is
    # wf:contract: accepted; any other value is rejected explicitly; the key
    # wf:contract: never enters the setpoints nor to_settings() output.
    assert "profile_version" not in GroupPolicy.from_settings({}).to_settings()
    accepted = GroupPolicy.from_settings({"profile_version": 1, "worker_max_number": 3})
    assert "profile_version" not in accepted.to_settings()
    assert accepted.worker_max_number == 3

    for value in (2, "1", True, None):
        with pytest.raises(GroupPolicyError) as caught:
            GroupPolicy.from_settings({"profile_version": value})
        assert caught.value.violations == [
            f"profile_version: only version 1 is supported, got {value!r}"
        ]


def test_the_retirement_quiet_is_a_non_negative_duration():
    # wf:contract: cpu_retirement_quiet_seconds is a finite number >= 0 — zero
    # wf:contract: is a legitimate duration ("judge as soon as nobody is
    # wf:contract: closed"), null is not (the quiet is never "off"), and a
    # wf:contract: negative one is out of range.
    assert GroupPolicy.from_settings({"cpu_retirement_quiet_seconds": 0}).\
        cpu_retirement_quiet_seconds == 0
    assert GroupPolicy.from_settings({"cpu_retirement_quiet_seconds": 0.5}).\
        cpu_retirement_quiet_seconds == 0.5

    for refused, expected in (
        (-1.0, "out of range"),
        (None, "null is not allowed"),
        (float("inf"), "must be a finite number"),
        ("60", "expected a number"),
    ):
        with pytest.raises(GroupPolicyError) as error:
            GroupPolicy.from_settings({"cpu_retirement_quiet_seconds": refused})
        assert expected in str(error.value)
        assert "cpu_retirement_quiet_seconds" in str(error.value)

    # It survives the round trip a profile takes, like every other setpoint.
    written = GroupPolicy.from_settings({"cpu_retirement_quiet_seconds": 12.5}).to_settings()
    assert written["cpu_retirement_quiet_seconds"] == 12.5
    assert json.dumps(written, allow_nan=False)


def test_the_close_threshold_follows_the_reopen_threshold_unless_set():
    assert GroupPolicy.from_settings({}).cpu_close_percent is None
    with pytest.raises(GroupPolicyError) as caught:
        GroupPolicy.from_settings(
            {
                "cpu_admission_close_percent": 50.0,
                "cpu_admission_reopen_percent": 30.0,
                "cpu_close_percent": 35.0,
            }
        )
    assert "cpu_close_percent" in caught.value.violations[0]
    kept = GroupPolicy.from_settings(
        {"cpu_admission_close_percent": 50.0, "cpu_admission_reopen_percent": 30.0}
    )
    assert kept.cpu_close_percent is None
    # With the admission policy off the retirement still judges on temperature,
    # and an explicit threshold is its own.
    assert GroupPolicy.from_settings({"cpu_close_percent": 90.0}).cpu_close_percent == 90.0


def test_the_offload_threshold_stands_on_the_admission_closure():
    # cpu_offload_percent off by default; set, it requires cpu_admission_close_percent
    # and must sit ABOVE it: the offload relies on the source being closed to
    # new users, or the offloaded user could be placed right back.
    assert GroupPolicy.from_settings({}).cpu_offload_percent is None
    assert GroupPolicy.from_settings({"cpu_offload_percent": None}).cpu_offload_percent is None

    policy = GroupPolicy.from_settings(
        {
            "cpu_admission_reopen_percent": 30.0,
            "cpu_admission_close_percent": 50.0,
            "cpu_offload_percent": 75.0,
        }
    )
    assert policy.cpu_offload_percent == 75.0
    assert policy.to_settings()["cpu_offload_percent"] == 75.0

    with pytest.raises(GroupPolicyError) as caught:
        GroupPolicy.from_settings({"cpu_offload_percent": 75.0})
    assert "requires cpu_admission_close_percent" in caught.value.violations[0]

    for refused in (50.0, 40.0):
        with pytest.raises(GroupPolicyError) as caught:
            GroupPolicy.from_settings(
                {"cpu_admission_close_percent": 50.0, "cpu_offload_percent": refused}
            )
        assert "must sit above cpu_admission_close_percent" in caught.value.violations[0]

    with pytest.raises(GroupPolicyError) as caught:
        GroupPolicy.from_settings(
            {"cpu_admission_close_percent": 50.0, "cpu_offload_percent": 101.0}
        )
    assert "out of range" in caught.value.violations[0]
