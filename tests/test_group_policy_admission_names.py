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

"""The CPU admission thresholds are named for what they do: close and reopen.

Contract tests. The setpoints ``cpu_admission_close_percent`` and
``cpu_admission_reopen_percent`` replace ``cpu_grow_percent`` and
``cpu_grow_rearm_percent``; the old names are unknown, with no alias.
"""

from __future__ import annotations

import pytest

from kajenn_orchestra.orchestration.group_handler import GroupHandler
from kajenn_orchestra.orchestration.group_policy import GroupPolicy, GroupPolicyError


def test_the_new_names_are_setpoints_with_the_old_defaults():
    policy = GroupPolicy.from_settings({})

    assert policy.cpu_admission_close_percent is None
    assert policy.cpu_admission_reopen_percent == 40.0


def test_the_new_names_carry_the_hysteresis_rule():
    policy = GroupPolicy.from_settings(
        {"cpu_admission_close_percent": 50.0, "cpu_admission_reopen_percent": 30.0}
    )
    assert policy.cpu_admission_close_percent == 50.0
    assert policy.cpu_admission_reopen_percent == 30.0

    with pytest.raises(GroupPolicyError) as refusal:
        GroupPolicy.from_settings(
            {"cpu_admission_close_percent": 30.0, "cpu_admission_reopen_percent": 50.0}
        )
    assert "cpu_admission_reopen_percent" in str(refusal.value)


def test_the_old_names_are_unknown_setpoints():
    with pytest.raises(GroupPolicyError) as refusal:
        GroupPolicy.from_settings({"cpu_grow_percent": 50.0, "cpu_grow_rearm_percent": 30.0})

    violations = refusal.value.violations
    assert "cpu_grow_percent: unknown setpoint" in violations
    assert "cpu_grow_rearm_percent: unknown setpoint" in violations


def test_the_offload_rule_names_the_close_threshold():
    with pytest.raises(GroupPolicyError) as refusal:
        GroupPolicy.from_settings({"cpu_offload_percent": 75.0})

    assert "cpu_admission_close_percent" in str(refusal.value)
    assert "cpu_grow" not in str(refusal.value)


def test_the_group_exposes_the_new_names_and_not_the_old():
    assert isinstance(GroupHandler.__dict__["cpu_admission_close_percent"], property)
    assert isinstance(GroupHandler.__dict__["cpu_admission_reopen_percent"], property)
    assert "cpu_grow_percent" not in GroupHandler.__dict__
    assert "cpu_grow_rearm_percent" not in GroupHandler.__dict__
