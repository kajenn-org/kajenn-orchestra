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

"""The reception is a worker like any other: no reserve, no cap of its own.

Contract tests. ``reception_reserved_percent`` is an unknown setpoint,
``GroupHandler.get_worker_cap`` is gone, and the reception admits under the
same ``worker_memory_admission_percent`` as every other worker.
"""

from __future__ import annotations

import pytest

from kajenn_orchestra.orchestration.group_handler import GroupHandler
from kajenn_orchestra.orchestration.group_policy import GroupPolicy, GroupPolicyError

from .test_orchestration_cpu_growth import arrival
from .test_orchestration_group_handler import commander  # noqa: F401
from .test_orchestration_group_handler import group_settings  # noqa: F401
from .test_orchestration_group_handler import instance_root  # noqa: F401
from .test_orchestration_group_handler import make_group  # noqa: F401


def test_the_reception_reserve_is_gone():
    assert "reception_reserved_percent" not in GroupPolicy.SETPOINTS
    with pytest.raises(GroupPolicyError) as refusal:
        GroupPolicy.from_settings({"reception_reserved_percent": 0.0})
    assert "reception_reserved_percent: unknown setpoint" in refusal.value.violations
    assert not hasattr(GroupHandler, "get_worker_cap")


async def test_the_reception_admits_up_to_the_common_cap(make_group, commander):
    # Under the memory veto every one of them fits ONE worker. With the old
    # reserve of 50 the reception stopped at six and the seventh was born a
    # worker of his own.
    group = make_group(worker_memory_admission_percent=80.0)
    await group.start_worker()

    for index in range(7):
        await arrival(commander, group, f"user_{index}")

    assert len(group.worker_handler_map) == 1
    assert set(group.user_worker_map.values()) == {group.reception.name}
