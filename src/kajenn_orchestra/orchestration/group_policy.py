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

"""The setpoints of one group, validated once and then immutable.

``GroupPolicy`` owns the whole schema of what a profile may say about a group:
the defaults (the same values the ``GroupHandler`` constructor carries), the
per-key ranges and the cross rules between keys.  ``from_settings`` IS the
validation — it accepts any subset of the keys, fills the rest with the
defaults and raises ``GroupPolicyError`` listing EVERY violation it found, so
an invalid policy object cannot exist.

``null`` in a profile means unlimited or off: ``worker_max_users`` and
``user_idle_freeze_minutes`` become ``math.inf`` inside the policy,
``cpu_admission_close_percent`` becomes ``None`` (CPU admission policy off),
``cpu_offload_percent`` becomes ``None`` (no user is ever offloaded for CPU;
set, it requires ``cpu_admission_close_percent`` and must sit above it) and
``worker_memory_max_percent`` becomes the derivation
``100 / worker_max_number``.  ``to_settings`` translates all of that back, so
its output always survives ``json.dumps(..., allow_nan=False)``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, ClassVar

__all__ = ["PROFILE_VERSION", "STRUCTURAL_KEYS", "GroupPolicy", "GroupPolicyError"]

#: The only profile format this code reads; an absent key means this version.
PROFILE_VERSION = 1

#: Keys of the group recipe that describe processes, not setpoints: they build
#: the group once and a profile may never carry them.
STRUCTURAL_KEYS = frozenset(
    {
        "entry_module",
        "executable",
        "worker_class",
        "worker_kwargs",
        "engine_factory",
        "engine_kwargs",
        "main_threadpool_size",
        "aux_threadpool_size",
    }
)


class GroupPolicyError(ValueError):
    """Settings rejected, with the complete list of what is wrong with them."""

    def __init__(self, violations: list[str]) -> None:
        self.violations = list(violations)
        super().__init__("; ".join(self.violations))


@dataclass(frozen=True)
class GroupPolicy:
    """One group's setpoints, complete and valid by construction."""

    #: key -> (integer, nullable, low bound, low bound exclusive, high bound)
    SETPOINTS: ClassVar[dict[str, tuple[bool, bool, float, bool, float | None]]] = {
        "worker_memory_admission_percent": (False, False, 0.0, False, 100.0),
        "restart_occupancy_max_percent": (False, False, 0.0, False, 100.0),
        "cpu_close_percent": (False, True, 0.0, False, 100.0),
        "cpu_admission_close_percent": (False, True, 0.0, False, 100.0),
        "cpu_admission_reopen_percent": (False, False, 0.0, False, 100.0),
        "cpu_offload_percent": (False, True, 0.0, False, 100.0),
        "cpu_retirement_quiet_seconds": (False, False, 0.0, False, None),
        "cpu_heating_seconds": (False, False, 0.0, True, None),
        "cpu_cooling_seconds": (False, False, 0.0, True, None),
        "worker_admission_interval_seconds": (False, False, 0.0, False, None),
        "worker_min_life_seconds": (False, False, 0.0, False, None),
        "worker_max_users": (True, True, 1, False, None),
        "user_idle_freeze_minutes": (False, True, 0.0, True, None),
        "memory_max_percent": (False, False, 0.0, True, 100.0),
        "worker_max_number": (True, False, 1, False, None),
        "worker_memory_max_percent": (False, True, 0.0, True, None),
    }

    worker_memory_admission_percent: float = 80.0
    restart_occupancy_max_percent: float = 95.0
    cpu_close_percent: float | None = None
    cpu_admission_close_percent: float | None = None
    cpu_admission_reopen_percent: float = 40.0
    cpu_offload_percent: float | None = None
    cpu_retirement_quiet_seconds: float = 60.0
    cpu_heating_seconds: float = 1.0
    cpu_cooling_seconds: float = 5.0
    worker_admission_interval_seconds: float = 1.0
    worker_min_life_seconds: float = 60.0
    worker_max_users: float = math.inf
    user_idle_freeze_minutes: float = math.inf
    memory_max_percent: float = 100.0
    worker_max_number: int = 6
    #: What the profile said explicitly; None leaves the derivation in charge.
    worker_memory_max_percent_explicit: float | None = None

    @property
    def worker_memory_max_percent(self) -> float:
        """What ONE worker may hold: the explicit value, else the derivation."""
        if self.worker_memory_max_percent_explicit is not None:
            return self.worker_memory_max_percent_explicit
        return 100.0 / self.worker_max_number

    def to_settings(self) -> dict[str, Any]:
        """The setpoints as a profile writes them: inf back to null, JSON-safe."""
        settings = {key: getattr(self, key) for key in self.SETPOINTS}
        settings["worker_memory_max_percent"] = self.worker_memory_max_percent_explicit
        for key in ("worker_max_users", "user_idle_freeze_minutes"):
            if settings[key] == math.inf:
                settings[key] = None
        return settings

    @classmethod
    def from_settings(cls, settings: dict[str, Any]) -> GroupPolicy:
        """Validate any subset of the setpoints and materialize a complete policy.

        Raises GroupPolicyError carrying every violation; never returns a
        partially valid policy.
        """
        violations: list[str] = []
        values: dict[str, Any] = {}
        for key, value in settings.items():
            if key == "profile_version":
                if isinstance(value, bool) or value != PROFILE_VERSION:
                    violations.append(
                        f"profile_version: only version {PROFILE_VERSION} is supported, "
                        f"got {value!r}"
                    )
                continue
            if key in STRUCTURAL_KEYS:
                violations.append(f"{key}: structural, not a profile key")
                continue
            if key not in cls.SETPOINTS:
                violations.append(f"{key}: unknown setpoint")
                continue
            cls._check_value(key, value, values, violations)
        for key in ("worker_max_users", "user_idle_freeze_minutes"):
            if values.get(key, math.inf) is None:
                values[key] = math.inf
        if "worker_memory_max_percent" in values:
            values["worker_memory_max_percent_explicit"] = values.pop("worker_memory_max_percent")
        if violations:
            raise GroupPolicyError(violations)
        policy = cls(**values)
        policy._check_cross_rules(violations)
        if violations:
            raise GroupPolicyError(violations)
        return policy

    @classmethod
    def _check_value(
        cls,
        key: str,
        value: Any,
        values: dict[str, Any],
        violations: list[str],
    ) -> None:
        """Type, finiteness and range of one setpoint; records it or a violation."""
        integer, nullable, low, low_exclusive, high = cls.SETPOINTS[key]
        if value is None:
            if nullable:
                values[key] = None
            else:
                violations.append(f"{key}: null is not allowed")
            return
        if isinstance(value, bool) or not isinstance(value, int if integer else (int, float)):
            wanted = "an integer" if integer else "a number"
            violations.append(f"{key}: expected {wanted}, got {type(value).__name__}")
            return
        if not math.isfinite(value):
            violations.append(f"{key}: must be a finite number, got {value}")
            return
        below = value <= low if low_exclusive else value < low
        if below or (high is not None and value > high):
            bound = f"{'>' if low_exclusive else '>='} {low}"
            if high is not None:
                bound += f" and <= {high}"
            violations.append(f"{key}: {value} is out of range, must be {bound}")
            return
        values[key] = value

    def _check_cross_rules(self, violations: list[str]) -> None:
        """The rules that only the complete policy can answer."""
        if self.worker_memory_admission_percent >= self.restart_occupancy_max_percent:
            violations.append(
                f"worker_memory_admission_percent ({self.worker_memory_admission_percent}) must stay below "
                f"restart_occupancy_max_percent ({self.restart_occupancy_max_percent})"
            )
        if self.cpu_admission_close_percent is not None and not (
            0.0 <= self.cpu_admission_reopen_percent < self.cpu_admission_close_percent <= 100.0
        ):
            violations.append(
                f"cpu_admission_reopen_percent ({self.cpu_admission_reopen_percent}) "
                f"must sit below cpu_admission_close_percent "
                f"({self.cpu_admission_close_percent}), both inside 0-100"
            )
        if (
            self.cpu_close_percent is not None
            and self.cpu_admission_close_percent is not None
            and self.cpu_close_percent > self.cpu_admission_reopen_percent
        ):
            violations.append(
                f"cpu_close_percent ({self.cpu_close_percent}) must not exceed "
                f"cpu_admission_reopen_percent ({self.cpu_admission_reopen_percent}): "
                "a closure must never reopen the growth cycle"
            )
        if self.cpu_offload_percent is not None:
            if self.cpu_admission_close_percent is None:
                violations.append(
                    f"cpu_offload_percent ({self.cpu_offload_percent}) requires "
                    "cpu_admission_close_percent: the offload stands on the CPU admission closure"
                )
            elif not (self.cpu_admission_close_percent < self.cpu_offload_percent <= 100.0):
                violations.append(
                    f"cpu_offload_percent ({self.cpu_offload_percent}) must sit above "
                    f"cpu_admission_close_percent ({self.cpu_admission_close_percent}), "
                    "inside 0-100"
                )
