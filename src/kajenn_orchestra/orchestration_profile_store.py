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

"""One directory of named JSON orchestration profiles, read and written safely.

The module is neutral: it imports nothing from ``applications/`` nor from the
``kajenn_orchestra`` package, so both the mounted profile archive and
the spa application read through the same component.  It owns the whole delicate part of that storage — name
validation, path resolution, symlink refusal, the 1 MiB limit in both
directions, object-only JSON with non-finite literals rejected at read time,
and the atomic write — and raises its own ``ValueError`` subclasses, which the
callers translate into HTTP statuses or apply violations.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

__all__ = [
    "MAX_ORCHESTRATION_PROFILE_BYTES",
    "ORCHESTRATION_PROFILE_NAME",
    "OrchestrationProfileContentError",
    "OrchestrationProfileNameError",
    "OrchestrationProfileNotFoundError",
    "OrchestrationProfileStore",
]

ORCHESTRATION_PROFILE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
MAX_ORCHESTRATION_PROFILE_BYTES = 1024 * 1024


class OrchestrationProfileNameError(ValueError):
    """The requested profile name is not a legal profile name."""


class OrchestrationProfileNotFoundError(ValueError):
    """No profile with that name exists in the folder."""


class OrchestrationProfileContentError(ValueError):
    """The profile file cannot be used: symlink, oversize or not a JSON object."""


class OrchestrationProfileStore:
    """The JSON profiles stored in one directory.

    Args:
        folder: Directory holding the ``<name>.json`` files; created if absent.
    """

    def __init__(self, folder: str | Path) -> None:
        self.folder = Path(folder).expanduser().resolve()
        self.folder.mkdir(parents=True, exist_ok=True)

    def get_profile_name(self, name: str) -> str:
        """Return the validated profile name, with any ``.json`` suffix removed.

        Args:
            name: Profile name, with or without the ``.json`` suffix.

        Raises:
            OrchestrationProfileNameError: The name does not match
                ``ORCHESTRATION_PROFILE_NAME``.
        """
        profile_name = name.removesuffix(".json")
        if not ORCHESTRATION_PROFILE_NAME.fullmatch(profile_name):
            raise OrchestrationProfileNameError(
                "profile names must be 1-64 characters: letters, digits, dot, dash or underscore"
            )
        return profile_name

    def get_profile_path(self, name: str) -> Path:
        """Return the file path of one named profile inside the folder.

        Args:
            name: Profile name, with or without the ``.json`` suffix.
        """
        return self.folder / f"{self.get_profile_name(name)}.json"

    def read(self, name: str) -> dict[str, Any]:
        """Return the stored JSON object of one named profile.

        Args:
            name: Profile name, with or without the ``.json`` suffix.

        Raises:
            OrchestrationProfileNotFoundError: The profile does not exist.
            OrchestrationProfileContentError: Symlink, oversize, unreadable,
                malformed JSON, a non-finite literal, or a top level that is
                not an object.
        """
        profile_name = self.get_profile_name(name)
        path = self.folder / f"{profile_name}.json"
        self._require_regular_file(profile_name, path)
        if path.stat().st_size > MAX_ORCHESTRATION_PROFILE_BYTES:
            raise OrchestrationProfileContentError(
                f"profile {profile_name!r} exceeds the size limit"
            )
        try:
            profile = json.loads(
                path.read_text(encoding="utf-8"), parse_constant=self._reject_nonfinite
            )
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise OrchestrationProfileContentError(
                f"profile {profile_name!r} is not valid JSON"
            ) from exc
        if not isinstance(profile, dict):
            raise OrchestrationProfileContentError(
                f"profile {profile_name!r} must contain a JSON object"
            )
        return profile

    def write(self, name: str, profile: dict[str, Any]) -> Path:
        """Serialize one profile and replace its file atomically.

        Args:
            name: Profile name, with or without the ``.json`` suffix.
            profile: The JSON object to persist.

        Returns:
            The path now holding the profile.

        Raises:
            OrchestrationProfileContentError: Not a JSON object, a symlink
                target, oversize, or a value ``json.dumps`` refuses
                (``allow_nan=False``).
        """
        profile_name = self.get_profile_name(name)
        path = self.folder / f"{profile_name}.json"
        if not isinstance(profile, dict):
            raise OrchestrationProfileContentError("the profile body must be a JSON object")
        if path.is_symlink():
            raise OrchestrationProfileContentError(
                f"profile {profile_name!r} cannot be a symbolic link"
            )
        try:
            payload = json.dumps(
                profile, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False
            )
        except (TypeError, ValueError) as exc:
            raise OrchestrationProfileContentError(
                f"profile {profile_name!r} is not serializable: {exc}"
            ) from exc
        encoded = (payload + "\n").encode("utf-8")
        if len(encoded) > MAX_ORCHESTRATION_PROFILE_BYTES:
            raise OrchestrationProfileContentError("the profile exceeds the size limit")

        descriptor, temporary_name = tempfile.mkstemp(
            dir=self.folder, prefix=f".{profile_name}.", suffix=".tmp"
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        return path

    def delete(self, name: str) -> str:
        """Remove one named profile and return its validated name.

        Args:
            name: Profile name, with or without the ``.json`` suffix.

        Raises:
            OrchestrationProfileNotFoundError: The profile does not exist.
            OrchestrationProfileContentError: The path is a symbolic link.
        """
        profile_name = self.get_profile_name(name)
        path = self.folder / f"{profile_name}.json"
        self._require_regular_file(profile_name, path)
        path.unlink()
        return profile_name

    def _require_regular_file(self, profile_name: str, path: Path) -> None:
        if path.is_symlink():
            raise OrchestrationProfileContentError("symbolic-link profiles are not allowed")
        if not path.is_file():
            raise OrchestrationProfileNotFoundError(f"profile {profile_name!r} not found")

    def _reject_nonfinite(self, literal: str) -> Any:
        raise OrchestrationProfileContentError(f"the literal {literal} is not allowed in a profile")
