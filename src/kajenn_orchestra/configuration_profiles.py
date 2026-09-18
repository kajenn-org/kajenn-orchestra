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

"""A mounted REST + MCP archive of named JSON orchestration profiles.

The application deliberately knows nothing about the orchestration runtime: it
only persists JSON objects in a directory.  Mounting it at ``_sysop`` exposes
the small browser page and REST API below ``/_sysop/configuration`` and the
same CRUD operations as MCP tools at ``/_sysop/mcp``.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

from genro_routes import RoutingClass, route

from kajenn.exceptions import HTTPBadRequest, HTTPNotFound
from .orchestration_profile_store import (
    OrchestrationProfileContentError,
    OrchestrationProfileNameError,
    OrchestrationProfileNotFoundError,
    OrchestrationProfileStore,
)
from kajenn.applications.mcp import McpOpenApiApplication

__all__ = ["ConfigurationProfiles", "ConfigurationProfilesApplication"]

RESOURCES_DIR = Path(__file__).parent / "resources"


class ConfigurationProfiles(RoutingClass):
    """REST and MCP surface backed by one directory of orchestration profiles."""

    openapi_info: ClassVar[dict[str, str]] = {
        "title": "Orchestration profiles",
        "version": "1.0.0",
        "description": "Read and write named orchestration profiles in a directory.",
    }

    def __init__(self, folder: str | Path) -> None:
        self.store = OrchestrationProfileStore(folder)
        self.folder = self.store.folder
        self.route.plug("channel")
        self.route.channel.configure(channels="rest")

    @route(media_type="text/html", channel_channels="rest")
    def index(self) -> str:
        """Serve the small profile editor."""
        return (RESOURCES_DIR / "configuration_profiles.html").read_text()

    @route(channel_channels="mcp,rest")
    def profiles(self) -> dict[str, Any]:
        """List the JSON profiles currently present in the configured folder."""
        result: list[dict[str, Any]] = []
        for path in sorted(self.folder.glob("*.json"), key=lambda item: item.name.lower()):
            if path.is_symlink() or not path.is_file():
                continue
            stat = path.stat()
            result.append(
                {
                    "name": path.stem,
                    "filename": path.name,
                    "size": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
                }
            )
        return {"folder": str(self.folder), "profiles": result}

    @route(channel_channels="mcp,rest")
    def read(self, name: str) -> dict[str, Any]:
        """Read one named JSON profile.

        Args:
            name: Profile name, with or without the ``.json`` suffix.
        """
        with self._http_errors():
            return {"name": self.store.get_profile_name(name), "profile": self.store.read(name)}

    @route(openapi_method="post", channel_channels="mcp,rest")
    def save(self, name: str, body_data: dict[str, Any] | None = None) -> dict[str, Any]:
        """Create or replace one named JSON profile atomically.

        Args:
            name: Profile name, with or without the ``.json`` suffix.
            body_data: The JSON object to persist.
        """
        with self._http_errors():
            profile_name = self.store.get_profile_name(name)
            path = self.store.write(name, body_data)
        return {"name": profile_name, "filename": path.name, "profile": body_data}

    @route(openapi_method="delete", channel_channels="mcp,rest")
    def delete(self, name: str) -> dict[str, Any]:
        """Delete one named JSON profile.

        Args:
            name: Profile name, with or without the ``.json`` suffix.
        """
        with self._http_errors():
            return {"name": self.store.delete(name), "deleted": True}

    @contextmanager
    def _http_errors(self) -> Iterator[None]:
        """Translate the store's errors into the archive's 404 and 400 answers."""
        try:
            yield
        except OrchestrationProfileNotFoundError as error:
            raise HTTPNotFound(str(error)) from error
        except (OrchestrationProfileNameError, OrchestrationProfileContentError) as error:
            raise HTTPBadRequest(str(error)) from error


class ConfigurationProfilesApplication(McpOpenApiApplication):
    """Ready-to-mount profile archive.

    Example::

        ConfigurationProfilesApplication(folder="/srv/site/data/_orchestration_profiles")

    Its defaults produce ``/_sysop/configuration`` and ``/_sysop/mcp``.
    Mounting is the gate: recipes that do not need remote profile editing must
    leave this application out.
    """

    code = "_sysop"
    mount = "_sysop"
    mcp_name = "kajenn-configuration-profiles"

    def __init__(self, folder: str | Path, **kwargs: Any) -> None:
        self.profile_store = ConfigurationProfiles(folder)
        super().__init__(
            routing_class=self.profile_store,
            api_name="configuration",
            **kwargs,
        )
