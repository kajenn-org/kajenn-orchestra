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

"""Contract tests for the mounted JSON configuration-profile archive."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

import pytest

from kajenn import (
    AsgiServer,
    RoutedApplication,
)
from kajenn.types import Message, Scope
from kajenn_orchestra import ConfigurationProfilesApplication


class Empty(RoutedApplication):
    """Do-nothing primary application for the mounted sysop app."""


@pytest.fixture
def drive() -> Callable[..., object]:
    """Drive a request with query string and body through a real server."""

    async def _drive(
        server: object,
        path: str,
        *,
        method: str = "GET",
        query: bytes = b"",
        headers: list[tuple[bytes, bytes]] | None = None,
        body: bytes = b"",
    ) -> list[Message]:
        scope: Scope = {
            "type": "http",
            "method": method,
            "path": path,
            "query_string": query,
            "headers": list(headers or []),
        }
        sent: list[Message] = []

        async def receive() -> Message:
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message: Message) -> None:
            sent.append(message)

        await server(scope, receive, send)  # type: ignore[operator]
        return sent

    return _drive


def profile_server(folder: Path) -> AsgiServer:
    return AsgiServer(
        applications=[
            (Empty, {"mount": ""}),
            (ConfigurationProfilesApplication, {"folder": folder}),
        ],
        plugins={"openapi": True},
    )


async def test_page_and_rest_crud(
    tmp_path, drive, response_status, response_body
) -> None:
    folder = tmp_path / "profiles"
    server = profile_server(folder)

    page = await drive(server, "/_sysop/configuration/")
    assert response_status(page) == 200
    assert b"Orchestration profiles" in response_body(page)

    saved = await drive(
        server,
        "/_sysop/configuration/save",
        method="POST",
        query=b"name=fast",
        headers=[(b"content-type", b"application/json")],
        body=b'{"cpu_admission_close_percent": 50}',
    )
    assert response_status(saved) == 200
    assert json.loads(response_body(saved))["name"] == "fast"
    assert json.loads((folder / "fast.json").read_text()) == {"cpu_admission_close_percent": 50}

    listing = await drive(server, "/_sysop/configuration/profiles")
    assert [item["name"] for item in json.loads(response_body(listing))["profiles"]] == [
        "fast"
    ]

    read = await drive(server, "/_sysop/configuration/read", query=b"name=fast.json")
    assert json.loads(response_body(read))["profile"] == {"cpu_admission_close_percent": 50}

    deleted = await drive(
        server,
        "/_sysop/configuration/delete",
        method="DELETE",
        query=b"name=fast",
    )
    assert json.loads(response_body(deleted)) == {"name": "fast", "deleted": True}
    assert not (folder / "fast.json").exists()


async def test_rejects_path_traversal_and_non_object_body(
    tmp_path, drive, response_status
) -> None:
    server = profile_server(tmp_path / "profiles")
    traversal = await drive(
        server,
        "/_sysop/configuration/save",
        method="POST",
        query=b"name=..%2Fevil",
        headers=[(b"content-type", b"application/json")],
        body=b"{}",
    )
    assert response_status(traversal) == 400

    array = await drive(
        server,
        "/_sysop/configuration/save",
        method="POST",
        query=b"name=wrong",
        headers=[(b"content-type", b"application/json")],
        body=b"[]",
    )
    assert response_status(array) == 400


async def test_an_empty_folder_lists_no_profiles(tmp_path, drive, response_body) -> None:
    server = profile_server(tmp_path / "profiles")
    listing = await drive(server, "/_sysop/configuration/profiles")
    assert json.loads(response_body(listing))["profiles"] == []


async def test_save_overwrites_atomically_and_leaves_no_temporaries(
    tmp_path, drive, response_body
) -> None:
    folder = tmp_path / "profiles"
    server = profile_server(folder)
    for payload in (b'{"cpu_admission_close_percent": 50}', b'{"cpu_admission_close_percent": 70}'):
        await drive(
            server,
            "/_sysop/configuration/save",
            method="POST",
            query=b"name=fast",
            headers=[(b"content-type", b"application/json")],
            body=payload,
        )
    read = await drive(server, "/_sysop/configuration/read", query=b"name=fast")
    assert json.loads(response_body(read))["profile"] == {"cpu_admission_close_percent": 70}
    assert [path.name for path in folder.iterdir()] == ["fast.json"]


async def test_a_missing_profile_answers_404(tmp_path, drive, response_status) -> None:
    server = profile_server(tmp_path / "profiles")
    read = await drive(server, "/_sysop/configuration/read", query=b"name=ghost")
    assert response_status(read) == 404
    deleted = await drive(
        server, "/_sysop/configuration/delete", method="DELETE", query=b"name=ghost"
    )
    assert response_status(deleted) == 404


async def test_invalid_names_answer_400(tmp_path, drive, response_status) -> None:
    server = profile_server(tmp_path / "profiles")
    for name in (b"name=", b"name=.hidden", b"name=a%2Fb", b"name=" + b"x" * 65):
        read = await drive(server, "/_sysop/configuration/read", query=name)
        assert response_status(read) == 400


async def test_a_corrupted_file_is_listed_but_refuses_to_read(
    tmp_path, drive, response_status, response_body
) -> None:
    folder = tmp_path / "profiles"
    folder.mkdir()
    (folder / "broken.json").write_text("{not json")
    server = profile_server(folder)
    listing = await drive(server, "/_sysop/configuration/profiles")
    assert [item["name"] for item in json.loads(response_body(listing))["profiles"]] == [
        "broken"
    ]
    read = await drive(server, "/_sysop/configuration/read", query=b"name=broken")
    assert response_status(read) == 400


async def test_symlinks_are_skipped_and_refused(
    tmp_path, drive, response_status, response_body
) -> None:
    folder = tmp_path / "profiles"
    folder.mkdir()
    target = tmp_path / "outside.json"
    target.write_text('{"secret": true}')
    (folder / "link.json").symlink_to(target)
    server = profile_server(folder)
    listing = await drive(server, "/_sysop/configuration/profiles")
    assert json.loads(response_body(listing))["profiles"] == []
    read = await drive(server, "/_sysop/configuration/read", query=b"name=link")
    assert response_status(read) == 400
    saved = await drive(
        server,
        "/_sysop/configuration/save",
        method="POST",
        query=b"name=link",
        headers=[(b"content-type", b"application/json")],
        body=b"{}",
    )
    assert response_status(saved) == 400
    assert target.read_text() == '{"secret": true}'


async def test_the_size_limit_gates_both_directions(
    tmp_path, drive, response_status
) -> None:
    folder = tmp_path / "profiles"
    folder.mkdir()
    server = profile_server(folder)
    big = json.dumps({"blob": "x" * (1024 * 1024)}).encode()
    saved = await drive(
        server,
        "/_sysop/configuration/save",
        method="POST",
        query=b"name=big",
        headers=[(b"content-type", b"application/json")],
        body=big,
    )
    assert response_status(saved) == 400
    (folder / "fat.json").write_text('{"pad": "%s"}' % ("y" * (1024 * 1024)))
    read = await drive(server, "/_sysop/configuration/read", query=b"name=fat")
    assert response_status(read) == 400


async def test_a_relative_folder_resolves_against_the_cwd(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    app = ConfigurationProfilesApplication(folder="profiles")
    assert app.profile_store.folder == (tmp_path / "profiles").resolve()
    assert app.profile_store.folder.is_dir()


async def test_mcp_lists_and_calls_profile_tools(
    tmp_path, drive, response_status, response_body
) -> None:
    server = profile_server(tmp_path / "profiles")

    async def mcp(envelope: dict[str, Any]) -> dict[str, Any]:
        sent = await drive(
            server,
            "/_sysop/mcp",
            method="POST",
            headers=[(b"content-type", b"application/json")],
            body=json.dumps(envelope).encode(),
        )
        assert response_status(sent) == 200
        return json.loads(response_body(sent))

    listed = await mcp({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert {tool["name"] for tool in listed["result"]["tools"]} == {
        "delete",
        "profiles",
        "read",
        "save",
    }

    saved = await mcp(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "save",
                "arguments": {"name": "cpu50", "body_data": {"cpu_admission_close_percent": 50}},
            },
        }
    )
    assert saved["result"]["structuredContent"]["name"] == "cpu50"

    read = await mcp(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "read", "arguments": {"name": "cpu50"}},
        }
    )
    assert read["result"]["structuredContent"]["profile"] == {
        "cpu_admission_close_percent": 50
    }

    deleted = await mcp(
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {"name": "delete", "arguments": {"name": "cpu50"}},
        }
    )
    assert deleted["result"]["structuredContent"] == {"name": "cpu50", "deleted": True}
    assert not (tmp_path / "profiles" / "cpu50.json").exists()
