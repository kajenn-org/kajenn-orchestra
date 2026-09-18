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

"""Shared ASGI-level test helpers, promoted from ``test_middleware_std.py``.

Every helper is a fixture returning a callable, so any test file can drive a
request through a composed server's ``__call__`` and read the recorded
messages without re-declaring the boilerplate: ``http_request`` runs one
request and returns the ``send`` messages; ``response_status`` /
``response_headers`` / ``response_body`` read that message list.

Beside those fixtures live ``LifespanRunner``, ``ask_app`` and ``get_answer_header``,
three plain callables: a front backed by a pool is driven INSIDE its own lifespan
scope, and its requests carry a cookie, a query string and a client address that
the fixtures above do not pack. The two mechanisms coexist on purpose — the
fixtures have a dozen consumers among the contract tests, and unifying them is
its own task, never smuggled into another change.

``kajenn_home`` is autouse: no test ever reads the developer's real
``~/.kajenn``, whose ``config.py`` would otherwise layer itself under every
``AsgiServer(config=...)`` built here.
"""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from typing import Any, Callable

import pytest
from genro_toolbox.smartasync import set_sync

from kajenn.config import HOME_ENV
from kajenn.types import Message, Scope

# D22 (core 1b): the server's storage API is SYNCHRONOUS — StorageMixin pins
# ``set_sync()`` at construction and every task inherits it. The suite runs
# under the same dispatch the server runs under, so a storage node answers
# with a VALUE here exactly as it does in production.
set_sync()


@pytest.fixture(autouse=True)
def kajenn_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Autouse: point ``KAJENN_HOME`` at an empty per-test directory.

    A test that WANTS a defaults recipe writes its own ``config.py`` in the
    returned directory; every other test gets a home with nothing in it.
    """
    home = tmp_path / "kajenn_home"
    home.mkdir()
    monkeypatch.setenv(HOME_ENV, str(home))
    return home


@pytest.fixture
def http_request() -> Callable[..., object]:
    """Fixture: drive one request through a server at the ASGI level."""

    async def _http_request(
        server: object,
        path: str = "/",
        method: str = "GET",
        headers: list[tuple[bytes, bytes]] | None = None,
    ) -> list[Message]:
        scope: Scope = {"type": "http", "method": method, "path": path, "headers": headers or []}
        sent: list[Message] = []

        async def receive() -> Message:
            return {"type": "http.request"}

        async def send(message: Message) -> None:
            sent.append(message)

        await server(scope, receive, send)  # type: ignore[operator]
        return sent

    return _http_request


@pytest.fixture
def response_status() -> Callable[[list[Message]], int]:
    """Fixture: the status of the ``http.response.start`` message."""

    def _response_status(sent: list[Message]) -> int:
        return next(m["status"] for m in sent if m["type"] == "http.response.start")

    return _response_status


@pytest.fixture
def response_headers() -> Callable[[list[Message]], dict[bytes, bytes]]:
    """Fixture: the ``http.response.start`` headers as a byte-keyed dict."""

    def _response_headers(sent: list[Message]) -> dict[bytes, bytes]:
        start = next(m for m in sent if m["type"] == "http.response.start")
        return dict(start["headers"])

    return _response_headers


@pytest.fixture
def response_body() -> Callable[[list[Message]], bytes]:
    """Fixture: the concatenated ``http.response.body`` bytes."""

    def _response_body(sent: list[Message]) -> bytes:
        return b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")

    return _response_body


class SseConnection:
    """A live SSE request: the response task plus the messages sent so far.

    ``sent[0]`` is the ``http.response.start``; each following message is one
    ``more_body`` chunk. The stream never ends on its own (the hub feed is
    infinite), so tests await the frames they expect and then ``close()`` —
    cancelling the response task, which unwinds the stream's ``finally``
    (hub unsubscribe).
    """

    def __init__(self, task: "asyncio.Task[None]", sent: list[Message]) -> None:
        self.task = task
        self.sent = sent

    def frames(self) -> list[bytes]:
        """The non-empty body chunks received so far (SSE wire records)."""
        return [
            m["body"]
            for m in self.sent
            if m["type"] == "http.response.body" and m.get("body")
        ]

    async def wait_frames(self, count: int, timeout: float = 2.0) -> list[bytes]:
        """Wait until ``count`` frames arrived (or fail the test on timeout)."""
        deadline = asyncio.get_running_loop().time() + timeout
        while len(self.frames()) < count:
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(
                    f"expected {count} SSE frames, got {len(self.frames())}: {self.frames()!r}"
                )
            await asyncio.sleep(0.01)
        return self.frames()

    async def close(self) -> None:
        """Cancel the response task (client gone) and swallow the cancellation."""
        self.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.task


@pytest.fixture
def sse_request() -> Callable[..., object]:
    """Fixture: open a GET request that stays live and collects SSE frames.

    ``receive`` never resolves after the initial ``http.request`` — the
    connection stays open the way a real SSE client holds it.
    """

    async def _sse_request(
        server: object,
        path: str = "/",
        headers: list[tuple[bytes, bytes]] | None = None,
    ) -> SseConnection:
        scope: Scope = {
            "type": "http",
            "method": "GET",
            "path": path,
            "query_string": b"",
            "headers": list(headers or []),
        }
        sent: list[Message] = []
        started = asyncio.Event()

        async def receive() -> Message:
            if not started.is_set():
                started.set()
                return {"type": "http.request", "body": b"", "more_body": False}
            await asyncio.Event().wait()        # hold the connection open forever
            raise AssertionError("unreachable")

        async def send(message: Message) -> None:
            sent.append(message)

        task = asyncio.get_running_loop().create_task(server(scope, receive, send))  # type: ignore[operator]
        # wait for the http.response.start so callers can read headers at once
        deadline = asyncio.get_running_loop().time() + 2.0
        while not sent:
            if task.done():
                task.result()                   # surface the failure
                raise AssertionError("stream ended before http.response.start")
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError("no http.response.start within 2s")
            await asyncio.sleep(0.01)
        return SseConnection(task, sent)

    return _sse_request


class LifespanRunner:
    """The lifespan, driven the way a real ASGI runner drives it.

    The protocol is a conversation and not two calls: the server stays inside its
    lifespan scope between the startup and the shutdown, which is exactly the span
    a test wants to look at.
    """

    def __init__(self, server: Any) -> None:
        self.server = server
        self.sent: list[dict[str, Any]] = []
        self._inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._living: asyncio.Task[None] | None = None

    async def startup(self) -> None:
        self._living = asyncio.ensure_future(
            self.server({"type": "lifespan"}, self._inbox.get, self._send)
        )
        await self._inbox.put({"type": "lifespan.startup"})
        await self._answered("lifespan.startup.")

    async def shutdown(self) -> None:
        await self._inbox.put({"type": "lifespan.shutdown"})
        await self._living

    async def _send(self, message: dict[str, Any]) -> None:
        self.sent.append(message)

    async def _answered(self, prefix: str) -> None:
        while not any(message["type"].startswith(prefix) for message in self.sent):
            await asyncio.sleep(0)


async def ask_app(
    app: Any, path: str, cookies: dict[str, str] | None = None
) -> dict[str, Any]:
    """One ASGI request, answered: the status, the headers and the body.

    Args:
        app: the server or application to drive.
        path: the path of the request.
        cookies: what the browser carries, as name/value pairs; none by default.

    Returns:
        The answer as ``status`` / ``headers`` (decoded pairs) / ``body``.
    """
    headers = [(b"host", b"site.example")]
    if cookies:
        jar = "; ".join(f"{name}={value}" for name, value in cookies.items())
        headers.append((b"cookie", jar.encode()))
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": b"",
        "headers": headers,
        "scheme": "http",
        "client": ("10.0.0.1", 5555),
    }
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await app(scope, receive, send)
    start = next(message for message in sent if message["type"] == "http.response.start")
    body = b"".join(
        message.get("body", b"") for message in sent if message["type"] == "http.response.body"
    )
    return {
        "status": start["status"],
        "headers": [(name.decode(), value.decode()) for name, value in start["headers"]],
        "body": body,
    }


def get_answer_header(answer: dict[str, Any], name: str) -> str | None:
    """One header of an answer, by name.

    Args:
        answer: what ``ask_app`` returned.
        name: the header to read, lowercase.

    Returns:
        Its value, or None when the answer carries no such header.
    """
    return next((value for key, value in answer["headers"] if key.lower() == name), None)
