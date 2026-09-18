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

"""The hosted application's ASGI and WSGI endpoint adapters.

AsgiSeam reconstructs the scope from the HTTP record opened by the worker,
adds trusted genro.identity/page_id/reply_path, and delegates to the neutral
BufferedAsgiEndpoint. Bodies are bytes in both directions. The original SPA
second-read disconnect and finite response-chunk behavior remain available.

WsgiSeam wraps a synchronous application as ASGI and runs it through the
worker's traffic pool, preserving the active request slot. It translates
root_path/path to SCRIPT_NAME/PATH_INFO and preserves duplicate headers and
cookies using the WSGI joining rules. No Python session or Avatar crosses the
transport; the worker receives only explicitly defined routing context.
"""

from __future__ import annotations

import io
import sys
from typing import Any, Callable, Iterable

from kajenn.asgi_endpoint import BufferedAsgiEndpoint

__all__ = ["AsgiSeam", "WsgiSeam"]

# The two headers PEP 3333 keeps out of the HTTP_ namespace.
UNPREFIXED_HEADERS = {"content-type": "CONTENT_TYPE", "content-length": "CONTENT_LENGTH"}

# What travels from the CALL's dict into the hosted code, when it is there.
CALL_KEYS = ("genro.page_id", "genro.reply_path")


class AsgiSeam:
    """One ASGI application, called from the facts of a CALL."""

    def __init__(self, asgi_app: Any) -> None:
        """Initialize this instance.

        Args:
            asgi_app: the application to call, ``(scope, receive, send)``.
        """
        self.asgi_app = asgi_app

    def build_scope(self, http: dict[str, Any], identity: str | None = None) -> dict[str, Any]:
        """The ASGI scope for one ``http`` dict.

        Args:
            http: the facts the front packed.
            identity: the CALL's identity, ``None`` when the caller named none.

        Returns:
            An ``http`` scope. ``root_path`` is empty: the path the front
            forwards is already mount-relative, so the whole of it is ``path``.
            ``server`` comes from the Host header, the only place the front's
            own address survives the packing.
        """
        headers = [
            (str(name).encode("latin-1"), str(value).encode("latin-1"))
            for name, value in http.get("headers") or []
        ]
        host, port = self.server_address(
            next((v.decode("latin-1") for n, v in headers if n.lower() == b"host"), "")
        )
        client = http.get("client") or []
        scope: dict[str, Any] = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": http.get("http_version", "1.1"),
            "method": http.get("method", "GET"),
            "scheme": http.get("scheme") or "http",
            "path": http.get("path", "/"),
            "raw_path": http.get("raw_path", str(http.get("path", "/")).encode("utf-8")),
            "root_path": http.get("root_path", ""),
            "query_string": str(http.get("query_string", "")).encode("latin-1"),
            "headers": headers,
            "server": tuple(http["server"]) if http.get("server") else (host, int(port)),
            "client": (str(client[0]), int(client[1])) if len(client) > 1 else None,
            "genro.identity": identity,
        }
        for key in CALL_KEYS:
            if http.get(key.split(".")[1]) is not None:
                scope[key] = http[key.split(".")[1]]
        return scope

    def server_address(self, host_header: str) -> tuple[str, str]:
        """Split a Host header into ``(name, port)``."""
        if not host_header:
            return "localhost", "80"
        host, _, port = host_header.partition(":")
        return host, port or "80"

    async def serve(self, http: dict[str, Any], identity: str | None = None) -> dict[str, Any]:
        """Call the application on one ``http`` dict and shape its reply.

        Args:
            http: the facts the front packed.
            identity: the CALL's identity.

        Returns:
            ``{"status", "headers", "body"}``, the body raw bytes — the same shape
            the WSGI road produced before this seam existed.

        Raises:
            RuntimeError: the application answered nothing.
        """
        scope = self.build_scope(http, identity)
        return await BufferedAsgiEndpoint(self.asgi_app, reject_streaming=False).serve(
            scope, http.get("body") or b""
        )


class WsgiSeam:
    """One WSGI callable, reached as an ASGI application.

    What a hosted ASGI application calls to delegate one request to the legacy,
    and the road the core itself takes for the ``wsgi_app`` shortcut: one way
    in, whether the consumer delegates or hosts nothing else.

    It holds no state of a request: the same instance serves concurrent
    requests, because the router of a consumer calls it that way.
    """

    def __init__(self, wsgi_app: Callable[..., Iterable[bytes]], worker: Any) -> None:
        """Initialize this instance.

        Args:
            wsgi_app: the consumer's WSGI callable, ``(environ, start_response)``.
            worker: the worker whose traffic pool runs it — WSGI is synchronous,
                and the request's slot follows the work onto that thread.
        """
        self.wsgi_app = wsgi_app
        self.worker = worker

    def build_environ(self, scope: dict[str, Any], body: bytes) -> dict[str, Any]:
        """The PEP 3333 environ for one ASGI scope.

        Args:
            scope: the http scope of the request being delegated.
            body: the whole request body, already drained.

        Returns:
            The environ. ``SCRIPT_NAME`` is the scope's ``root_path`` and
            ``PATH_INFO`` what is left of ``path`` once that prefix is taken
            off — so the legacy site's view of its own URLs does not change,
            whether the router in front of it moved the prefix into
            ``root_path`` or left the path whole.
        """
        script_name = str(scope.get("root_path") or "")
        path_info = str(scope.get("path") or "/")
        if script_name and path_info.startswith(script_name):
            path_info = path_info[len(script_name) :]
        headers = [
            (name.decode("latin-1"), value.decode("latin-1"))
            for name, value in scope.get("headers") or []
        ]
        environ: dict[str, Any] = {
            "REQUEST_METHOD": scope.get("method", "GET"),
            "SCRIPT_NAME": script_name,
            "PATH_INFO": path_info,
            "QUERY_STRING": bytes(scope.get("query_string") or b"").decode("latin-1"),
            "SERVER_PROTOCOL": "HTTP/1.1",
            "wsgi.version": (1, 0),
            "wsgi.url_scheme": scope.get("scheme") or "http",
            "wsgi.input": io.BytesIO(body),
            "wsgi.errors": sys.stderr,
            "wsgi.multithread": True,
            "wsgi.multiprocess": False,
            "wsgi.run_once": False,
            "genro.identity": scope.get("genro.identity"),
        }
        for key in CALL_KEYS:
            if key in scope:
                environ[key] = scope[key]
        environ.update(self.header_environ(headers))
        if body and "CONTENT_LENGTH" not in environ:
            environ["CONTENT_LENGTH"] = str(len(body))
        server = scope.get("server") or ("localhost", 80)
        environ["SERVER_NAME"] = str(server[0])
        environ["SERVER_PORT"] = str(server[1])
        client = scope.get("client")
        if client:
            environ["REMOTE_ADDR"] = str(client[0])
            if len(client) > 1:
                environ["REMOTE_PORT"] = str(client[1])
        return environ

    def header_environ(self, headers: list[tuple[str, str]]) -> dict[str, str]:
        """The header half of the environ: ``HTTP_*`` keys, duplicates joined.

        A repeated header is one environ key holding the values comma-joined —
        the reassembly PEP 3333 prescribes, and the reason the wire carries a
        pair-list instead of a mapping. ``Cookie`` is the one exception: its
        pairs rejoin with ``"; "`` (RFC 6265, restated by RFC 7540 §8.1.2.5) —
        a comma would fuse two cookies into one mangled value.
        """
        packed: dict[str, str] = {}
        for name, value in headers:
            key = UNPREFIXED_HEADERS.get(name.lower()) or f"HTTP_{name.upper().replace('-', '_')}"
            if key in packed:
                joiner = "; " if key == "HTTP_COOKIE" else ","
                packed[key] = f"{packed[key]}{joiner}{value}"
            else:
                packed[key] = value
        return packed

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        """Serve one ASGI request through the WSGI callable.

        Args:
            scope: the http scope; ``root_path`` becomes ``SCRIPT_NAME``.
            receive: drained to the last ``http.request`` before the app runs.
            send: gets one ``http.response.start`` and one
                ``http.response.body`` — ``Set-Cookie`` and ``Location`` travel
                like any other header, so a legacy redirect reaches the browser
                as it is.

        The callable is synchronous, so it runs on the worker's traffic pool
        through ``run_sync``: the request's slot follows it onto that thread,
        and whatever the site announces while serving rides this CALL's reply.

        An application that read the body itself delegates with an EMPTY one —
        what is left on ``receive`` by then is the disconnect — so delegate
        before reading, or hand the legacy what you read some other way.
        """
        body = await self.read_body(receive)
        environ = self.build_environ(scope, body)
        status, headers, payload = await self.worker.run_sync(
            lambda: self.serve_environ(environ)
        )
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (name.encode("latin-1"), value.encode("latin-1")) for name, value in headers
                ],
            }
        )
        await send({"type": "http.response.body", "body": payload})

    async def read_body(self, receive: Any) -> bytes:
        """Drain the request body to its last chunk."""
        chunks: list[bytes] = []
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                break
            chunks.append(message.get("body", b""))
            if not message.get("more_body"):
                break
        return b"".join(chunks)

    def serve_environ(self, environ: dict[str, Any]) -> tuple[int, list[tuple[str, str]], bytes]:
        """Run the WSGI callable on one environ, on the calling thread.

        Returns:
            The status, the headers and the whole body.

        The iterable is consumed FIRST and closed as PEP 3333 requires; the
        deprecated ``write`` callable is supported the only way a whole-body
        reply can support it — its chunks lead the bytes the iterable yields.
        """
        status = "200 OK"
        headers: list[tuple[str, str]] = []
        written: list[bytes] = []

        def start_response(
            answer: str, answer_headers: list[tuple[str, str]], exc_info: Any = None
        ) -> Callable[[bytes], None]:
            nonlocal status, headers
            status, headers = answer, list(answer_headers)
            return written.append

        result = self.wsgi_app(environ, start_response)
        try:
            chunks = list(result)
            body = b"".join(written + chunks)
        finally:
            close = getattr(result, "close", None)
            if close is not None:
                close()
        return int(status.split(" ", 1)[0]), headers, body
