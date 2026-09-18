"""Child-only SPA worker and custom TYTX value for the opaque relay proof."""

import os
import uuid
from dataclasses import dataclass
from http.cookies import SimpleCookie
from typing import Any

from genro_tytx import register_type, to_tytx

from kajenn_orchestra.orchestration.spa_worker import SpaWorker

COOKIE = "spa_connection_id"
PAGE = "opaque-page"


@dataclass(frozen=True)
class OpaqueValue:
    label: str


class OpaqueSpaWorker(SpaWorker):
    """A real child process whose WSGI site alone knows ``OpaqueValue``."""

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        register_type(OpaqueValue, "OZ", lambda value: value.label, OpaqueValue)
        self.wsgi_app = self.site

    def site(self, environ: dict[str, Any], start_response: Any) -> list[bytes]:
        cookie = SimpleCookie(environ.get("HTTP_COOKIE", ""))
        morsel = cookie.get(COOKIE)
        cid = morsel.value if morsel else f"opaque-{uuid.uuid4().hex}"
        connection = self.connection_register.get(cid)
        if connection is None:
            connection = self.new_connection(cid)
        owner = connection["user"]
        if self.page_register.get(PAGE) is None:
            self.new_page(owner, PAGE, connection_id=cid)

        path = environ["PATH_INFO"]
        method = environ["REQUEST_METHOD"]
        body = environ["wsgi.input"].read()
        headers = [
            ("Content-Type", "application/vnd.tytx+json"),
            ("X-Page-Id", PAGE),
            ("X-Worker-Pid", str(os.getpid())),
        ]
        if method == "WSK" and path == "/push":
            self.send_message(PAGE, "/client/push", OpaqueValue("worker-push"))
            answer = to_tytx(OpaqueValue("push-ack"), "json").encode()
        elif method == "WSK" and path == "/error":
            raise RuntimeError("opaque worker error")
        elif method == "WSK" and body:
            answer = body
        else:
            answer = to_tytx(OpaqueValue("worker-http"), "json").encode()
        start_response("200 OK", headers)
        return [answer]
