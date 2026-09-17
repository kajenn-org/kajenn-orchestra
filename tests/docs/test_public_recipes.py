"""Contract: runnable public recipes answer real HTTP and WSX requests.

Examples are extracted from the documentation, with only the listening port
changed. Each server runs in its own subprocess and private storage/home.
"""

import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pytest


class PublicRecipes:
    """Read complete top-level Python fences from the published guide sources."""

    def __init__(self):
        self.root = Path(__file__).resolve().parents[2]

    def get_code(self, page, marker):
        text = (self.root / "docs" / page).read_text(encoding="utf-8")
        blocks = re.findall(r"^```python\n(.*?)^```$", text, re.MULTILINE | re.DOTALL)
        matches = [block for block in blocks if marker in block]
        assert len(matches) == 1, f"{page}: expected one recipe containing {marker!r}"
        return matches[0]


class PublicServer:
    """Own one documentation subprocess, its logs and local HTTP address."""

    def __init__(self, code, directory):
        self.directory = directory
        self.process = None
        self.log = None
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]
        self.recipe = directory / "recipe.py"
        assert "port=8000" in code, "Runnable recipe must declare its testable port"
        self.recipe.write_text(code.replace("port=8000", f"port={self.port}"), encoding="utf-8")

    def start(self):
        self.environment = {
            key: value for key, value in os.environ.items()
            if not key.startswith(("GENRO_", "GNR_"))
        }
        self.environment.update(
            KAJENN_HOME=str(self.directory / "home"), TMPDIR=str(self.directory)
        )
        self.log = (self.directory / "server.log").open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            [sys.executable, "-I", str(self.recipe)],
            cwd=self.directory,
            env=self.environment,
            stdout=self.log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(self.get_log())
            try:
                self.get_response("/")
                return
            except (OSError, URLError):
                time.sleep(0.05)
        raise AssertionError(f"Documentation server did not become ready:\n{self.get_log()}")

    def get_response(self, path, data=None, headers=None):
        request = Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, headers=headers or {}
        )
        try:
            with urlopen(request, timeout=4) as response:
                return response.status, response.read()
        except HTTPError as response:
            with response:
                return response.code, response.read()

    def get_log(self):
        if self.log is not None:
            self.log.flush()
        return (self.directory / "server.log").read_text(encoding="utf-8")

    def stop(self):
        try:
            if self.process is not None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=4)
                    raise AssertionError(f"Documentation server did not stop:\n{self.get_log()}") from None
        finally:
            if self.log is not None:
                self.log.close()


@pytest.fixture
def recipes():
    return PublicRecipes()


@pytest.fixture
def serve_recipe(tmp_path):
    @contextmanager
    def serve(code):
        server = PublicServer(code, tmp_path)
        try:
            server.start()
            yield server
        finally:
            server.stop()

    return serve


def test_getting_started_http(recipes, serve_recipe):
    with serve_recipe(recipes.get_code("getting-started.md", "# hello.py")) as server:
        for path, name in (("/index", "world"), ("/greet?name=genro", "genro"), ("/greet", "world")):
            status, body = server.get_response(path)
            assert status == 200
            assert json.loads(body) == {"hello": name}
        assert server.get_response("/nowhere")[0] == 404


def test_request_bodies_and_multipart(recipes, serve_recipe):
    with serve_recipe(recipes.get_code("guides/requests.md", "class Bodies")) as server:
        status, body = server.get_response(
            "/document", b'{"name":"Ada"}', {"Content-Type": "application/json"}
        )
        assert status == 200
        assert json.loads(body) == {"received": {"name": "Ada"}}
        status, body = server.get_response(
            "/raw/count", b"abc", {"Content-Type": "application/octet-stream"}
        )
        assert status == 200
        assert json.loads(body) == {"bytes": 3}
        # The decoded application refuses the content-type the raw one serves.
        assert server.get_response(
            "/document", b"abc", {"Content-Type": "application/octet-stream"}
        )[0] == 415
        multipart = (
            '--docsboundary\r\nContent-Disposition: form-data; name="title"\r\n\r\n'
            'Example\r\n--docsboundary\r\nContent-Disposition: form-data; '
            'name="document"; filename="demo.txt"\r\nContent-Type: text/plain\r\n\r\n'
            'abc\r\n--docsboundary--\r\n'
        ).encode()
        status, body = server.get_response(
            "/upload", multipart, {"Content-Type": "multipart/form-data; boundary=docsboundary"}
        )
        assert status == 200
        assert json.loads(body) == {"title": "Example", "filename": "demo.txt", "bytes": 3}
        assert server.get_response("/raw/count")[0] == 400


def test_authentication_statuses(recipes, serve_recipe):
    with serve_recipe(recipes.get_code("guides/authentication.md", "class App")) as server:
        assert server.get_response("/secret", headers={"Accept": "application/json"})[0] == 401
        credential = base64.b64encode(b"alice:wonderland").decode()
        assert server.get_response("/secret", headers={"Authorization": f"Basic {credential}"})[0] == 200
        assert server.get_response("/secret", headers={"Authorization": "Bearer sk_live_xyz"})[0] == 403


def test_openapi_and_argument_errors(recipes, serve_recipe):
    with serve_recipe(recipes.get_code("guides/openapi.md", "class Shop")) as server:
        status, body = server.get_response("/_meta/schema_json")
        assert status == 200
        assert json.loads(body)["openapi"] == "3.1.0"
        assert server.get_response("/_meta/docs")[0] == 200
        assert server.get_response("/search?max_price=invalid")[0] == 400
        assert server.get_response("/search?extra=1")[0] == 400
        assert "400" in json.loads(body)["paths"]["/search"]["get"]["responses"]


def test_wsx_client_roundtrip(recipes, serve_recipe, tmp_path):
    hello = recipes.get_code("getting-started.md", "# hello.py")
    with serve_recipe(hello) as server:
        client = recipes.get_code("guides/websockets.md", "async def main")
        client = client.replace("127.0.0.1:8000", f"127.0.0.1:{server.port}")
        result = subprocess.run(
            [sys.executable, "-I", "-c", client], cwd=tmp_path, env=server.environment,
            capture_output=True, text=True, timeout=10,
        )
        assert result.returncode == 0, result.stdout + result.stderr


def test_mcp_tools(recipes, serve_recipe):
    with serve_recipe(recipes.get_code("guides/mcp.md", "class Calc")) as server:
        payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode()
        status, body = server.get_response("/mcp", payload, {"Content-Type": "application/json"})
        assert status == 200
        assert {"add", "mul"} <= {item["name"] for item in json.loads(body)["result"]["tools"]}
        payload = json.dumps({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "add", "arguments": {"a": 2, "b": 3}},
        }).encode()
        status, body = server.get_response("/mcp", payload, {"Content-Type": "application/json"})
        assert status == 200
        reply = json.loads(body)
        assert "error" not in reply
        assert json.loads(reply["result"]["content"][0]["text"]) == {"result": 5}


def test_complete_configuration_readback(recipes, tmp_path):
    from cryptography.fernet import Fernet

    code = recipes.get_code("guides/configuration.md", "from kajenn import AsgiServer")
    site = tmp_path / "site"
    site.mkdir()
    code = code.replace("/srv/shop", str(site))
    code += '''
server = AsgiServer(config=ServerConfiguration)
assert server.config("server.host") == "127.0.0.1"
assert server.config("server.port") == 8123
assert server.config("server.session.ttl") == 3600
assert server.config("middleware.cors") is True
assert server.config("applications.shop.catalog.page_size") == 20
shop = server.applications["shop"]
assert shop.config("parameters.currency") == "EUR"
assert shop.config("catalog.title") == "Outlet"
assert shop.config("catalog.locale", default="it") == "it"
'''
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("GENRO_", "GNR_", "SHOP_"))
    }
    environment.update(
        KAJENN_HOME=str(tmp_path / "home"), TMPDIR=str(tmp_path),
        SHOP_PORT="8123", SHOP_STORAGE_KEY=Fernet.generate_key().decode(),
        SHOP_ADMIN_PASSWORD="temporary-documentation-check",
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", code], cwd=tmp_path, env=environment,
        capture_output=True, text=True, timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
