"""A browser-to-front-to-child proof that custom TYTX bytes remain opaque."""

import asyncio
import json
import os
import socket
import shutil
import sys
import tempfile
from contextlib import asynccontextmanager

import httpx
import pytest
import websockets
from genro_tytx import from_tytx, register_type, to_tytx

from tests.boundary.opaque_spa_worker import OpaqueValue

PREFIX = "WSX://"
PAGE = "opaque-page"


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@asynccontextmanager
async def running_front(_tmp_path):
    short_root = tempfile.mkdtemp(prefix="osp-", dir="/tmp")
    port = free_port()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.boundary.opaque_spa_frontend",
        "--root",
        short_root,
        "--port",
        str(port),
        env={**os.environ, "PYTHONPATH": os.pathsep.join([os.getcwd(), os.path.join(os.getcwd(), "src")])},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    base = f"http://127.0.0.1:{port}"
    async with httpx.AsyncClient(base_url=base) as client:
        for _ in range(150):
            if process.returncode is not None:
                stderr = (await process.stderr.read()).decode()
                raise AssertionError(f"front exited before readiness: {stderr}")
            try:
                response = await client.get("/_probe", timeout=0.1)
                if response.status_code == 200:
                    break
            except (httpx.ConnectError, httpx.ReadTimeout):
                pass
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("front did not become ready")
        try:
            yield process, client, port
        finally:
            process.terminate()
            await asyncio.wait_for(process.wait(), 8)
            shutil.rmtree(short_root, ignore_errors=True)


def envelope(**fields):
    return PREFIX + json.dumps(fields, separators=(",", ":"))


async def receive_for_id(websocket, message_id, *, pushes=None):
    while True:
        message = json.loads((await asyncio.wait_for(websocket.recv(), 5))[len(PREFIX) :])
        if message.get("id") == message_id:
            return message
        if pushes is not None:
            pushes.append(message)


async def test_custom_payload_is_opaque_in_spawned_front_and_decoded_only_at_ends(tmp_path):
    register_type(OpaqueValue, "OZ", lambda value: value.label, OpaqueValue)
    async with running_front(tmp_path) as (front, client, port):
        probe = (await client.get("/_probe")).json()
        assert probe == {
            "pid": front.pid,
            "opaque_registered": False,
            "routing_codec_calls": {
                "wsx_to_tytx": 0,
                "payload_decode": 0,
                "response_encode": 0,
            },
        }

        response = await client.get("/main/")
        assert response.status_code == 200
        assert from_tytx(response.text, "json") == OpaqueValue("worker-http")
        worker_pid = int(response.headers["x-worker-pid"])
        assert worker_pid not in {os.getpid(), front.pid}
        assert response.headers["x-page-id"] == PAGE
        cookie = client.cookies.get("spa_connection_id")
        assert cookie

        async with websockets.connect(
            f"ws://127.0.0.1:{port}/main/",
            additional_headers={"Cookie": f"spa_connection_id={cookie}"},
        ) as websocket:
            await websocket.send(
                envelope(
                    id="open",
                    method="WSK",
                    path="/main/_wsx/openchannel",
                    page_id=PAGE,
                    data=to_tytx(None, "json"),
                )
            )
            assert (await receive_for_id(websocket, "open"))["status"] == 200
            after_open = (await client.get("/_probe")).json()["routing_codec_calls"]
            assert after_open == {
                "wsx_to_tytx": 0,
                "payload_decode": 0,
                "response_encode": 0,
            }

            encoded = to_tytx(OpaqueValue("client-rpc"), "json")
            await websocket.send(
                envelope(id="rpc", method="WSK", path="/main/rpc", page_id=PAGE, data=encoded)
            )
            reply = await receive_for_id(websocket, "rpc")
            assert reply["data"] == encoded
            assert from_tytx(reply["data"], "json") == OpaqueValue("client-rpc")

            await websocket.send(
                envelope(id="push", method="WSK", path="/main/push", page_id=PAGE, data=encoded)
            )
            pushed = []
            push_reply = await receive_for_id(websocket, "push", pushes=pushed)
            while not pushed:
                pushed.append(json.loads((await websocket.recv())[len(PREFIX) :]))
            assert from_tytx(push_reply["data"], "json") == OpaqueValue("push-ack")
            push = next(item for item in pushed if item.get("path") == "/client/push")
            assert from_tytx(push["data"], "json") == OpaqueValue("worker-push")
            assert (await client.get("/_probe")).json()["routing_codec_calls"] == after_open

            await websocket.send(
                envelope(id="error", method="WSK", path="/main/error", page_id=PAGE, data=encoded)
            )
            assert (await receive_for_id(websocket, "error"))["status"] == 502

            await websocket.send(
                envelope(method="WSK", path="/main/rpc", page_id=PAGE, data=encoded)
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(websocket.recv(), 0.15)

        final_probe = (await client.get("/_probe")).json()
        assert final_probe["opaque_registered"] is False
        assert final_probe["routing_codec_calls"]["wsx_to_tytx"] == 0
        assert final_probe["routing_codec_calls"]["payload_decode"] == 0
        # A front-owned error response may require endpoint conversion; routed
        # successful payloads above left this counter at zero.
        assert final_probe["routing_codec_calls"]["response_encode"] <= 1
