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
"""The group bus as the orchestration fixtures drive it: members, messages, payloads."""

import base64

import kbus
from kajenn.http_record import HttpRecord
from kajenn_orchestra.orchestration.lane_payload import LanePayload
from kajenn_orchestra.orchestration.worker_connector import GROUP_MEMBER, REGISTER_PATH


def bus_credentials(url):
    """The name, the token and the address a worker's URL carries."""
    scheme, _, rest = url.partition("://")
    credentials, _, location = rest.partition("@")
    name, _, token = credentials.partition(":")
    return name, token, f"{scheme}://{location}"


async def join_bus(url, handler):
    """Connect a member the way a worker does: name, token and address from its URL."""
    name, token, address = bus_credentials(url)
    member = kbus.Member(name, secret=token, handler=handler)
    await member.connect(address)
    return member


async def join_worker(worker, url):
    """Join a worker to its group's bus: the member handed to it before it connects."""
    name, token, address = bus_credentials(url)
    member = kbus.Member(name, secret=token, handler=worker.serve_call)
    worker.attach_member(member)
    await member.connect(address)
    return member


def incoming(path, data=None):
    """A call as kbus hands it to a worker's handler: its route in the meta."""
    if isinstance(data, dict) and "http" in data:
        message = http_message(data)
    else:
        message = LanePayload().control_message(data)
    message.meta["route"] = path
    return message


async def present(member, data):
    """The presentation call of a member to its group; returns the decoded answer."""
    reply = await member.call(f"{GROUP_MEMBER}.{REGISTER_PATH}", LanePayload().control_message(data))
    return LanePayload().decode_control(reply.payload)


def http_message(data):
    """The http form of a request written as ``{"http": {...}, "identity": ..., ...}``."""
    http = dict(data["http"])
    meta = {"format": "http", **{key: value for key, value in data.items() if key != "http"}}
    if "cid" in http:
        meta["cid"] = http.pop("cid")
    envelope = {key: http.pop(key) for key in ("page_id", "reply_path") if key in http}
    body = http.pop("body", b"")
    if isinstance(body, str):
        body = base64.b64decode(body)
    http["headers"] = [
        (n.encode("latin-1"), v.encode("latin-1")) for n, v in http.get("headers", [])
    ]
    http["query_string"] = http.get("query_string", "").encode("latin-1")
    return kbus.Message(meta, LanePayload().encode_http(envelope, HttpRecord().encode_request(http, body)))


def read_control(message, envelope=None):
    """A reply read the way the group reads it: payload and meta in one dict.

    ``envelope`` is the one the fold already read, when there is one: the photo
    in it is the one the fold filed.
    """
    if message.meta.get("format") == "http":
        decoded, record = LanePayload().decode_http(message.payload)
        payload = {"result": HttpRecord().decode_response(record), **(envelope or decoded)}
    elif envelope is not None:
        payload = envelope
    else:
        payload = LanePayload().decode_control(message.payload) if message.payload else None
    meta = {key: value for key, value in message.meta.items() if key != "format"}
    return {**(payload or {}), **meta} if meta else payload


def read_http_request(message):
    envelope, record = LanePayload().decode_http(message.payload)
    scope, body = HttpRecord().decode_request(record)
    http = {
        **scope,
        "body": body,
        "headers": [
            [n.decode("latin-1"), v.decode("latin-1")] for n, v in scope.get("headers", [])
        ],
        "query_string": scope.get("query_string", b"").decode("latin-1"),
        **envelope,
    }
    if "cid" in message.meta:
        http["cid"] = message.meta["cid"]
    return http


def http_reply(reply):
    """The reply message of a worker that answered ``reply``, http form or error."""
    if "error" in reply:
        meta = {"status": reply["status"]} if "status" in reply else {}
        envelope = {key: value for key, value in reply.items() if key != "status"}
        return LanePayload().control_message(envelope, **meta)
    result = reply.get("result", {})
    body = result.get("body", b"")
    if isinstance(body, str):
        body = base64.b64decode(body)
    envelope = {key: value for key, value in reply.items() if key != "result"}
    return kbus.Message(
        {"format": "http", "connection_id": result.get("connection_id")},
        LanePayload().encode_http(envelope, HttpRecord().encode_response({
            "status": result.get("status", 200),
            "headers": result.get("headers", []),
            "body": body,
        })),
    )


async def call_endpoint(connector, path, data, timeout=None):
    """Call an endpoint of a worker in the http form, or as a control value."""
    message = http_message(data) if "http" in data else LanePayload().control_message(data)
    return read_control(*await connector.call_message(path, message, timeout=timeout))
