"""Explicit control endpoints used by the orchestration wire fixtures."""

import base64

from kajenn.http_record import HttpRecord
from kajenn.channel.control import ControlPayload
from kajenn.channel.frame import Frame


def control_frame(*, data=None, **fields):
    info = dict(fields.pop("info", {}))
    if isinstance(data, dict) and "http" in data:
        http = dict(data["http"])
        for key in ("cid", "page_id", "reply_path"):
            if key in http:
                info[key] = http.pop(key)
        info.update({key: value for key, value in data.items() if key != "http"})
        info["format"] = "http"
        body = http.pop("body", b"")
        if isinstance(body, str):
            body = base64.b64decode(body)
        http["headers"] = [
            (n.encode("latin-1"), v.encode("latin-1")) for n, v in http.get("headers", [])
        ]
        http["query_string"] = http.get("query_string", "").encode("latin-1")
        return Frame(info=info, payload=HttpRecord().encode_request(http, body), **fields)
    payload = data
    if isinstance(data, dict):
        payload = dict(data)
        for key in ("worker_events", "worker_snapshot", "pid", "config"):
            if key in payload:
                info[key] = payload.pop(key)
    info["format"] = "control-json"
    return Frame(info=info, payload=ControlPayload().encode(payload), **fields)


def read_control(frame):
    if frame.info.get("format") == "http":
        payload = {"result": HttpRecord().decode_response(frame.payload)}
    else:
        payload = ControlPayload().decode(frame.payload) if frame.payload else None
    info = {key: value for key, value in frame.info.items() if key != "format"}
    return {**(payload or {}), **info} if info else payload


def read_http_request(frame):
    scope, body = HttpRecord().decode_request(frame.payload)
    http = {
        **scope,
        "body": body,
        "headers": [
            [n.decode("latin-1"), v.decode("latin-1")] for n, v in scope.get("headers", [])
        ],
        "query_string": scope.get("query_string", b"").decode("latin-1"),
    }
    for key in ("cid", "page_id", "reply_path"):
        if key in frame.info:
            http[key] = frame.info[key]
    return http


def http_reply(frame, reply):
    if "error" in reply:
        return Frame(id=frame.id, method="REPLY", path=frame.path, info=reply)
    result = reply.get("result", {})
    body = result.get("body", b"")
    if isinstance(body, str):
        body = base64.b64decode(body)
    info = {key: value for key, value in reply.items() if key != "result"}
    info.update(format="http", connection_id=result.get("connection_id"))
    return Frame(
        id=frame.id,
        method="REPLY",
        path=frame.path,
        info=info,
        payload=HttpRecord().encode_response(
            {
                "status": result.get("status", 200),
                "headers": result.get("headers", []),
                "body": body,
            }
        ),
    )


async def call_endpoint(connector, path, data, timeout=None):
    """Call an HTTP/WSX endpoint through the opaque Frame API."""
    frame = control_frame(method="CALL", path=path, data=data)
    return read_control(await connector.call_frame(frame, timeout=timeout))
