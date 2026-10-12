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

"""The payload of a message on a group's bus, in its two forms, on both sides.

The ``format`` of the message's meta names the form. ``control-json`` is one
JSON object: the ``result`` of a reply beside the envelope slots — the worker
events, the photo, the presentation fields. ``http`` is the 4-byte big-endian
length of the envelope JSON, the envelope JSON, then the ``HttpRecord`` bytes,
which nobody here opens. The JSON admits no NaN or infinity and no duplicate
key, and is written compact; whatever breaks a rule raises ``ValueError``.
"""

from __future__ import annotations

import json
from typing import Any

import kbus

#: How many bytes carry the length of the envelope of an ``http`` payload.
ENVELOPE_LENGTH_BYTES = 4

__all__ = ["ENVELOPE_LENGTH_BYTES", "LanePayload"]


def reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value!r}")


def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


class LanePayload:
    """Encode and decode the two payload forms of the group bus."""

    def encode_control(self, value: Any) -> bytes:
        try:
            return json.dumps(value, allow_nan=False, separators=(",", ":")).encode()
        except (TypeError, ValueError, RecursionError) as exc:
            raise ValueError(f"invalid control payload: {exc}") from exc

    def decode_control(self, payload: bytes) -> Any:
        try:
            return json.loads(payload, object_pairs_hook=unique_pairs,
                              parse_constant=reject_constant)
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise ValueError(f"invalid control payload: {exc}") from exc

    def encode_http(self, envelope: dict[str, Any], record: bytes) -> bytes:
        head = self.encode_control(envelope)
        return len(head).to_bytes(ENVELOPE_LENGTH_BYTES, "big") + head + record

    def decode_http(self, payload: bytes) -> tuple[dict[str, Any], bytes]:
        """The envelope and the untouched ``HttpRecord`` bytes of an ``http`` payload."""
        size = int.from_bytes(payload[:ENVELOPE_LENGTH_BYTES], "big")
        end = ENVELOPE_LENGTH_BYTES + size
        if len(payload) < end:
            raise ValueError("http payload shorter than its envelope length")
        return self.decode_control(payload[ENVELOPE_LENGTH_BYTES:end]), payload[end:]

    def envelope(self, message: kbus.Message) -> dict[str, Any]:
        """The envelope a message carries, whichever its form."""
        if message.meta.get("format") == "http":
            return self.decode_http(message.payload)[0]
        return self.decode_control(message.payload) if message.payload else {}

    def control_message(self, value: Any, **meta: Any) -> kbus.Message:
        """A ``control-json`` message carrying ``value``, with routing ``meta`` beside it."""
        return kbus.Message({"format": "control-json", **meta}, self.encode_control(value))
