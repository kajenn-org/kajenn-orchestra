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

"""LanePayload refusals: what a payload of the group bus may not carry."""

from __future__ import annotations

import pytest

from kajenn_orchestra.orchestration.lane_payload import ENVELOPE_LENGTH_BYTES, LanePayload


@pytest.mark.parametrize("payload", [b'{"a": NaN}', b'{"a": Infinity}'])
def test_a_non_finite_number_is_refused(payload):
    with pytest.raises(ValueError, match="non-finite JSON number"):
        LanePayload().decode_control(payload)


def test_a_duplicate_key_is_refused():
    with pytest.raises(ValueError, match="duplicate JSON key 'a'"):
        LanePayload().decode_control(b'{"a": 1, "a": 2}')


def test_bytes_that_are_not_json_are_refused():
    with pytest.raises(ValueError, match="invalid control payload"):
        LanePayload().decode_control(b"\xff\xfe")


def test_an_http_payload_shorter_than_its_envelope_is_refused():
    payload = (100).to_bytes(ENVELOPE_LENGTH_BYTES, "big") + b"{}"
    with pytest.raises(ValueError, match="shorter than its envelope length"):
        LanePayload().decode_http(payload)
