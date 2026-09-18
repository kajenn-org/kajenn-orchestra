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

"""Where the audit of a configuration attempt lands — three destinations (T22).

Design section 9: a boot that fails before the vertex exists can only speak on
the front's own module logger; anything that reaches the handler leaves an
orchestration row, refusals included; a body the request layer never hydrated is
the ONE thing that leaves no row at all.
"""

from __future__ import annotations

import logging

import pytest

from kajenn_orchestra.spa_app import ORCHESTRATION_ROOT
from kajenn_orchestra.orchestration.group_policy import GroupPolicyError
from kajenn_orchestra.orchestration.spa_commander import ORDERS_LOGGER_NAME

from ..test_spa_app_profiles import ask, boot, pool_server, written

SPA_APP_LOGGER = "kajenn_orchestra.spa_app"


async def test_boot_failure_logs_on_spa_app_logger(tmp_path, caplog):
    # wf:contract: T22a — a failed boot logs on the SpaApplication module logger
    # wf:contract: (caplog) with the violations, and leaves NO orchestration log
    # wf:contract: line: the commander does not exist yet.
    folder = tmp_path / "profiles"
    written(folder, "wrong", {"memory_max_percent": 0.0})
    server = pool_server(tmp_path, profiles_path=folder, profile_name="wrong")
    front = server.applications["site0"]

    with caplog.at_level(logging.INFO):
        with pytest.raises(GroupPolicyError) as refused:
            await boot(server)

    said = [
        record.getMessage() for record in caplog.records if record.name == SPA_APP_LOGGER
    ]
    assert len(said) == 1
    for violation in refused.value.violations:
        assert violation in said[0]
    # There is nothing to write an orchestration row with, and none was written.
    assert front._commander is None
    assert not [record for record in caplog.records if record.name == ORDERS_LOGGER_NAME]


async def test_reload_handler_errors_audited_as_rejected(tmp_path, caplog):
    # wf:contract: T22b — a reload of a missing or corrupt profile that reaches
    # wf:contract: the handler leaves a commander log_order line with outcome
    # wf:contract: "rejected: ...".
    folder = tmp_path / "profiles"
    server = pool_server(tmp_path, profiles_path=folder, control_enabled=True)
    await boot(server)
    (folder / "corrupt.json").write_text("{not json")

    with caplog.at_level(logging.INFO, logger=ORDERS_LOGGER_NAME):
        missing, _ = await ask(
            server, f"/{ORCHESTRATION_ROOT}/reload", method="POST", body={"name": "nowhere"}
        )
        corrupt, _ = await ask(
            server, f"/{ORCHESTRATION_ROOT}/reload", method="POST", body={"name": "corrupt"}
        )

    assert (missing, corrupt) == (404, 400)
    rows = [
        record.getMessage()
        for record in caplog.records
        if record.name == ORDERS_LOGGER_NAME and "apply_group_settings" in record.args
    ]
    assert len(rows) == 2
    for row in rows:
        assert "outcome=rejected: " in row
    # The vertex's own record says the same, and nothing moved.
    commander = server.applications["site0"].commander
    assert commander.last_apply["outcome"].startswith("rejected: ")
    assert commander.configuration_generation == 1


async def test_request_parser_400_is_not_orchestration_audit(tmp_path, caplog):
    # wf:contract: T22c — a body the handler cannot take is answered by the
    # wf:contract: dispatcher (400 here: the malformed JSON is refused while
    # wf:contract: decoding, issue #87) and leaves NO
    # wf:contract: orchestration log line: the single exclusion from the audit.
    server = pool_server(tmp_path, control_enabled=True)
    await boot(server)

    with caplog.at_level(logging.INFO, logger=ORDERS_LOGGER_NAME):
        status, _ = await ask(
            server, f"/{ORCHESTRATION_ROOT}/apply", method="POST", body=b"{not json"
        )

    assert status == 400
    assert not [record for record in caplog.records if record.name == ORDERS_LOGGER_NAME]
    commander = server.applications["site0"].commander
    # The last attempt on record is still the boot: this one never reached the vertex.
    assert commander.last_apply["source"] == "boot"
