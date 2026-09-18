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

"""``global_store`` is a base module: it imports nothing from ``orchestration``.

Reproduced by the bridge on 2026-09-08: importing ``global_store`` first, in a
fresh interpreter, failed with a circular import because the module reached up
into ``orchestration.worker_connector``. The test runs in a subprocess so the
import order is the one a consumer of the module alone would have.
"""

from __future__ import annotations

import subprocess
import sys


def test_global_store_imports_alone_in_a_fresh_interpreter() -> None:
    result = subprocess.run(
        [sys.executable, "-c", "import kajenn_orchestra.global_store"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
