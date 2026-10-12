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

"""Contract of the move to the kbus library: the old bus is gone, the behaviour stays.

Executable from the plan on. The old package is named by concatenation so that
this file never matches its own search.
"""

from __future__ import annotations

import importlib.metadata
import tomllib
from pathlib import Path

OLD_BUS = "kajenn" + ".kbus"
HERE = Path(__file__).resolve().parent


def repository_root() -> Path:
    for parent in HERE.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise AssertionError("no pyproject.toml above the contract tests")


def test_no_module_of_src_or_tests_names_the_old_bus():
    root = repository_root()
    offenders = [
        str(path.relative_to(root))
        for folder in ("src", "tests")
        for path in (root / folder).rglob("*.py")
        if OLD_BUS in path.read_text()
    ]
    assert offenders == []


def test_the_allowed_kajenn_imports_no_longer_list_the_old_bus():
    lines = (repository_root() / "kajenn_imports.txt").read_text().split()
    assert [line for line in lines if line.startswith(OLD_BUS)] == []


def test_the_package_declares_the_kbus_library_it_imports():
    pyproject = tomllib.loads((repository_root() / "pyproject.toml").read_text())
    assert "kajenn-kbus>=0.1.2" in pyproject["project"]["dependencies"]


def test_the_installed_kbus_has_the_deterministic_finish():
    installed = importlib.metadata.version("kajenn-kbus")
    assert tuple(int(part) for part in installed.split(".")[:3]) >= (0, 1, 2)

