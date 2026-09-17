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

"""Sphinx configuration for the kajenn-orchestra documentation."""

import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path

# The package is expected to be installed (``pip install -e ".[docs]"``); add
# ``src`` to the path as a fallback so autodoc resolves imports either way.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

project = "kajenn-orchestra"
copyright = "2025-2026, Softwell S.r.l."
author = "Genropy Team"
try:
    release = _pkg_version("kajenn-orchestra")
except PackageNotFoundError:
    release = "0.0.0.dev0"
version = release

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# MyST: the guides and narrative pages are Markdown; the toctree skeleton is rst.
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
myst_heading_anchors = 3

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_css_files = ["diagrams.css"]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "kajenn": ("https://kajenn.org", None),
}

# Napoleon: the codebase uses Google-style docstrings.
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True

# Autodoc: document members in source order; keep the ``__init__`` convention
# (kwargs live in the class docstring) readable.
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_default_options = {
    # A string default allows explicit directive member lists to narrow the API.
    "members": "",
    "show-inheritance": True,
}
