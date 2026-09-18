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
    "myst_parser",
    "sphinxcontrib.mermaid",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# MyST: the guides and narrative pages are Markdown; the toctree skeleton is rst.
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
myst_heading_anchors = 3
# ``:::{admonition}`` is a colon fence; without this extension MyST prints the
# fence as text instead of building the directive.
myst_enable_extensions = ["colon_fence"]
# A ```mermaid fence in Markdown is handed to the ``mermaid`` directive.
myst_fence_as_directive = ["mermaid"]

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_css_files = ["branding.css", "readability.css"]
html_logo = "_static/branding/kajenn-orchestra-mark.png"

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "kajenn": ("https://kajenn.readthedocs.io/en/latest/", None),
}

# Napoleon: the codebase uses Google-style docstrings.
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True

# Autodoc renders the annotations into the description itself. The
# ``sphinx-autodoc-typehints`` extension is deliberately absent: it injects its
# ``:type:``/``:rtype:`` fields in the wrong place when a docstring closes with a
# narrative paragraph after its ``Args:``/``Raises:`` block, which is this
# codebase's ordinary shape, and the misplaced field breaks the field list.

# Autodoc: document members in source order; keep the ``__init__`` convention
# (kwargs live in the class docstring) readable.
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_default_options = {
    # A string default allows explicit directive member lists to narrow the API.
    "members": "",
    "show-inheritance": True,
}

# Compact diagrams share the documentation palette and use readable labels.
mermaid_light_theme = "base"
mermaid_dark_theme = "base"
mermaid_init_config = {
    "startOnLoad": False,
    "theme": "base",
    "themeVariables": {
        "fontFamily": "Arial, sans-serif",
        "fontSize": "16px",
        "primaryColor": "#FFF8E8",
        "primaryTextColor": "#24262B",
        "primaryBorderColor": "#AD7410",
        "lineColor": "#526174",
        "secondaryColor": "#EDF1F5",
        "tertiaryColor": "#FFFFFF",
    },
    "flowchart": {"nodeSpacing": 24, "rankSpacing": 28, "useMaxWidth": False},
}

mermaid_width = "auto"
mermaid_height = "auto"
