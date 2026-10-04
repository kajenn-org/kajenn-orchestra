# Building the documentation

From the repository root, use an isolated environment and build with warnings
as errors:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[docs]'
sphinx-build -W --keep-going -b html docs docs/_build/html
python .mkdocs/check_links.py docs/_build/html
python -m http.server 8769 --bind 127.0.0.1 --directory docs/_build/html
```

Open `http://127.0.0.1:8769/`. Stop that HTTP server with Ctrl-C. If the port is
occupied, select another port; do not terminate someone else's server. The link
check validates generated local targets and anchors, including viewcode
backlinks. For a release verification use a fresh output directory: Sphinx can
retain old viewcode HTML when the source module itself has not changed, even
with `-E`. To check the same configuration from the documentation directory:

```bash
cd docs
../.venv/bin/sphinx-build -W --keep-going -b html . _build/rtd-style
```

`conf.py` resolves `src` relative to itself, so invoking Sphinx from either
directory documents the same checkout. The package must be installed with its
real dependencies for autodoc; there are no mocked imports. `kajenn` must be
installed too, because every module of this package imports it. Beside a kajenn
checkout, `[tool.uv.sources]` in `pyproject.toml` points at `../kajenn`:

```bash
uv venv .venv
uv pip install -p .venv/bin/python -e ".[dev,docs,internals]"
```

The Python and kajenn intersphinx inventories are fetched over HTTPS: report a
failed network fetch separately from local import, markup or link failures.

## The diagrams

The architecture diagrams are Mermaid, written as ```` ```mermaid ```` fences in
the Markdown sources. `sphinxcontrib.mermaid` (in the `docs` extra) turns each
fence into a `<div class="mermaid">`, and `myst_fence_as_directive` is what lets
a Markdown fence reach that directive. The drawing itself happens **in the
browser**, from `cdn.jsdelivr.net`: `sphinx-build` never reports a diagram that
fails to parse, so open the built page and check that every fence became an
`<svg>` before publishing.

## Read the Docs

The repository's `.readthedocs.yaml` declares Ubuntu 24.04, Python 3.12,
installation of the `docs` extra, `docs/conf.py` and failure on warnings. This is
build configuration, not evidence that a build was published.

The
[kajenn-orchestra Read the Docs project](https://app.readthedocs.org/projects/kajenn-orchestra/)
is connected to this repository and builds `main` as `latest`. Check the remote
build result before claiming an update is live. The configured documentation URL
is https://kajenn-orchestra.readthedocs.io/en/latest/. Custom-domain availability
is separate from a successful documentation build.

The remote build installs the `docs` extra from PyPI, so it resolves `kajenn`
from PyPI as well; `[tool.uv.sources]` is a development convenience and Read the
Docs ignores it.

## Internals reader

The developer dossier is a separate MkDocs site over `internals/`. In the same
checkout install `python -m pip install -e '.[internals]'`, then run
`mkdocs serve`. Its default address is `http://127.0.0.1:8771/`; its source views
show the local checkout, including uncommitted changes. See `.mkdocs/README.md`
in the repository for reader verification and alternate-port commands.

## Branding assets

The documentation sidebar uses the symbol; the home page shows the full wordmark.
The home-page brand panel follows the browser color preference, with a charcoal
panel for the opaque dark-background variant. The rest of the classic Read the
Docs theme is unchanged. Asset copies in `docs/_static/branding/` must match the
canonical PNGs in `assets/branding/`; update both when the approved logo changes.
