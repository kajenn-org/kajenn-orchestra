# Building the documentation

> **Status:** Draft; implementation checked against the development source on 2026-09-08.

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
occupied, select another port; do not terminate someone else's server. The
link check validates generated local targets and anchors, including viewcode
backlinks. For a release verification use a fresh output directory: Sphinx can
retain old viewcode HTML when the source module itself has not changed, even
with `-E`. To check the same configuration from the documentation directory:

```bash
cd docs
../.venv/bin/sphinx-build -W --keep-going -b html . _build/rtd-style
```

`conf.py` resolves `src` relative to itself, so invoking Sphinx from either
directory documents the same checkout. The package must be installed with its
real dependencies for autodoc; there are no mocked imports. The Python
intersphinx inventory is fetched over HTTPS: report a failed network fetch
separately from local import, markup or link failures.

## Read the Docs

The repository's `.readthedocs.yaml` declares Ubuntu 24.04, Python 3.12,
installation of the `docs` extra, `docs/conf.py` and failure on warnings. This
is build configuration, not evidence that the remote project is connected or
that a build was published.

No Read the Docs project is connected to this repository yet. A maintainer must
confirm the project URL, repository integration and version/branch settings
before publication is claimed. Successful local builds do not establish remote
publication.

## Internals reader

The developer dossier is a separate MkDocs site over `internals/`. In the same
checkout install `python -m pip install -e '.[internals]'`, then run
`mkdocs serve`. Its default address is `http://127.0.0.1:8771/`; its source views
show the local checkout, including uncommitted changes. See `.mkdocs/README.md`
in the repository for reader verification and alternate-port commands.
