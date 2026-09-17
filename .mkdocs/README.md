# Documentation development

Use a separate virtual environment in each checkout. Commands below run from
the repository root with Python 3.12, matching Read the Docs.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[docs,internals,dev]'
python -m mkdocs serve
```

The internals reader defaults to `http://127.0.0.1:8771/`. Stop with Ctrl-C in
its terminal. For concurrent work, use `--dev-addr 127.0.0.1:18771` or another
free port; do not stop a colleague's process. Restart after editing a hook.
Markdown, source, tests, configuration and source-view templates are watched.

## Reproducible checks

```bash
python -m pytest tests/docs tests/test_documentation_recipes.py -o addopts=''
python -m mkdocs build --strict
python .mkdocs/check_links.py .mkdocs/site
python -m sphinx -b html -W --keep-going docs docs/_build/html
python .mkdocs/check_links.py docs/_build/html
```

Both builds must finish without warnings. The HTML checker also validates local
assets and fragments. Sphinx intersphinx needs network access to the Python
inventory; report network failures separately from broken local imports,
links or docstrings. Never mock imports to conceal defects. The documentation
workflow runs these checks on pull requests targeting `develop`.

For the configuration-file invocation used by Read the Docs, also run:

```bash
python -m sphinx -b html -W --keep-going -c docs docs docs/_build/rtd
```

A local build does not establish publication or remote RTD settings.

## Local source views

`source_views.py` is a MkDocs hook. It generates only the source files linked
from the dossier and rewrites links in the rendered copy. The original
Markdown stays usable on GitHub and in editors. A view displays the actual
checkout, including uncommitted content; a static build is a snapshot of it.
Generated views are hidden from navigation and search. The navigation labels
distinguish Design, Decisions, Implementation status and Technical notes.

The allowlist is `src/**/*.py`, `tests/**/*.py` and root `pyproject.toml`.
Path normalization and symlink containment reject targets outside their
intended tree. No file-serving endpoint or arbitrary file path is added.
Line anchors use `#L<number>`; GitHub ranges open at their first line and both
endpoints must exist. Query strings and unsupported anchors fail the build.

## Browser verification

After the automated checks, inspect the home page, a nested implementation
status and its deep anchor. Follow a source link and compare its displayed
content with the checkout. Search for a distinctive documented term and open
the result. Inspect a Mermaid diagram in both palettes and at a narrow viewport;
its labels must stay readable and overflow must remain inside the panel.
Mermaid is loaded from the configured CDN, so offline diagram rendering is not
guaranteed.

Finally make a reversible text edit in the isolated checkout, observe automatic
reload, and restore it. Repeat for a linked source file to establish source
fidelity. Keep private temporary paths for recipes and dedicated processes for
live HTTP examples; terminate only the processes started for verification.

The complete-recipe test executes the five existing configurations in isolated
subprocesses. It proves construction only. Future-design fragments and entries
without complete recipes remain explicitly outside that assertion; record them
in the renewal coverage matrix rather than claiming execution.

## Dated audit

The [2026-09-08 renewal report](renewal-2026-09-08.md) records coverage,
verification, recovered decision provenance and the remaining owner queue.
