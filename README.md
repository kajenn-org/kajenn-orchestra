# kajenn-orchestra

**Status**: Alpha · version 0.1.0.

Orchestration for [kajenn](https://github.com/kajenn-org/kajenn): the SPA front
(`SpaApplication`), the commander and its worker groups, sticky routing of a user to one
worker process, freeze and reassignment, the commander-owned global store, the registers,
the configuration profiles and the pool inspector.

`kajenn_orchestra` imports `kajenn`; `kajenn` never imports `kajenn_orchestra`. The core
loads the front by class reference from the configuration recipe. The modules of `kajenn`
this package may import are listed in `kajenn_imports.txt` and checked by
`tools/import_graph.py` in the pre-commit hook.

Based on genropy history and genro-modules.

## Installation

```
pip install kajenn-orchestra
```

kajenn is installed with it. Beside a kajenn checkout, `[tool.uv.sources]` in
`pyproject.toml` points at `../kajenn`:

```
uv venv .venv
uv pip install -p .venv/bin/python -e ".[dev,internals]"
```

## Project

- Documentation: https://kajenn.org
- Internals dossier: `internals/`, read with `mkdocs serve`
- Organization: https://github.com/kajenn-org

## License

Apache License 2.0, copyright Softwell S.r.l.
