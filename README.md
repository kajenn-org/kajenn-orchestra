# kajenn-orchestra

[![PyPI](https://img.shields.io/pypi/v/kajenn-orchestra)](https://pypi.org/project/kajenn-orchestra/)
[![Tests](https://github.com/kajenn-org/kajenn-orchestra/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/kajenn-org/kajenn-orchestra/actions/workflows/tests.yml)
[![Codecov](https://codecov.io/gh/kajenn-org/kajenn-orchestra/branch/main/graph/badge.svg)](https://app.codecov.io/gh/kajenn-org/kajenn-orchestra)
[![Documentation](https://readthedocs.org/projects/kajenn-orchestra/badge/?version=latest)](https://kajenn-orchestra.readthedocs.io/en/latest/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://github.com/kajenn-org/kajenn-orchestra/blob/main/pyproject.toml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](LICENSE)

**Status**: Alpha · version 0.1.0.

Orchestration for [kajenn](https://github.com/kajenn-org/kajenn): one mountable
application, `SpaApplication`, and a supervised pool of worker processes behind
it. Every page a user opens is served by the same process and his state lives in
that process's memory, so the second request finds what the first one built.

A commander in the server process owns the indexes — whose a connection is,
which connection a page belongs to, where each user lives — and one group per
worker grammar owns the processes. A group launches one worker at boot and grows
only when a real user finds none that can admit him; nothing in the recipe says
how many workers there are.

A user reaches another process through one path: he is held, his state is
written to the freezer on disk, his placement is cleared, and his next request
wakes him wherever the placement lands. Idleness, CPU pressure and the departure
of a worker all take that same path. Nothing replicates between processes; the
only shared state is one dictionary the commander owns and every worker reaches
by call.

The setpoints of a group — memory shares, admission thresholds, the idle horizon
— are composed defaults ⊕ recipe ⊕ profile ⊕ environment and can be replaced on
a running pool, validated whole or refused whole.

`kajenn_orchestra` imports `kajenn`; `kajenn` never imports `kajenn_orchestra`.
The core loads the front by class reference from the configuration recipe. The
modules of `kajenn` this package may import are listed in `kajenn_imports.txt`
and checked by `tools/import_graph.py` in the pre-commit hook.

Based on genropy history and genro-modules.

## Installation

```
pip install kajenn-orchestra
```

kajenn is installed with it. Beside a kajenn checkout, `[tool.uv.sources]` in
`pyproject.toml` points at `../kajenn`:

```
uv venv .venv
uv pip install -p .venv/bin/python -e ".[dev,docs,internals]"
```

## A minimal pool

```python
# worker.py
from kajenn_orchestra.orchestration.spa_worker import SpaWorker


class SiteWorker(SpaWorker):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.wsgi_app = my_wsgi_site


# config.py
from kajenn.config import AsgiConfigBuilder
from kajenn_orchestra.spa_app import SpaApplication


class ServerConfiguration(AsgiConfigBuilder):
    default_config = False

    def main(self, root):
        cfg = root.configuration()
        cfg.server(host="127.0.0.1", port=8000)
        front = cfg.applications().application(
            code="site", mount="", app_class=SpaApplication
        )
        commander = front.orchestration().commander(
            frozen_users_path="/var/spa/frozen", instance_dir="/run/kajenn"
        )
        commander.groups(default="standard").group(
            name="standard",
            entry_module="kajenn_orchestra.orchestration.worker_entry",
            worker_class="worker:SiteWorker",
        )
```

```
kajenn serve ./config.py
```

The front answers on `/`, mints nothing, and writes the `spa_connection_id`
cookie with the connection id the hosted site named while serving.

## Project

- Documentation: https://kajenn-orchestra.readthedocs.io/en/latest/
- kajenn documentation: https://kajenn.readthedocs.io/en/latest/
- Internals dossier: `internals/`, read with `mkdocs serve`
- Organization: https://github.com/kajenn-org

## License

Apache License 2.0, copyright Softwell S.r.l.
