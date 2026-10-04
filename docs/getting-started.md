# Getting started

This page builds the smallest orchestrated server that answers real HTTP
requests: one `SpaApplication` at the site root, one commander, one worker
group, and a `SpaWorker` subclass that hosts a two-line WSGI site. Two files and
one command.

## Installation

Requires Python 3.11 or newer. Use a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install kajenn-orchestra
```

`kajenn` is installed with it. The two import packages stay separate:
`kajenn` is the server, `kajenn_orchestra` is everything on this page, and
`import kajenn` loads none of it.

For the exact APIs documented here, install from a checkout:

```bash
git clone https://github.com/kajenn-org/kajenn-orchestra.git
cd kajenn-orchestra
python -m pip install -e .
```

## The worker

A worker process hosts your site. The base `SpaWorker` hosts nothing: it serves
the orchestration orders, and on a site request `hosted_app_seam` raises, which
the front answers 502. A running pool therefore names a subclass, and the
subclass assigns its application to `asgi_app`, or to `wsgi_app` for a
synchronous one.

```python
# smoke_worker.py
"""Worker of the smoke test: a WSGI site that names its own connection."""

import os
import uuid
from http.cookies import SimpleCookie
from typing import Any

from kajenn_orchestra.orchestration.spa_worker import SpaWorker

COOKIE = "spa_connection_id"
PAGE = "smoke-page"


class SmokeSpaWorker(SpaWorker):
    """Answers every path with the pid of the worker process that served it."""

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        self.wsgi_app = self.site

    def site(self, environ: dict[str, Any], start_response: Any) -> list[bytes]:
        cookie = SimpleCookie(environ.get("HTTP_COOKIE", ""))
        morsel = cookie.get(COOKIE)
        cid = morsel.value if morsel else f"smoke-{uuid.uuid4().hex}"
        connection = self.connection_register.get(cid)
        if connection is None:
            connection = self.new_connection(cid)
        if self.page_register.get(PAGE) is None:
            self.new_page(connection["user"], PAGE, connection_id=cid)
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [f"smoke front answered from worker pid {os.getpid()}\n".encode()]
```

Three things happen in that handler, and they are the whole contract.

- The site reads the `spa_connection_id` cookie and, when the browser carries
  none, **mints the connection id itself**. The front mints nothing: the
  identity is the hosted site's to give.
- `new_connection` puts that connection in the worker's `connection_register`.
  A connection is born a guest, and its `user` field carries the guest name.
- `new_page` puts a page under that connection. Both calls announce themselves
  to the commander on the reply of this very request, which is how the
  commander's indexes learn who this browser is.

## The recipe

The pool is declared under the front's own `orchestration` node. That node is
required: an `SpaApplication` without a commander under it does not boot.

```python
# config.py
"""Smoke recipe: one SpaApplication on the site root, one worker group."""

from pathlib import Path
from typing import Any

from kajenn.config import AsgiConfigBuilder
from kajenn_orchestra.spa_app import SpaApplication

HERE = Path(__file__).resolve().parent


class ServerConfiguration(AsgiConfigBuilder):
    default_config = False

    def main(self, root: Any) -> None:
        cfg = root.configuration()
        cfg.server(host="127.0.0.1", port=8321)
        front = cfg.applications().application(
            code="smoke", mount="", app_class=SpaApplication
        )
        commander = front.orchestration().commander(
            frozen_users_path=str(HERE / "frozen"),
            instance_dir=str(HERE / "instance"),
            orchestration_log_path=str(HERE / "orders.log"),
            memory_max_percent=90.0,
            machine_memory_alarm_percent=95.0,
        )
        commander.groups(default="smoke").group(
            name="smoke",
            worker_memory_max_percent=50.0,
            worker_memory_admission_percent=80.0,
            restart_occupancy_max_percent=95.0,
            worker_min_life_seconds=0.0,
            user_idle_freeze_minutes=60.0,
            entry_module="kajenn_orchestra.orchestration.worker_entry",
            worker_class="smoke_worker:SmokeSpaWorker",
            main_threadpool_size=4,
            aux_threadpool_size=1,
        )
```

`instance_dir` holds one Unix socket per worker and `frozen_users_path` is the
freezer root. Both paths are the installation's and are declared once, on
`commander`. `worker_class` is a `module:Class` reference the child process
imports, so the module must be importable from the directory the server runs in.
`entry_module` is what `python -m` runs in that child.

:::{admonition} Keep the socket directory short
:class: warning
`instance_dir` holds `AF_UNIX` paths, whose total length the operating system
caps. A deeply nested directory makes the bind fail with `AF_UNIX path too long`
and the group starts `broken`. A short absolute path avoids it.
:::

## Run it

From the directory holding both files:

```console
$ kajenn serve ./config.py --port 8347
kajenn serving http://127.0.0.1:8347
INFO:     Started server process [27739]
INFO:     Waiting for application startup.
INFO:__main__:Worker smoke_0001: serving on uds:.../instance/smoke_0001.sock (pid 27788)
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8347 (Press CTRL+C to quit)
```

The startup line naming `smoke_0001` is the group's reception: one worker is
launched at boot and awaited, and the server answers only once it has presented
itself. Nothing in the recipe says how many workers there are — the group grows
on demand from there.

## Call it

The first request carries no cookie:

```console
$ curl -i -c cookies.txt http://127.0.0.1:8347/
HTTP/1.1 200 OK
content-type: text/plain
content-length: 43
set-cookie: spa_connection_id=smoke-7dc36bf7857347a3ab2011ceca777d21; Max-Age=86400; Path=/; HttpOnly; SameSite=Lax
set-cookie: session_id=3YvkYGMfl8NPjxgT2LOcoa0gW6sBKqEUI5_nDtmRILo; Max-Age=86400; Path=/; HttpOnly; SameSite=Lax

smoke front answered from worker pid 27788
```

`spa_connection_id` is the routing cookie, and its value is the id the site
minted inside the worker. The front read it off the reply and wrote it back,
because the connection the site settled on is not the one the browser sent. It
lives 24 hours (`Max-Age=86400`), is `HttpOnly` and `SameSite=Lax`, and is set on
`/`. The `session_id` cookie beside it belongs to the core, not to the pool.

The second request carries it, and nothing is rewritten:

```console
$ curl -i -b cookies.txt http://127.0.0.1:8347/hello
HTTP/1.1 200 OK
content-type: text/plain
content-length: 43

smoke front answered from worker pid 27788
```

Same pid: this browser is pinned to `smoke_0001`. No `set-cookie` header either
— the request reused the connection it arrived with, so the front writes
nothing.

A third request with no cookie is a different browser, and gets a connection id
of its own:

```console
$ curl -i http://127.0.0.1:8347/
HTTP/1.1 200 OK
set-cookie: spa_connection_id=smoke-7fdd6a99c9eb46be8647a31f7feeadb1; ...
```

Every path that is not one of the front's own roots goes to the hosted site, so
`/hello` reached the worker although no route of that name exists anywhere in
the recipe. The front's own roots are `_wsx` always, and `_orchestration` when
the recipe turns the control surface on.

## What the run left behind

```console
$ ls instance frozen
instance:
smoke_0001.sock

frozen:

$ cat orders.log
2026-09-18 14:26:42,902 decided_by=smoke order=start_worker subject=smoke_0001 numbers={'workers': 1} outcome=None
```

`instance` holds the worker's socket, `frozen` is empty because nobody left
memory, and `orders.log` is one line per order the machine gave — here the
birth of the reception. A `orders.decisions.jsonl` file appears beside it,
carrying the same decisions in a structured form.

## Next

- [Concepts](concepts.md) defines the words used above, each with the class
  that embodies it.
- [Hosting an application](guides/hosting-an-application.md) covers the
  `SpaWorker` contract in full: `asgi_app`, `wsgi_app` and what a hosted request
  receives.
- [Configuration reference](configuration.md) lists every key of the recipe with
  its type, default and effect.
