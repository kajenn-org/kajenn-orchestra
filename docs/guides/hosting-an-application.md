# Hosting an application

## What it does

A worker process hosts your site. `SpaWorker`
(`orchestration/spa_worker.py`) is the process's whole server side: it holds the
three registers, serves the orders the commander gives, and hands site requests
to whatever you assigned to it. The base class hosts nothing — its
`hosted_app_seam` raises and the front answers **502** — so a running pool names
a subclass of it in `worker_class`.

## When to use it

Always, once you want the pool to serve a site. There is no way to host an
application other than by subclassing `SpaWorker`.

## Setup: the contract

A subclass assigns exactly **one** of two attributes.

| Attribute | What it takes | How it runs |
|---|---|---|
| `asgi_app` | an ASGI application | called on the worker's loop, through `AsgiSeam` |
| `wsgi_app` | a PEP 3333 callable | wrapped in `AsgiSeam(WsgiSeam(wsgi_app, self))` and run on the worker's traffic pool |

Assigning both raises `RuntimeError`, and `WorkerEntry` reads the seam at boot
for exactly that case, so the process dies before the wire exists. Assigning
neither is the base worker, which is legitimate until an HTTP call finally asks
it to serve.

```python
from kajenn_orchestra.orchestration.spa_worker import SpaWorker


class SiteWorker(SpaWorker):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(name, **kwargs)
        self.asgi_app = build_my_asgi_app()
```

The recipe names it as `worker_class="my_package.workers:SiteWorker"`, and the
module must be importable from the directory the server runs in. Whatever the
recipe wrote in `worker_kwargs` arrives as the constructor's keyword arguments.

## The four seams a consumer may override

| Seam | Signature | Purpose |
|---|---|---|
| `build_registry` | `() -> RegisterRegistry` | pair your own row classes with the tree; nothing else in the class names the concrete registry |
| `build_request_slot` | `() -> RequestSlot` | add the per-request state your verbs carry |
| `on_request_served` | `() -> None` | run at the end of every served request, failed ones included, on the pool thread, with the slot still open |
| `run_sync` | `(work) -> Any` | run synchronous work on the traffic pool under a copy of the calling task's context, so what it announces rides this request's reply |

## What a hosted request receives

The front packs routing facts and a bounded opaque HTTP record; the seam
rebuilds a scope or an environ from them.

For an ASGI application (`AsgiSeam.build_scope`):

- `root_path` is empty and `path` is the whole mount-relative path, because the
  path the front forwards is already mount-relative.
- `server` comes from the Host header — the only place the front's own address
  survives the packing — unless the record carried one.
- `genro.identity` is the CALL's identity, `None` when the caller named none.
- `genro.page_id` and `genro.reply_path` are present only for a message born on
  the websocket; a plain HTTP request carries neither.

For a WSGI callable (`WsgiSeam.build_environ`): `SCRIPT_NAME` is the scope's
`root_path` and `PATH_INFO` what is left of `path` once that prefix is taken
off, so a legacy site's view of its own URLs does not change. Duplicate headers
are joined by the WSGI rules, `wsgi.input` holds the whole drained body, and the
same three `genro.*` keys are copied across.

No Python session and no avatar crosses the transport. The worker receives the
routing context and nothing else.

A consumer whose ASGI router delegates some paths to a legacy site builds its own
`WsgiSeam` for them; the core has no legacy path prefixes. Such a router must set
`root_path` and `path` consistently, because that is what `SCRIPT_NAME` and
`PATH_INFO` are computed from.

## Naming a connection and a page

The site names its own connection while serving, and the worker's verbs put it
in the registers:

- `new_connection(identity, **fields)` — `identity` is the connection id, which
  becomes the value of the `spa_connection_id` cookie. Without a `user` field
  the connection is born a guest.
- `new_page(identity, page_id, **fields)` — `identity` is the user, and
  `connection_id` names the connection the page hangs from. It brings the
  connection and the user above it into being when they are unseen.
- `change_connection_user(cid, user)` — the login. The live connection row is
  re-labelled; nothing is re-keyed and no live store is lost.
- `drop_page`, `drop_connection`, `drop_user` — the cascades. Dropping something
  already gone is that same outcome: no error, and nothing announced.

Every one of these queues its announcement on the slot of the request being
served, and the announcements leave together on that request's reply. Outside a
served request there is no slot and a mutation raises.

## The group engine

A group that declares `engine_factory` runs one template process which builds the
one expensive object the whole group shares, and forks every worker out of
itself. The factory is a `module:Class` the deployment provides; it is
instantiated with `engine_kwargs` and asked for the object by calling
`build_group_engine()` on it. Both keys are mandatory once the template exists: a
template with no factory has nothing to share.

The object never travels in the spawn payload — that is JSON — it arrives as a
constructor argument of the worker, because a fork copies the memory it already
lives in. `WorkerEntry.build_worker` passes it only when there is one, and the
base worker does not declare it: it hosts no engine.

## Gotchas

- **Complete bodies, both ways.** No streaming uploads, no incremental downloads
  or SSE through the worker path. An endless hosted response never completes its
  call. Serve streaming routes directly on the core.
- **Two pools, and what runs where.** The traffic pool takes the WSGI stitching
  and long synchronous calls; the service pool, much smaller, takes the freezer
  IO. Neither ever takes a wait: waiting for a busy freezer folder is a coroutine
  on the loop. Their sizes are `main_threadpool_size` and `aux_threadpool_size`.
- **The global store is synchronous from a pool thread.** `get`, `set` and
  `delete` block the calling thread on the worker's loop, so they belong on the
  traffic pool and never on the loop itself.
- **Keep the process fork-safe when a template is declared.** The template is a
  synchronous, single-threaded process on purpose; the engine build runs your
  code in it, and a `print()` there lands in the logs rather than on the answer
  channel, which the template arranges by duplicating its stdout at birth.
