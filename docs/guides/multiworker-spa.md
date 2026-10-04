# The multiworker SPA

## What it does

`kajenn-orchestra` turns one kajenn application into a front for a pool of
worker processes, and pins every user to one of them. It is a distribution of
its own, on top of the core:

| Distribution | Import package | Responsibility |
|---|---|---|
| `kajenn` | `kajenn` | server, mounted applications, HTTP, websocket channel, middleware, core services |
| `kajenn-orchestra` | `kajenn_orchestra` | the SPA front, the commander, the worker processes, the registers, the freezer and the hosted-application seams |

Installing `kajenn-orchestra` installs `kajenn` with it. Importing the core does
not load any of this: the orchestration names live under `kajenn_orchestra` and
nowhere else, in configuration strings (`entry_module`, `worker_class`,
`python -m`) exactly as in Python imports.

```python
from kajenn_orchestra.spa_app import SpaApplication
from kajenn_orchestra.orchestration.spa_worker import SpaWorker
from kajenn_orchestra.environ import AsgiSeam, WsgiSeam
```

The modules of `kajenn` this package may import are listed in
`kajenn_imports.txt` at the repository root, and `tools/import_graph.py` fails on
anything outside it.

## When to use it

Use it when one user's session is expensive to build and cheap to keep: a
single-page application whose server side holds a live model per user, a legacy
site with a per-user database handle, anything where the second request must
find what the first one built. Use plain kajenn when your handlers are stateless
and any process can serve any request.

## Setup

Mount a `SpaApplication` and declare its `orchestration` subtree, with a
`commander` and at least one group naming your `SpaWorker` subclass. The smallest
working recipe is in [Getting started](../getting-started.md); every key is in
the [configuration reference](../configuration.md).

Use **one** orchestrated application per server. Two SPA fronts on one server is
a state the design declares impossible, and the second boot says so when the
inspector is enabled; there is no unconditional check for it.

## One identity, and it belongs to the site

The front mints nothing. The `spa_connection_id` cookie carries the **hosted
site's own connection id**: the site names its connection while it serves, the
worker announces the birth, and the reply carries that id back out. The front
writes the cookie only when the id the site settled on differs from the one the
browser sent — a first visit, or a replacement the site made because the
connection its own cookie named did not validate.

The cookie lives 24 hours, is `HttpOnly`, `SameSite=Lax` and is set on `/`. It
is the only thing the routing reads. A refusal never carries it: the site never
served that request, so there is no connection to name, and a 503 must not
overwrite what the browser already holds.

Because the cookie and the site's own connection id are one value, nothing in
the chain from the cookie to the worker translates anything. The commander's
`connection_user_map` is eternal for the same reason the cookie is: a browser
that comes back a week later is the same person, whatever happened to the process
it used to talk to.

## The two identities a worker knows

A **connection** is born a guest, and its `user` field carries the guest name. A
guest never leaves his group's reception — the oldest living worker — however
full it is; only a login makes a browser placeable. The login is
`change_connection_user`: the live connection row is re-labelled onto the real
identity, its id moves between the two users' `connections` sets, and nothing is
re-keyed or re-born. The pages need no re-labelling, because a page row stores no
user: its owner is derived by walking up to its connection.

## Verifying stickiness

Answer with the pid of the serving process and call the front twice with the same
cookie jar. Both answers name the same pid; the second carries no `set-cookie`.
[Getting started](../getting-started.md#call-it) shows the exact exchange.

## Global store

`worker.global_store` is a `GlobalStoreClient` onto one commander-owned
`dict[str, Any]`. Keys are literal strings — `"a.b"` is one key, never a path —
and values must survive the TYTX transport: a scalar, a dict, a Bag, a datetime.
There are no replicas and no objects shared between processes.

`get`, `set` and `delete` are synchronous: call them from a worker pool thread,
not from the worker's event-loop thread. `get(key, default)` returns the default
only for an absent key; a stored `None` stays `None`, because the reply carries
`exists` beside `value`.

A read-modify-write turn works with `with` on a pool thread or `async with` on
the worker loop:

```python
# Inside a synchronous handler running on a worker pool thread:
with worker.global_store.for_update("visits") as turn:
    turn.value = (turn.value if turn.exists else 0) + 1
```

```python
# Inside asynchronous worker code:
async with worker.global_store.for_update("settings") as turn:
    if not turn.exists:
        turn.value = {}
    turn.value["theme"] = "dark"
```

The lease yields itself. `value` is a private working copy the grant decoded;
the exit sends the **complete** value back and the commander replaces the
selected key, or the whole dictionary when `for_update()` selected none.

### Gotchas

- One FIFO lock protects **all** operations, reads of unrelated keys included.
  Keep turns short.
- A nested turn, or a `get`/`set`/`delete` from the context already holding one,
  raises at once rather than parking on a lock it already holds. Other threads
  of the same worker wait normally.
- An exception in the block, or `turn.abort()`, publishes nothing. `abort()`
  keeps the lock until the block exits. A grant that cannot be decoded or a
  value that cannot be encoded also releases without applying, re-raising the
  original error.
- `GlobalStoreCommitUnconfirmed` means the commit was sent and its answer never
  came: the value **may already be published**. The client never retries and
  never pretends the turn was aborted. A commit the commander *refused* is a
  different exception, `CommanderCallFailed`, and nothing was published.
- The store is not persistent on its own. It is written into the photo a soft
  quit takes and read back by the next boot; arrange durable storage separately
  when your application needs more than that.

## What this transport does not carry

The front packs a **complete** request body and the seams collect the
**complete** response body before replying. The worker path offers neither
streaming uploads nor incremental downloads or SSE, and an endless hosted
response never completes its call. Serve streaming routes directly on the core
when that behaviour is required.

Worker events belong to the request that produced them and travel on that
request's reply. A mutation made outside a served request — the quit's transfer
cycle is the one producer that does this — is sent with an announcement call of
its own, `/group/announce`, and is folded exactly as a reply's envelope is.

:::{admonition} Under review
:class: warning
`internals/20_spa/020_orchestration/status.md` names that announcement call
`/commander/worker_events`. The path in the code is `/group/announce`
(`ANNOUNCE_OP_PATH` in `orchestration/worker_handler.py`).
:::
