# Bridge contract

**Version**: 0.2 · **Last Updated**: 2026-09-08 · **Status**: 🔴 DA REVISIONARE

**The need.** genropy-asgi is the first customer: it must run legacy genropy sites on this core WITHOUT the core learning genropy's logic. The bridge implements a contract; the core offers generalized mechanisms.

What the hosted site must provide and what it may consume: the WSGI
callable behind `WsgiSeam`; the site names its OWN connection while serving
(the `spa_connection_id` identity decision, 2026-08-22); what the bridge pins
(frozen tag v0.35.0) until migrated to this core. NO release of kajenn
from develop until this contract is honoured by a migrated bridge.

The site's own data plane — datachanges, dbevents, table subscriptions, the
user view — is the bridge's since #59 (2026-09-04), attached through the seams
the core names, composition never a subclass of the worker or the vertex:

- `SpaApplication.commander_class`: the bridge's subclass of `SpaCommander`.
- `SpaCommander.commander_dispatcher.add_branches(...)`: the bridge's own
  operations under `/commander/<name>/…`, called by the worker up the lane.
- `SpaWorker.worker_dispatcher.commander_orders.add_branches(...)`: the
  bridge's own orders to the worker, called by the vertex down the lane.
- `SpaCommander.on_worker_presented(worker_handler)`: a process has just
  presented itself — where the bridge pushes what a newborn must know.
- `SpaCommander.envelope_handler` (property): the last layer of the envelope
  chain; the bridge returns its subclass of `CommanderEnvelopeHandler` and reads
  in its own `on_<op>`, after the core's, what a worker event carries for it —
  the tables a newborn page subscribes, above all.
- `SpaWorker.build_request_slot()` / `on_request_served()`: what a request
  carries, and the tail of every served request.
- `RegisterRegistry.page_row_class` + `subscribe_page_store` / `detach_page` /
  `new_store`: the bridge's row fields, its capture, its store type.
- `SpaCommander.new_global_store()`: the vertex's data at birth — an empty
  `dict[str, Any]`. A consumer may fill it, never change its type; its values must
  be types the TYTX codec knows, because a grant carries them down the lane.

Interactions: spa-application (the cookie) · orchestration (the worker that hosts, the two dispatchers) · global-store (the operations).

## The global store on the consumer's side

The core offers `SpaWorker.global_store`: `get`, `set`, `delete` and
`for_update(key=None)`, on literal string keys with opaque values (see
`040_global-store/design.md`). It offers nothing path-shaped: there is no change
batch, no capturing store and no `apply_global_store_changes`.

genropy-asgi adapts the legacy dotted paths on the first segment: the first
segment is the dictionary key, the rest is resolved inside the legacy Bag stored
as that key's value, in the worker. A read of a subpath is one `get` plus a local
read; a write of a subpath is a keyed turn on the first segment, publishing the
complete Bag. `with globalStore()` is one whole-dictionary turn — `for_update()`
with no key. The core knows none of this.
