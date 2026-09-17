# Global store — current state

**Version**: 0.3 · **Last Updated**: 2026-09-08 · **Status**: 🔴 DA REVISIONARE

Verified against develop baseline `2465fcc`. The September dictionary/lease
semantics below remain in force; issue #72 is not assumed implemented.

2026-08-22: the replica-era seams (`snapshot`/`load_snapshot`, `/global/*` paths) were removed (a79449e); a site restore does NOT restore the global store.

2026-09-08 (issue #74): the master is one `dict[str, Any]` on the commander — `SpaCommander.global_register`, built by `new_global_store` — with literal string keys and opaque values. Built:

- `GlobalStoreLock` on the commander: one `asyncio.Lock` plus `holder`, `holder_worker` and `holder_key`. Every operation waits on it, reads of other keys included.
- `GlobalStoreOperations` on the commander's dispatcher: `/commander/store/{get,set,del,lock,unlock}`. `get` answers `exists` beside `value`. A non-string key raises `TypeError`.
- `SpaWorker.global_store`, a `GlobalStoreClient`: `get`, `set`, `delete`, `for_update`.
- `GlobalStoreLease`, the turn: `with` or `async with`, yielding itself with `value` and `exists`, plus `abort()`. The exit sends the COMPLETE value with `apply=True`; the commander replaces the selected key, or the whole dictionary when no key was selected. A body that raises, a grant that does not decode, a value that does not encode and an aborted turn release with `apply=False`.
- `GlobalStoreCommitUnconfirmed`: the commit left and the wire failed before the reply. Nothing is retried.
- The death protocol: `WorkerHandler.on_child_lost` calls `GlobalStoreOperations.release_worker_lock`, which frees only that worker's turn and applies nothing. No lease timer, no expiry.

Removed in the same change: the master Bag, the worker-side `store_get`/`store_set`/`store_del`, the read that took no lock, `CapturingGlobalStore`, the change batch on the release and `apply_global_store_changes`. Operations on the store are by key, never by path.

Claim anchors: [`SpaCommander`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L494), [`new_global_store`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L977), [`GlobalStoreLock`](../../../src/kajenn_orchestra/global_store.py#L110), [`GlobalStoreOperations`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L280), [`GlobalStoreClient`](../../../src/kajenn_orchestra/global_store.py#L154), [`for_update`](../../../src/kajenn_orchestra/global_store.py#L201), [`GlobalStoreLease`](../../../src/kajenn_orchestra/global_store.py#L206), [`abort`](../../../src/kajenn_orchestra/global_store.py#L231), [`GlobalStoreCommitUnconfirmed`](../../../src/kajenn_orchestra/global_store.py#L87), [`WorkerHandler`](../../../src/kajenn_orchestra/orchestration/worker_handler.py#L209).

## Source and test evidence

- [Global store client and lease](../../../src/kajenn_orchestra/global_store.py)
- [Commander store operations](../../../src/kajenn_orchestra/orchestration/spa_commander.py)
- [Worker death cleanup](../../../src/kajenn_orchestra/orchestration/worker_handler.py)
- [Dictionary and lease contracts](../../../tests/spa/orchestration/test_contract_global_store_dict.py)
- [Client contracts](../../../tests/spa/test_spa_global_store.py)

Simple client get/set/delete calls run from a worker pool thread; leases also
support `async with`. Nested access from the context holding a lease is refused.
A stored `None` remains distinct from an absent key. Whole-store publication
uses clear/update under the one lock; keyed publication replaces one value.
A TYTX-looking string can collide with the codec, an explicitly retained limit.
