# Orchestration — current state

**Version**: 0.2 · **Last Updated**: 2026-09-08 · **Status**: 🔴 evidence refreshed; design ratification unchanged

Verified against source revision `2465fcc` (develop baseline). Test references
below identify the executable contracts; they are not a new coverage percentage.

## Ownership and process creation

`SpaCommander` owns global routing indexes and groups. `GroupHandler` owns
placement policy and worker handlers; `WorkerHandler` owns one process and its
wire; `SpaWorker` owns the live user/connection/page rows and the hosted
application. This package is `kajenn_orchestra`, distinct from the
single-process core. A user's pages remain with that user's store in one worker.

A group with `engine_factory` builds one synchronous template process and forks
workers from its initialized engine. Without a factory, workers use the spawn
path. `WorkerEntry` passes an inherited `group_engine` to the worker. Assigning
both ASGI and WSGI applications is rejected at worker boot; neither is valid for
a base worker serving only orders.

Claim anchors: [`SpaCommander`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L494), [`GroupHandler`](../../../src/kajenn_orchestra/orchestration/group_handler.py#L320), [`WorkerHandler`](../../../src/kajenn_orchestra/orchestration/worker_handler.py#L209), [`SpaWorker`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L564), [`WorkerEntry`](../../../src/kajenn_orchestra/orchestration/worker_entry.py#L90).

## Requests, worker events and mobility

`resolve_worker` is shared by HTTP and WSX requests: it handles per-user holds,
reception and placement. Request-owned `worker_events` ride that request's
reply. Lifecycle announcements use `/commander/worker_events`; they do not steal
another concurrent request's events. The envelope chain folds the events before
the reply unblocks its caller. `RegisterRegistry` maintains ownership edges;
row `item_lock` and per-page `call_lock` have different purposes.

Mobility follows hold, freeze, reassign and adoption. `freeze_hosted_user`
coordinates the group decision; `FreezeHandler` stores parcels with filesystem
locking. A sudden worker death discards affected live placement rather than
performing a direct worker-to-worker transfer. A failed parent fold still needs
its own resynchronization/escalation policy; the design's F48/F49 concern is not
resolved by documenting successful requests.

Claim anchors: [`resolve_worker`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L753), [`worker_events`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L782), [`freeze_hosted_user`](../../../src/kajenn_orchestra/orchestration/group_handler.py#L990), [`FreezeHandler`](../../../src/kajenn_orchestra/orchestration/freeze_handler.py#L88).

## CPU policy, memory limits and live profiles

`GroupPolicy` owns the validated setpoints. The commander samples process CPU
with psutil and filters temperature; the group uses CPU admission, hottest-open
placement and offload decisions. Memory is an admission veto, not a per-user
cost estimate. New workers are born for actual placement demand; retirement
checks CPU quiet time and whether survivors can absorb the spare.

The decisions journal records reasons and measurements beside the human
orchestration log. `apply_group_settings` validates a composed profile before
committing it under a lock; it changes setpoints of one existing group, not the
general application/deployment topology.

Claim anchors: [`GroupPolicy`](../../../src/kajenn_orchestra/orchestration/group_policy.py#L70), [`apply_group_settings`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L1448).

## Shutdown and shared state

`quit` persists frozen routing/user state for lazy wake; `stop` is dry shutdown.
The commander dictionary global store is not restored from a site reboot.
Store calls use one lock and explicit leases; no worker replicas or genropy
change batches remain. Hosted request/response bodies are still fully buffered
in the current ASGI seam. Distribution, subcommanders and issue #72 are not
implemented by these mechanisms.

Claim anchors: [`quit`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L445), [`quit`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L1710), [`quit`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L2088).

## Source and test evidence

- [src/kajenn_orchestra/orchestration/spa_commander.py](../../../src/kajenn_orchestra/orchestration/spa_commander.py)
- [src/kajenn_orchestra/orchestration/group_handler.py](../../../src/kajenn_orchestra/orchestration/group_handler.py)
- [src/kajenn_orchestra/orchestration/group_policy.py](../../../src/kajenn_orchestra/orchestration/group_policy.py)
- [src/kajenn_orchestra/orchestration/worker_handler.py](../../../src/kajenn_orchestra/orchestration/worker_handler.py)
- [src/kajenn_orchestra/orchestration/worker_entry.py](../../../src/kajenn_orchestra/orchestration/worker_entry.py)
- [src/kajenn_orchestra/orchestration/template_entry.py](../../../src/kajenn_orchestra/orchestration/template_entry.py)
- [src/kajenn_orchestra/orchestration/spa_worker.py](../../../src/kajenn_orchestra/orchestration/spa_worker.py)
- [src/kajenn_orchestra/orchestration/freeze_handler.py](../../../src/kajenn_orchestra/orchestration/freeze_handler.py)
- [tests/spa/orchestration/test_orchestration_worker_events_channels.py](../../../tests/spa/orchestration/test_orchestration_worker_events_channels.py)
- [tests/spa/orchestration/test_orchestration_template_entry.py](../../../tests/spa/orchestration/test_orchestration_template_entry.py)
- [tests/spa/orchestration/test_orchestration_placement.py](../../../tests/spa/orchestration/test_orchestration_placement.py)
- [tests/spa/orchestration/test_orchestration_cpu_offload.py](../../../tests/spa/orchestration/test_orchestration_cpu_offload.py)
- [tests/spa/orchestration/test_orchestration_audit_destinations.py](../../../tests/spa/orchestration/test_orchestration_audit_destinations.py)
- [tests/spa/orchestration/test_orchestration_foundations_e2e.py](../../../tests/spa/orchestration/test_orchestration_foundations_e2e.py)
