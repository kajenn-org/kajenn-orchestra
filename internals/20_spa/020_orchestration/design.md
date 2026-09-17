# Orchestration

**Version**: 0.1 · **Last Updated**: 2026-08-22 · **Status**: 🔴 DA REVISIONARE

**The need.** Many users with LIVE server-side state must scale across processes without a user ever splitting: all his pages live in the process that holds his store.

The pool machine: `SpaCommander` (global indexes, lifecycle, per-user
barrier, request chain, single-writer fold via `EnvelopeHandler`, freezer
via `FreezeHandler`) → n `GroupHandler` (placement,
capacity, growth and shrink) → n `WorkerHandler` (process, wire,
surveillance) → `SpaWorker` (live users/connections/pages and the hosted
WSGI site behind `WsgiSeam`). Usersticky principle: ALL pages of one user
live in the process that holds the user's store. Mobility has ONE path:
hold → freeze → reassign → unfreeze. A sudden worker death restarts the
few users involved — an accepted, observable risk.

Interactions: spa-application (above) · channel (below) · global-store (it carries it) · storage (freezer) · restart.

## The chain and its registers

```mermaid
flowchart TD
    F["SpaApplication — stateless front"] --> C["SpaCommander
    user_map · connection_user_map · page_connection_map · user_hold_event_map
    EnvelopeHandler · FreezeHandler · global_register + global_lock"]
    C --> G1["GroupHandler — placement, capacity, growth/shrink"]
    C --> G2["GroupHandler (one per group)"]
    G1 --> W1["WorkerHandler — process, wire, surveillance"]
    G1 --> W2["WorkerHandler (one per worker)"]
    W1 --> P1["SpaWorker
    user_register · connection_register · page_register
    hosted WSGI site behind WsgiSeam"]
```

Every index above the worker is written ONLY by the single-writer fold of the
envelope chain: the worker announces, the parent applies.

## Mobility — the one path

```mermaid
stateDiagram-v2
    RESIDENT --> ON_HOLD: hold — requests park on the per-user barrier
    ON_HOLD --> FROZEN: freeze — FreezeHandler writes the parcel
    FROZEN --> RESIDENT: reassign + unfreeze — the destination worker adopts
```

Used for compaction, ordered replacement and wake. There is no direct
worker-to-worker move. A sudden worker death skips the path entirely: the
users involved restart.
