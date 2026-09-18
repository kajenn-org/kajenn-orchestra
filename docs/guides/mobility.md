# Freeze and reassignment

## What it does

A user's state lives in the memory of one worker process. Freeze writes that
state to disk and takes the user out of the process; reassignment is his next
request landing on whatever worker the placement gives him, which reads the state
back. It is the only path a user takes from one process to another — nothing is
transferred between two live processes, and nothing is replicated.

## The four steps

```{mermaid}
flowchart LR
    hold["hold<br/>SpaCommander.hold_user"] --> freeze["freeze<br/>SpaWorker.freeze_user"]
    freeze --> reassign["reassign<br/>placement cleared"]
    reassign --> unfreeze["unfreeze<br/>SpaWorker.adopt_user"]
```

**Hold.** `SpaCommander.hold_user` writes `on_hold` on the user's row and creates
the event a request parks on. From that instant `resolve_user` raises
`UserOnHold` for him, so a request of his waits instead of walking into a process
that is emptying. The barrier goes up *before* the order, which closes the window
the photo alone cannot.

**Freeze.** `GroupHandler.freeze_hosted_user` sends `/group/freeze_user` to his
worker and waits for the reply. Inside the child,
`SpaWorker.freeze_designated_user` waits for whatever holds the user — an
adoption in flight, his own calls — then `freeze_user` takes the folder lock and
writes his store and one parcel per connection, each carrying that connection and
its pages. It announces `user_frozen` and the reply *is* the confirmation. A user
this worker does not host is refused out loud in that same reply.

**Reassign.** The `user_frozen` event rides that reply and the envelope chain
reads it before the caller is unblocked. `GroupEnvelopeHandler.on_user_frozen`
sets his entry in the group's `user_worker_map` to `None` — to be assigned —
and `CommanderEnvelopeHandler.on_user_frozen` marks his row frozen. There is
nothing left for `freeze_hosted_user` to write when its reply arrives, so it only
releases the hold.

**Unfreeze.** The next request of his takes the ordinary path.
`resolve_worker` finds no placement and calls `assign_user`, which walks the
group's workers or births one. The frame carries the commander's `user_frozen`
verdict, and `SpaWorker.adopt_user` reads the parcel, deletes it — and the folder
with it when it was the last thing inside — and announces `user_adopted`, which
turns the mark off.

## What orders a freeze

| Trigger | Where | Condition |
|---|---|---|
| Idleness | `GroupHandler.check_user_activity` | the user's real clocks have been silent past `user_idle_freeze_minutes` |
| CPU offload | `GroupHandler.check_cpu_offload` | a CPU-closed worker is past `cpu_offload_percent`, and the least busy material contributor is ceded, one per beat |
| Departure of a worker | `GroupHandler.check_occupancy`, `restart_worker`, `quit_all` | everybody on that process leaves, one at a time |

:::{admonition} Under review
:class: warning
The module docstring of `orchestration/group_handler.py` names `close_worker` as
a method beside `restart_worker`. No such method exists: the closure of a spare
worker is decided in `check_occupancy`, which sends the order under the name
`close_worker` through `_order_quit`.
:::

`check_user_activity` also *drops* a user who has been silent past his own
expiry: he is forgotten whole, row and folder, rather than frozen.

A user never changes group. There is no fallback group and no policy key for
one.

## What a browser sees

Nothing, in the ordinary case. A freeze that happens between two requests is
invisible: the next request waits at most the time the placement and the disk
read take, and the answer is the site's own.

A request that arrives *during* the freeze parks on the hold. The whole time one
request may spend waiting is `REQUEST_HOLD_MAX_SECONDS`, five seconds, counted
across however many times it has to wait. Past that the front answers **503**
with the `Retry-After` the commander composed — a move is an evict and an
install, milliseconds when things are well, so seconds mean something is wrong
and a polite refusal is the honest answer.

The websocket survives a freeze. A page row's `wsx` field travels in the parcel,
so a user parked for idleness and woken by his next request still has a page the
worker will accept messages for. The browser noticed nothing.

## What a wild death costs

A process that dies without being asked to saves nobody. Its users' traces are
purged, whatever it left in the freezer is discarded and counted, and their next
request is a re-login. The declared price of the low tolerance: a mute process is
killed and its OS death awaited rather than waited on, which is seconds of error
instead of minutes of spinner.

An *ordered* death is the opposite: the worker answers the quit order at once
with the photo of everybody flagged for cession, the commander parks them, and
the worker then drains, freezing one user at a time.

## Reading what happened

Every freeze, birth, restart and death leaves a row in the file
`orchestration_log_path` names, and the judgment behind it lands as one JSON
object in the `.decisions.jsonl` file beside it, carrying a stable reason code
and the candidates the judge saw. Standing conditions — a worker with one single
material contributor, an offload deferred because every candidate has a call in
flight — are journaled once per condition and subject, never at every beat.
