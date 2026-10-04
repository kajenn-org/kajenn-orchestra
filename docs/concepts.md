# Concepts

The vocabulary of an orchestrated server, each word with the class that embodies
it. Everything below lives in `kajenn_orchestra`; the server, the mount and the
configuration recipe belong to
[kajenn](https://kajenn.readthedocs.io/en/latest/).

## Front

`SpaApplication` (`spa_app.py`) is the mounted application: one door, and no
state of its own. Every path that is not one of its own first-level roots is
packed into a frame and handed to the commander; what comes back is rebuilt into
the HTTP response. It reads the `spa_connection_id` cookie on the way in and
writes it on the way out when the connection the site settled on differs from the
one the browser sent — it never mints an identity itself.

## Commander

`SpaCommander` (`orchestration/spa_commander.py`) is the one object that knows
the whole picture. It owns three indexes — `connection_user_map` (whose a
connection id is), `page_connection_map` (which connection a page belongs to) and
`user_map` (one row per identity, carrying `group`, `frozen` and `on_hold`) —
plus the global store's only copy, the orchestration log and the one clock the
whole machine beats on. It is built at the front's startup, out of the
`orchestration.commander` node of the recipe.

## Group

`GroupHandler` (`orchestration/group_handler.py`) is the workers built from one
grammar: the same child class, the same interpreter, the same setpoints. It owns
`user_worker_map`, which says where each of its users lives, and it decides its
own shape — when to grow, when to restart a process, when to close one. Nothing
in the recipe says how many workers a group has: it launches one at boot, grows
on demand and shrinks by waste, so the count is a reading.

## Worker

Two objects wear the name. `WorkerHandler`
(`orchestration/worker_handler.py`) lives in the server process: it owns one
Unix socket, one short name such as `smoke_0001`, and the process under it, which
is replaceable. `SpaWorker` (`orchestration/spa_worker.py`) lives in the child
process: it holds the three registers and serves the requests routed to it. A
handler survives the death of its process; a placement points at the handler, so
it survives with it.

## Template

`TemplateConnector` (`orchestration/template_connector.py`) and `TemplateEntry`
(`orchestration/template_entry.py`) are the optional birth path of a group. A
group that declares `engine_factory` runs one extra process, named
`template-<group>`, which builds the expensive object the whole group shares —
the group engine — once, and then forks every worker out of itself, so the
children find it already in their own memory. A group without `engine_factory`
has no template and spawns a fresh interpreter per worker.

## User, connection, page

The three levels of the ownership tree, held by `RegisterRegistry`
(`register_registry.py`) in each worker. A page belongs to a connection, a
connection belongs to a user. A connection is born a guest — its `user` is the
guest prefix plus its own id — and a login re-labels that live row without
re-keying anything. A page carries no user label at all: its owner is derived by
walking up, so it cannot go stale.

## Register

`Register` (`register.py`) is one in-process dataset of schemaless dict items
keyed by a string, with optional secondary indexes. `RegisterRegistry` hosts the
three primary ones — `user_items`, `connection_items`, `page_items` — and owns
the lifecycle vocabulary and the cascades. `RegisterRow` and its three subclasses
(`register_row.py`) say what a row is born with, what its parcel leaves behind
and what a birth announces.

## Freeze

`FreezeHandler` (`orchestration/freeze_handler.py`) is the freezer: one directory
per frozen user under one root, holding the user's own store and one file per
connection, guarded by a lock file. A user is frozen when his worker writes his
state there and announces `user_frozen`; from that moment he has no process, his
placement is cleared, and his next request wakes him wherever the placement
lands. It is the only place in the package that talks to the filesystem
directly.

## Global store

One `dict[str, Any]` living on the commander and nowhere else. A worker reaches
it through `GlobalStoreClient` (`global_store.py`), assigned as its
`global_store`: `get`, `set` and `delete` are single calls served under one FIFO
lock, and `for_update` is a read-modify-write turn that holds that lock from the
grant to the release. There are no replicas.

## Profile

A named JSON object of setpoints, stored as one file in a directory.
`GroupPolicy` (`orchestration/group_policy.py`) is the schema: it holds the
defaults, the per-key ranges and the cross rules, and validating a profile means
building one. The effective configuration of a group is composed
defaults ⊕ recipe ⊕ profile ⊕ environment, and every apply recomposes that stack
rather than stacking onto what is in force. `OrchestrationProfileStore`
(`orchestration_profile_store.py`) reads and writes the files;
`ConfigurationProfilesApplication` (`configuration_profiles.py`) is the mountable
archive over the same directory.

## Order, decision, beat

An order is something the machine did to a process or a user — a birth, a
restart, a freeze. It is written by `SpaCommander.log_order` to the file
`orchestration_log_path` names. A decision is the structured judgment behind it,
written as JSONL to a `.decisions.jsonl` file beside it, carrying a stable reason
code and the candidates the judge saw. A beat is one turn of the commander's
`heartbeat_loop`: every group gets its turn, and the periodic methods marked
`@every` (`orchestration/beats.py`) run when their own count of turns says so.
