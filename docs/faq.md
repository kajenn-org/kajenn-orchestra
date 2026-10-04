# Frequently asked questions

## Install and first run

### Which package do I install?

`kajenn-orchestra`, in a virtual environment with Python 3.11 or newer. `kajenn`
is installed with it. The two import packages stay separate: `import kajenn`
loads nothing of the orchestration.

See [Getting started](getting-started.md#installation).

### What is the shortest thing that runs?

Two files: a `SpaWorker` subclass that assigns `wsgi_app` or `asgi_app`, and a
recipe mounting one `SpaApplication` with an `orchestration.commander` node and
one group naming that class. Then `kajenn serve ./config.py`.

See [Getting started](getting-started.md).

### My server will not start and says it declares no `orchestration` node.

A SPA front *is* its pool, so the recipe must write
`applications.<code>.orchestration` with a `commander` under it. A front with the
node and no commander is refused for the same reason. Wanting no pool means
declaring no SPA front.

See the [configuration reference](configuration.md).

### The group starts `broken` with `AF_UNIX path too long`.

`instance_dir` holds one socket per worker and the operating system caps the
length of a Unix socket path. Point it at a short absolute directory.

See [Getting started](getting-started.md#the-recipe).

## Routing and identity

### Who creates the `spa_connection_id` cookie?

The hosted site. It names its own connection while serving, the worker announces
the birth, and the front reads the id off the reply and writes the cookie — only
when it differs from the one the browser sent. The front mints nothing.

See [The multiworker SPA](guides/multiworker-spa.md#one-identity-and-it-belongs-to-the-site).

### Why does my site never see one of my paths?

The first segment is one of the front's own roots. `_wsx` always is, and
`_orchestration` is when `control_enabled` is on. Everything else falls through
to the site.

See the [configuration reference](configuration.md#the-fronts-own-roots).

### Can I pin a user to a group?

No. A user is placed in the group that received him and never changes group:
there is no fallback group and no policy key for one.

See [Freeze and reassignment](guides/mobility.md#what-orders-a-freeze).

### How many workers will I get?

As many as the traffic needs. The group launches one at boot — the reception —
grows inside a placement that found no worker able to admit a real user, and
shrinks when a worker's capacity is spare. `worker_max_number` sizes the
per-worker memory ceiling and caps nothing.

See [the admission decision](architecture/overview.md#the-admission-decision-of-a-group).

## Running and failing

### What does a 503 from the front mean?

Nobody could take this user, or he stayed between two homes longer than the
front is willing to wait — five seconds in total. The `Retry-After` header
carries when the machine will have decided again. The real reason goes to the
log, never to the browser.

See [Freeze and reassignment](guides/mobility.md#what-a-browser-sees).

### What does a 502 mean?

The hosted site failed inside a healthy process, or its wire is gone while the
server is running. The site is this gateway's upstream and its breakage is not
the client's fault. The exception text stays in the log.

See [The life of a request](architecture/overview.md#the-life-of-a-request).

### A worker died and its users had to log in again. Why?

The death was not ordered. A process that goes for reasons of its own leaves
nothing that can be trusted, so its users' traces are purged and whatever it left
in the freezer is discarded. An ordered death is the opposite: the worker parks
everybody on the way out.

See [what a wild death costs](guides/mobility.md#what-a-wild-death-costs).

### Does a freeze break an open websocket?

No. A page row's channel field travels in the parcel, so a user parked for
idleness and woken by his next request still has a page the worker accepts
messages for.

See [Freeze and reassignment](guides/mobility.md#what-a-browser-sees).

## State

### Can two workers share a Python object?

No. The one thing shared is a dictionary living on the commander, reached through
`worker.global_store`. There are no replicas and no shared memory.

See [Global store](guides/multiworker-spa.md#global-store).

### Is the global store persistent?

Not on its own. It is written into the photo a soft quit takes and read back by
the next boot. Arrange durable storage separately when your application needs
more.

See [Global store](guides/multiworker-spa.md#global-store).

### Can I stream a response through a worker?

No. The front packs a complete request body and the seams collect a complete
response body before replying: no streaming uploads, no incremental downloads,
no SSE, and an endless response never completes its call. Serve streaming routes
directly on the core.

See [Hosting an application](guides/hosting-an-application.md#gotchas).

## Configuration

### How do I change a setpoint without restarting?

Turn `control_enabled` on and POST to `/_orchestration/apply` with the setpoints,
or store a profile and POST `{"name": ...}` to `/_orchestration/reload`.

See [Configuration profiles](guides/configuration-profiles.md#how-a-profile-is-applied-at-runtime).

### Why was my profile refused?

`GroupPolicy.from_settings` lists every violation it found in one message: an
unknown key, a structural key a profile may never carry, a wrong type, a value
out of range, or one of the cross rules between keys.

See [how a profile is validated](guides/configuration-profiles.md#how-a-profile-is-validated).

### My apply answers 409.

A profile governs exactly one group, and this machine has zero or several.

See [Configuration profiles](guides/configuration-profiles.md#gotchas).

### Can I change `worker_class` at runtime?

No. The structural keys build the group's processes once and only the recipe can
move them.

See the [configuration reference](configuration.md).

## Watching

### How do I see what the pool is doing?

Set `KAJENN_INSPECTOR` and mount the base server application: the section answers
at `/_server/inspector/page`, `/census` and `/stream`. For a question nothing
foresaw, mount the MCP console and evaluate an expression inside any process.
Neither belongs in production.

See [Watching a pool](guides/console.md).

### Where are the decisions written?

`orchestration_log_path` names the human log, one row per order. A
`.decisions.jsonl` file beside it carries the same judgments structured, with a
stable reason code and the candidates the judge saw.

See [Freeze and reassignment](guides/mobility.md#reading-what-happened).
