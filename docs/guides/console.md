# Watching a pool

Two instruments read a running pool, and neither has an authentication rule of
its own: **mounting is the gate**. One exists only when an environment variable
is set, the other only when the recipe constructs it on purpose. Neither belongs
in production.

## The inspector

`InspectorSection` (`inspector_section.py`) is a section on the base server
application, attached by the SPA front during its own startup, only when the
environment variable `KAJENN_INSPECTOR` is set.

Three addresses:

| Address | What it answers |
|---|---|
| `GET /_server/inspector/page` | the page: the commander above, one row per worker below |
| `GET /_server/inspector/census` | the whole pool as JSON, keyed by the front's application code |
| `GET /_server/inspector/stream` | the observation stream, as SSE: one `census` event, then every mutation |

### Reaching it

The front attaches the section to the application mounted under the code
`_server`. A recipe that mounts no such application gets a warning in the log —
the inspector has nowhere to go — and no inspector.

```console
$ KAJENN_INSPECTOR=1 kajenn serve ./config.py --port 8351
$ curl http://127.0.0.1:8351/_server/inspector/census
{"smoke":{"user_map":{},"connection_user_map":{},"page_connection_map":{},
 "counters":{},"default_group":"smoke",
 "groups":{"smoke":{"user_worker_map":{},"living_workers":["smoke_0001"],
  "memory_occupied_percent":0.0949859619140625,"memory_accounting":"rss_fallback",
  "worker_max_number":6,
  "workers":{"smoke_0001":{"state":"running","rss_bytes":24477696,
   "cpu_temperature_percent":0.087, ...}}}}}}
```

The census is the commander's own reading: `user_map`, `connection_user_map` and
`page_connection_map`, the counters, and per group the placement map, the living
workers and each worker's state and gauges. Each worker's own census is fetched
off its lane; a worker that does not answer contributes an error entry instead of
holding up the page.

The section never traverses the hosted site. No cookie is minted, no connection
is opened, no site path is called — an observer that changes what it observes is
useless.

A second SPA front booting on the same server finds the section already attached
and refuses the boot: a server has one orchestrated application.

### The observation stream

`SpaCommander.subscribe_observation` puts a queue on the stream and switches the
workers' reporting on with the first subscriber; `unsubscribe_observation`
switches it off with the last. So a pool nobody is watching pays nothing for the
instrument.

## The MCP console

`SpaConsoleMcpApplication` (`spa_console.py`) is an MCP application whose whole
tool surface is a debug door into the pool's processes. It exists only where a
recipe mounts it.

Two tools:

- `targets` — every process the door can look into, by SPA application code. The
  list is `commander` followed by the name of every worker handler of every
  group.
- `eval` — evaluate one Python expression in one of them and answer its `repr`.
  `target` defaults to `commander`; `app` names the SPA application code and is
  needed only when the server mounts more than one front. The namespace holds
  `commander` on the vertex and `worker` inside a child.

There are no predicted questions and no read-only mode: full `eval` is what the
door is, because there is no read-only `eval` in Python. Whatever was not
foreseen is readable by composing an expression. **Never mount this application
in production.**

The reach into a child costs nothing extra: the expression travels on the lane
the commander already holds to every worker, as the `commander/eval` order.
