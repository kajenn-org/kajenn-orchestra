# Configuration profiles

## What it does

A profile is a named JSON object of group setpoints, stored as one file in a
directory. Two surfaces use the same directory: the mountable archive under
`_sysop`, which stores profiles and applies nothing, and the front's own
`_orchestration` root, which puts one in force on a running pool.

## When to use it

Use a profile when the setpoints of a running installation must change without a
restart — a busy-hours policy, a maintenance policy, an emergency ceiling. Leave
both surfaces out of the recipe when nobody reconfigures the machine: the front
never claims `_orchestration` unless asked, and the hosted site keeps that path.

## Setup

Mount the archive and turn the control surface on:

```python
from kajenn_orchestra.configuration_profiles import ConfigurationProfilesApplication

apps = cfg.applications()
apps.application(code="_sysop", app_class=ConfigurationProfilesApplication,
                 folder="/var/spa/profiles")
front = apps.application(code="spa", mount="", app_class=SpaApplication)
front.orchestration(profiles_path="/var/spa/profiles", control_enabled=True)
```

The two paths must name the same folder for a stored profile to be reachable by
name from the control surface. `profile_name` on the same node puts one in force
at boot; named without a folder, missing, or invalid, the server does not start.

Mounting is the gate. A server that does not construct
`ConfigurationProfilesApplication` has no write surface at all.

## The archive

`ConfigurationProfilesApplication` (`configuration_profiles.py`) is a
`McpOpenApiApplication` with `code = mount = "_sysop"`, mounting its routing
class under `api_name="configuration"`. The same CRUD operations answer over
REST and as MCP tools.

| Face | Address |
|---|---|
| Browser page | `GET /_sysop/configuration/` |
| List | `GET /_sysop/configuration/profiles` |
| Read | `GET /_sysop/configuration/read?name=busy_hours` |
| Save | `POST /_sysop/configuration/save?name=busy_hours` with a JSON object body |
| Delete | `DELETE /_sysop/configuration/delete?name=busy_hours` |
| MCP | `POST /_sysop/mcp` — the tools `profiles`, `read`, `save`, `delete` |

```console
$ curl -X POST "http://127.0.0.1:8351/_sysop/configuration/save?name=busy_hours" \
    -H 'content-type: application/json' \
    -d '{"worker_max_users": 50, "user_idle_freeze_minutes": 10}'
{"name":"busy_hours","filename":"busy_hours.json","profile":{"worker_max_users":50,"user_idle_freeze_minutes":10}}

$ curl "http://127.0.0.1:8351/_sysop/configuration/profiles"
{"folder":"/var/spa/profiles","profiles":[{"name":"busy_hours","filename":"busy_hours.json","size":63,"modified_at":"2026-09-18T13:19:33.441951+00:00"}]}
```

## How a profile is validated

Two validations, in this order, and they check different things.

**The store.** `OrchestrationProfileStore` (`orchestration_profile_store.py`)
owns the storage: the name must match `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` with an
alphanumeric first character, so dotfiles and traversal are rejected and a
`.json` suffix may be given or omitted; symlinks are never listed, read,
overwritten or deleted; the content must be a JSON object, never an array or a
scalar, at most 1 MiB in either direction, with non-finite literals rejected at
read time; and a write is atomic — temporary file in the same folder, `fsync`,
`os.replace`. Its three exceptions become 400 for a bad name or bad content and
404 for a missing profile.

**The schema.** `GroupPolicy.from_settings` (`orchestration/group_policy.py`)
accepts any subset of the setpoint keys, fills the rest from the defaults, and
raises `GroupPolicyError` listing **every** violation it found — so an invalid
policy object cannot exist. It rejects an unknown key, a structural key a
profile may never carry, a `profile_version` other than `1`, a value of the
wrong type or out of range, and any of the cross rules in the
[configuration reference](../configuration.md#the-cross-rules).

```console
$ curl -X POST http://127.0.0.1:8351/_orchestration/apply \
    -H 'content-type: application/json' -d '{"worker_max_users": 0}'
{"error":"worker_max_users: 0 is out of range, must be >= 1"}
```

Note that the archive itself validates only the storage: it knows nothing about
the orchestration runtime, so saving a nonsense profile succeeds and applying it
is what fails.

## How a profile is applied at runtime

`OrchestrationControl` is mounted under the front's `_orchestration` root when
`control_enabled` is on, and carries three routes.

| Route | Method | What it does |
|---|---|---|
| `/_orchestration/apply` | POST | puts the body in force as the profile level — an inline configuration. Nothing stored stays active afterwards. |
| `/_orchestration/reload` | POST | reads a stored profile off the disk again and puts it in force. `{"name": ...}` names it; without a name the active profile is reread, and with no active profile it answers 400. |
| `/_orchestration/status` | GET | what is in force right now. No lock is taken to answer. |

```console
$ curl http://127.0.0.1:8351/_orchestration/status
{"active_profile":null,"generation":1,
 "last_apply":{"ts":"2026-09-18T13:06:17.831589+00:00","source":"boot",
               "active_profile":null,"digest":null,"outcome":"applied","generation":1},
 "effective_settings":{"worker_memory_admission_percent":80.0, ...}}

$ curl -X POST http://127.0.0.1:8351/_orchestration/reload \
    -H 'content-type: application/json' -d '{"name":"busy_hours"}'
{"outcome":"applied","source":"profile","active_profile":"busy_hours","generation":2,
 "changed_settings":{"worker_max_users":50,"user_idle_freeze_minutes":10},
 "effective_settings":{...}}
```

The effective configuration is composed **defaults ⊕ recipe ⊕ profile ⊕
environment**, and every apply recomposes that stack from the immutable recipe
and environment levels rather than stacking onto what is in force. So applying an
empty body returns the group to recipe and environment alone.

Inside `SpaCommander.apply_group_settings` the whole apply is serialized on one
lock, the profile read included, and runs in three stages: everything fallible
happens before anything moves; the swap itself is assignments with no `await`
between them, so no task can read the new policy without the new generation and
record; and the audit and the anticipated round come after and cannot undo it. A
refusal is written into `last_apply` as the last *attempt*, while the generation
and the active profile stay where they were.

## Gotchas

- **A profile governs exactly one group.** With zero or several, the boot fails
  and a runtime apply answers **409**.
- **Structural keys are refused by name.** `entry_module`, `executable`,
  `worker_class`, `worker_kwargs`, `engine_factory`, `engine_kwargs`,
  `main_threadpool_size` and `aux_threadpool_size` build the group's processes
  once and can only change in the recipe.
- **`env_settings` wins.** It is the strongest level of the overlay, so a
  setpoint the installation fixed by environment cannot be moved by a profile.
- **The archive applies nothing.** Saving a profile changes no running pool;
  `reload` is what puts it in force.
- **A body that is not a JSON object is a 400 before the commander sees it**, so
  it never reaches the orchestration log.
- **A pool that is not running takes no configuration.** Before the front has
  built its commander, or once the server has left `RUNNING`, all three routes
  answer **503** with `Retry-After`.
