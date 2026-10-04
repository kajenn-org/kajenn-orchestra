# Configuration reference

Every key the SPA front adds to a kajenn recipe, with its type, default and
effect. The grammar is `SpaApplicationGrammar` in `spa_app.py`; the setpoint
schema, including the defaults listed here, is `GroupPolicy` in
`orchestration/group_policy.py`.

The shape is one subtree under the front's own application node:

```python
front = cfg.applications().application(
    code="spa", mount="", app_class=SpaApplication
)
orchestration = front.orchestration(
    profiles_path="/var/spa/profiles", profile_name="busy_hours", control_enabled=True
)
commander = orchestration.commander(
    frozen_users_path="/var/spa/frozen", instance_dir="/run/kajenn"
)
commander.groups(default="standard").group(name="standard")
```

`orchestration` is required. A front declared without it, or with it and no
`commander` under it, raises `FatalBootError` and the server does not start.
Wanting no pool means declaring no SPA front.

Every numeric key accepts a `BagResolver` in place of a literal, which is how a
recipe defers a value.

## `orchestration`

| Key | Type | Default | Effect |
|---|---|---|---|
| `profiles_path` | `str` | `None` | The folder stored profiles are read from — the same one the `_sysop` archive writes. |
| `profile_name` | `str` | `None` | The profile the boot must find and put in force. Named without a folder, missing, or invalid: the server does not start. |
| `control_enabled` | `bool` | `False` | Mounts the runtime configuration under the front's `_orchestration` root. Off, that path belongs to the hosted site. |

`env_settings` is not a grammar word. It is a `dict` passed as a constructor
keyword argument of `SpaApplication`, and it is the strongest level of the
overlay — above anything a recipe or a profile says.

Three words that used to sit on the application element are refused by name with
the new path in the message: `profiles_path`, `profile_name` and
`orchestration_control`, the last renamed `control_enabled`.

## `commander`

Declared once; its two paths are shared by every group.

| Key | Type | Default | Effect |
|---|---|---|---|
| `frozen_users_path` | `str` | — | The freezer root, read by the commander and written by every worker. |
| `instance_dir` | `str` | — | Where the workers' Unix sockets live. Keep it short: the OS caps `AF_UNIX` path length. |
| `memory_max_percent` | `float` | all of it | What this server may hold of the machine — the concession every percentage below is a share of. |
| `machine_memory_alarm_percent` | `float` | — | The health line of the whole machine. Past it, `state` becomes `saturated` and nothing grows. |
| `orchestration_log_path` | `str` | `None` | The file every order lands on. Omitted, the rows stay on the logger. A `.decisions.jsonl` file is written beside it. |
| `orchestration_log_max_bytes` | `int` | — | The size at which that file rotates. |
| `orchestration_log_backup_count` | `int` | — | How many rotations are kept. |
| `user_expiry_hours` | `float` | — | How long a frozen user is kept before the machine forgets him whole. |
| `guest_expiry_hours` | `float` | — | The same for a browser that never logged in; shorter. |
| `cpu_temperature_sample_seconds` | `float` | 0.1 s | The cadence of commander-side, traffic-independent worker CPU sampling. |

The beat, the patience of a departure and the other technical times are module
constants, not grammar.

## `groups`

| Key | Type | Default | Effect |
|---|---|---|---|
| `default` | `str` | first declared | The group that receives whoever arrives with no past. |

Each child `group` is labelled by its `name`, giving stable configuration paths
`applications.<code>.orchestration.commander.groups.<name>`.

## `group` — identity of the child

These keys build the group's processes once. A profile may never carry them:
`GroupPolicy.from_settings` rejects each with `structural, not a profile key`.

| Key | Type | Default | Effect |
|---|---|---|---|
| `name` | `str` | — | The collection key. It names the workers too (`<name>_0001`), and their sockets, so keep it short. |
| `entry_module` | `str` | — | The module the child runs under `python -m`. Normally `kajenn_orchestra.orchestration.worker_entry`. |
| `executable` | `str` | this interpreter | The interpreter to spawn with. Two groups are how two versions of a site live side by side. |
| `worker_class` | `str` | the base worker | The `module:Class` the child loads. A real deployment names a `SpaWorker` subclass; the base one hosts no site. |
| `worker_kwargs` | `dict` | `None` | What that class is built with; travels as `kwargs` in the spawn payload. |
| `main_threadpool_size` | `int` | interpreter default | The child's traffic pool: WSGI stitching and long calls. |
| `aux_threadpool_size` | `int` | interpreter default | The child's service pool: freezer IO. Much smaller. |
| `engine_factory` | `str` | `None` | The `module:Class` whose `build_group_engine()` builds the one expensive object this group's workers share. Declared, the group runs a template process and forks every worker out of it. |
| `engine_kwargs` | `dict` | `None` | What that factory class is built with. |

## `group` — setpoints

These are the profile keys. Every one of them may be overridden at runtime by a
profile or by `env_settings`; `null` means unlimited or off where the table says
so.

| Key | Type | Default | Effect |
|---|---|---|---|
| `memory_max_percent` | `float`, 0 exclusive to 100 | `100.0` | This group's share of the server's concession. |
| `worker_max_number` | `int` >= 1 | `6` | How many workers the quota is *sized for*: the per-worker ceiling is `100 / worker_max_number`. It caps nothing — the number of processes stays a reading. |
| `worker_memory_max_percent` | `float` > 0, nullable | `null` | What one worker may hold of the group's quota. `null` leaves the derivation above in charge; an explicit value wins. |
| `worker_memory_admission_percent` | `float`, 0 to 100 | `80.0` | Past this share of its ceiling a worker admits no new user. Must stay below `restart_occupancy_max_percent`. |
| `restart_occupancy_max_percent` | `float`, 0 to 100 | `95.0` | Past this memory occupancy a process is replaced instead of kept. |
| `worker_max_users` | `int` >= 1, nullable | `null` | How many placed users one worker takes. `null` is unlimited. |
| `worker_min_life_seconds` | `float` >= 0 | `60.0` | Before this age a worker is never the one closed: its occupancy measures its own birth. |
| `worker_admission_interval_seconds` | `float` >= 0 | `1.0` | How long after admitting a user a worker is skipped by the placement, so its load shows in the temperature first. `0` switches the rule off. |
| `user_idle_freeze_minutes` | `float` > 0, nullable | `null` | The silence past which the group parks a user in the freezer. `null` parks nobody. |
| `cpu_close_percent` | `float`, 0 to 100, nullable | `null` | The temperature, shared onto the survivors, under which a worker is a closure candidate. `null` means `cpu_admission_reopen_percent` itself, and a set value must not exceed it. |
| `cpu_admission_close_percent` | `float`, 0 to 100, nullable | `null` | Turns on soft CPU admission: a worker above it takes no new users. `null` leaves the policy off. |
| `cpu_admission_reopen_percent` | `float`, 0 to 100 | `40.0` | Below this a worker's admission reopens. The band between the two is hysteresis; it must sit below `cpu_admission_close_percent`. |
| `cpu_offload_percent` | `float`, 0 to 100, nullable | `null` | Past this, a CPU-closed worker cedes one active user per beat to the freezer. Requires `cpu_admission_close_percent` and must sit above it. |
| `cpu_retirement_quiet_seconds` | `float` >= 0 | `60.0` | How long the CPU must stay silent — no worker blocked or reopened — before the closure judge runs again. |
| `cpu_heating_seconds` | `float` > 0 | `1.0` | The time constant of the temperature filter while a worker heats up. |
| `cpu_cooling_seconds` | `float` > 0 | `5.0` | The same while it cools down; longer, so one idle sample never reopens a worker. |

Nothing here says how many workers a group has. It launches one at boot, grows
inside `assign_user` when a real user finds no worker that admits him, and
shrinks when a worker's capacity is spare.

### The cross rules

`GroupPolicy.from_settings` raises `GroupPolicyError` listing every violation it
found, so an invalid policy object cannot exist. Beyond the per-key ranges:

- `worker_memory_admission_percent` must stay strictly below
  `restart_occupancy_max_percent`.
- With CPU admission on,
  `0 <= cpu_admission_reopen_percent < cpu_admission_close_percent <= 100`.
- `cpu_close_percent`, when both it and CPU admission are set, must not exceed
  `cpu_admission_reopen_percent`: a closure must never create the condition for
  the next birth.
- `cpu_offload_percent` requires `cpu_admission_close_percent` and must sit
  strictly above it. The order is reopen < close < offload.

### Profile format

A profile is a JSON object of the setpoint keys above. `profile_version` is
optional and only version `1` is accepted. `null` reads as unlimited or off
exactly as the table says, and `GroupPolicy.to_settings` writes the values back
in the same form, so its output always survives
`json.dumps(..., allow_nan=False)`.

```json
{
  "profile_version": 1,
  "worker_max_users": 50,
  "user_idle_freeze_minutes": 10
}
```

## The memory cascade

Four rungs, and only the bottom one is bytes.

| Rung | Read against | Key |
|---|---|---|
| Machine | what the OS or the cgroup reports | `machine_memory_alarm_percent` |
| Concession | the machine | `commander.memory_max_percent` |
| Group quota | the concession | `group.memory_max_percent` |
| Worker ceiling | the group quota | `group.worker_memory_max_percent`, or `100 / worker_max_number` |

A server inside a container reads the cgroup limit under `/sys/fs/cgroup` as
well and uses it in place of both host figures where it is smaller, so a growth
gate never sees 64 GiB where the container may take 2.

## The front's own roots

| Root | When it exists | What it serves |
|---|---|---|
| `_wsx` | always | `openchannel`, the command a page must send before any other message reaches its worker |
| `_orchestration` | `control_enabled=True` | `apply`, `reload`, `status` — the runtime configuration |

Both are first-level segments the hosted site loses. Any other path goes to the
site.
