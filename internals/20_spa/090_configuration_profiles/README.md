# Orchestration profile archive

**Version**: 0.2 · **Last Updated**: 2026-09-08 · **Status**: 🔴 DA REVISIONARE

## Purpose

`ConfigurationProfilesApplication` is a persistent archive of named JSON
orchestration profiles. It stores JSON objects in one directory and exposes
them over three faces: a browser page, a REST API and MCP tools. It applies
nothing: the application has no dependency on the orchestration runtime, and
saving a profile changes nothing in a running pool.

Applying a stored profile to a live pool is a separate mechanism and lives
elsewhere: the `SpaApplication` front reads a profile at boot and exposes the
hot apply under its own `_orchestration` root — see *Applying a profile* below.
Both sides share one storage component, `OrchestrationProfileStore`
(`src/kajenn_orchestra/orchestration_profile_store.py`): name validation, symlink refusal, the
1 MiB limit, the object-only JSON read and the atomic write live there, and
this archive delegates to it.

## Composition

- `ConfigurationProfiles` (a `RoutingClass`) owns the directory and the CRUD
  routes; the constructor takes `folder: str | Path` (created if missing,
  relative paths resolve against the CWD).
- `ConfigurationProfilesApplication` (a `McpOpenApiApplication`) mounts it
  under `api_name="configuration"` with `code = mount = "_sysop"`.

Mounting is the gate: a server that does not construct this application has no
write surface at all.

## Surfaces

| Face | Address |
|------|---------|
| Browser page | `GET /_sysop/configuration/` (trailing slash; the page redirects itself) |
| REST list | `GET /_sysop/configuration/profiles` |
| REST read | `GET /_sysop/configuration/read?name=foo` |
| REST save | `POST /_sysop/configuration/save?name=foo` (JSON object body) |
| REST delete | `DELETE /_sysop/configuration/delete?name=foo` |
| MCP JSON-RPC | `POST /_sysop/mcp` — tools `profiles`, `read`, `save`, `delete` |
| OpenAPI docs | `/_sysop/_meta/docs` |

Errors follow the server's single JSON error path: `{"error": ...}` with 400
for an invalid name, a non-object body, a corrupted or oversized file, or a
symlink; 404 for a missing profile.

## Profile names and format

- A profile is one file, `<name>.json`, in the configured folder.
- Names match `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` — the leading character is
  alphanumeric, so dotfiles and traversal are rejected; `.json` may be given
  or omitted in every call.
- Symlinks are never listed, read, overwritten or deleted.
- The content must be a JSON object (never an array or scalar), at most 1 MiB.
- Writes are atomic: temporary file in the same folder, `fsync`, `os.replace`.

Example profile:

```json
{
  "cpu_admission_close_percent": 50,
  "cpu_admission_reopen_percent": 40,
  "cpu_retirement_quiet_seconds": 60,
  "cpu_heating_seconds": 1.0,
  "cpu_cooling_seconds": 5.0,
  "cpu_close_percent": 35,
  "worker_admission_interval_seconds": 1.0,
  "worker_memory_admission_percent": 80
}
```

## Applying a profile

Reading a profile and putting it in force belongs to the SPA front
(`src/kajenn_orchestra/spa_app.py`), never to this archive. The front
reads three words off its own `orchestration` node — `profiles_path`,
`profile_name`, `control_enabled`, at `applications.<code>.orchestration` — plus
`env_settings`, a runtime dict the Python recipe hands to the application element
and no grammar declares. The commander, the groups and the workers hang under
that same node: `application → orchestration → commander → groups → group`. The
node is REQUIRED and so is the commander under it: a spa front declared without
either does not boot — a front with no pool serves nothing, so the incomplete
configuration is refused rather than started.

**Boot read — four levels, one composition.** `boot_group_settings` composes
the effective configuration BEFORE the vertex is built, as
`defaults ⊕ recipe_settings ⊕ profile ⊕ env_settings`, through
`GroupPolicy.from_settings` (`src/kajenn_orchestra/orchestration/group_policy.py`
— the frozen dataclass that declares the setpoints, IS the validation and
collects every violation). The defaults are the dataclass fields; the recipe
level is what the recipe wrote; the profile level is the named stored profile;
`env_settings` is the last word. The two immutable levels — recipe and env —
are kept as SEPARATE dicts on the vertex and never pre-merged: every later
apply recomposes from them. Structural keys (group shape, not setpoints) are
handed through untouched and are refused as profile keys. A named profile that
does not exist, a profile that does not validate, or an env level with nowhere
to land raises `FatalBootError` ([`src/kajenn/lifespan.py`](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/lifespan.py)) — the one
exception an `on_startup` hook may raise to make the boot fail: the lifespan
answers `lifespan.startup.failed` and the server exits. There is no silent
fallback.

**Hot apply — one lock, three stages.** `SpaCommander.apply_group_settings`
holds `_configuration_lock` for the whole apply, the off-loop profile read
included, so concurrency serialises instead of colliding. Stage 1 is fallible
and mutates nothing: the profile read, the composition, the effective settings,
the changed diff, the sha256 digest of the canonical JSON, the CPU
reconciliation list and the response payload, all built in advance. Stage 2 is
a plain `def` of guaranteed assignments with no `await`: `GroupHandler.apply_policy`,
then `active_profile`, `configuration_generation` (+1 on every success, even an
idempotent one) and `last_apply`. Stage 3 is best effort: the audit lines and
`ping_now()`, each in its own `try/except`. A refusal preserves effective policy, active profile and generation; it still
updates rejection diagnostics in `last_apply` and the audit destinations.

**One group per profile — a phase constraint.** A named profile addresses
exactly one group. A machine with several groups fails the boot when a profile
or an env level is given, and answers 409 (`SingleGroupRequired`) on a hot
apply or a status read.

**Routes**, mounted under `_orchestration` only when `control_enabled` is on
(gate off, the root is not claimed and the path reaches the hosted site). The
mount is the LAST thing the boot does, after the vertex is built and started, so
a boot that fails leaves the router exactly as it was; a second startup on the
same front mounts nothing twice, and a root the front's own router already claims
is a fatal boot rather than a silent half-mount:

| Route | Effect |
|-------|--------|
| `POST /_orchestration/apply` | the body IS the profile level — an inline configuration; the active profile becomes null |
| `POST /_orchestration/reload` | `{"name": ...}`, or the active profile again; 400 with neither |
| `GET /_orchestration/status` | read-only, no lock: `active_profile`, `generation`, `last_apply`, `effective_settings` |

Refusals: 400 an invalid profile (every violation joined in the message) or a
body that is not a JSON object, 404 a profile that does not exist, 409 not
exactly one group, 503 no pool or a server that has left RUNNING.

**Audit destinations.** Every apply and every refusal writes to the
orchestration log through `SpaCommander.log_order` — `apply_group_settings`
(with generation, digest, source, the changed keys, the violations on a
refusal; outcome `applied` or `rejected: <first violation>+N`) and
`cpu_policy_reconciled` — falling back to the module logger if that log fails.
The last attempt, applied or rejected, also stays readable on `last_apply`.

## Security posture (current, deliberate)

The mount carries **no authentication**: whoever reaches the port can read and
write profiles. This is a temporary, conscious choice for development and lab
use — the application is opt-in at composition time and MUST NOT be mounted on
a production server until the sysop surface is gated.

## genropy usage

The genropy-asgi recipe mounts the archive only when
`GNR_ASGI_ORCHESTRATION_PROFILES` is set; the folder defaults to
`<site>/data/_orchestration_profiles`, overridable with
`GNR_ASGI_ORCHESTRATION_PROFILES_PATH`. See `docs/configuration.rst` in
genropy-asgi.
