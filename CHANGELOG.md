# Changelog

All notable changes to kajenn-orchestra are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Releases before 0.6.0 are listed
on [GitHub Releases](https://github.com/kajenn-org/kajenn-orchestra/releases).

## [0.8.0] - 2026-11-17

### Breaking

- kajenn-orchestra requires kajenn 0.10.0 or later. The breaking changes of
  kajenn 0.10.0 apply, listed in its changelog: authentication belongs to the
  application mounted with code `_auth`, and the recipe element
  `authentication` is removed.

### Added

- `SpaApplication.prometheus_metrics` adds the pool's families, prefixed
  `kajenn_orchestra_`, to the kajenn endpoint `GET /_server/metrics`
  (kajenn-org/kajenn#141):
  - per group (label `group`): living workers, the sum and the largest value
    of one worker of what the workers report, the memory quota and the memory
    concession;
  - for the pool: users, connections and pages the commander knows, and the
    commander's events counted by `event`.
  Before the pool is built only the kajenn families are returned (#44).
- `prometheus_client` is a dependency.

## [0.7.0] - 2026-11-13

### Breaking

- `TemplateEntry` and `WorkerEntry` are no longer exported by
  `kajenn_orchestra.orchestration`. Import them from their modules,
  `kajenn_orchestra.orchestration.template_entry` and
  `kajenn_orchestra.orchestration.worker_entry` (#36).

### Added

- The container image `ghcr.io/kajenn-org/kajenn-orchestra`: kajenn-orchestra
  on top of the kajenn image, started with `kajenn serve` on `/app/config.py`.
  Published at every release as `X.Y.Z` and `latest`, and every night from
  `develop` as `develop` and `develop-YYYYMMDD`. `/image-packages.txt` lists
  its Python packages with their versions (#38).

### Fixed

- A template or a worker started with `python -m` loads its entry module
  once: the `RuntimeWarning` at every start is gone (#36).
- The template closes the answer channel it opened: the `ResourceWarning`
  about an unclosed file when a template leaves is gone (#36).

## [0.6.1] - 2026-11-12

### Fixed

- An exception raised by the application hosted in a worker answers
  `500 Internal Server Error` instead of `502 Bad Gateway`, over HTTP and on
  the page socket. The traceback goes to the log, never in the body; a
  response the application had started is dropped whole. An `HTTPException`
  raised by the application keeps its status. `502` stays for a worker that
  produced no answer (#27).

## [0.6.0] - 2026-11-10

### Breaking

- Requires kajenn 0.9.0.
- `ConfigurationProfilesApplication` is a `RoutedApplication` that attaches its
  routing class as the branch `configuration`. The HTTP URLs
  `/_sysop/configuration/...` do not change. MCP answers on
  `/_sysop/_meta/mcp` and the tools are named `configuration.profiles`,
  `configuration.read`, `configuration.save`, `configuration.delete`.
  `openapi_info` moves from `ConfigurationProfiles` to the application.
- `SpaConsoleMcpApplication` is a `RoutedApplication` that attaches
  `SpaConsole` as the branch `console`. MCP answers on `<mount>/_meta/mcp` and
  the tools are named `console.targets` and `console.eval`.
- The routes of the profile archive declare the channel `api` instead of
  `rest`.
- A `SpaApplication` that cannot build its pool no longer stops the server. It
  gives up through `server.application_failed`; being `mandatory` by default,
  it leaves only `_server` answering and `/readyz` not ready, and the process
  stays up. A recipe that wants the other applications to keep serving writes
  `mandatory=False` on the application node.

### Added

- `SpaApplication.mandatory`, `True` by default.
- `SpaApplication.build_pool()`: the boot of the pool that `on_startup` runs.
  Called directly, a refusal still raises `FatalBootError`.

### Fixed

- `/_wsx/openchannel` declares the channel `wsx`, so the websocket reaches it
  under kajenn 0.9, where a route that declares no channel answers on `api`
  only.
