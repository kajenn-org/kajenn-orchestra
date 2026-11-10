# Changelog

All notable changes to kajenn-orchestra are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Releases before 0.6.0 are listed
on [GitHub Releases](https://github.com/kajenn-org/kajenn-orchestra/releases).

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
