# Guides

Task-focused recipes for the worker pool `kajenn-orchestra` adds to a `kajenn`
server. The server itself, its mounted applications and its configuration recipe
are documented in
[kajenn](https://kajenn.readthedocs.io/en/latest/).

```{toctree}
:hidden:

multiworker-spa
hosting-an-application
mobility
configuration-profiles
console
```

- **[The multiworker SPA](multiworker-spa.md)** — the package boundary, the
  identity a request carries, and the global store.
- **[Hosting an application](hosting-an-application.md)** — the `SpaWorker`
  subclass contract, the `asgi_app` and `wsgi_app` seams, and what a hosted
  request receives.
- **[Freeze and reassignment](mobility.md)** — the one path a user takes between
  two processes, what orders it, and what a browser sees.
- **[Configuration profiles](configuration-profiles.md)** — the `_sysop`
  archive, how a profile is validated, and how it is applied at runtime.
- **[Watching a pool](console.md)** — the inspector section and the MCP console,
  and why mounting is the only gate either has.
