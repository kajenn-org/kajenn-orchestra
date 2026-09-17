# How-to Guides

> **Status:** Draft; implementation checked against the development source on 2026-09-08.

Task-focused recipes for the worker pool `kajenn-orchestra` adds to a `kajenn`
server. The server itself, its routed applications and its configuration recipe
are documented in `kajenn`.

```{toctree}
:hidden:

multiworker-spa
```

## The guides

- **[Multiworker SPA](multiworker-spa.md)** — package boundary, hosted applications and global store.

## How each guide is structured

Capability recipes use the following sections where applicable:

1. **What it does** — the capability in one or two sentences.
2. **When to use it** — the situation that calls for it.
3. **Setup** — the constructor kwargs or base class you need in place.
4. **Minimal snippet** — the smallest copy-pasteable example that works.
5. **How to verify it** — a concrete check (a `curl`, a request, an observed
   effect) that proves it is working.
6. **Gotchas** — the sharp edges worth knowing before you hit them.
