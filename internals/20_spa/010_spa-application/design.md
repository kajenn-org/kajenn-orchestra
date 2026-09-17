# SPA application (the front)

**Version**: 0.1 · **Last Updated**: 2026-08-22 · **Status**: 🔴 DA REVISIONARE

**The need.** A browser must reach its hosted SPA site through one stable door that never forgets who the browser is — without the door keeping any state of its own.

The stateless door of the SPA world: the `spa_connection_id` cookie
carrying the hosted site's OWN connection id (the front mints nothing),
the two-stage demux (internal roots vs the hosted site), and the HTTP
translation of the pool's answers — 503 with `Retry-After` for a refusal,
502 for a site failure, generic lines outward and the real text in the log.

Interactions: orchestration (`SpaCommander.serve_request` is the single forward) · configuration (grammar) · inspector/console.

## One request, from cookie to answer

```mermaid
sequenceDiagram
    participant B as Browser
    participant F as SpaApplication (front)
    participant C as SpaCommander
    participant W as SpaWorker
    participant S as hosted WSGI site
    B->>F: request (spa_connection_id cookie, when the site named one)
    F->>C: serve_request(cid, http, hold_timeout)
    C->>C: resolve_user(cid) — waits on the per-user barrier if on hold
    C->>W: forward on the lane (guest → reception; known user → his home)
    W->>S: WSGI call through WsgiSeam
    S-->>W: answer — the site names its OWN connection while serving
    W-->>C: REPLY (announcements fold up the envelope chain and write the indexes)
    C-->>F: reply payload
    F-->>B: HTTP answer — cookie rewritten only when the id differs
```

The two failure translations: `AssignmentRefused` → **503** with the
`Retry-After` the vertex composed; `SiteFailedRequest` / a dead wire → **502**.
Outward always a generic line; the real text goes to the log.
