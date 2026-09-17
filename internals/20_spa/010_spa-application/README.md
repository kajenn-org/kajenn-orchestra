# SPA application (the front)

**Version**: 0.1 · **Last Updated**: 2026-08-24 · **Status**: 🔴 DA REVISIONARE

Verification: `kajenn-meta/verification/20_spa/010_spa-application.md` — DIVERGENT, 0 CONVERGE / 1 DIVERGE / 3 SILENT.

The stateless door of the SPA world: the `spa_connection_id` cookie carrying the
hosted site's OWN connection id, the two-stage demux between the internal roots
and the hosted site, and the HTTP translation of the pool's answers — 503 with
`Retry-After` for a refusal, 502 for a site failure.
