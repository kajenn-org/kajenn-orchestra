# Console

**Version**: 0.1 · **Last Updated**: 2026-08-24 · **Status**: 🔴 DA REVISIONARE

Verification: `kajenn-meta/verification/20_spa/070_console.md` — DIVERGENT, 0 CONVERGE / 0 DIVERGE / 0 SILENT.

The pool's debug door, served as MCP tools by `SpaConsoleMcpApplication`: `eval`
of a Python expression in the commander process or in any worker, over the lane
the commander already holds. Mounting IS the gate — it is full eval by
construction, and it is never mounted in production.
