# Orchestration

**Version**: 0.1 · **Last Updated**: 2026-08-24 · **Status**: 🔴 DA REVISIONARE

Verification: `kajenn-meta/verification/20_spa/020_orchestration.md` — DIVERGENT, 5 CONVERGE / 2 DIVERGE / 0 SILENT.

Many users with live server-side state, scaled across processes, with no user
ever split: all of a user's pages live in the process that holds his store. The
chain is `SpaCommander` → n `GroupHandler` → n `WorkerHandler` → `SpaWorker`,
and mobility has one path only — hold, freeze, reassign, unfreeze.
