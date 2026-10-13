# Group bus

**Version**: 0.2 · **Last Updated**: 2026-10-12 · **Status**: 🔴 DA REVISIONARE

Verification: `kajenn-meta/verification/20_spa/030_channel.md` — DIVERGENT, 1 CONVERGE / 3 DIVERGE / 0 SILENT (verified before the move to `kbus`).

The wire every conversation between a group and its worker processes rides: one
`kbus.Dispatcher` per `GroupHandler`, on one Unix socket per group. The group
joins it in-process as member `group`; each worker joins as a member under its
own name.
