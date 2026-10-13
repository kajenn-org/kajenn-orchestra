# Group bus

**Version**: 0.2 · **Last Updated**: 2026-10-12 · **Status**: 🔴 DA REVISIONARE

Processes exchange messages over a Unix socket through the `kbus` library
(kajenn-kbus ≥ 0.1.2).

History: until 0.4 the wire was kajenn's own framing over one Unix socket per
worker, with a connector per socket correlating calls and replies by id. Since
0.5.0 the transport is `kbus`: each group owns a `kbus.Dispatcher`, the group
and its workers are members of it, and `kbus` owns framing, correlation,
cancel and member admission. Orchestra keeps the routes, the payload codec
(`LanePayload`), the presentation and the worker lifecycle.

Interactions: orchestration (worker wire, presentation, envelope chain) ·
console (eval over a worker call).
