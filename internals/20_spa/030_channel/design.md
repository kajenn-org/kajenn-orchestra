# Channel

**Version**: 0.1 · **Last Updated**: 2026-09-08 · **Status**: 🔴 DA REVISIONARE

Processes exchange framed messages over Unix sockets through channels.

The wire between processes: `Frame` / `FrameStream` over Unix sockets,
`ChannelHub` / `ChannelClient` / `LocalChannel`. Consumed by the
communication mixin (`parent_channel`) and by the orchestration worker wire
(`worker_connector`, presentation, the lane).

Interactions: orchestration (worker wire) · communication mixin · console (eval over the lane).
