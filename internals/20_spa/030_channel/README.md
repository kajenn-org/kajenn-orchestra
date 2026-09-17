# Channel

**Version**: 0.1 · **Last Updated**: 2026-08-24 · **Status**: 🔴 DA REVISIONARE

Verification: `kajenn-meta/verification/20_spa/030_channel.md` — DIVERGENT, 1 CONVERGE / 3 DIVERGE / 0 SILENT.

The wire every conversation between processes rides: `Frame` and `FrameStream`
over Unix sockets, `ChannelHub`, `ChannelClient`, `LocalChannel`. It is consumed
by the communication mixin and by the orchestration worker wire.
