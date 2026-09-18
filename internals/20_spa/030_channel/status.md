# Channel — current state

**Version**: 0.2 · **Last Updated**: 2026-09-08 · **Status**: 🔴 evidence refreshed; design ratification unchanged

Verified against source revision `2465fcc` (develop baseline). Test references
below identify the executable contracts; they are not a new coverage percentage.

## Generic channel and SPA worker protocol

`Frame` and `FrameStream` live in `kajenn.channel.frame`. The wire is a
four-byte big-endian length followed by `WSX://` and JSON. `FrameStream` limits
size (default 16 MiB), rejects invalid framing and reports EOF as no next frame.
`ChannelHub`, `ChannelClient` and `LocalChannel` serve generic communication.

The SPA `WorkerConnector` and worker layer use correlated CALL/REPLY frames on
one socket, in both directions. Replies resolve pending calls inline; inbound
calls run as tasks so a slow operation does not stop frame reading. A dead wire
fails parked calls and notifies the owning worker handler. The underlying
Frame id is therefore already used for request/reply, despite older generic
frame docstrings calling that a future extension.

Claim anchors: [`Frame`](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/channel/frame.py#L61), [`FrameStream`](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/channel/frame.py#L258), [`ChannelHub`](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/channel/hub.py#L136), [`ChannelClient`](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/channel/client.py#L54), [`LocalChannel`](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/channel/local.py#L113), [`WorkerConnector`](../../../src/kajenn_orchestra/orchestration/worker_connector.py#L144).

## Payload ownership and current limits

The connector passes envelopes to the handler; the orchestration envelope chain
interprets worker events and snapshots. HTTP body bytes are currently base64
inside JSON-safe payloads. The global store has no replicated worker snapshot:
reads and leases are explicit calls to the commander. Historical connector
comments about replicas are superseded by #74's implementation.

Browser `WsxEnvelope` is a different codec: its data uses TYTX within the WSX
text envelope. Do not substitute it for the internal Frame format merely
because both start with `WSX://`. The proposed opaque transport of #72 is not
part of this baseline. The earlier cross-entry 'no application WebSocket'
friction was overtaken by the delivered WSX and raw application seam.

Claim anchors: [`WsxEnvelope`](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/wsx.py#L102).

## Source and test evidence

- [src/kajenn/channel/frame.py](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/channel/frame.py)
- [src/kajenn/channel/hub.py](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/channel/hub.py)
- [src/kajenn/channel/client.py](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/channel/client.py)
- [src/kajenn/channel/local.py](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/channel/local.py)
- [src/kajenn_orchestra/orchestration/worker_connector.py](../../../src/kajenn_orchestra/orchestration/worker_connector.py)
- [src/kajenn_orchestra/orchestration/spa_worker.py](../../../src/kajenn_orchestra/orchestration/spa_worker.py)
- [src/kajenn/wsx.py](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/wsx.py)
- [tests/core/test_channel.py](https://github.com/kajenn-org/kajenn/blob/main/tests/core/test_channel.py)
- [tests/core/test_channel_hub.py](https://github.com/kajenn-org/kajenn/blob/main/tests/core/test_channel_hub.py)
- [tests/core/test_channel_local.py](https://github.com/kajenn-org/kajenn/blob/main/tests/core/test_channel_local.py)
- [tests/spa/orchestration/test_orchestration_worker_connector.py](../../../tests/spa/orchestration/test_orchestration_worker_connector.py)
- [tests/spa/orchestration/test_contract_phase7_worker_call_lane.py](../../../tests/spa/orchestration/test_contract_phase7_worker_call_lane.py)
