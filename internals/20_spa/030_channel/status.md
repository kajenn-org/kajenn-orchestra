# Group bus — current state

**Version**: 0.3 · **Last Updated**: 2026-10-12 · **Status**: 🔴 evidence refreshed; design ratification unchanged

Verified against the source of the `wf/orchestra-on-kbus` branch. Test
references below identify the executable contracts.

## Topology

Each `GroupHandler` owns one `kbus.Dispatcher` (`self.bus`) and an in-process
member named `group` (`GROUP_MEMBER`). `open_bus` runs on the first launch of a
worker: it creates `<instance_dir>/<group>.sock` in a 0700 directory, clears a
stale socket, listens on `unix://` and connects the `group` member. `stop`
closes the bus after the workers.

Every launch mints a fresh `secrets.token_hex(32)` into
`dispatcher.secrets[<worker name>]` (`WorkerConnector.start`). The previous
token stops admitting, so a predecessor process cannot rejoin. The worker gets
`unix://<name>:<token>@<socket path>` as `spawn_payload["kbus_url"]`, inside
`KAJENN_WORKER`; the forked path carries the same payload.

Claim anchors: [`GroupHandler.open_bus`](../../../src/kajenn_orchestra/orchestration/group_handler.py#L1517), [`GroupHandler.bus`](../../../src/kajenn_orchestra/orchestration/group_handler.py#L503), [`WorkerConnector.start`](../../../src/kajenn_orchestra/orchestration/worker_connector.py#L114), [`WorkerHandler.spawn_payload`](../../../src/kajenn_orchestra/orchestration/worker_handler.py#L340), [`GROUP_MEMBER`](../../../src/kajenn_orchestra/orchestration/worker_connector.py#L43).

## Routes and presentation

A route is `<member>.<path>`: `standard_0001./group/ping` downwards,
`group./group/announce` upwards. The worker side always addresses `group`.
After `Member.connect` the worker's first call is `group./register`
(`REGISTER_PATH`) with pid, config and worker snapshot. The reply carries what
`WorkerHandler.read_envelope` composed, the global store included. A call
before the presentation is refused with `kbus.Refused`.

Claim anchors: [`WorkerEntry.connect`](../../../src/kajenn_orchestra/orchestration/worker_entry.py#L188), [`SpaWorker.attach_member`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L1172), [`WorkerConnector.serve`](../../../src/kajenn_orchestra/orchestration/worker_connector.py#L193), [`REGISTER_PATH`](../../../src/kajenn_orchestra/orchestration/worker_connector.py#L46).

## Meta and payload

`meta` carries routing scalars only: `format`, `cid`, `identity`,
`user_frozen`, `status`, `connection_id`. Envelope slots (worker events,
worker snapshot, presentation fields) and the `error` of a reply, whose text
has no bound, travel in the payload.
`LanePayload` encodes both forms on both sides. `control-json` is one compact
JSON object with `result` and the envelope slots; NaN, infinity and duplicate
keys raise `ValueError`. `http` is the 4-byte big-endian length of the envelope
JSON, the envelope JSON, then the `HttpRecord` bytes. HTTP is one call and one
reply; no stream is used.

Claim anchors: [`LanePayload`](../../../src/kajenn_orchestra/orchestration/lane_payload.py#L51), [`SpaApplication.build_response`](../../../src/kajenn_orchestra/spa_app.py#L1146), [`SpaApplication.pack_http`](../../../src/kajenn_orchestra/spa_app.py#L1190).

## Death and errors

`on_disconnect` routes to `GroupHandler.on_member_lost`, which calls
`WorkerConnector.lose`. A presented child that was not stopped becomes
`WorkerHandler.on_child_lost`. No relaunch happens on disconnect. A deliberate
`WorkerConnector.stop` forgets the token and announces nothing.

At the call boundary of both sides `kbus.LinkLost`, `kbus.NoSuchMember` and
`kbus.Unreachable` (`LINK_GONE`), and `kbus.Overloaded`, become
`ConnectionError`. `SpaCommander` and `SpaApplication.wire_lost_response` turn
it into 502 while running, 503 while leaving. On the worker side `kbus.Refused`
and `kbus.RemoteError` become `CommanderCallFailed`; the group refuses a call
from a worker it no longer has with `kbus.Refused`.

A call runs on a task of its own and its caller waits on it shielded. A caller
whose timeout runs out, or who is cancelled, leaves the order running on the
other side: no cancel is sent, and on the group side the reply that lands late
is still folded, so the events it carries reach the chain. A reply over the
frame ceiling raises `kbus.FrameTooLarge` and is replaced by an error reply.

Claim anchors: [`GroupHandler.on_member_lost`](../../../src/kajenn_orchestra/orchestration/group_handler.py#L1537), [`WorkerConnector.lose`](../../../src/kajenn_orchestra/orchestration/worker_connector.py#L212), [`LINK_GONE`](../../../src/kajenn_orchestra/orchestration/worker_connector.py#L60), [`WorkerConnector.call_message`](../../../src/kajenn_orchestra/orchestration/worker_connector.py#L170), [`SpaApplication.wire_lost_response`](../../../src/kajenn_orchestra/spa_app.py#L1101).

## Browser codec

Browser `WsxEnvelope` is a different codec: its data uses TYTX within the WSX
text envelope. It is not the internal bus format.

Claim anchors: [`WsxEnvelope`](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/wsx.py#L102).

## Source and test evidence

- [src/kajenn_orchestra/orchestration/group_handler.py](../../../src/kajenn_orchestra/orchestration/group_handler.py)
- [src/kajenn_orchestra/orchestration/worker_connector.py](../../../src/kajenn_orchestra/orchestration/worker_connector.py)
- [src/kajenn_orchestra/orchestration/lane_payload.py](../../../src/kajenn_orchestra/orchestration/lane_payload.py)
- [src/kajenn_orchestra/orchestration/spa_worker.py](../../../src/kajenn_orchestra/orchestration/spa_worker.py)
- [src/kajenn_orchestra/orchestration/worker_entry.py](../../../src/kajenn_orchestra/orchestration/worker_entry.py)
- [tests/orchestration/test_contract_group_bus.py](../../../tests/orchestration/test_contract_group_bus.py)
- [tests/orchestration/test_contract_kbus_migration.py](../../../tests/orchestration/test_contract_kbus_migration.py)
- [tests/orchestration/test_orchestration_worker_connector.py](../../../tests/orchestration/test_orchestration_worker_connector.py)
