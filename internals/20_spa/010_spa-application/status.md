# SPA application (the front) — current state

**Version**: 0.2 · **Last Updated**: 2026-09-08 · **Status**: 🔴 evidence refreshed; design ratification unchanged

Verified against source revision `2465fcc` (develop baseline). Test references
below identify the executable contracts; they are not a new coverage percentage.

## Package and startup

`SpaApplication` lives in `kajenn_orchestra.spa_app`. It reads its
required orchestration/commander subtree at startup and builds the commander
then, after server attachment. A missing subtree raises `FatalBootError`.
Its own `SpaApplicationGrammar` declares pool vocabulary; the core package
imports no SPA implementation.

`internal_roots` plus `resolves_natively` form the two-stage demux. A route
under an internal root is served natively if structurally present, even when
its auth rule denies the caller. Other paths go to the hosted application.
`_orchestration` control is opt-in; `WsxControl` provides `_wsx/openchannel`.

Claim anchors: [`SpaApplication`](../../../src/kajenn_orchestra/spa_app.py#L522), [`SpaApplicationGrammar`](../../../src/kajenn_orchestra/spa_app.py#L156), [`internal_roots`](../../../src/kajenn_orchestra/spa_app.py#L620), [`resolves_natively`](../../../src/kajenn_orchestra/spa_app.py#L630), [`WsxControl`](../../../src/kajenn_orchestra/spa_app.py#L459), [`openchannel`](../../../src/kajenn_orchestra/spa_app.py#L476), [`openchannel`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L507).

## HTTP packing, identity and response translation

`pack_http` buffers the request body and sends a kbus message of `format`
`http`. Its payload (`LanePayload.encode_http`) is the 4-byte big-endian length
of the envelope JSON, the envelope JSON, then the `HttpRecord` bytes: the
request metadata as JSON, headers included, followed by the opaque body bytes. `SpaCommander.serve_request`
handles placement and forwards the call. The site's returned `connection_id`
becomes `spa_connection_id` only when it differs from the arriving id. The front
mints no site connection itself.

`AssignmentRefused` becomes 503 with the provided retry delay. An unexpected
site failure is a generic 502; an explicit worker refusal status and cause are
preserved, including the 409 for a page whose channel was not opened. A dead
wire during shutdown becomes 503, while one lost in RUNNING becomes 502.

Claim anchors: [`pack_http`](../../../src/kajenn_orchestra/spa_app.py#L1190), [`SpaCommander`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L496), [`serve_request`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L697).

## WSX page requests

`handshake_cookie` requires `spa_connection_id`; its absence closes an accepted
WSX handshake with 1008. `openchannel` validates the page against the front's
connection mapping, reaches the worker row, then permits the connection to bind
that page to its socket. Messages naming a page require its opened channel.
Sequential mode queues the entire call on that page's `call_lock`, not a
worker-wide or user-wide lock. Page addressing and identity metadata travel
beside the HTTP payload.

The separately recorded authenticating connection/core session link is not
written by `build_response`: a site connection change is not automatically a
core avatar login. Issue #72's opaque transport is not in this baseline.

Claim anchors: [`handshake_cookie`](../../../src/kajenn_orchestra/spa_app.py#L531), [`openchannel`](../../../src/kajenn_orchestra/spa_app.py#L476), [`openchannel`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L507), [`build_response`](../../../src/kajenn_orchestra/spa_app.py#L1146).

## Source and test evidence

- [src/kajenn_orchestra/spa_app.py](../../../src/kajenn_orchestra/spa_app.py)
- [src/kajenn_orchestra/orchestration/spa_commander.py](../../../src/kajenn_orchestra/orchestration/spa_commander.py)
- [src/kajenn_orchestra/orchestration/spa_worker.py](../../../src/kajenn_orchestra/orchestration/spa_worker.py)
- [src/kajenn/wsx.py](https://github.com/kajenn-org/kajenn/blob/main/src/kajenn/wsx.py)
- [tests/test_spa_application.py](../../../tests/test_spa_application.py)
- [tests/test_spa_app_profiles.py](../../../tests/test_spa_app_profiles.py)
- [tests/orchestration/test_orchestration_websocket_e2e.py](../../../tests/orchestration/test_orchestration_websocket_e2e.py)
