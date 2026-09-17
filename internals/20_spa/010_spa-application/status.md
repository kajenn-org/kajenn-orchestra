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

Claim anchors: [`SpaApplication`](../../../src/kajenn_orchestra/spa_app.py#L517), [`SpaApplicationGrammar`](../../../src/kajenn_orchestra/spa_app.py#L152), [`internal_roots`](../../../src/kajenn_orchestra/spa_app.py#L612), [`resolves_natively`](../../../src/kajenn_orchestra/spa_app.py#L622), [`WsxControl`](../../../src/kajenn_orchestra/spa_app.py#L454), [`openchannel`](../../../src/kajenn_orchestra/spa_app.py#L471), [`openchannel`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L507).

## HTTP packing, identity and response translation

`pack_http` buffers the request body, carries headers as pairs and encodes body
bytes as base64 in the JSON-safe worker payload. `SpaCommander.serve_request`
handles placement and forwards the call. The site's returned `connection_id`
becomes `spa_connection_id` only when it differs from the arriving id. The front
mints no site connection itself.

`AssignmentRefused` becomes 503 with the provided retry delay. An unexpected
site failure is a generic 502; an explicit worker refusal status and cause are
preserved, including the 409 for a page whose channel was not opened. A dead
wire during shutdown becomes 503, while one lost in RUNNING becomes 502.

Claim anchors: [`pack_http`](../../../src/kajenn_orchestra/spa_app.py#L1163), [`SpaCommander`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L494), [`serve_request`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L691).

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

Claim anchors: [`handshake_cookie`](../../../src/kajenn_orchestra/spa_app.py#L524), [`openchannel`](../../../src/kajenn_orchestra/spa_app.py#L471), [`openchannel`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L507), [`build_response`](../../../src/kajenn_orchestra/spa_app.py#L1122).

## Source and test evidence

- [src/kajenn_orchestra/spa_app.py](../../../src/kajenn_orchestra/spa_app.py)
- [src/kajenn_orchestra/orchestration/spa_commander.py](../../../src/kajenn_orchestra/orchestration/spa_commander.py)
- [src/kajenn_orchestra/orchestration/spa_worker.py](../../../src/kajenn_orchestra/orchestration/spa_worker.py)
- [src/kajenn/wsx.py](../../../src/kajenn/wsx.py)
- [tests/spa/test_spa_application.py](../../../tests/spa/test_spa_application.py)
- [tests/spa/test_spa_app_profiles.py](../../../tests/spa/test_spa_app_profiles.py)
- [tests/spa/orchestration/test_orchestration_websocket_e2e.py](../../../tests/spa/orchestration/test_orchestration_websocket_e2e.py)
