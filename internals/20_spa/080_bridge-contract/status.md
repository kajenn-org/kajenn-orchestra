# Bridge contract — current state

**Version**: 0.2 · **Last Updated**: 2026-09-08 · **Status**: 🔴 evidence refreshed; design ratification unchanged

Verified against source revision `2465fcc` (develop baseline). Test references
below identify the executable contracts; they are not a new coverage percentage.

## Hosted application seam

`SpaWorker.hosted_app_seam` selects `AsgiSeam(asgi_app)` or wraps a supplied
`wsgi_app` in `AsgiSeam(WsgiSeam(...))`. Supplying both fails worker boot;
supplying neither is valid for an order-only base worker. Mixed routing belongs
to the consumer's ASGI app. WSGI execution uses the worker pool.

`AsgiSeam` reconstructs an HTTP scope, delivers the complete request body and
collects all response chunks into one reply. It propagates `genro.identity` and
optional page/reply-path metadata. The WSGI adapter derives SCRIPT_NAME from
root_path and PATH_INFO from the remaining path. This is not streaming across
the worker channel; #72 remains a proposal at this revision.

Claim anchors: [`SpaWorker`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L564), [`hosted_app_seam`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L689), [`AsgiSeam`](../../../src/kajenn_orchestra/environ.py#L71), [`WsgiSeam`](../../../src/kajenn_orchestra/environ.py#L174).

## Extension seams and genropy ownership

The consumer can select a commander subclass through the front's
`commander_class`, attach dispatcher branches, customize request slots and the
`on_request_served` tail, respond to `on_worker_presented`, and select the
commander envelope-handler layer through its property. Row classes and
`RegisterRegistry` provide the store and page capture hooks.

genropy-specific datachanges, table subscriptions and delivery desks are absent
from these core/SPA implementations after #59. The shared store is a commander
`dict[str, Any]`; consumers adapt their own path semantics around literal keys
and complete-value leases. The local code verifies the offered seam, not whether
an external bridge deployment has migrated successfully.

Claim anchors: [`on_request_served`](../../../src/kajenn_orchestra/orchestration/spa_worker.py#L746), [`on_worker_presented`](../../../src/kajenn_orchestra/orchestration/spa_commander.py#L963), [`RegisterRegistry`](../../../src/kajenn_orchestra/register_registry.py#L125).

## Unresolved contract history

The recorded hosted identity return block and authenticating connection/core
session link are not implemented by the current front's response builder.
A site's pool login does not prove a core avatar was attached.

The design retains a release condition tied to bridge migration and language
about composition beside explicit subclass seams. Their historical intent must
be reconciled with the owner/provenance record; no new prohibition, permission
or release decision is inferred from this implementation audit.

Target evidence: [recorded target](design.md); this paragraph records unresolved
design distance, not an executable contract.


## Source and test evidence

- [src/kajenn_orchestra/environ.py](../../../src/kajenn_orchestra/environ.py)
- [src/kajenn_orchestra/spa_app.py](../../../src/kajenn_orchestra/spa_app.py)
- [src/kajenn_orchestra/orchestration/worker_entry.py](../../../src/kajenn_orchestra/orchestration/worker_entry.py)
- [src/kajenn_orchestra/orchestration/spa_worker.py](../../../src/kajenn_orchestra/orchestration/spa_worker.py)
- [src/kajenn_orchestra/orchestration/spa_commander.py](../../../src/kajenn_orchestra/orchestration/spa_commander.py)
- [src/kajenn_orchestra/register_registry.py](../../../src/kajenn_orchestra/register_registry.py)
- [src/kajenn_orchestra/register_row.py](../../../src/kajenn_orchestra/register_row.py)
- [tests/spa/orchestration/test_contract_consumer_seams.py](../../../tests/spa/orchestration/test_contract_consumer_seams.py)
- [tests/spa/orchestration/test_orchestration_asgi_seam.py](../../../tests/spa/orchestration/test_orchestration_asgi_seam.py)
- [tests/spa/orchestration/test_contract_phase2_site_verbs.py](../../../tests/spa/orchestration/test_contract_phase2_site_verbs.py)
