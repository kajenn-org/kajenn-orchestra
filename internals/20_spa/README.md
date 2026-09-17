# 20_spa — the SPA world

**Version**: 0.1 · **Last Updated**: 2026-08-24 · **Status**: 🔴 DA REVISIONARE

The world that hosts a single-page site with live server-side state, standing on
the machine of [10_server](../10_server/README.md). A stateless front takes the request, an
orchestration chain places every user in one process and keeps all his pages
there, and the hosted site's own data plane lives in its bridge, attached through
named seams. The last entry states the contract the hosted site implements, so
the core never learns the site's own logic.

| Entry | In one line |
|---|---|
| [010 spa-application](010_spa-application/README.md) | one stable door to the hosted site, no state in the door |
| [020 orchestration](020_orchestration/README.md) | many users with live state, scaled across processes, never split |
| [030 channel](030_channel/README.md) | the wire: frames, hub, the lane |
| [040 global-store](040_global-store/README.md) | one shared state, safe read-modify-write |
| [070 console](070_console/README.md) | ask a live pool the questions nobody predicted |
| [080 bridge-contract](080_bridge-contract/README.md) | what genropy-asgi implements and consumes — generalized core, site logic in the bridge |
| [090 configuration_profiles](090_configuration_profiles/README.md) | the archive of named orchestration profiles, and how one is put in force |
