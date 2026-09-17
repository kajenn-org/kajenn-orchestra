# Internals — the technical dossier of kajenn-orchestra

**Version**: 0.1 · **Last Updated**: 2026-09-17 · **Status**: 🔴 DA REVISIONARE

The world that hosts a single-page site with live server-side state, standing on
the machine kajenn provides. A stateless front takes the request, an
orchestration chain places every user in one process and keeps all his pages
there, and the hosted site's own data plane lives in its bridge, attached through
named seams. One entry states the contract the hosted site implements, so the
core never learns the site's own logic; the last one holds the named
configurations the pool is run with.

Start from [00_overview](00_overview/README.md): it states the documents every
entry owns, the cycle they serve and the rules they are written under. The
entries of the machine itself — the server, its configuration, its applications
— live in the `internals/` of kajenn, and the entries below link to them.

| Entry | In one line |
|---|---|
| [00_overview](00_overview/README.md) | how to read the dossier: the documents, the cycle, the rules |
| [20_spa](20_spa/README.md) | the SPA world: front, orchestration, data plane, the bridge contract |
| [20_spa/010 spa-application](20_spa/010_spa-application/README.md) | one stable door to the hosted site, no state in the door |
| [20_spa/020 orchestration](20_spa/020_orchestration/README.md) | many users with live state, scaled across processes, never split |
| [20_spa/030 channel](20_spa/030_channel/README.md) | the wire: frames, hub, the lane |
| [20_spa/040 global-store](20_spa/040_global-store/README.md) | one shared state, safe read-modify-write |
| [20_spa/070 console](20_spa/070_console/README.md) | ask a live pool the questions nobody predicted |
| [20_spa/080 bridge-contract](20_spa/080_bridge-contract/README.md) | what genropy-asgi implements and consumes — generalized core, site logic in the bridge |
| [20_spa/090 configuration_profiles](20_spa/090_configuration_profiles/README.md) | the archive of named orchestration profiles, and how one is put in force |

## Reading it as a site

From the repository root:

```bash
python -m pip install -e '.[internals]'
python -m mkdocs serve
```

That serves this folder at <http://127.0.0.1:8771/> with navigation, full-text
search and rendered diagrams. It reads this checkout and this branch, including
uncommitted changes, and publishes nothing.

## The state of the entries

Every entry carries `Status: 🔴 DA REVISIONARE`. Each one whose decisions were
checked against the code, the docstrings, the handoffs and the transcripts
carries, under its status line, the verification report that checked them and
its counts. The rows that did not converge are in the entry's own
`open_decisions.md`; `decisions.md` keeps the ratified rows only.
`090_configuration_profiles` has no report: it was a sub-entry with no
`decisions.md`.
