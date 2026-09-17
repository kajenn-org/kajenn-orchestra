# Bridge contract — open decisions

**Version**: 0.1 · **Last Updated**: 2026-09-17 · **Status**: 🔴 DA REVISIONARE

Source of every row below: `kajenn-meta/verification/20_spa/080_bridge-contract.md`.

Rows of that report whose verdict is `DIVERGE` or `SILENT`. They are neither ratified nor
dropped: each one is a decision for the owner. `decisions.md` of this entry keeps the
`CONVERGE` rows only.

## Rows

| # | Entry in decisions.md | Implementation (code file:line, test) | Docstring (file:line, text) | Handoff/finaldoc (file, date, text) | Transcript (uuid, date, role, text) | Verdict |
|---|---|---|---|---|---|---|
| 2 | "The same path gains an **identity block**: at the site's own guest→user transition, the answer declares \"this connection is now user X\" — **identity only, never tags**." with the mark "Not yet implemented" | SILENT. The return path carries no identity field: `src/genro_asgi_multiworker_spa/environ.py:113-114` "``{\"status\", \"headers\", \"body\"}``, the body raw bytes — the same shape the WSGI road produced before this seam existed", and `spa_app.py:1158` reads `reply.info.get("connection_id")` alone. `grep -rni "identity block" src/` returns no line. No test exercises a guest→user declaration on the return | SILENT | SILENT | `2390696f-….jsonl` (2026-08-24 09:59, assistant, uuid `a556bc52-c1d3-47d3-b057-e1cbb01b1f11`) "l'informazione che il ritorno deve portare alla transizione guest→user è un blocco identità minimo, sullo stesso binario che già porta il connection id: il sito dichiara «questa connessione ora è l'utente X»" — confirmed by the owner in the next turn (uuid `0c98ac76-df1a-4ad3-b596-1526c6f31c34`, 10:00) "proviamo cosiù: intanto mettiamo queste cosde nel disegno e poi sappiamo che dovremo implementarle" | SILENT |
| 3 | "Tags are the server's authorization vocabulary and never travel from the hosted site (see [050 authentication](../../10_server/050_authentication/decisions.md))." | SILENT for the prohibition: no code refuses tags arriving from the hosted site, because the return path has no tags field to refuse (`environ.py:113-114`, `spa_app.py:1158`). What the code does do is mint tags from server-side configuration only: `src/genro_asgi/auth/core.py:105` `"tags": _split_and_strip(config.get("tags"))` (same at `:117`, `:135`), `:179` `return Avatar(result["identity"], result["tags"])`, `src/genro_asgi_server_app/oidc_method.py:253` `avatar = Avatar(identity, self.provider.get("tags", []))`. No test of `tests/spa/` exercises a tags field on the return | SILENT for the statement about tags. The nearest text is `src/genro_asgi_multiworker_spa/environ.py:25-26` "No Python session or Avatar crosses the transport; the worker receives only explicitly defined routing context.", which speaks of the server→worker direction and not of the return. `src/genro_asgi/auth/user_store.py:18-19` says "Its tags are the verified claims of the local source; they ride in the authentication result as trusted input", without naming the hosted site | SILENT | `2390696f-….jsonl` (2026-08-24 09:59, assistant, uuid `a556bc52-…`) "b) **il sito dichiara solo l'identity**, e i tag li assegna la configurazione dell'app SPA sul server (come fa il provider OIDC) — il vocabolario resta del server, il sito non deve conoscerlo. Consiglio: **b)**" — confirmed by the owner at 10:00 (uuid `0c98ac76-…`) and summarised at 10:30 (uuid `6a7a3cb7-832a-45a4-8dfe-870e4d471979`, assistant) after the owner's question "ok: riassumendo?" (uuid `8e52ba1e-dd79-4cbf-b05f-c932e769b428`, 10:30) | SILENT |

## Divergences

### #2 — the identity block on the return

- decisions.md says: "**Source: owner, 2026-08-24 (interview on 020_applications S2). Not yet
  implemented.** […] The same path gains an **identity block**: at the site's own guest→user
  transition, the answer declares \"this connection is now user X\" — **identity only, never
  tags**."
- the implementation says: SILENT. The response the front rebuilds is `{"status", "headers", "body"}`
  (`src/genro_asgi_multiworker_spa/environ.py:113-114`) plus `reply.info["connection_id"]` read at
  `spa_app.py:1158`: no identity field. `grep -rni "identity block" src/` returns no line, and no
  test of `tests/spa/` exercises a guest→user declaration on the return.
- the docstring says: SILENT. `grep -rni "identity block\|blocco identit"` over `src/` returns no
  hit.
- the handoffs and finaldocs say: SILENT. No file of `temp/` or `.subtasks/` names the identity
  block on the return.
- the transcript `2390696f-1c5b-4b9d-aaea-370d6eff0d84.jsonl` of 2026-08-24 speaks of it, and also
  records having written it: assistant turn uuid `810c7f35-e630-437d-92e1-09744278d8b3`, 10:01:
  "Messo nel disegno — quattro `decisions.md` seminati, con fonte «owner, 2026-08-24» e la marca
  «Not yet implemented»: - **080 bridge-contract**: il blocco identità sul ritorno alla
  transizione guest→user — solo identity, mai tag".
- `status.md` of the same entry (2026-09-08) says: "The recorded hosted identity return block and
  authenticating connection/core session link are not implemented by the current front's response
  builder." The "Not yet implemented" mark of the register and `status.md` agree with each other;
  they remain without a docstring source and without a handoff source.

### #3 — tags never travel from the hosted site

- decisions.md says: "Tags are the server's authorization vocabulary and never travel from the
  hosted site".
- the implementation says: SILENT for the prohibition. Nothing refuses a tags field on the return,
  because no tags field exists there (`environ.py:113-114`, `spa_app.py:1158`). Where tags are
  built, they come from the server's own configuration: `src/genro_asgi/auth/core.py:105`
  `"tags": _split_and_strip(config.get("tags"))`, `src/genro_asgi_server_app/oidc_method.py:253`
  `avatar = Avatar(identity, self.provider.get("tags", []))`.
- the docstring says: SILENT for this statement. `src/genro_asgi/auth/user_store.py:18-19` says "Its
  tags are the verified claims of the local source; they ride in the authentication result as
  trusted input", and `src/genro_asgi/auth/core.py:37-38` says "Tag normalization (e.g. a JWT null
  ``tags`` claim) happens at the ``Avatar`` boundary, never ``list(None)``": neither says that tags
  cannot arrive from the hosted site.
- the handoffs and finaldocs say: SILENT.
- the transcript `2390696f-….jsonl` of 2026-08-24 carries the choice and its confirmation:
  - assistant turn uuid `a556bc52-c1d3-47d3-b057-e1cbb01b1f11`, 09:59, putting the two forms:
    "a) **il sito dichiara anche i tag** nel blocco di ritorno — massima flessibilità, ma il
    vocabolario dei tag del server finisce in mano al sito ospitato; b) **il sito dichiara solo
    l'identity**, e i tag li assegna la configurazione dell'app SPA sul server";
  - user turn uuid `0c98ac76-df1a-4ad3-b596-1526c6f31c34`, 10:00: "proviamo cosiù: intanto
    mettiamo queste cosde nel disegno e poi sappiamo che dovremo implementarle";
  - user turn uuid `044a648b-5e23-462e-9d02-28764ba02ab4`, 10:03, correcting the wording:
    "«i tag dalla configurazione del metodo» non è veroi nel metodo ci sono le rules non i tag";
  - user turn uuid `026b564d-b6d5-44ea-a09d-28824a636187`, 10:10: "io lo avevo letto i tag dalla
    configurazione del entry point servito".
