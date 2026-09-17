# Global store — open decisions

**Version**: 0.1 · **Last Updated**: 2026-09-17 · **Status**: 🔴 DA REVISIONARE

Source of every row below: `kajenn-meta/verification/20_spa/040_global-store.md`.

Rows of that report whose verdict is `DIVERGE` or `SILENT`. They are neither ratified nor
dropped: each one is a decision for the owner. `decisions.md` of this entry keeps the
`CONVERGE` rows only.

## Rows

| # | Entry in decisions.md | Implementation (code file:line, test) | Docstring (file:line, text) | Handoff/finaldoc (file, date, text) | Transcript (uuid, date, role, text) | Verdict |
|---|---|---|---|---|---|---|
| 1 | "Record boundary — 2026-09-08": "This decision register is still a scaffold." · "The September contract and its claimed owner decisions currently live in [design](design.md) and [status](status.md); the source and test links in status verify implementation, not ratification." | SILENT. No code and no test states anything about the register's own ratification state. | SILENT | SILENT | SILENT | SILENT |
| 2 | friction "Typed-looking strings": "design calls leaving `\"42::L\"` unescaped an “owner decision”. The exact owner instruction approving this loss of string round-trip fidelity has not yet been recovered. Current codec behavior and issue #74 delivery alone do not ratify acceptance of that limitation." | The codec carries no escaping step: `src/genro_asgi_multiworker_spa/global_store.py:185` `return from_tytx(reply["value"], "json") if reply["exists"] else default` and `:192` `GLOBAL_STORE_SET_OP_PATH, {"key": key, "value": to_tytx(value, "json")}`; commander side `orchestration/spa_commander.py:323` `"value": to_tytx(store.get(key), "json")` and `:340` `decoded = from_tytx(value, "json")`. No test: `grep -rn "42::L" tests/` returns no line; the type round-trip test `tests/spa/orchestration/test_contract_global_store_dict.py::test_values_keep_their_types_across_the_wire` exercises Bag, attributes and datetime and no typed-looking string. | SILENT | `temp/GLOBAL_STORE_DICT_PLAN_2026-09-07.md` (2026-09-07) "The owner accepts collisions between literal strings and TYTX `::` type suffixes as a protocol limitation. No new escaping, codec redesign or serialization prerequisite belongs to this plan. Do not require literal preservation of strings such as `42::L` in acceptance tests." · `temp/handoff_2026-09-08.md` (2026-09-08) "Limiti accettati dal titolare: stringhe che sembrano TYTX (`\"42::L\"`) collidono col codec, nessun escape" | `8d142ed0-d026-487a-a798-23e2e932c91b.jsonl` (2026-09-07 16:56, assistant) "`42::L`: limite accettato dal titolare, tolto dai test. Coerente con quanto misurato." — repeated by the handoff of 2026-09-08. No `user` turn quotes `42::L` | DIVERGE |
| 3 | friction "Lease contract": "the design specifies a commander-only dictionary, one global lock including reads, literal keys, selected-key/full-dictionary leases, abort without apply, worker-death release, and an unconfirmed-commit error without blind retry. Source/tests establish that these are delivered contracts; dated owner provenance for approving this complete target still needs to be consolidated here." | Every clause is delivered. Commander-only dictionary: `orchestration/spa_commander.py:1002-1009` `def new_global_store(self) -> dict[str, Any]` returning `{}`. One lock including reads: `global_store.py:110-151` `GlobalStoreLock`, and `spa_commander.py:321` `async with self.spa_commander.global_lock.lock:` inside `get`. Literal keys: `spa_commander.py:303-304` `if not isinstance(key, str): raise TypeError(...)`. Selected-key / whole-dictionary lease: `spa_commander.py:429-435` `if global_lock.holder_key is None: … store.clear(); store.update(decoded) else: store[global_lock.holder_key] = decoded`. Abort without apply: `global_store.py:231-237` `abort`, `:252-255` `if exc_type is not None or self.aborted: await self._abort()`. Worker-death release: `orchestration/worker_handler.py:692` `…commander_dispatcher.global_store.release_worker_lock(` and `spa_commander.py:440-454` "released, nothing applied". Unconfirmed commit without retry: `global_store.py:266-271` `except ConnectionError as exc: … raise GlobalStoreCommitUnconfirmed(self.request_id, self.key, exc) from exc`. Tests: `tests/spa/orchestration/test_contract_global_store_dict.py::test_the_master_is_a_dictionary_with_literal_keys`, `::test_only_string_keys_are_accepted`, `::test_a_read_of_any_key_waits_while_a_turn_is_in_force`, `::test_a_keyed_turn_replaces_only_its_key`, `::test_a_turn_with_no_key_replaces_the_whole_dictionary`, `::test_an_aborted_turn_publishes_nothing_and_frees_the_lock_at_exit`, `::test_a_dead_holder_frees_only_its_turn_with_the_master_untouched`, `::test_a_commit_whose_answer_is_lost_is_reported_uncertain`, `::test_a_stale_release_neither_writes_nor_frees_a_newer_turn`. | `src/genro_asgi_multiworker_spa/global_store.py:19-21` "There are no replicas: every access a worker makes is a CALL on the lane, served under ONE FIFO lock (issue #74, owner decision 2026-09-07)." · `global_store.py:111` "The commander's lock on the dictionary: FIFO, one holder, no lease and no timer." · `global_store.py:88` "The commit of a turn was sent and its answer never came: the value MAY be published." · `global_store.py:207` "One turn on the global store: ``with`` or ``async with``, yielding itself." · `orchestration/spa_commander.py:1003` "The vertex's data at birth: one empty dictionary." | `temp/GLOBAL_STORE_DICT_PLAN_2026-09-07.md` (2026-09-07) "One FIFO lock protects the entire dictionary. Ordinary reads, writes, deletions and read-modify-write turns use the same queue. Selecting one key limits the transferred content, not the lock scope." · `temp/handoff_2026-09-08.md` (2026-09-08) "uscita normale → `apply=True` con il valore completo; eccezione, grant non decodificabile, valore non codificabile o `abort()` → `apply=False`." | `8d142ed0-….jsonl` (2026-09-07 15:35, user) "Una lettura normale aspetta eventuali lock, restituisce il valore e non riserva nulla." · (2026-09-07 15:47, user) "Una release fuori turno non scrive e non libera nulla. La morte di un worker libera soltanto il turno che quel worker detiene." · (2026-09-07 16:27, user) "se manca la risposta al commit, l’esito può essere incerto." | DIVERGE |
| 4 | friction "Legacy datetime boundary": "design assigns adaptation to genropy-asgi. Preserve this allocation as written, with exact approving provenance still to be recovered rather than inferred from the core implementation." | The core carries no datetime logic: `grep -rn "astimezone\|tzinfo" src/` returns no line, so neither the commander nor the client normalises. A datetime crosses the store as a value under the codec alone: `tests/spa/orchestration/test_contract_global_store_dict.py::test_values_keep_their_types_across_the_wire`, "a Bag with attributes and a datetime travel as values of the dictionary under the existing TYTX conventions". No code in this repository performs the legacy adaptation. | SILENT | `temp/GLOBAL_STORE_DICT_PLAN_2026-09-07.md` (2026-09-07) "Preserve the bridge's existing datetime handling: when an aware datetime reaches the legacy boundary, convert it to process-local time and remove timezone information with `value.astimezone().replace(tzinfo=None)`" · `temp/handoff_2026-09-08.md` (2026-09-08) "le date legacy sono responsabilità dell'adapter del bridge" and "Nessuna logica sulle date nel commander" | `8d142ed0-….jsonl` (2026-09-08 05:49, user) "Non cambierei il dict né introdurrei logica sulle date nel commander. La normalizzazione delle date legacy è responsabilità dell’adapter." | DIVERGE |

## Divergences

### #1 — Record boundary

- decisions.md says: "This decision register is still a scaffold." and "consolidating each decision
  with its exact owner provenance remains open."
- the implementation says: SILENT. No module and no test of `src/` or `tests/` states anything about
  the state of a decision register.
- the docstring says: SILENT. No docstring of `src/` speaks of the state of the decision register.
- the handoffs and finaldocs say: SILENT. `grep -rn "Record boundary\|scaffold"` over `temp/` and
  `.subtasks/` finds no occurrence referring to this register.
  `temp/step4_docs_journal_2026-09-08.md` (2026-09-08) lists
  "`internals/20_spa/040_global-store/design.md`, `README.md`, `decisions.md`, `status.md`" among the
  files read, and under "Written" records only "`internals/20_spa/040_global-store/design.md` —
  rewritten on this design (version 0.2)": `decisions.md` does not appear among the files written.
- no transcript speaks of it. The search for `Record boundary` and `ratification gap` in the
  transcripts from 2026-09-08 returns no hit.

### #2 — Typed-looking strings

- decisions.md says: "The exact owner instruction approving this loss of string round-trip
  fidelity has not yet been recovered. Current codec behavior and issue #74 delivery alone do not
  ratify acceptance of that limitation."
- the implementation at `src/genro_asgi_multiworker_spa/global_store.py:185` and `:192` does:
  `from_tytx(reply["value"], "json")` and `to_tytx(value, "json")`, with no escaping step before or
  after; the commander side does the same at `orchestration/spa_commander.py:323` and `:340`. The
  limitation is therefore present in the delivered codec. No test asserts it: `grep -rn "42::L"
  tests/` returns no line.
- the docstring says: SILENT on the collision. `grep -rn '42::L'` over `src/` returns no hit;
  `grep -rni 'escap'` over `src/` returns only `src/genro_asgi/auth/user_store.py:194` and
  `src/genro_asgi/mcp/tools.py:75`, neither about the global store codec. The module docstring says
  only, at `src/genro_asgi_multiworker_spa/global_store.py:44`, "**The wire is TYTX.** Every value
  travels ``to_tytx(..., \"json\")`` and is hydrated by its reader".
- the handoff of 2026-09-07 says: "The owner accepts collisions between literal strings and TYTX
  `::` type suffixes as a protocol limitation." (`temp/GLOBAL_STORE_DICT_PLAN_2026-09-07.md`)
- the handoff of 2026-09-08 says: "Limiti accettati dal titolare: stringhe che sembrano TYTX
  (`\"42::L\"`) collidono col codec, nessun escape" (`temp/handoff_2026-09-08.md`)
- the prompt of 2026-09-07 says: "`\"42::L\"` e simili: collisione col suffisso TYTX accettata dal
  titolare come limite del" (`temp/prompt_step3_bridge_global_store_dict_2026-09-07.md`)
- the transcript `8d142ed0-d026-487a-a798-23e2e932c91b.jsonl` of 2026-09-07 says, assistant turn of
  16:47: "Il documento vieta uno strato di codifica in più e insieme chiede che `42::L`
  sopravviva: le due cose non stanno insieme finché genro-tytx non ha un escape per le stringhe."
  and, assistant turn of 16:56: "`42::L`: limite accettato dal titolare, tolto dai test."
  The `user` turn immediately before (uuid `c5ec057f-51f1-4e3b-97de-220c34abc801`,
  2026-09-07 16:55) contains only a file path:
  "/Users/gporcari/Documents/ChatGPT/genro-asgi/GLOBAL_STORE_DICT_PLAN_2026-09-07.md".
  No `user` turn of the transcripts contains the string `42::L`.

### #3 — Lease contract

- decisions.md says: "dated owner provenance for approving this complete target still needs to be
  consolidated here."
- the implementation delivers the complete target listed in the entry, with the dated provenance
  written into the code itself: `src/genro_asgi_multiworker_spa/global_store.py:19-21` "served under
  ONE FIFO lock (issue #74, owner decision 2026-09-07)". Every clause is covered by a test of
  `tests/spa/orchestration/test_contract_global_store_dict.py` (list in the table, row 3).
- the docstring says: "served under ONE FIFO lock (issue #74, owner decision 2026-09-07)"
  (`src/genro_asgi_multiworker_spa/global_store.py:19-21`).
- the handoff of 2026-09-07 says: "Version: 1.1. Date: 2026-09-07 (1.1: the `get` contract carries
  `exists`; owner decision)." and "`get` contract (owner, 2026-09-07): the wire reply carries
  `exists` and `value`." (`temp/GLOBAL_STORE_DICT_PLAN_2026-09-07.md`)
- the transcript `8d142ed0-d026-487a-a798-23e2e932c91b.jsonl` of 2026-09-07 carries three `user`
  turns — the owner's own words — on the points the entry lists:
  - uuid `0d2bba41-dc5b-4941-9b9b-f41a7c6a483a`, 15:35, user: "Una lettura normale aspetta
    eventuali lock, restituisce il valore e non riserva nulla. Una lettura `for_update` acquisisce
    il lock e lo mantiene fino alla restituzione del ramo modificato." and "Se il blocco solleva
    un’eccezione, libera il lock senza applicare modifiche."
  - uuid `06470b64-d5b6-4fe3-94b5-48bd6d11cdd9`, 15:47, user: "Una release fuori turno non scrive
    e non libera nulla. La morte di un worker libera soltanto il turno che quel worker detiene." and
    "con un protocollo opaco preferirei un campo esplicito `apply=False`."
  - uuid `b0d41dfa-ba20-4ccc-a4bd-0b4a8536753d`, 16:27, user: "Una CALL fallita non implica che la
    modifica non sia avvenuta. […] se manca la risposta al commit, l’esito può essere incerto."
- for the points "commander-only dictionary" and "literal keys" alone, no `user` turn of the
  transcripts carries the owner's words: the content comes from
  `GLOBAL_STORE_DICT_PLAN_2026-09-07.md`, a document outside the repository handed over by the owner
  as a file path (user turn `725c7285-2a45-427f-afd8-44d57b83740b`, 2026-09-07 16:45, whole text:
  "/Users/gporcari/Documents/ChatGPT/genro-asgi/GLOBAL_STORE_DICT_PLAN_2026-09-07.md"). The
  assistant turn that follows (`e6f55aea-6763-4038-a468-01465cf20005`, 16:47) writes: "La radice
  `dict` toglie i seam B2 e B3 di v0.3 e fissa il tipo per tutti i consumer. […] Contraddice D5 di
  v0.3, che è tua: va ratificata da te."

### #4 — Legacy datetime boundary

- decisions.md says: "with exact approving provenance still to be recovered rather than inferred
  from the core implementation."
- the implementation does no datetime adaptation at all: `grep -rn "astimezone\|tzinfo"` over `src/`
  returns no line, so the allocation to the bridge is what the code leaves standing. The only
  datetime evidence is the codec round-trip in
  `tests/spa/orchestration/test_contract_global_store_dict.py::test_values_keep_their_types_across_the_wire`,
  whose assertion is `read["x"].replace(tzinfo=None) == datetime(2026, 1, 1, 12)`.
- the docstring says: SILENT. `grep -rn 'genropy-asgi'` over `src/` gives
  `src/genro_asgi_multiworker_spa/__init__.py:29` and
  `src/genro_asgi_multiworker_spa/register_row.py:37`, neither about dates. The only mention of
  datetime in the module is `global_store.py:45`: "so a Bag stays a Bag and a datetime a
  datetime".
- the handoff of 2026-09-07 says: "Preserve the bridge's existing datetime handling: when an aware
  datetime reaches the legacy boundary, convert it to process-local time and remove timezone
  information with `value.astimezone().replace(tzinfo=None)`; leave naive values unchanged."
  (`temp/GLOBAL_STORE_DICT_PLAN_2026-09-07.md`)
- the handoff of 2026-09-08 says: "le date legacy sono responsabilità dell'adapter del bridge
  (naive → fuso locale con ora preservata in scrittura; aware → naive locale in lettura)" and
  "Nessuna logica sulle date nel commander" (`temp/handoff_2026-09-08.md`)
- the transcript `8d142ed0-d026-487a-a798-23e2e932c91b.jsonl` of 2026-09-08 says, user turn
  uuid `7d358099-ab33-4034-a58c-aa90985c00a2`, 05:49: "La mia opinione: la correzione appartiene
  al bridge, sul lato scrittura. […] Non cambierei il dict né introdurrei logica sulle date nel
  commander. La normalizzazione delle date legacy è responsabilità dell’adapter."
  A second user turn, uuid `9a6bf94b-0c7f-406b-a3b7-de1e11dc048b`, 2026-09-08 04:48, says:
  "Per il resto confermo: primo segmento come chiave, Bag legacy come valore, compatibilità dei
  `with globalStore()`, TYTX invariato e gestione delle date conservata."
