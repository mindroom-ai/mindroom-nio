# Durable Ingestion Lean Replacement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to continue this plan task-by-task. Use
> `superpowers:test-driven-development` for every behavior change and
> `superpowers:verification-before-completion` before any completion claim.
> Do not dispatch subagents unless the active session instructions or user
> explicitly authorize it.

**Goal:** Replace the fork-specific sync recovery architecture with one
crash-safe, transport-neutral durable ingestion path while preserving the
pre-fork public matrix-nio behavior used by Desktop.

**Architecture:** Classic and Sliding normalize into one authenticated Frame,
preparation, reducer, Work FIFO, settlement, and completion state machine. The
same SQLite owner holds ordinary MatrixStore/E2EE state and the five ingestion
tables; MindRoom separately owns durable receipt/application admission, and
callbacks execute only outside both database transactions.

**Tech Stack:** Python 3.14/3.15-compatible matrix-nio, asyncio, SQLite/Peewee,
Olm/Megolm, pytest, Ruff, Black, mypy, and the linked MindRoom repository.

**Design:** `docs/superpowers/specs/2026-08-13-durable-ingestion-lean-replacement-design.md`

**Repos:** `/work/dev/mindroom-nio`, `/work/dev/mindroom`

**Starting code boundary:** Classic-qualified nio `b313ae284be5dcc7797a7f7de83c70ad80520beb`; post-prune MindRoom `0acaea2baf05a4c41cce7497cfcdf4880afc6d04`

**Linked branches:** nio `docs/durable-ingestion-rewrite-plan`; MindRoom `wip/matrix-journal-ingress-cutover`

## Global Constraints

- Preserve the exact pre-fork sync methods, responses, callbacks, `AsyncClient.rooms`, and desktop behavior at `69aa99c51f354980bf5306a85b4c7b7d71ff3217`.
- Return ingestion schema to v1 and exactly five durable tables.
- Classic and Sliding share Frame normalization, reducer, materializer, Work, batch, and ack.
- Never use canary-specific production tables, scope, control room, `probe`, occurrence ledger, rotation ledger, or materializer.
- Response and cursor commit together.
- MindRoom application effects, existing event-journal identity, batch receipt, and frontier commit together.
- Desktop remains on upstream-style `sync()`/`sync_forever()` through an internal profile that disables fork-only limited-timeline recovery while retaining tokens, E2EE, callbacks, and headers; it is not connected to the durable batch engine.
- Legacy fork recovery stays while the public sync internals are restored upstream-style; deletion waits for both MindRoom and desktop zero-use observations, then desktop is regression-tested again.
- Preserve upstream `secure_delete = "fast"` and its regression from `origin/main`.
- Final nio net budgets versus `origin/main@6ed2b98`: runtime ≤ +4,500; tests ≤ +15,000; docs/scripts ≤ +1,000.
- Final MindRoom net budgets versus post-prune `0acaea2ba`: runtime ≤ +350; tests ≤ +1,000; the candidate is also net smaller than `925df7f3c`.

---

## Live execution status and restart handoff — 2026-08-15

This section is the authoritative restart point. Read it together with the
controlling design and the remaining task descriptions below. If conversation
state is lost, do not infer progress from unchecked prose elsewhere in the
plan: verify this checkpoint first, then continue from the **Task 8 active
checkpoint**.

### Repository and commit boundary

| Repository | Branch | Required commit boundary | State |
| --- | --- | --- | --- |
| `/work/dev/mindroom-nio` | `docs/durable-ingestion-rewrite-plan` | Task 7 feature commit `6e4f14aa2b438ba431d8f7c7ab2ff176f4e11a94`; the docs-ledger commit containing this row follows it | Task 7 crash/parity verification is committed and GREEN; after the ledger commit, tracked state is clean |
| `/work/dev/mindroom` | `wip/matrix-journal-ingress-cutover` | Task 7 commit `2cbeecb6f8e68f380a7fabb3cbe28cd88f3e1a2f` | Task 7 cross-database crash/parity verification is committed and GREEN; tracked state is clean |

Do not push, amend, rebase, force-push, merge, release, or claim cutover. Use
ordinary commits only after a complete reviewed slice is green. Preserve the
following unrelated untracked user files/directories:

- nio: `docs/superpowers/reviews/`, `src/mindroom_nio.egg-info/`;
- MindRoom: `.claude/CODEX_REVIEW_6.md`,
  `.claude/REVIEW_2026-08-08-recovery-delivery.md`, `.claude/worktrees/`, and
  `CODEX_REVIEW.md`.

At the completed Task4E documentation checkpoint, the expected nio tracked
dirty-file list is empty. The implementation commit contains exactly:

```text
docs/superpowers/plans/2026-08-13-durable-ingestion-lean-replacement.md
docs/superpowers/specs/2026-08-13-durable-ingestion-lean-replacement-design.md
src/nio/client/base_client.py
src/nio/ingest/coordinator.py
src/nio/ingest/reducer.py
src/nio/store/_sync_journal.py
tests/ingest/coordinator_test.py
tests/ingest/delivery_test.py
```

The exact pre-commit Task4E code/test patch (`git diff --binary -- src tests`)
had SHA-256
`ebf1c70c29d0329a28205e38631a7d1b73e95577b3279774d113cf1977515b29`.
It is committed as `be24cf315c27250d3c2b95a5862278420ee8d270`.
On restart verify commit ancestry and a clean tracked tree rather than expecting
that former dirty-patch hash; do not reset or overwrite unrelated files.

### Completed commits and gates

| Slice | State | Commit/evidence |
| --- | --- | --- |
| Tasks 1–3 pruning/reset | Complete | Through nio `febbe9a5`; detailed authority remains below |
| Desktop no-recovery profile | Complete | MindRoom `1fd376583` |
| Desktop query-only identity reader | Complete | MindRoom `a5d3edc33` |
| Task 4A configured in-place adoption | Complete | nio `044a74e` |
| Task 4B owned bootstrap/session lease | Complete | nio docs `c06b88a`, implementation `f27f5e3c149d3e5215f9641f9daa9b291502c179` |
| Local membership contract | Complete | nio docs `5e85330941d828751365d34512e3a73ad248dfe3` |
| Task 5A MindRoom receipt/idempotency kernel | Complete | MindRoom `da6ac44e44f6ea65a131dbee1bfc7bdd63ff0fbc` |
| Task 4C callback-free preparation | Complete | nio `19587c4cf195e62d4265a03a70d061e407dceec4` |
| Task 4D pure prepared planner | Complete | nio `96ff99bf36135e7eef66355953a9c1b4c8c9b2e8` |
| Task 4D owned all-kind materialization | Complete | nio `eb77f5d0e1c17658ea322016ef04a16bac6e40e9`; full suite 2,622 passed/3 skipped |
| Task 4E restore/settlement | Complete | nio `be24cf315c27250d3c2b95a5862278420ee8d270`; full suite 2,672 passed/3 skipped |
| Task 5B MindRoom all-record adapter/pump | Complete | MindRoom `3a80947593bbf6fe508ca2e7499f19e951f416d3`; owning 319-test file, full repository suite, and all pre-commit hooks GREEN |
| Task 4F coordinator progress/completion | Complete | nio `ec9fe0da38378e7a7d5d100737787fb7584d8662`; exact patch `1ca31931...`; 2,731 passed/3 skipped full suite |
| Task 5C durable session factory/controller resolver | Complete | MindRoom `ba0da04b5553bac9ce36b92cb1f0133211b1c3aa`; nio typed prerequisite `b61e186b2b63e2b81149c40ca67c0435a60ede60`; all recorded gates GREEN |
| Task 5D durable MindRoom activation | Complete | nio `1ea9e4aae108c0f1fd2d1bec1ba81f188af408c4`; MindRoom `47180cc07e7d0177ff4981bd7ccf1f1ec65eb047`; full suites/hooks GREEN and final review READY YES |
| Task 6 | Complete | nio `70d21bcb6b08a9528104d16a9c6c4537b4bf1a8a`; MindRoom `253c76245174f106162368993bf1393d452c6698`; full suites, configured hooks/statics, review, deletion mapping, and fixed-budget evidence GREEN |
| Task 7 | Complete | nio `6e4f14aa2b438ba431d8f7c7ab2ff176f4e11a94`; MindRoom `2cbeecb6f8e68f380a7fabb3cbe28cd88f3e1a2f`; affected suites/statics and exact crash manifest GREEN |
| Task 8 | In progress | Classic exposed a membership race; nio fix `1c6bd92e42a7c8633c0a84bb2dff2978713cc3d3` is fully GREEN and exact wheel/canary rebuild is next |
| Tasks 9–10 | Not started | Preserve the remaining dependency order |

### Task 5C completed checkpoint

Task 5C implementation is complete in both repositories. The MindRoom tree is
tracked-clean at the required commit above. nio's three-file typed prerequisite
is committed at `b61e186...`; the next nio commit is this docs-only completion
handoff. Verify its subject and parent with the restart commands rather than
embedding its self-referential commit ID here.

Historical discovery completed before the first RED; every gap below is now
resolved by the active implementation checkpoint later in this section:

- `src/mindroom/matrix/client_session.py` owns `_MindRoomAsyncClient`,
  `create_authenticated_client()`, password `login()`, token login, and restored
  login. Password login currently keeps one configured client after HTTP login;
  token login closes a temporary credential client and then constructs the
  authenticated client.
- `src/mindroom/matrix/users.py` selects restored, password, or appservice
  credentials; the appservice path can obtain credentials through HTTPX without
  constructing a temporary MatrixStore/AsyncClient.
- `src/mindroom/bot.py::Bot.start()` previously received only a client. Task 5C
  now makes the bot own the private ingestion session separately;
  `Bot.sync_forever()` remains unchanged because Task 5D owns durable-engine
  activation.
- `src/mindroom/desktop/identity.py` previously read the per-device SQLite store
  directly. Task 5C now uses an injected resolver over the target bot's
  already-owned live client, with no SQLite read/open in that identity path.
- nio's private `_open_fresh_ingestion_store()`,
  `_open_configured_ingestion_store()`, and `_open_owned_ingestion()` are the
  only authorized construction path. Do not introduce a public nio API, a
  second store open, or an independent ingestion database.

The configured-store-class ambiguity was resolved on 2026-08-14 with Claude
Fable through the installed `agent-cli dev` workflow. The documented
`agent-cli-dev` wrapper name was not on `PATH`; the available backend was
`/home/basnijholt/.local/bin/agent-cli`. Its configured Vertex credential path
was stale/missing, so the successful Fable invocation used the isolated
`task5c-store-adoption-fable` worktree and unset only the Vertex-routing
variables, falling back to Claude's existing local authentication. Fable's
decisive recommendation was the smallest option:

1. call the existing configured opener with exact `DefaultStore`;
2. if and only if nio raises a dedicated typed, machine-readable no-DML signal
   that the store is already marked and therefore requires `SqliteStore`, close
   that failed bootstrap/owner completely and retry the exact same
   account/device/generation/source/path with exact `SqliteStore`;
3. never branch on exception text, inspect SQLite before the lease, try
   `SqliteStore` optimistically, or persist a MindRoom-side class marker.

The lease gap is acceptable because the first marked-store attempt performs no
DML and releases all ownership, while the second opener repeats full ordinary
and ingestion graph authentication under a fresh exclusive lease before writer
or source mutation. If the current `FreshIngestionRequired` class is too broad
to distinguish this case without text matching, add the narrowest private nio
subclass/signal; do not add an auto-classifying public opener.

Task 5C TDD order and state:

1. [x] add a focused MindRoom credential-to-owned-session factory RED proving the
   temporary no-store/no-sync credential client's HTTP closes on success,
   login error, owned-factory failure, and cancellation, while appservice
   credentials create no temporary MatrixStore/client;
2. [x] add configured adoption/marked-reopen rows using the typed nio signal and
   prove exact account/session/token/E2EE preservation plus one owned session;
3. [x] add bot ownership and cleanup ordering: runner/pump are still unwired,
   session closes/releases before HTTP, every cleanup lane runs, and
   replacement waits for lease release;
4. [x] replace the raw desktop identity reader with the injected live-bot resolver
   and cover target/missing/mismatch/replacement without any SQLite open;
5. [x] commit MindRoom and the nio typed prerequisite ordinarily, with
   pre-commit, final diff/budget audit, and the full suite GREEN. Commit this
   final handoff docs-only before Task 5D.

First checkpoint is GREEN:

- MindRoom `_MatrixCredentials`, `_create_credential_client()`, and
  `_login_password_credentials()` keep password HTTP on an explicit storeless
  client and close it before returning credentials; success, permanent login
  rejection, and `CancelledError` are 3/3 GREEN. `_restore_credentials()` now
  performs the persisted-token `whoami` check on the same storeless boundary
  and closes that temporary HTTP client before returning exact credentials.
- nio `_MarkedStoreRequiresSqlite` is a private subclass of
  `FreshIngestionRequired`; the exact marked-Default probe raises that type,
  emits no DML, and preserves the full graph. Its focused probe plus the 11-row
  configured negative matrix are 12/12 GREEN.
- Exact touched production Ruff and formatting checks are GREEN after import
  normalization; do not force Ruff over excluded nio tests.

The real credential-to-owned-session factory checkpoint is also GREEN:

- `_open_owned_matrix_session()` loads/creates one MindRoom consumer, uses
  nio's fresh or configured bootstrap, binds the returned stream ID back to the
  exact consumer, constructs one pristine `SqliteStore`-configured client
  without calling `restore_login()`/`load_store()`, and transfers it through
  `_open_owned_ingestion()`.
- real fresh creation, real historical DefaultStore adoption, and typed marked
  SqliteStore reopen preserve the exact Olm identity and stream binding;
  transfer failure, a simultaneously failing HTTP cleanup lane, and
  cancellation all release the bootstrap so the same graph reopens.
- complete `tests/test_matrix_client_session.py` is now 21/21 GREEN; exact
  production Ruff, Ruff format, ty, and diff-check are GREEN. The nio typed
  signal/negative selector remains 12/12 GREEN with Black/Ruff/diff-check.

The high-level agent-owned session checkpoint is GREEN:

- appservice credential acquisition is factored into a direct HTTPX-only
  helper; the legacy `login_appservice_user()` remains for old callers, while
  Task 5C creates no temporary Matrix client/store on that path;
- `_owned_agent_credentials()` preserves the existing restored/password/
  appservice selection semantics while returning exact `_MatrixCredentials`;
- `login_agent_owned_session()` obtains credentials first, calls the owned
  factory exactly once, validates identity, persists the session, and performs
  cross-signing. A post-open failure closes the ingestion session before the
  client's HTTP lane while preserving the primary exception;
- the complete three-file auth/factory suite is 82/82 GREEN
  (`test_matrix_client_session.py` 21, `test_matrix_appservice.py` 14,
  `test_matrix_agent_manager.py` 47), with production Ruff and ty clean.

The Bot owned-session lifecycle checkpoint is GREEN:

- `_bot_ingestion_config()` freezes the existing Classic timeout/filter or the
  Sliding connection/list/room-subscription/extension settings into canonical
  nio source bytes;
- `Bot.start()` passes its exact existing journal principal as the durable
  consumer store, generates only a candidate generation, calls
  `login_agent_owned_session()` once, and retains the returned ingestion
  session separately from `client`;
- startup error and `CancelledError` after ownership both release the ingestion
  session before HTTP, continue the second cleanup lane if the first raises,
  preserve the primary failure, and clear both Bot fields;
- `Bot.stop()` releases the ingestion session before HTTP and still runs HTTP
  cleanup when session close raises. The focused Bot lifecycle/scheduling
  group is 89/89 GREEN; all former Bot-start mocks now use the owned carrier,
  and affected sync-continuity/multi-agent/streaming selectors are GREEN;
- exact touched production Ruff and ty plus diff-check are GREEN. The durable
  runner/pump and `Bot.sync_forever()` remain unchanged as required.

The completion-sink and live-controller checkpoint is GREEN:

- `login_agent_owned_session()` and the private owned-session factory accept
  and forward the exact completion sink. `Bot.start()` installs the bound
  `_on_ingestion_frame_completion` seam before any future source activation;
  the method is intentionally a no-op until Task 5D, and no runner/pump is
  activated in Task 5C;
- `controller_identity_for_live_bot()` validates a currently running target
  Bot, registry/entity name, persisted/live Matrix user and device IDs, and
  the live Olm ed25519 fingerprint. It never opens SQLite or a second store;
- the orchestrator resolves the target from its current registry on every
  call, so replacement is observed dynamically. Desktop command setup and
  confirmation receive that resolver through `CommandTurnExecutorDeps` and
  `CommandHandlerContext`; missing, stopped, mismatched, or replaced targets
  fail closed;
- the final post-boundary auth/factory group is 82/82 GREEN and the combined
  Bot/controller focused group is 164/164 GREEN. Earlier turn/command and
  scheduling groups remain GREEN, and continuity/multi-agent/streaming is
  138 passed/2 skipped;
- the final full MindRoom run on the current dirty bytes is 13,704 passed and
  123 skipped. Exact changed production Ruff and ty, changed formatting, and
  `git diff --check` are GREEN. One initial full-run failure was a hand-built
  `object.__new__(AgentBot)` fixture missing the new field; it now explicitly
  initializes `_ingestion_session = None`, and the full rerun is GREEN;
- nio `tests/ingest/journal_test.py` is 86/86 GREEN; Black, Ruff, and
  `git diff --check` are GREEN. The raw two-file mypy command still reports six
  unchanged pre-existing diagnostics at preflight lines 813 and 1312–1313;
  no Task 5C changed hunk owns those diagnostics.
- Fable's ownership correction is implemented. Exact Tach dependency/interface
  validation and Privata `--methods` are GREEN; the final all-files MindRoom
  pre-commit run passes every configured hook when invoked with
  `UV_NO_SYNC=1` after installing the linked nio checkout editable. The flag is
  required because an ordinary internal `uv run` otherwise restores the
  released `mindroom-nio==0.37.0` pin before `ty`, which cannot see Task4F;
- the complete staged MindRoom Task5C patch is
  `fd617dcca42d7cf740e665c6ffd7d7dbab9f97a93f82edbc11ceba73c644c49f`.
  It is +609 net runtime lines and +639 net test lines versus MindRoom HEAD.
  The whole branch versus fixed post-prune `0acaea2ba` is currently +1,358
  runtime and +2,543 tests; this is the explicitly expected intermediate
  over-budget state before Tasks 6/9 old-engine deletion and consolidation,
  not a final budget claim.

The committed MindRoom Task5C manifest (29 files) is:

```text
src/mindroom/bot.py
src/mindroom/command_turn_executor.py
src/mindroom/commands/desktop_commands.py
src/mindroom/commands/handler.py
src/mindroom/desktop/identity.py
src/mindroom/matrix/_owned_session.py
src/mindroom/matrix/appservice.py
src/mindroom/matrix/client_session.py
src/mindroom/matrix/sync_loop.py
src/mindroom/matrix/users.py
src/mindroom/orchestrator.py
src/mindroom/runtime_protocols.py
tach.toml
tests/bot_helpers.py
tests/test_bot_scheduling.py
tests/test_bot_sync_lifecycle.py
tests/test_commands.py
tests/test_desktop_cloudflare_access.py
tests/test_desktop_identity.py
tests/test_desktop_pairing.py
tests/test_matrix_agent_manager.py
tests/test_matrix_appservice.py
tests/test_matrix_client_session.py
tests/test_matrix_sync_continuity.py
tests/test_multi_agent_bot.py
tests/test_multi_agent_e2e.py
tests/test_streaming_e2e.py
tests/test_sync_task_cancellation.py
tests/test_turn_controller_focused.py
```

These exact bytes are committed as
`ba0da04b5553bac9ce36b92cb1f0133211b1c3aa` with subject
`feat: own durable Matrix ingestion sessions`. The nio prerequisite is
committed as `b61e186b2b63e2b81149c40ca67c0435a60ede60`; its journal file is
86/86 GREEN, and exact changed-file Black/Ruff, focused mypy, `py_compile`,
diff-check, and changed-file pre-commit all pass. Immediate next: commit this
final handoff docs-only, then begin Task 5D from its plan section. If a causal
failure appears, fix it without an approval pause. If a material ambiguity
survives local evidence, consult Claude Fable through the isolated
`agent-cli-dev` workflow, record the decision here, and continue. Do not
activate the runner/pump or change `Bot.sync_forever()`; that remains Task 5D.

The first all-files pre-commit run exposed one test-style finding plus genuine
Tach/Privata ownership failures. Per the autonomy rule, Claude Fable was
consulted through an isolated `task5c-boundary-fable-20260814` `agent-cli dev`
worktree with `--model=fable`; the stale Vertex credential variables were
unset for the successful local-auth invocation. Its accepted decision is:

1. move the entire exact owned-session cluster into the private package module
   `mindroom.matrix._owned_session`; use public symbol names *inside that
   private module* so `matrix.users` can consume them without Privata evasion,
   while Tach visibility permits only `matrix.users`. Keep the concrete
   `IngestionConsumer` dependency there rather than weakening exact-type and
   equality checks to a structural approximation;
2. keep `client_session` slim. Expose only the three low-level client primitives
   needed by `_owned_session` through a second Tach interface visible solely to
   that private module; do not add the owned factory to the broad existing
   client-session interface;
3. move canonical Bot ingestion-config construction into `matrix.sync_loop` as
   one public function, leaving its three sliding request-shape helpers private
   and co-located with the live sync request construction;
4. retain `desktop.identity` as the dependency-free controller identity value
   and resolver module, declare the deliberate Bot/orchestrator Tach edges,
   replace untyped `getattr` probing with private read-only structural
   Protocols, and keep command/runtime consumers on the narrow callable value
   seam;
5. rename the direct appservice credential helper public within its already
   one-consumer module boundary instead of reaching it through a private
   attribute. No runner/pump, store, SQLite, or controller semantics change.

This exact ownership correction is implemented and all named gates are GREEN.
The unrelated nio
all-files Ruff findings are two pre-existing timeout-parameter rules in
`scripts/live_sliding_sync_check.py`; Task 5C's exact changed-file Black/Ruff
gates are GREEN, so do not edit that unrelated script.

### Task 4E completed and committed

The following Task 4E slices are implemented, causally tested, independently
reviewed, and must not be reopened without a new failing case:

- authenticated journal settlement and room-restore views:
  `SqliteIngestionJournal._load_batch_settlement()` and
  `_load_room_restore_view()`;
- open-time room restoration before session exposure, including exact
  attach/detach rollback of `rooms`, `invited_rooms`, and `next_batch`;
- snapshot routing by current continuity: `invite|knock` to
  `invited_rooms`, and `join|leave|ban|None` to `rooms`; no persisted snapshot
  means no fabricated room skeleton;
- stale snapshot `own_membership`/`membership_epoch` values are presentation
  carrier fields, not lifecycle routing authority;
- in-place snapshot overlay preserves auxiliary room state and existing
  `MatrixUser` presence fields;
- exact fact validation and authenticated settlement before parsing, state
  application, or callback;
- one session-local settlement lock, same-task recursion rejection, public ack
  fencing, duplicate/already-acked idempotency, callback error/cancellation
  replay, and before-/after-commit ack uncertainty;
- poison and close rechecks before ack; external close cancels and drains an
  active settlement before detach; callback self-close rejects before lease
  preflight; cancellation suppression cannot ack after close intent;
- `GLOBAL_ACCOUNT_DATA` callback settlement;
- SOURCE/NONE `TO_DEVICE` callback settlement and SOURCE/NONE no-route
  ack-only settlement without crypto replay;
- `ROOM_LIFECYCLE` snapshot overlay with no nio callback;
- `LossRecord` state-free, callback-free ack-only settlement;
- TIMELINE/NONE settlement: authenticated snapshot overlay first, parse only
  for `semantic_event_new`, `_on_event` only for a new semantic event, and no
  response/admission/decrypt/live replay.
- decrypted ROOM_KEY `TO_DEVICE` reconstruction directly from authenticated
  sanitized clear JSON plus outer Olm sender key, without generic parsing,
  session lookup, persisted-store load, decryption, preparation, or replay;
- decrypted TIMELINE reconstruction after authenticated in-place snapshot
  overlay via `Event.parse_decrypted_event(clear)`, restoring the authenticated
  verification bit plus outer Megolm sender/session keys and durable room ID,
  without live decryption or timeline replay.
- failed-Megolm TIMELINE reconstruction from the authenticated outer source,
  restoring only durable `room_id` after snapshot overlay and never retrying
  decryption, session lookup, wedge detection, key-request collection, or
  prepared outbound-plan mutation.
- first-seen non-JOIN room membership no longer creates an impossible JOIN-only
  hydration barrier: cold invite and knock Aggregates retain their snapshots,
  have no hydration intent, and release lifecycle/STATE Work immediately;
  first-seen JOIN hydration remains unchanged.
- SOURCE/NONE STATE settlement: invite callback routes overlay first, parse
  only with `InviteEvent`, apply inviter state, re-overlay the final snapshot,
  receipt-gate `_on_invited_rooms`, then ack; no-route joined/knock STATE only
  reconciles the authenticated snapshot in place and never guesses a parser.
- SOURCE/NONE auxiliary settlement: EPHEMERAL and ROOM_ACCOUNT_DATA overlay
  the authenticated room snapshot, parse and idempotently apply typing/tags,
  expose the applied state to receipt-gated callbacks, then ack; PRESENCE has
  no Aggregate, updates every matching user in `client.rooms`, exposes that
  update to `_on_presence`, preserves unrelated room/user state, then acks.

The main production seams are currently at:

```text
src/nio/store/_sync_journal.py:403   _load_batch_settlement
src/nio/store/_sync_journal.py:443   _load_room_restore_view
src/nio/client/base_client.py:574    _room_from_snapshot
src/nio/client/base_client.py:694    _overlay_room_snapshot
src/nio/client/base_client.py:1173   _attach_ingestion_store
src/nio/client/base_client.py:1275   _detach_ingestion_store
src/nio/ingest/coordinator.py:556    _apply_room_lifecycle_snapshot
src/nio/ingest/coordinator.py:643    _load_settlement_event
src/nio/ingest/coordinator.py:817    _OwnedIngestionSession._settle_batch
src/nio/ingest/coordinator.py:1648   _open_owned_ingestion
```

Line numbers are navigation aids only; names and behavior are authoritative.

### Task 4E detailed completion record (historical)

The latest test-only slice is:

```text
tests/ingest/coordinator_test.py:11072
test_owned_settle_reconstructs_decrypted_events_without_crypto_replay
```

It uses a real empty Classic warm Frame followed by the existing real owned
crypto Frame, Task 4C materialization, hydration, exact FIFO selection, close,
and fresh reopen. It has two parameter rows:

1. decrypted `TO_DEVICE` `ROOM_KEY`;
2. decrypted `$secret` `TIMELINE`.

The verified delivery FIFO for that real frame is:

| Position | Work | Frame index | Room sequence | Readiness |
| --- | --- | ---: | ---: | --- |
| 1 | decrypted room-key `TO_DEVICE` | 0 | — | initial revision, ordinal 0 |
| 2 | `ROOM_LIFECYCLE` join | 2 | 1 | initial revision, ordinal 1 |
| 3 | encryption `STATE` | 1 | 0 | after hydration, ordinal 0 |
| 4 | Alice-member `STATE` | 2 | 2 | after hydration, ordinal 1 |
| 5 | Bob-member `STATE` | 3 | 3 | after hydration, ordinal 2 |
| 6 | decrypted LIVE `$secret` `TIMELINE` | 4 | 4 | after hydration, ordinal 3 |

The room-key row claims position 1 without hydration. The timeline row applies
the one real hydration, directly acknowledges the exact five predecessors,
claims position 6, and then closes/reopens with the target still outstanding.
Capture crypto/E2EE invariance only after Task 4C preparation, because
preparation legitimately consumes the OTK and stores the inbound sessions.

The current exact command is:

```bash
uv run pytest -q --tb=short \
  tests/ingest/coordinator_test.py::test_owned_settle_reconstructs_decrypted_events_without_crypto_replay
```

Current result at this checkpoint is **2 passed**. Before production, the
hardened node failed both and only on
`LocalProtocolError: unsupported durable settlement record`:

```text
room-key -> src/nio/ingest/coordinator.py:912
timeline -> src/nio/ingest/coordinator.py:883
```

The minimal two production predicates are now implemented. The causal fixture
and the following hardening required by its prior independent review are
verified:

- [x] tripwire `session_store.get` and `inbound_group_store.get`, not only
      writes;
- [x] tripwire persisted-store crypto load methods;
- [x] tripwire preparation/materialization entrypoints during settlement;
- [x] assert exact metadata `record_id`, origin/room/event scalar fields;
- [x] assert the delivery frontier/Work is still outstanding inside every
      route/callback and immediately before ack.

The exact node is now **2 passed** with all generic/session-store/
persisted-store/preparation tripwires remaining hard. Focused settlement/close
is 29 passed, delivery is 48 passed, and the full coordinator file is 210
passed. Black, production Ruff `--no-fix`, isolated coordinator mypy,
`py_compile`, and diff-check are green.

The failed-Megolm test-only slice is now GREEN:

```text
tests/ingest/coordinator_test.py:11571
test_owned_settle_failed_megolm_without_crypto_or_rerequest_replay
```

It appends one real `$missing` encrypted timeline event with authenticated
`session_id="missing-session"` to the proven warm Classic crypto frame. Task 4C
legitimately fails decryption and freezes one exact pending room-key-request
operation in the retained prepared Frame. After hydration, exact FIFO drain,
close, and fresh reopen, the sole outstanding Work is LIVE TIMELINE sequence 5,
frame index 5, with SOURCE/MEGOLM_FAILED/EVENT metadata and no clear JSON.

Before production it had **1 failure**, solely
`LocalProtocolError: unsupported durable settlement record`. Its contract
requires snapshot overlay before
`Event.parse_event(source)`, exact `MegolmEvent.room_id` restoration, callback
before ack, no decrypt/session lookup/wedge check/key-request collection/store
load or write/reprepare path, unchanged E2EE and in-memory crypto queues, and
byte-identical retained outbound plan. Ack may only DELETE Work and UPDATE
Meta. The exact node is now 1 passed; focused settlement/close is 30 passed,
delivery is 48 passed, full coordinator is 211 passed, and Black, production
Ruff `--no-fix`, isolated mypy, `py_compile`, and diff-check are green.

The STATE settlement family is now GREEN:

```text
tests/ingest/coordinator_test.py:12070
test_owned_settle_state_uses_invite_parser_or_snapshot_only
```

Before production both rows failed solely with
`LocalProtocolError: unsupported durable settlement record` at the dispatcher:

- cold INVITE STATE uses real name + older/final own-member events. The target
  older member Work must overlay the authenticated final snapshot, parse only
  with `InviteEvent.parse_event`, call the exact invited-room handler to
  restore `inviter`, re-overlay so the older member cannot regress final
  snapshot fields, run `_on_invited_rooms` for `receipt_new`, then ack;
- joined STATE targets the real `$local-name` Work after hydration/reopen. Its
  route is None, so settlement must only overlay the final snapshot in place,
  run no event parser/handler/callback, then ack.

The tests hard-trip generic/invite parser misuse, response/sync/invite/join/
timeline/preparation replay, wrong callback families, room replacement, and
parse/apply before authenticated-read exit. Black and diff-check are green.
The exact STATE + cold invited selectors are 4 passed; focused settlement/
close/cold is 34 passed, reducer is 48 passed, selected first-seen/hydration
materializer is 58 passed, delivery is 48 passed, and full coordinator is 215
passed. Black, production Ruff `--no-fix`, isolated coordinator+reducer mypy,
`py_compile`, and diff-check are green.

The three-row auxiliary family is now GREEN:

```text
tests/ingest/coordinator_test.py:12502
test_owned_settle_auxiliary_state_before_receipt_callback
```

Before production all rows failed solely with
`LocalProtocolError: unsupported durable settlement record` after real Classic
materialization, JOIN hydration, exact FIFO selection, close, and fresh reopen:

- global PRESENCE has no correlated Aggregate; it must parse with exact
  `PresenceEvent.from_dict`, update every matching user in `client.rooms`,
  preserve unrelated user/room state, callback, then ack;
- room EPHEMERAL must overlay the authenticated snapshot, parse the typing
  event, call `room.handle_ephemeral_event`, callback, then ack;
- ROOM_ACCOUNT_DATA must overlay, parse the tag event, call
  `room.handle_account_data`, callback, then ack.

Every parser/apply/route/callback asserts authenticated-read exit and the Work
still outstanding. Wrong parsers/handlers/callback families plus response,
sync, join/invite/timeline, preparation, and materialization paths are hard
tripwires. The target auxiliary state is visible before callback while typing,
tags, fully-read state, snapshot state, and Alice presence outside the target
remain preserved. Ack is the only DML (Work DELETE + Meta UPDATE). Black and
diff-check are green. Exact auxiliary is 3 passed; focused
settlement/close/cold-room is 37 passed, delivery is 48 passed, reducer is 48
passed, selected first-seen/hydration materializer is 58 passed, and full
coordinator is 218 passed. Black, production Ruff `--no-fix`, isolated
coordinator+reducer mypy, `py_compile`, and diff-check are green.

The synthetic to-device preparation phases are now GREEN:
`EXPIRED_VERIFICATION` must reconstruct its intentionally typeless
`KeyVerificationCancel` directly and use the expired-verification callback
path without `olm.clear_verifications`; `COLLECTED_KEY_REQUEST` must parse its
authenticated source and use `_on_to_device` without
`olm.collect_key_requests`. Both require real Task 4C-produced Work, exact
phase/route metadata, callback-before-ack/outside-transaction assertions, and
unchanged crypto/key-request state.

The real two-row synthetic-phase test is frozen at:

```text
tests/ingest/coordinator_test.py:9330
test_owned_settle_synthetic_to_device_phase_callbacks_without_replay
```

One real Classic frame contains a source key-request cancellation, one typeless
expired verification returned by Task 4C's `clear_verifications()` phase, and
the resulting collected cancellation. Each row proves exact source → expired
→ collected FIFO, directly acknowledges only its predecessors, leaves the
target outstanding across close/fresh reopen, authenticates exact phase,
effective type, NONE decryption, TO_DEVICE route, origin index, and no room
fields, then requires the phase-specific parser and callback before ack. Before
production both failed solely at
`LocalProtocolError: unsupported durable settlement record`. Exact synthetic
is now 2 passed; focused settlement/close/cold-room is 39 passed, delivery and
reducer are 48 passed each, selected first-seen/hydration materializer is 58
passed, and full coordinator is 220 passed. Black, production Ruff `--no-fix`,
isolated coordinator+reducer mypy, `py_compile`, and diff-check are green.

The remaining decrypted SOURCE/TO_DEVICE kind matrix is now GREEN:
FORWARDED_ROOM_KEY, DUMMY, UNKNOWN, BAD, and UNKNOWN_BAD all authenticate their
frozen kind and sanitized clear source across close/reopen, reconstruct the
exact event class without Olm/session/store replay, receipt-gate
`_on_to_device`, then ack.

The five-row causal RED is frozen at:

```text
tests/ingest/coordinator_test.py:9693
test_owned_settle_remaining_decrypted_to_device_exact_classes
```

Each row uses a valid encrypted source event and Task 4C's real preparation,
reducer, materializer, authenticated Work, FIFO claim, close, and fresh reopen.
The prepared decrypted result is exact ForwardedRoomKeyEvent, DummyEvent,
UnknownToDeviceEvent, BadEvent, or UnknownBadEvent. The test binds the frozen
kind/effective type/source/clear/origin metadata, requires direct construction
for forwarded/unknown-bad and the class-specific parser for dummy/unknown/bad,
forbids generic parsing plus live client/Olm/session/MatrixStore/reprepare
paths, and pins callback-before-ack with unchanged E2EE rows. Before production
all five failed solely with
`LocalProtocolError: unsupported durable settlement record`. Exact remaining
decrypted kinds are now 5 passed; focused settlement/close/cold-room is 44
passed, delivery and reducer are 48 passed each, selected
first-seen/hydration materializer is 58 passed, and full coordinator is 225
passed. Black, production Ruff `--no-fix`, isolated coordinator+reducer mypy,
`py_compile`, and diff-check are green.

The authenticated malformed-semantic and final disposition matrix is now
GREEN. MAC-valid corruptions keep Work/header/authentication valid while making
inner semantic carriers impossible; they fail with `JournalIntegrityError`,
zero callback/state/ack, and byte-identical Work/frontier. One literal
successful fate is verified for every RecordKind and LossRecord.

The first bounded malformed matrix is frozen at:

```text
tests/ingest/coordinator_test.py:13844
test_owned_settle_rejects_authenticated_malformed_presence_without_ack
tests/ingest/coordinator_test.py:13977
test_owned_settle_rejects_authenticated_malformed_collected_request
```

All cases mutate only canonical inner source JSON, rebuild the authenticated
Work envelope/digest with the real journal codec, preserve its clear header and
metadata, close, and pass full reopen preflight. Three PRESENCE schema failures
already fail closed with exact `JournalIntegrityError` and no callback/DML.
Before production the collected key-request case was the causal RED: its
authenticated source lacks `requesting_device_id`, generic parsing returns a
BadEvent, and the old dispatcher wrongly reached `_on_to_device`. Settlement
now requires exact `RoomKeyRequest` or `RoomKeyRequestCancellation` before
callback. All six malformed rows pass; the graph, outstanding Work, and
frontier remain unchanged on failure.

The final literal disposition selector is 13 passed and covers all eight
RecordKinds plus LossRecord. The complete checkpoint is: malformed 6 passed,
focused settlement/close/cold-room 48 passed, delivery and reducer 48 passed
each, selected first-seen/hydration materializer 58 passed, and full
coordinator 229 passed. Black, production Ruff `--no-fix`, isolated
coordinator+reducer mypy, `py_compile`, and diff-check are green.

Task4E's final review, consolidation decision, budget measurement, full suite,
static gates, and ordinary implementation commit are complete. At that
historical checkpoint the next boundary was Task5B in `/work/dev/mindroom`.
Task5B is now complete at the commit recorded above; the current restart
boundary is Task4F, not this historical Task4E checklist.

That final review is now complete with no P0–P2 blocker. No mechanical
Task4E-only consolidation was applied: the only clear duplication is a crypto
fixture shared conceptually with an already committed standalone crash-test
module, and extracting it now would add a cross-module test dependency for
little net reduction. Full repository verification on the exact recorded hash
is **2,672 passed, 3 skipped, 5 warnings** in 411.50 seconds. Final Black,
production Ruff `--no-fix`, isolated coordinator+reducer mypy, `py_compile`,
and diff-check are green. The implementation is committed at
`be24cf315c27250d3c2b95a5862278420ee8d270`; this living-document checkpoint
records that boundary.

The STATE prerequisite was discovered and closed causally at
`test_owned_cold_invited_state_is_ready_without_join_hydration` (invite and
knock). Before the reducer fix, both rows created a `HydrationIntent` that
`PendingHydration` could never represent because hydration is exact-JOIN-only.
The no-transition and prepared-transition reducer paths now create a hydration
barrier only when the effective/current membership is `join`. Evidence: 2 cold
integration, all 48 reducer, 58 selected first-seen/hydration materializer, and
4 invited-room reopen tests pass; reducer Black/Ruff/mypy/compile/diff gates
are green.

The required reconstruction contract is already fixed:

- decrypted room key: never call generic `ToDeviceEvent.parse_event(clear)` or
  `RoomKeyEvent.from_dict(clear, ...)`; Task 4C deliberately stripped `keys`
  and `content.session_key`. Directly construct `RoomKeyEvent` from the
  authenticated sanitized clear source plus the authenticated outer Olm
  sender key and clear `room_id`, `session_id`, and `algorithm`;
- decrypted timeline: overlay the authenticated current snapshot first, call
  `Event.parse_decrypted_event(clear)`, then restore `decrypted=True`, the
  authenticated verification bit, outer Megolm `sender_key`/`session_id`, and
  the record `room_id`; never invoke Olm or a live timeline handler.

### Task 4E completed execution checklist (historical)

This checklist records how Task4E was completed. It is not the current restart
point; all items remain checked as historical evidence.

- [x] Add only the five test hardenings listed above.
- [x] Run the exact node and confirm the same two sole unsupported-record REDs.
- [x] Reconcile the prior independent RED review: every requested P1/P2
      hardening is present and the exact causal failures are unchanged.
- [x] Implement the minimal two exact decrypted predicates and constructors.
- [x] Run the exact GREEN first, then the existing settlement/close, full
      coordinator, delivery, formatting, lint, type, compile, and diff gates.
- [x] Audit the production slice for authenticated/canonical input, snapshot-
      before-parse ordering, no crypto/store/reprepare path, callback-before-
      ack, poison/close fencing, and exact kind/metadata predicates.
- [x] Add a test-only causal `MEGOLM_FAILED` RED from a real Task 4C failure;
      prove settlement neither decrypts/checks wedge nor queues another key
      request and preserves the post-preparation E2EE/request graph.
- [x] Add only the exact SOURCE/MEGOLM_FAILED/EVENT timeline settlement branch,
      reconstruct via `Event.parse_event(source)`, restore `room_id`, and use
      the existing overlay/callback/ack fence.
- [x] Run the exact GREEN, focused settlement/close, delivery, full
      coordinator, and all static gates; then update this handoff before STATE.
- [x] Add the smallest real STATE test family: INVITE+EVENT parses with
      `InviteEvent.parse_event`, restores inviter without regressing the final
      snapshot, callbacks only for `receipt_new`; joined STATE with route None
      performs snapshot-only in-place reconciliation and no event parsing or
      callback.
- [x] Implement only exact SOURCE/NONE STATE metadata: EVENT route requires an
      invited projection, invite parser/handler, final re-overlay, and invited
      callback; route None performs overlay-only reconciliation.
- [x] Run the exact GREEN, cold invite/knock and all prior settlement gates,
      full coordinator/delivery, reducer/materializer regressions, and static
      checks; update the handoff before auxiliary kinds.
- [x] Add one real joined-room auxiliary frame and three causal rows:
      EPHEMERAL typing, ROOM_ACCOUNT_DATA tags, and PRESENCE. Each must overlay
      the authenticated snapshot before parsing, idempotently apply its
      non-snapshot state, expose that state before the exact callback, preserve
      unrelated auxiliary fields, receipt-gate callbacks, and ack last.
- [x] Implement only exact SOURCE/NONE metadata for those three kinds, using
      their exact parsers/state handlers and the existing callback/ack fence;
      do not add any other RecordKind.
- [x] Run the exact GREEN, all prior settlement/close/cold-room and full
      coordinator/delivery/reducer/materializer/static gates, then update this
      handoff before synthetic to-device phases.
- [x] Add a real Task 4C-produced two-phase synthetic to-device family:
      typeless expired verification and collected key request. Prove exact
      phase metadata, parser/constructor, callback family, no crypto replay,
      callback-before-ack, restart, and FIFO behavior.
- [x] Confirm each exact node first fails only at the unsupported-record
      dispatcher before adding any production behavior.
- [x] Implement only two exact non-room `TO_DEVICE` predicates: direct
      `KeyVerificationCancel.from_dict` plus `_on_expired_verifications`, and
      `ToDeviceEvent.parse_event` plus `_on_to_device`; retain the common
      poison/close/ack fence and do not call either Olm collection method.
- [x] Run exact GREEN, focused settlement/close, delivery, full coordinator,
      reducer/materializer and static gates; update this handoff before the
      remaining decrypted to-device kinds.
- [x] Add the smallest real Task 4C-produced remaining-decrypted-to-device RED
      matrix for forwarded room key, dummy, unknown, bad, and unknown-bad;
      prove exact class reconstruction, parser/constructor choice, restart,
      callback-before-ack, and zero crypto/store replay.
- [x] Confirm every row fails only at the unsupported-record dispatcher before
      adding production behavior.
- [x] Implement only the five exact SOURCE/DECRYPTED/TO_DEVICE kind cases with
      their direct/class-specific reconstruction; retain receipt-gated
      `_on_to_device` and the common poison/close/ack fence.
- [x] Run exact GREEN, focused settlement/close, delivery, full coordinator,
      reducer/materializer and static gates; update this handoff before the
      malformed-semantic/final disposition matrix.
- [x] Add a bounded authenticated malformed-semantic settlement matrix that
      corrupts parseable inner source/clear fields while resealing Work; prove
      fail-closed `JournalIntegrityError`, no side effects, and replayability.
- [x] Require collected key-request reconstruction to produce exact
      `RoomKeyRequest` or `RoomKeyRequestCancellation` before callback; rerun
      the malformed and valid synthetic nodes.
- [x] Add/confirm one literal successful disposition for every RecordKind and
      LossRecord, with the correct receipt/semantic gate and callback/state
      fate.
- [x] Review and consolidate the full Task4E diff without weakening causal or
      crash/restart coverage; measure fixed-baseline budgets.
- [x] Run full repository tests and complete static gates, update the handoff,
      and commit `feat: settle durable records through compatibility callbacks`
      only if the final review has no blocker.
- [x] Commit the six implementation/test files with that exact message, then
      update repository HEAD/state/hash instructions and commit this living
      documentation checkpoint separately.

Task 5B is complete. Its execution checklist is retained here as evidence:

- [x] Re-read the complete MindRoom `AGENTS.md`, the controlling Task 5B
      description, Task 5A admission contract, and the committed nio owned
      settlement contract before editing.
- [x] Locate the existing MindRoom sync lifecycle, Task 5A admission seam, and
      shutdown/reload ownership; record the smallest focused adapter/pump seam
      in this handoff.
- [x] Add the first real `max_records=1` causal RED at that owning seam. It must
      prove Task 5A returns separate exact `receipt_new` and
      `semantic_event_new` facts and that MindRoom passes those facts to
      `session._settle_batch()` without separately acknowledging the batch.
- [x] Extend the RED matrix to every durable disposition: actionable/context
      timeline, lifecycle/departure fencing, loss/history recovery, and the
      six compatibility-receipt families. Malformed/unknown values must remain
      outstanding and no known value may wedge the FIFO.
- [x] Implement the smallest all-record adapter and one-record pump, preserving
      the existing MindRoom event-journal transaction as application authority
      and nio settlement as callback/ack authority.
- [x] Prove cancellation, restart/replay, callback failure, duplicate receipt,
      and orderly stop/reload behavior before the ordinary Task 5B commit
      `feat: consume every durable ingestion record`.
- [x] Run the exact, focused, full MindRoom, formatting, lint, type, import-
      graph, Tach (if its boundary changes), and diff gates; independently
      review the completed slice, update this handoff, and commit only GREEN.

The Task 5B owning seam is
`/work/dev/mindroom/src/mindroom/matrix/durable_ingestion.py`, with focused
tests in `tests/test_durable_ingestion_admission.py`. At the clean Task 5A
boundary, `validate_ingestion_batch()` accepts only one Classic LIVE TIMELINE
`EventRecord`, and `consume_one_ingestion_batch()` admits it then directly
called public `session.acknowledge_batch()`. Task 5B replaced that last step
with the non-exported owned-session `_settle_batch()` call and extended the
same validator/admission seam to both `RecordOrigin` and `SystemOrigin`, every
`RecordKind`, and `LossRecord`. It uses a local structural private-session
Protocol without widening nio's public `IngestionSession` contract. The pump
loops this one-record function and wakes MindRoom semantic dispatch after
admission, but it is not wired into `bot.py`, `orchestrator.py`, or the live
sync lifecycle until Tasks 5C/5D provide the owned factory and sole-engine
activation. Task 5B owns pump cancellation/stop behavior in isolation; Task 4F
supplies the runner/pump wake and Frame-progress state machine.

Task 5B is committed at MindRoom
`3a80947593bbf6fe508ca2e7499f19e951f416d3`. Its implementation commit contains
exactly:

```text
src/mindroom/event_journal/journal.py
src/mindroom/matrix/client_session.py
src/mindroom/matrix/durable_ingestion.py
src/mindroom/matrix/journal_ingress.py
tests/test_durable_ingestion_admission.py
```

Immediately before commit, the exact binary patch over `src tests` had SHA-256
`56de0e7e5dea4c5930411d644e01d0ef2bbdc20dd254b4f0bc1d0ab63102d8b1`.
On restart verify the clean commit, not that former dirty-patch hash. The first
RED proved current MindRoom called public `acknowledge_batch()` after admission;
the private-owned fake made that a hard tripwire. The adapter now uses a local
structural owned-session Protocol and awaits
`session._settle_batch(batch, receipt_new=..., semantic_event_new=...)` for all
three exact fact pairs. Existing cancellation, admission error, malformed fact,
settlement error, and duplicate replay tests were migrated to that authority;
19 focused adapter cases pass.

The literal validator matrix now has 18 GREEN rows: Classic and Sliding
LIVE/RECOVERED/HISTORY timeline, decrypted timeline, failed Megolm,
nonsemantic timeline, state/section/timeline reported lifecycle, local
lifecycle, all six compatibility kinds, and transport/system losses. Initial
reported lifecycle preserves its real `previous_membership=None` evidence only
at epoch zero; Task 5A now accepts that exact case, while LOCAL transitions
remain string-to-string with current membership restricted to `join|leave`.
Reported lifecycle evidence is bound to Task 4C's exact section/state/timeline
source correlations, and non-timeline compatibility event IDs are bound to the
canonical source. Lifecycle commits its receipt and fencing effects without
claiming a semantic Matrix event: its first-admission facts are exactly
`receipt_new=True` and
`semantic_event_new=False`. The isolated `run_ingestion_pump()` drains only
`max_records=1`, passes every fact pair to owned settlement, wakes MindRoom
dispatch only for semantic work, waits on an injected non-polling wake boundary
when idle, and propagates cancellation.

Final evidence on 2026-08-14 is 319 collected/passing cases in the complete
owning `tests/test_durable_ingestion_admission.py` file across SQLite and
Postgres, plus an exit-zero full MindRoom suite on the exact committed bytes.
The direct full pre-commit run also passed every configured hook: whitespace,
EOF/docstring/YAML checks, Ruff check/format, ty, vulture, documentation/model
regeneration, extras, Tach, privacy, `pyproject-fmt`, Prettier, ESLint, Bun lock,
and TypeScript type checking. Exact-file Ruff format/check, ty, compileall, and
`git diff --check` were rerun after the suite and were GREEN.

Because the released `mindroom-nio` wheel does not yet contain this unmerged
ingestion surface, install the linked checkout editable before MindRoom tests.
Do not use a repository-wide `PYTHONPATH`: sandbox-worker subprocesses inherit
it and then fail to import their isolated MindRoom environment.

```bash
cd /work/dev/mindroom
uv pip install --python .venv/bin/python --no-deps -e /work/dev/mindroom-nio
.venv/bin/python -m pytest -q -n 0 \
  tests/test_durable_ingestion_admission.py
```

Historical transition satisfied: Task 5C now owns the private session factory,
credential/cleanup lifecycle, completion-sink seam, and live-controller
identity resolver. Task 5B/5C still deliberately do not activate the pump in
`bot.py` or `orchestrator.py`; Task 5D owns candidate-engine activation.

The committed Task 5B slice is +331 net runtime lines and +914 net test lines
versus MindRoom parent `da6ac44e4`. The complete branch versus the fixed
post-prune `0acaea2ba` baseline is +749 runtime and +1,904 tests. This is an
intermediate state, not a budget pass: Tasks 6 and 9 must perform the planned
old-engine deletion and test consolidation before the fixed final +350/+1,000
limits can be claimed.

After the decrypted pair, complete Task 4E in this order:

1. [x] `MEGOLM_FAILED` reconstruction without decrypt/wedge/key-request replay;
2. [x] STATE invite callback and joined/no-route snapshot-only reconciliation;
3. [x] EPHEMERAL, ROOM_ACCOUNT_DATA, and PRESENCE idempotent state apply plus
   receipt-gated callbacks;
4. [x] expired-verification and collected-key-request synthetic phases;
5. [x] remaining decrypted to-device kinds: forwarded room key, dummy, unknown,
   bad, and unknown-bad;
6. [x] authenticated malformed-semantic fail-closed cases and final complete
   RecordKind/Loss disposition matrix;
7. [x] full Task 4E verification, review, consolidation, budget check, and the
   ordinary commit `feat: settle durable records through compatibility callbacks`.

### Historical Task 6 start instruction (superseded)

This was the clean handoff into Task 6. It remains only as decision history;
the active authority is the Task 6 checkpoint below and the restart-command
section near the end of this document.

A fresh session at that former boundary could be started with: “Read the controlling design and the live
restart handoff in this plan, verify Task 5D commits
`1ea9e4aae108c0f1fd2d1bec1ba81f188af408c4` and
`47180cc07e7d0177ff4981bd7ccf1f1ec65eb047` plus clean tracked trees, preserve
the recorded unrelated untracked files, install the nio checkout editable into
MindRoom, and begin Task 6 from its exact upstream boundary and TDD matrix:
restore the pre-fork public desktop sync path while keeping MindRoom on the
durable engine. Continue autonomously without approval pauses; consult Claude
Fable through isolated `agent-cli dev` only for a material ambiguity that local
evidence cannot resolve.”

### Task 6 completed checkpoint — 2026-08-15

Task 6 started from clean tracked boundaries: nio ledger tip `726771d` over
feature commit `1ea9e4aae108c0f1fd2d1bec1ba81f188af408c4`, and MindRoom
`47180cc07e7d0177ff4981bd7ccf1f1ec65eb047`. Preserve the unrelated untracked
paths in the repository table. The exact local upstream comparison boundary
`69aa99c51f354980bf5306a85b4c7b7d71ff3217` exists and is an ancestor of the
current nio branch.

Initial read-only mapping found that the required pre-deletion MindRoom source
scan is intentionally RED, so do not remove nio public symbols yet:

- `bot.py` still constructs `SyncCheckpointTrust`, prepares/acknowledges/resets
  Classic recovery, defines the old Sync/Sliding response-certification path,
  refers to recovered/unrecovered response fields, and registers
  `SyncResponse | SlidingSyncResponse` plus SyncError callbacks even though
  Task 5D now uses only the owned durable runner/pump;
- `matrix/journal_ingress.py` still exposes the old
  `add_event_admission_callback` registration path; `sync_loop.py`,
  `client_session.py`, and certification/checkpoint helpers retain fork-only
  Sliding/recovery types for legacy tests;
- nio still publicly exposes `AsyncClient.sliding_sync()` /
  `sliding_sync_forever()`, `SlidingSyncResponse`, the admission callback, and
  Classic reset/ack/recovered-state APIs. The delta from `69aa99c...` is large
  and also contains later E2EE/security work, so whole-file checkout is
  forbidden.

Active Task 6 TDD order:

1. add a fail-closed production-source scan over the exact MindRoom files named
   in Task 6 and capture the current forbidden references as the first RED;
2. remove only dead old-engine registrations/calls/types from MindRoom while
   retaining the durable callback/application behavior and leaving historical
   modules present but unused for the later zero-use observation gate;
3. freeze public AsyncClient behavior/signatures against `69aa99c...` with a
   retained compatibility matrix, then restore methods one behavior slice at a
   time without undoing later E2EE/cross-signing/security changes;
4. remove fork-only public Sliding/recovery exports only after the MindRoom scan
   is GREEN, run desktop/durable Classic+Sliding compatibility, coverage
   zero-use, full cross-repo/static/review gates, update this ledger, and commit
   `test: bind durable sync compatibility`.

First Task 6 RED→GREEN boundary is complete in the still-uncommitted MindRoom
worktree. `TestAgentBot.test_agent_bot_start` was first changed to require zero
public admission and zero sync-response callback registrations while retaining
the owned-session configuration and the invite/RTC compatibility callbacks. It
failed causally because `JournalIngress._admit` was still registered exactly
once. The minimal production change removed `Bot.start()`'s legacy checkpoint
startup call, `JournalDispatcher.register(client)`, and both Sync/SyncError
response-callback registrations. The exact node is now GREEN:

```text
UV_NO_SYNC=1 .venv/bin/python -m pytest -q -n0 \
  tests/test_multi_agent_bot.py::TestAgentBot::test_agent_bot_start
1 passed
```

The temporary six-file source gate remains RED by design because unreachable
legacy methods/types still exist. The next action is to remove the dead Bot
certification/response block and its dispatcher admission adapter while
preserving `_run_sync_response_side_effects()` as the frame-completion-owned
health/ready/outbox path. Then shrink `sync_loop.py` to owned ingestion config,
narrow `MindRoomAsyncClient._handle_olm_events()` to ordinary `SyncResponse`,
and leave the certification/checkpoint modules present but unimported and free
of the forbidden public nio references before touching nio itself.

That MindRoom prerequisite is now GREEN. The implementation removed the dead
Bot response-certification/rewind/error/membership-scan methods and state,
startup/shutdown checkpoint calls, public response/admission registrations,
the JournalDispatcher-owned `JournalIngress` adapter, and the old provenance
filter around RTC compatibility callbacks. Frame completion still exclusively
drives `_mark_sync_progress()`, first-ready/outbox/orchestrator side effects,
and one call-manager reconciliation. Batch admission still wakes the same
JournalDispatcher application worker. `sync_loop.py` now owns only Classic or
Sliding private `IngestionConfig` construction, and
`MindRoomAsyncClient._handle_olm_events()` accepts the retained public Classic
`SyncResponse`. `sync_certification.py` and `sync_checkpoint_trust.py` remain as
inert documented marker modules for external zero-use coverage; production no
longer imports them.

The RTC behavior was separately RED then GREEN: a callback delivered by the
owned authenticated settlement path initially did not reach the call manager
without the legacy context-variable provenance marker; removing that duplicate
gate made membership and call-state compatibility callbacks run exactly once.
The current focused gate is:

```text
tests/test_multi_agent_bot.py::TestAgentBot::test_agent_bot_start
tests/test_multi_agent_bot.py::TestAgentBot::test_owned_frame_completion_without_work_drives_sync_health_and_ready_once
tests/test_bot_ready_hook.py::test_call_manager_room_callbacks_run_without_legacy_delivery_provenance
tests/test_matrix_public_sync_cutover.py
9 passed
```

The exact six-file source scan is `6 passed`: no production reference remains
to the public admission callback, public Sliding response type, Classic
ack/reset/recovery methods, or recovered/unrecovered response fields. It is now
safe to remove those nio symbols. The next TDD slice is the retained public
desktop compatibility matrix against `69aa99c...`, beginning with method
signatures and ordinary Classic response/callback ordering before deleting
nio's public Sliding surface.

The first nio compatibility node now exists at
`tests/public_sync_compatibility_test.py`. Its retained signature table is
derived literally from `69aa99c...` and is GREEN for `sync()`,
`sync_forever()`, `stop_sync_forever()`, `receive_response()`,
`run_response_callbacks()`, `add_response_callback()`, `close()`, and the six
retained event callback registration families. The separate removal assertion
was observed RED against the real current class:

```text
uv run pytest -q tests/public_sync_compatibility_test.py
1 passed, 1 failed
```

Its sole failure is that all seven fork-only `AsyncClient` members remain
visible: `sliding_sync`, `sliding_sync_forever`,
`add_event_admission_callback`, `acknowledge_classic_sync`,
`reset_classic_sync_state`, `has_uncommitted_classic_sync_state`, and
`clear_persisted_sync_recovery`. The same test also requires the top-level
`nio.SlidingSyncResponse` and `nio.SlidingSyncError` exports to disappear. No
nio production file has been changed in this Task 6 slice yet. Continue by
removing only this public surface, rerun the exact node GREEN, then add the
first real Classic response/callback behavior RED before changing active sync
implementation code.

That public-surface slice is now GREEN in uncommitted nio bytes. The seven
methods remain only as underscore-prefixed historical implementation for the
later Task 9 deletion, while the public names are absent. `SlidingSyncResponse`
and `SlidingSyncError` are no longer top-level exports, and the fork-only
`RecoveryAbandonment` / `SlidingWindowToken` exports are also absent. Classic
`SyncResponse` no longer carries `recovered_room_ids`,
`unrecovered_room_ids`, or `abandoned_rooms`.

Two subsequent behavioral RED→GREEN slices restored the active Classic path:

- a real timeline callback synchronously applying a second Classic
  `SyncResponse` failed only at the fork recovery executor's re-entry guard;
  `receive_response()` and `_handle_sync()` now use the pinned upstream order
  directly (token persistence, to-device, invited/joined projection and
  callbacks, presence/global account data, then Olm maintenance), and the
  nested response ends at the literal newer token;
- a real HTTP `sync(since="s0")` under deliberately conflicting retired
  recovery configuration failed only at the recovery-ownership check; the
  public `sync()` request construction and response application now follow the
  upstream path and ignore those retired settings.

The ordinary store boundary is also GREEN. `AsyncClient.load_store()` no
longer reads recovery or Sliding token state. A fresh real SQLite store creates
only active E2EE/trust/sync-token tables and omits
`pendingtimelineevents`, `slidingwindowtokens`,
`syncrecoveryabandonedrooms`, and `syncrecoverygaps`. A real higher-version
store seeded with those historical tables retains them byte/schema-wise on
reopen and ordinary token loading does not touch them. Existing version rows
are advanced nondestructively without running the retired recovery migrations.

The required external zero-use gate is now executable, not a source-text
assertion. In an isolated Python process, coverage starts before `import nio`,
then constructs an ordinary `AsyncClient`, applies a Classic response, runs
response callbacks, and creates a real in-memory SQLite store. It reports zero
executed lines in all six named modules:

```text
client/sliding_membership.py
client/sync_recovery.py
client/sync_reset_fence.py
client/sync_response_ordering.py
recovery_abandonment.py
sliding_sync_tokens.py
```

Module-level runtime imports and recovery-object construction were removed;
historical type references are `TYPE_CHECKING`-only and legacy database APIs
use local imports only if explicitly invoked. The recovery source files remain
present for the Task 9 observation/deletion boundary.

The current retained compatibility checkpoint is `24 passed`: the full new
public matrix plus existing Classic sync, presence, callback-exception, and
`sync_forever` nodes plus ordinary encrypted send and error-response behavior.
The matrix pins all retained method signatures, removed
surface, response fields, callback family projection/order, response callback
registration-order re-entry and exception short-circuiting, Classic callback
response re-entry, real HTTP sync behavior, fresh/historical store fates, and
external recovery zero-use. `sync_forever()` no longer depends on the fork
generation object; the later explicit fixes that retain the first full-state
request across `SyncError` and drain cancelled child tasks remain covered.

The ordinary-send gate first failed causally because `room_send()` still
entered the retired recovery room state and dereferenced `_recovery` before
encryption. The public send path now calls the existing E2EE preparation
directly, retains membership/key-query/group-session behavior, and no longer
consults a recovery gap or Classic rebuild marker. The external six-module
zero-use assertion remains GREEN.

The first owned-ingestion regression after removing ordinary recovery state
found two real cross-boundary couplings and captured both before repair. First,
configured adoption still required the four retired recovery tables even
though a fresh `SqliteStore` now creates only active tables. Preflight now
accepts exactly either the active ordinary schema or that schema plus all four
historical tables; partial/malformed layouts remain rejected. Second, the
owned-client pristine check still dereferenced the deleted recovery fence,
generation, and state objects. It now validates only live ownership facts:
store/Olm/HTTP absence, empty cursor and room projections, no active callback
scope, no sharing/synced state, and no prior ingestion attachment. Owned attach
also loads historical recovery state only when a historical recovery carrier
actually exists. Obsolete tests that mutated the removed generation locks or
registered the removed public admission callback were narrowed to the active
private boundaries. Evidence after both fixes:

```text
tests/ingest/journal_test.py::test_configured_sqlite_store_adoption_and_marked_reopen_preserve_trust
1 passed

tests/ingest/journal_test.py -k 'configured or fresh_owned'
80 passed, 6 deselected

tests/ingest/coordinator_test.py -k 'owned_factory or owned_settle or owned_close'
78 passed, 207 deselected
```

The complete retained `AsyncClient` file is now `131 passed` after restoring
ordinary `room_leave()` / `room_forget()` to the direct upstream-style request
path and retaining the later E2EE behavior. Eighteen tests that directly
required the removed public Sliding engine or recovery-generation close state
are preserved in that file as `_retired_*` source for Task 9 rather than being
collected. The retained public Classic matrix is `24 passed`.

Collection cleanup is also explicit. The wholly obsolete public-engine files
`tests/backfill_test.py` and `tests/per_room_recovery_contract_test.py` were
renamed to `tests/retired_backfill_contracts.py` and
`tests/retired_per_room_recovery_contracts.py`; they remain visible for Task 9
but are not pytest-discoverable. Historical pure helper tests import their
types from private modules rather than recreating top-level exports. Current
collection after those moves is `2,367 tests`.

A fresh full-suite run then provided the authoritative remaining boundary:

```text
uv run pytest -q --tb=short --maxfail=20
20 failed, 2227 passed, 3 skipped in 312.88s
```

One failure was only an assertion on the deleted private
`AsyncClient._sync_response_seen` carrier in the storeless-login prerequisite.
The other nineteen reached the old recovery persistence methods against a
fresh active-only store and failed because the retired tables correctly do not
exist. Recreating those tables would violate Task 6. The contiguous private
recovery/migration block in `tests/store_test.py` is therefore retained as
`_retired_*` source, while active store tests and configured historical-schema
adoption tests remain collected. Seven additional `sync_recovery_test.py`
assertions were retired narrowly: the removed `SlidingSyncResponse`
`abandoned_rooms` field plus six deleted recovery-dispatch `close()` contracts.
All remaining tests in that boundary are GREEN:

```text
uv run pytest -q --tb=short \
  tests/ingest/owned_login_prerequisite_test.py tests/store_test.py \
  tests/sync_recovery_test.py tests/sliding_membership_test.py \
  tests/sync_response_ordering_test.py
130 passed in 3.96s
```

The post-retirement full nio suite is now freshly GREEN:

```text
uv run pytest -q --tb=short
2303 passed, 3 skipped, 5 warnings in 305.64s
```

The warnings are the existing Python 3.14 `asyncio.iscoroutinefunction` and
multi-threaded-fork deprecations; no test warning was introduced by this
cutover. The active collected suite is therefore 2,306 cases, while the
retired source remains present and visibly named for Task 9.

The authoritative post-cleanup nio gate on the final current bytes is:

```text
uv run pytest -q --tb=short
2303 passed, 3 skipped, 5 warnings in 307.25s
```

The public/ordinary-store compatibility boundary is **142 passed**. Production
Ruff, Black over all changed Python files, `compileall`, and `git diff --check`
are GREEN. Exact changed-production mypy with `--follow-imports=skip` reports
38 baseline diagnostics versus 59 on a clean HEAD clone; every current
diagnostic matches a HEAD category/location modulo line shifts, so there are
zero new changed-hunk diagnostics and 21 removed diagnostics.

MindRoom's legacy-contract cleanup is also complete. The affected engine,
cutover, client-session, and scheduled-restoration gate selects **106 nodes**
and exits zero. The authoritative parallel complete non-live suite is **13,444
passed, 110 skipped, 12 warnings in 97.97 seconds**. Changed-production Ruff,
full-repository `ty`, formatter, `compileall`, and `git diff --check` are GREEN;
the configured all-files pre-commit stack passes every hook, including Vulture,
Privata, Tach, generated docs/models, frontend lint/format/type checks, and
lockfile consistency. The final private OTK override accepts `object` and
locally casts the Classic carrier, preserving the runtime path while remaining
substitutable with nio's private Classic/Sliding base signature without
restoring a public Sliding type.

Five files wholly owned by the deleted certification/recovery engine are
preserved under `tests/retired_*.py.txt` as exact Git renames, so static tools do
not treat archival source as live Python. Mixed suites retain valid routing,
room-member, watchdog, supervisor, journal, and lifecycle tests; obsolete
checkpoint/raw-response/admission bodies were deleted rather than masked with
test-only stubs. The first pre-commit pass usefully exposed this distinction.
Dormant production compatibility seams were made private and are explicitly
named in one documented Vulture whitelist block that Task 9 deletes with the
observation-only source.
The production zero-use scan is clean for every removed public recovery,
checkpoint, Sliding-response, and admission API; the only generic `.register(`
hit is the unrelated Matrix account-registration request.

Task 6 itself is deletion-heavy. Relative to its two clean start commits, nio
is **−382 runtime, +563 tests, +354 docs** and MindRoom is **−1,310 runtime,
−1,269 tests**. The fixed final Task 10 budgets are deliberately not claimed
yet: nio is currently +19,833 runtime, +56,027 tests, and +3,494 docs/scripts
from `6ed2b98`; MindRoom is +195 runtime and +1,760 tests from `0acaea2`, while
the combined MindRoom source/test tree is already 892 net lines smaller than
the canary baseline `925df7f`. Tasks 9/10 still own recovery-source deletion
and branch-wide consolidation; no baseline or limit moved.

Task 6 is committed and no longer an active dirty-tree handoff. If a later
regression appears, use focused causal REDs and the smallest active-contract
fix; never restore fresh-store recovery tables or the removed public
Sliding/recovery surface. Keep the six recovery source modules present through
the Task 8/9 observation gate.

The final Task 6 review is complete with no blocking finding. It covered every
changed production hunk, the public API and ordinary-store compatibility
matrix, the MindRoom deletion/retirement mapping, archived test fates, the
Vulture observation whitelist, and the fixed budget comparisons. The exact
pre-commit patch identities are:

- nio functional patch excluding this self-updating ledger:
  `ebc29535742d36ecced14b39b643af2f99d3d53736d8809377831d5db95ebbb3`;
- complete MindRoom staged patch:
  `41dd0831ceda0eda7c1124787537ba985e4b65a5455ff5317395e4f06e128442`.

No Claude consultation was needed because causal tests, the pinned upstream
comparison, and complete local/static evidence resolved the final boundaries.
MindRoom is committed at
`253c76245174f106162368993bf1393d452c6698` with subject
`test: bind durable sync compatibility`; nio is committed at
`70d21bcb6b08a9528104d16a9c6c4537b4bf1a8a` with the same subject. This
ordinary docs-ledger commit records both repository SHAs and establishes the
clean Task 7 restart boundary. No unrelated untracked path is included.

### Task 7 completed checkpoint — 2026-08-15

Task 7 begins from the two clean Task 6 feature commits above plus nio
docs-ledger commit `739f3f44ac0c5a5ca1c7d68b160a09f86a9570c7`.
Task 7 is committed in nio at
`6e4f14aa2b438ba431d8f7c7ab2ff176f4e11a94` and MindRoom's
`tests/test_durable_ingestion_cross_database_crash.py` is committed at
`2cbeecb6f8e68f380a7fabb3cbe28cd88f3e1a2f`; production is unchanged.
Reuse existing journal transition hooks, process-kill harnesses,
Classic/Sliding owned fixtures, and MindRoom admission/frontier helpers before
creating anything new.

The required observable chain is:

```text
HTTP response -> Frame -> shared materializer -> Work -> batch
-> MindRoom event/projection/receipt/frontier -> nio ack
```

The Task 7 matrix must prove response/cursor atomicity, deterministic replay,
exactly one application dispatch, integrity failure for conflicting identity or
digest, no Work/Frame loss, frontier advancement only in its owning
transaction, and callbacks outside database transactions. Include Sliding
reopen and exact `M_UNKNOWN_POS` after admission-before-ack. Run focused RED /
GREEN before any broad gate. Preserve the unrelated untracked paths in the
repository table. Consult Claude Fable through isolated `agent-cli dev` only
if local code and causal evidence leave a material design ambiguity; record the
question and decision here before continuing.

Read-only Task 7 inventory is complete. Existing coverage is deep but remains
split at the repository boundary:

- nio already process-kills Classic source staging at every transaction hook
  (`test_stage_crash_boundary_reopens_to_exact_old_or_new_graph`),
  kills all 18 real materializer DML positions plus `before_commit` / `commit`,
  kills delivery and local-membership transactions, and kills Sliding reopen;
- nio separately proves cold Classic/Sliding semantic parity, prepared-Frame
  retention, callback-before-ack transaction exclusion, ack rollback/commit
  uncertainty, and positioned `M_UNKNOWN_POS` reset/replan ordering;
- MindRoom already runs fresh admission, rollback at each statement class,
  post-commit uncertainty, hidden-until-commit facts, replay/conflict taxonomy,
  and concurrent classification against both the SQLite and PostgreSQL
  backends; its adapter tests prove exact one-record fact transfer and
  redelivery after settlement failure;
- MindRoom's nine-boundary turn crash matrix proves one terminal dispatch and
  at most one visible response after admission, but it starts from an already
  admitted event rather than a real nio batch.

The uncovered Task 7 boundary is therefore one real cross-repository carrier:
the same authenticated nio Work must enter a real MindRoom backend, survive a
crash after MindRoom admission but before nio ack, reopen, classify as an exact
receipt replay, suppress the second callback/application dispatch, and finally
ack without losing the retained Frame. The equivalent Classic and Sliding
inputs must yield the same transport-neutral persisted event/projection facts.
After that base matrix is GREEN, extend the Sliding row through positioned
`M_UNKNOWN_POS` after this admission-before-ack restart.

The first causal node is
`tests/test_durable_ingestion_cross_database_crash.py::test_real_owned_batch_replays_after_admission_before_nio_ack`,
parameterized by Classic/Sliding and the existing `journal_database`
SQLite/PostgreSQL fixture. It uses a real warm transport cursor followed by one
joined-room LIVE timeline message, real nio source normalization,
materialization, hydration, one-record FIFO delivery, MindRoom
`consume_one_ingestion_batch()`, and the existing nio `before_commit`
transition hook to abort the first ack. The preceding lifecycle/state Work is
admitted through MindRoom as receipt-only work so the two durable consumer
frontiers remain identical; privately acknowledging that prefix was the first
fixture error and correctly produced `IngestionBatchSequenceError`. The only
other setup corrections were creating the nio store directory, constructing
the client before adopting the bootstrap-owned SQLite store, and treating
MindRoom's `PendingPage` as its documented tuple subtype.

The exact four-case matrix is GREEN unchanged production:

```text
UV_NO_SYNC=1 .venv/bin/python -m pytest -q -n0 \
  tests/test_durable_ingestion_cross_database_crash.py::test_real_owned_batch_replays_after_admission_before_nio_ack \
  --tb=short
# 4 passed in 6.2s: Classic/Sliding × SQLite/PostgreSQL
```

Each row proves one first-receipt callback outside nio's transaction after the
MindRoom receipt/event/projection/frontier has committed; the injected nio ack
rollback leaves the exact Work and prepared Frame outstanding. A valid
same-sequence competing nio batch with a recomputed but conflicting canonical
digest is rejected by MindRoom with `IngestionBatchIntegrityError` and leaves
the original semantic event intact. Close/fresh-client reopen returns the same
original batch; MindRoom classifies it as `(receipt_new=False,
semantic_event_new=False)`; no second callback runs; final ack removes the Work
while the Frame remains its completion owner. The real `PendingEventWorker`
then dispatches that pending semantic event exactly once, settles it, and a
second drain returns zero. Both transports persist and dispatch the same
literal MindRoom event signature. No production hook or table was added.

The positioned Sliding continuation is also GREEN unchanged production in
`test_sliding_unknown_position_after_admission_before_nio_ack`, parameterized by
the same SQLite/PostgreSQL fixture. It deliberately retries the already-admitted
batch on the still-positioned session first, proving replay facts `(False,
False)` and no second callback. The runner then retires the prepared Frame,
sends the exact `pos=p2` request, receives exact HTTP 400
`M_UNKNOWN_POS`, atomically advances the source epoch and resets only `pos`
while preserving `to_device_since=td2`, replans request 0 without a position,
and survives a fresh configured-store/client/session reopen with no Work or
Frame loss. The combined file is:

```text
UV_NO_SYNC=1 .venv/bin/python -m pytest -q -n0 \
  tests/test_durable_ingestion_cross_database_crash.py --tb=short
# 6 passed in 6.5s
```

The verification sequence next mapped the existing per-transaction process-kill
matrices onto the complete end-to-end chain and added only genuinely missing
cross-repository boundaries rather than duplicating already causal
source/materializer/admission/claim/ack tests. The cross-repository carrier now
binds both conflicting canonical digest rejection and one real
`PendingEventWorker` dispatch, so neither is an open gap. Run the exact existing
source-stage, Task4C/freeze/materializer, batch-claim, MindRoom admission, nio
ack, and turn-crash selectors as one Task 7 verification manifest. Add a new
case only if that manifest exposes an unowned before/after boundary. Then run
focused affected suites and all configured statics, record patch
identities/review, and commit the Task 7 slice.

The first manifest run found exactly one test-coverage asymmetry: the five-hook
subprocess source-stage crash test was Classic-only. Directly parameterizing
that subprocess with Sliding is invalid because configured Sliding reopen
intentionally rotates the connection-scoped source before the child can apply a
parent-built proposal; the resulting proposal is correctly rejected as stale.
The narrow companion
`test_sliding_stage_failure_is_atomic_before_rotated_reopen` therefore builds
the proposal under one live Sliding owner, injects a `BaseException` at each
exact stage hook, proves all-old for the four precommit boundaries and all-new
at `commit`, then closes and proves the intentional positionless rotated reopen
preserves the corresponding absent/present Frame. Classic subprocess death plus
Sliding exact transaction failure now pass `10/10`. No production change was
needed.

Initial composite crash manifest evidence:

```text
# nio: Classic stage process death, full 18-DML materializer process death,
# preparation freeze/poison, batch claim, ack, parity, and positioned reset
37 passed in 12.14s

# MindRoom: the 6 cross-repository rows, every admission rollback/commit
# boundary on both backends, and the complete downstream turn crash matrix
98 passed, exit 0
```

The rerun and complete affected gates are GREEN:

```text
# nio complete tests/ingest suite, including the 10 Classic/Sliding staging rows
1704 passed in 256.92s

# MindRoom complete admission + downstream crash + cross-repository files
# (329 + 76 + 6 collected nodes)
411 passed, exit 0

# touched-file gates
nio: Black, Ruff pre-commit, mypy, compileall, whitespace/end-of-file, diff-check GREEN
MindRoom: Ruff check/format, full configured ty hook, Tach, privata,
          compileall, whitespace/end-of-file, diff-check GREEN
```

No production file changed in either repository. No Claude consultation was
needed: the only design-looking issue was resolved causally by the existing
Sliding connection-rotation contract and exact tests. The focused review and
hashing below are complete; MindRoom is committed at
`2cbeecb6f8e68f380a7fabb3cbe28cd88f3e1a2f`; nio is committed at
`6e4f14aa2b438ba431d8f7c7ab2ff176f4e11a94`. This docs-ledger change
establishes Task 8's clean restart boundary.

The focused final review found no P0-P2 correctness issue. The Task 7
functional patch identities before commit are:

- nio source-stage test patch:
  `b0b1835fa383a3cb1b36a2183d01279849be700adf85cbb3b19498c84e2a603b`;
- MindRoom new cross-repository test patch:
  `81837bc15b9231f0d4d0598034077ce8dd41f65b0975b3eecf6a0c39797118ce`.

Suppression inventory: nio adds none; MindRoom adds only two test-local
`# noqa: PLR0915` markers on the two intentionally literal end-to-end scenarios
and no type-ignore, coverage, lint, or production suppression. Fixed budgets
remain a Task 9/10 blocker rather than a Task 7 regression: nio is currently
`+19,833` runtime, `+56,082` tests, and `+3,722` docs from `6ed2b98`;
MindRoom remains `+195` runtime and becomes effectively `+2,448` tests from
`0acaea2` after including the new untracked 688-line test. It remains 204 net
source/test lines smaller than `925df7f`. Task 9 recovery deletion and Task 10
consolidation must close the fixed overages without moving either baseline or
limit.

### Task 8 active checkpoint — 2026-08-15

Task 8 begins from clean committed candidate features: nio
`6e4f14aa2b438ba431d8f7c7ab2ff176f4e11a94` and MindRoom
`2cbeecb6f8e68f380a7fabb3cbe28cd88f3e1a2f`, plus the nio docs-ledger commit
containing this checkpoint. No Task 8 production or test edit exists yet.
Continue autonomously without approval pauses, but do not call a local
simulation an external canary PASS.

Immediate next action is read-only inventory of the existing canary, wheel,
Synapse, Docker, and evidence tooling in both repositories. Bind the exact
pinned Synapse version, ordinary Classic/Sliding configuration, homeserver
credentials, clean isolated environment, and wheel-build commands before
starting anything. Reuse an existing harness if it meets Task 8; do not add
product diagnostic state, a control room, `probe`, or canary-only production
path. If external credentials/infrastructure are unavailable, complete every
local build/harness validation that is honest, record the exact external
blocker, and leave Task 8 in progress rather than inventing evidence. Claude
Fable remains reserved for a material design ambiguity that local sources and
causal tests cannot resolve.

#### Task 8 inventory and wheel checkpoint — 2026-08-15

The first inventory pass found three concrete facts that supersede the generic
"inventory next" instruction above:

- `scripts/live_sliding_sync_check.py` is not a reusable Task 8 harness. It
  imports and calls the public `SlidingSyncResponse`, `sliding_sync()`, and
  `sliding_sync_forever()` surfaces deliberately removed by Task 6. Do not
  restore those APIs merely to revive the old script.
- MindRoom's local Synapse compose uses `matrixdotorg/synapse:latest`, so it is
  not an explicitly pinned Task 8 environment yet. Inspect the locally
  available image/digest and the complete homeserver config before starting it;
  pin an exact version or digest in the disposable operator environment, not
  through a canary-only production path.
- MindRoom declared `mindroom-nio[e2e]>=0.37,<0.39`, while the exact nio
  candidate wheel reports version `0.39.0`. A focused packaging test now
  requires the durable minor exactly (`0.38.99` rejected, `0.39.0` accepted,
  `0.40.0` rejected). The causal RED was the old specifier accepting `0.38.99`
  and excluding the candidate. The narrow working-tree fix is
  `mindroom-nio[e2e]>=0.39,<0.40` plus the regenerated `uv.lock`; the focused
  node and the complete ten-node `tests/test_hatch_build.py` file are GREEN.

There is an important release-candidate distinction: the registry's existing
`mindroom-nio==0.39.0` wheel does not contain `nio.ingest`, even though it has
the same version as the unreleased candidate checkout. A plain
`uv run --locked` therefore installs the registry wheel and fails MindRoom's
startup import. For repository tests, restore the linked candidate explicitly:

```bash
cd /work/dev/mindroom
uv pip install --python .venv/bin/python -e /work/dev/mindroom-nio
.venv/bin/pytest -q tests/test_hatch_build.py
```

For Task 8, do not use the registry artifact and do not treat its shared version
number as identity. Build the exact nio candidate wheel from commit
`6e4f14aa2b438ba431d8f7c7ab2ff176f4e11a94`, build the MindRoom wheel from the
eventual commit containing the dependency fix, record both wheel SHA-256 values,
and install those two explicit local wheel paths into a fresh isolated
environment. Verify `nio.ingest` imports from the installed candidate wheel
before any canary attempt.

The packaging closure is committed in MindRoom as
`6f415e41f` (`build: require durable nio candidate`). Exact detached worktrees
were then created at nio
`6e4f14aa2b438ba431d8f7c7ab2ff176f4e11a94` and MindRoom
`6f415e41f`, and both wheels built successfully. The exact artifacts were:

- `mindroom_nio-0.39.0-py3-none-any.whl`, SHA-256
  `051841f78d259765f5fb9d85dbbf9dd4e20a35fd50e94859e9d2d75317e1faf1`;
- `mindroom-2026.8.30.post1.dev491+g6f415e41f-py3-none-any.whl`, SHA-256
  `ab70ef8c61762992c3e5317b12adacbc47c3cd7fe13288c9e910bcfb7c7be65f`.

They currently live under `/tmp/mindroom-task8-wheels.2FeteO/wheels/`; that path
is disposable and the hashes/commits above, not `/tmp`, are the authority. The
build used `uv build --wheel --out-dir ...` in each detached worktree. A new
Python 3.13 virtual environment installed both explicit local wheel paths plus
all 154 resolved base/E2EE dependencies. From that isolated environment,
`mindroom.bot` and `nio.ingest` imported from `site-packages`; metadata reported
the exact MindRoom development version above and nio `0.39.0`. This is a wheel
build/install/import PASS only, not a Classic or Sliding canary PASS.

Current restart state after the wheel checkpoint:

- nio HEAD remains docs ledger `5bee5e44a6038c39923fc039132925fc2ea66bdb`;
  this living document is the only intended tracked nio edit;
- MindRoom HEAD is packaging commit `6f415e41f`, whose parent is Task 7
  `2cbeecb6f8e68f380a7fabb3cbe28cd88f3e1a2f`; its tracked tree is clean;
- unrelated untracked paths listed in the Task 7 checkpoint remain user-owned
  and must be preserved.

Immediate continuation: finish the read-only Docker/Synapse/homeserver
credential inventory and bind an ordinary production-path canary harness. Only
after an explicitly pinned real Synapse and that harness are bound may Classic
run; Sliding may run only after the new Classic evidence is accepted. Never
label a local unit/integration simulation an external PASS.

The pinned infrastructure preflight is now GREEN. The locally cached Synapse
image is exactly version `1.158.0`, upstream git SHA
`7a3e98b6f77ee3a5fe4dbeb934b0a0c1721e6afe`, linux/arm64 digest
`sha256:39f47bcea65c544ef3a6d58848ee98e2ea98387e9991df30474d25b54477a0af`.
Its installed source confirms the simplified MSC3575/MSC4186 endpoint and that
`msc3575_enabled` defaults true. The disposable Task 8 manifest makes it
explicitly true and pins Postgres 15-alpine digest
`sha256:3d0f7584ed7d04e27fa050d6683a74746608faf21f202be78460d679cc56461f`
and Redis 7-alpine digest
`sha256:e7723ff73d963f5cc6d9c4643ea3d989527a402a319239054e9472a7fb9219a2`.
It runs as Compose project `mindroom-task8-canary` on host port `18008` with
fresh named volumes; disposable operator files live under
`/tmp/mindroom-task8-wheels.2FeteO/synapse/`.

Using the fresh exact-wheel virtual environment, a new real Synapse account was
registered and the candidate's own `ClassicSource` and `SlidingSource` each
planned an ordinary initial request, received HTTP 200 from Synapse, and
normalized the real response to `SourceResultKind.FRAME` with a candidate
cursor. This is `transport-preflight=PASS`: it proves the pin, endpoint, auth,
wire shapes, and candidate normalizers interoperate. It is still not the
MindRoom Classic/Sliding canary because it has not yet exercised the production
bot, durable store, MindRoom admission, or a visible application result.

Immediate continuation is therefore narrower: adapt only operator-side harness
launching so the exact installed `mindroom` executable runs its ordinary
`mindroom run` configuration against this pinned Synapse and deterministic
OpenAI-compatible model stub. Reuse the existing live harness's account/room,
model-stub, message/result, process restart, and journal observation logic where
possible, but do not use its Tuwunel provisioning or `uv run` child launch.
Run the complete Classic canary first; retain the stack and artifacts only if
its concise evidence is accepted, then run Sliding.

The first exact-wheel Classic attempt is a causal external RED and Task 8 is
blocked on it. Pinned Synapse and the ordinary installed `mindroom run` process
both started; MindRoom created the configured public room, a real external
account joined it, and the warm-up event was accepted by Synapse. The durable
sync loop then repeatedly terminated with
`JournalConflictError("local membership intent does not match current state")`.
After 90 seconds the application oracle reported the warm-up event
`not_admitted`, zero pending MindRoom journal events, and no agent reply. This
must not be labelled a Classic PASS.

Source inspection identifies a real local/reported membership race. Startup
room reconciliation can read MindRoom's membership authority as `leave/0`
while a reported `join/0` lifecycle Work is being admitted. Once that reported
Work commits and is acknowledged, nio's Aggregate is `join/0`, but the already
queued local command still asks for `leave/0 -> join`; the Work queue is now
empty, so `_record_local_membership_intent()` reaches its stale predecessor
check and makes the owned session terminal. The intended target is already
satisfied, so retrying the transition would be wrong and terminal failure is
unnecessary.

Immediate TDD continuation: add a focused owned-session RED in nio where a
membership transition reaches the requested target before a stale queued local
command records its intent. Require the command to reconcile as a successful
no-op, emit no new Work/DML, and leave the session reusable. Implement the
narrow target-already-current branch in the journal/session command path, rerun
the focused and complete relevant suites, build a new exact nio wheel/updated
MindRoom wheel if commit identity changes, and then rerun Classic from a fresh
Synapse volume. Preserve the failed attempt only as RED evidence.

That focused TDD boundary is now implemented in the working tree. The new node
`test_owned_local_command_reconciles_when_lifecycle_reaches_target` queues the
stale `leave/0 -> join` command first, publishes and settles a distinct real
lifecycle transition that reaches `join/0`, then advances the stale command.
Against the prior production bytes it failed only at
`JournalConflictError("local membership intent does not match current state")`.
The narrow GREEN recognizes an already-current target only after authenticated
Frame and Work queues are empty and no Aggregate barrier is present; it returns
without HTTP or journal DML. The owned session treats the resulting absence of
a pending intent as successful reconciliation, clears the command, signals
progress, and remains reusable. The test proves byte-identical graph state
across the no-op and then successfully executes a new real `join/0 -> leave`
command. Exact focused result: `1 passed in 0.26s`.

This is not yet the external Classic PASS. Immediate continuation is focused
and broad nio verification, static checks, independent diff review, a committed
nio candidate boundary, exact wheel rebuild/reinstall, and a fresh-volume
rerun of the pinned Classic canary. Update the hashes and canary evidence here
before treating any disposable `/tmp` artifact as authoritative.

Verification of the working-tree closure is now complete:

- the 49-node local-membership selector passed;
- full `coordinator_test.py + source_journal_test.py` passed `555/555`;
- the complete nio suite passed `2309`, skipped `3`, and emitted only five
  pre-existing deprecation warnings in `304.86s`;
- Black left the two production files and focused test unchanged; Ruff
  `--no-fix`, targeted mypy, `py_compile` coverage through the test run, and
  `git diff --check` are clean.

The external Classic result remains RED until the exact committed wheel is
rebuilt and the fresh-volume ordinary MindRoom harness passes. Next restart
action is to rebuild/reinstall the nio wheel from exact feature commit
`1c6bd92e42a7c8633c0a84bb2dff2978713cc3d3` (`fix: reconcile fulfilled
membership commands`) and rerun the pinned Classic canary. The configured
pre-commit hooks on the three implementation/test files passed EOF,
whitespace, Black, and Ruff; the feature commit contains only those two
production files and the causal test. This living-document edit remains the
only intended tracked change after that commit; the unrelated untracked review
directory and generated egg-info remain user-owned and must not be removed.

### Task 5D completed checkpoint — 2026-08-15

Task 5D began from clean committed boundaries: nio
`d1c364ac5d432d2ac58ec6ae5effb91ce9a39c49` (the docs-only Task 5C handoff,
parent `b61e186...`) and MindRoom
`ba0da04b5553bac9ce36b92cb1f0133211b1c3aa`. The only expected tracked change
before the first Task 5D RED is this living document; preserve the unrelated
untracked paths recorded above.

Initial source mapping is complete:

- `Bot.start()` already owns one `_ingestion_session`, registers the durable
  compatibility callbacks on its ingestion-owned client, and installs the
  private frame-completion sink before `running=True`;
- `Bot.sync_forever()` still calls `run_matrix_sync_forever()`, which reaches
  public `client.sync_forever()` or `client.sliding_sync_forever()`. This is the
  legacy candidate engine and must become unreachable for managed bots;
- nio's private owned session already owns `session.run()`, source transport,
  hydration, local membership, maintenance, completion, close cancellation,
  Work acknowledgements, and the progress generation/event;
- `mindroom.matrix.durable_ingestion.run_ingestion_pump()` already validates,
  admits, settles, and wakes semantic dispatch one record at a time, but no Bot
  calls it yet;
- the principal-bound event journal is both the ingestion consumer/admission
  store, and `self._journal_dispatcher.wake` is the existing semantic-dispatch
  wake boundary;
- the missing integration contract is one runner+pump lifetime under
  `Bot.sync_forever()`, including a private lost-wakeup-safe Work wait and
  frame-completion translation to health/first-sync/runtime side effects.

Task 5D TDD order:

1. [x] Add a causal Bot RED proving `sync_forever()` starts exactly the already
   owned nio runner and the one-record MindRoom pump, selects Classic/Sliding
   only through the frozen `IngestionConfig`, and never calls either legacy
   public sync-forever method.
2. [x] Freeze the smallest private Work-availability wait/wake boundary; prove
   the pump cannot poll, miss a materialization wake, or outlive/cancel its
   runner sibling.
3. [x] Make authenticated frame completion drive the existing sync-health,
   first-sync, ready/recovery, invite/membership, presence, E2EE/pairing, and
   RTC-compatible side effects exactly once and outside database transactions.
4. [x] Cover runner/pump failure, permanent authentication, cancellation,
   watchdog restart, ordinary stop, config replacement, and cleanup ordering;
   the owned session must close/revoke before Matrix HTTP and every cleanup
   lane must run.
5. [~] Route local departure/rejoin through nio's private durable membership
   command without a source poll crossing its intent or Work; restart must
   reconcile the exact prior fate without fabrication.
6. [ ] Run Classic/Sliding behavioral parity, focused lifecycle/E2E suites,
   the full MindRoom suite, exact Ruff/ty/Tach/Privata/pre-commit gates, linked
   nio regressions/statics, diff/budget review, update this handoff, and commit
   the complete slice as `feat: activate durable matrix ingestion`.

First causal RED checkpoint:

- MindRoom
  `TestAgentBot::test_agent_bot_enters_sync_without_startup_cleanup` now starts
  a real initialized Bot with its exact owned-session carrier, holds the owned
  runner and pump siblings open, and installs hard tripwires on the legacy
  selector plus both public Classic/Sliding loops. It fails solely because the
  current `Bot.sync_forever()` invokes `run_matrix_sync_forever()`:
  `AssertionError('managed bot invoked the legacy Matrix sync engine')`.
- nio
  `test_owned_work_wait_wakes_when_materialization_publishes_delivery` starts a
  read-only waiter with no Work, stages and really materializes one canonical
  GLOBAL_ACCOUNT_DATA delivery, and requires the waiter to expose that exact
  batch. It fails solely with `AttributeError` because
  `_OwnedIngestionSession._wait_for_work` does not yet exist.
- The intended private seam snapshots the progress generation, rechecks the
  authenticated ready batch, and then uses the existing generation/event
  double-check. Successful owned materialization signals progress. This closes
  both the already-waiting and materialization-before-wait race without a poll,
  public API, or new durable state.

These REDs are accepted for the first narrow GREEN. No production behavior has
been changed at that checkpoint.

First narrow GREEN:

- nio now has a private `_wait_for_work()` that snapshots the progress
  generation, checks for an authenticated ready batch, and then enters the
  existing double-checked event wait. A successfully committed owned
  materialization signals progress. The new RED plus both existing ack/resume
  rows are 3/3 GREEN; exact Black, production Ruff, and coordinator mypy pass.
- `Bot.sync_forever()` now starts only `session.run()` and
  `run_ingestion_pump()` as named sibling tasks. The pump receives the exact
  principal admission view, Matrix user/device identity, private Work waiter,
  and semantic-dispatch wake. First child completion/failure cancels and joins
  the sibling; parent cancellation joins both. The legacy selector import is
  removed, and the exact Bot RED is GREEN.
- authenticated frame completion now marks health, sets first-sync once,
  invokes the existing ready/recovery/orchestrator/deferred-work side-effect
  boundary, schedules pending RTC reconciliation, and clears frame-scoped
  member provenance without fabricating a Sync response. Its two-completion
  truth test and the runner/pump test are 2/2 GREEN; Bot Ruff and ty pass.
- full `test_multi_agent_bot.py` is 42/42 GREEN and the focused nio close,
  waiter, ack/resume, and completion selector is 19/19 GREEN.

The first broader lifecycle run found seven failures plus one now-dead pure
backoff characterization in
`test_sync_task_cancellation.py`. They instantiate public
`client.sync_forever()`/`sliding_sync_forever()` and assert `full_state` or the
fork-only Classic rebuild loop. Those expectations directly contradict Task
5D's sole-engine acceptance criterion. All eight obsolete public-loop tests
were deleted rather than skipped or shimmed. The surviving file is now 94
collected tests and runs 93 passed/1 skipped; no surviving Classic/Sliding
transport configuration or lifecycle assertion was removed.

The remaining boundary ambiguity was submitted to Claude Fable through
`agent-cli dev` on 2026-08-15. The first broad read-only prompt was terminated
after ten minutes without output; a narrower no-tools Fable prompt returned a
decisive answer. The temporary worktree and branch
`task5d-boundary-fable-20260815` were removed cleanly afterward. Adopted
decisions:

1. settlement remains the per-record data plane; frame completion is the
   control-plane signal for health and first/runtime readiness;
2. delete the seven contradictory legacy-loop tests now rather than adding a
   shim or skips, and rewrite only surviving behavior at the durable seam;
3. derive local operation identity deterministically with UUIDv5 over the
   durable user/room/prior-epoch/target transition, use the EventJournal as the
   sole previous-membership/epoch authority, and emit no operation when startup
   desired state already equals durable state;
4. keep one structured runner/pump owner, cancel/join the sibling on terminal
   exit, and add causal child-failure, cancellation, restart, and no-orphan
   tests.

One Fable suggestion was rejected because it conflicts with the already frozen
Task 4F contract: MindRoom delivery recovery, deferred tasks, and RTC work will
not be moved into nio maintenance before the completion claim. Task 4F
intentionally persists the claim before invoking the best-effort private sink;
the existing background/idempotent MindRoom side effects remain on that sink.
Changing that ordering would reopen completed crash semantics and is outside
Task 5D.

Next RED order after this decision:

1. empty-frame completion drives first ready without any Work;
2. runner or pump failure cancels/joins its sibling and restart replays the
   exact unfinished owner without duplicate semantic effects;
3. close/cancellation leaves no runner, pump, source HTTP, or Work waiter;
4. local membership crash/retry reuses one deterministic operation and one
   Matrix HTTP fate;
5. startup already-joined emits no local operation, while durable leave plus
   desired join emits exactly one operation from journal-authoritative state.

Structured runner/pump checkpoint:

- `test_owned_sync_child_failure_cancels_and_joins_sibling` is GREEN for both
  runner-first and pump-first failure. Each exact child exception is rethrown
  by identity only after the sibling observes cancellation and exits.
- `test_owned_sync_parent_cancellation_joins_runner_and_pump` is GREEN. Parent
  cancellation reaches both children, both observe `CancelledError`, and the
  structured owner returns only after both have joined.
- Fresh evidence:
  `pytest -q -n0 tests/test_multi_agent_bot.py` is 46/46 GREEN;
  `pytest -q -n0 tests/test_sync_task_cancellation.py` is 93 passed/1 skipped;
  the two child-failure rows plus parent-cancellation row are 3/3 GREEN.
- No restart, membership, or full-suite claim is made at this checkpoint. The
  next implementation boundary is journal-authoritative local membership;
  first map the existing EventJournal membership carrier and write the
  startup-no-op plus crash/retry REDs before changing its production path.

Local-membership design checkpoint:

- A second isolated Claude Fable consultation was run through
  `agent-cli dev` in temporary worktree
  `task5d-membership-fable-20260815`, then the worktree and branch were removed
  cleanly. Fable's decisive boundary is that the lifecycle layer remains the
  policy/decision owner while nio's HTTP-owning durable command sits behind a
  dedicated executor/gateway seam; the generic `join_room` and
  `leave_non_dm_rooms` helpers remain unchanged for unmanaged callers.
- EventJournal already has the necessary durable prior-state carrier:
  `(departure_fenced, membership_epoch)`. The typed read projection will be
  `leave/0` for an absent row, `join/epoch` for an unfenced row, and
  `leave/epoch` for a fenced row. `owed_departure_reports` is bookkeeping, not
  membership identity.
- The gateway operation namespace is frozen as UUID
  `0bd4e975-c3e9-5b10-8d46-68fdda1adc07`, UUIDv5 of URL
  `https://mindroom.chat/matrix/local-membership/v1`. Each operation name is a
  compact JSON array `[user_id, room_id, previous_epoch, target_membership]`,
  so delimiters inside Matrix identifiers cannot alias another transition.
- Before reading EventJournal state, the gateway must await a private nio
  local-membership-idle boundary. This is required on restart: a prior HTTP may
  already have published lifecycle Work that the new pump has not admitted
  yet. Reading the old EventJournal row first could incorrectly certify a
  startup no-op. The idle wait uses the existing nio progress generation/event
  and Work acknowledgement signal; it must not poll.
- The gateway must not wait for its newly published lifecycle Work to be
  admitted before returning when called inside a pump callback (notably an
  invite). The pump cannot admit that new Work until the current callback
  returns, so post-command admission waiting would deadlock. A later gateway
  call serializes, waits for the prior Work to drain, then rereads the sole
  durable EventJournal authority.

Continue this loop autonomously. Do not stop for an ordinary RED review or
implementation approval. If a material design ambiguity remains after the
controlling spec, code, and causal evidence are exhausted, use Claude Fable
through the isolated `agent-cli dev` workflow recorded above, write the
decision here, and continue.

Journal-authoritative local-membership implementation checkpoint:

- MindRoom now exposes exact `RoomMembershipPosition(membership,
  membership_epoch)` through `PrincipalStore.membership_position(room_id)`.
  The journal projection is `leave/0` when absent, `leave/epoch` when fenced,
  and `join/epoch` when unfenced; malformed stored types fail closed. The
  SQLite/Postgres test
  `TestMembershipEpoch::test_membership_position_is_the_durable_local_command_authority`
  is 2/2 GREEN for absent -> local leave -> admitted restart.
- nio now exposes private
  `_OwnedIngestionSession._wait_for_local_membership_idle()`. It uses the
  existing progress generation/event and acknowledgement wake, does not poll,
  and returns only when neither a pending local intent nor pending local
  lifecycle Work remains. The intent/Work parameterization plus existing
  publication/ack wake rows are 4/4 GREEN.
- `AgentBot._change_local_membership()` is the single managed gateway. It
  serializes local decisions, waits for the previous durable command to become
  idle, reads the EventJournal position, returns without HTTP when the target
  is already current, and otherwise calls nio's private durable transition
  with the frozen UUIDv5 identity. Two repeated `leave/0 -> join` attempts use
  literal operation UUID `1ffbf4c2-3f57-50dc-9dd1-5f0e76fad4e8`; a subsequent
  journal-current `join/0 -> join` emits no operation.
- Managed startup joins, authorized invites, unconfigured-room leaves, and
  cleanup leaves now delegate to that gateway. The generic room helper accepts
  an optional leave executor so unmanaged callers retain their pre-cutover
  direct-HTTP behavior. `_on_room_joined` and `_fence_left_room` no longer
  mutate EventJournal membership directly; the admitted durable lifecycle Work
  is now the only mutation authority.
- Causal managed-path tests prove that an already joined `join/4` startup room
  runs setup without a fabricated Matrix join, while an unconfigured
  `join/4 -> leave` uses exact operation UUID
  `46fba72c-1729-5252-a01c-ca8a8746cc6e`, performs no direct
  `client.room_leave`, and retains the local source-echo fence. The complete
  invite/lifecycle file is 41/41 GREEN.
- A real EventJournal-backed gateway chain is GREEN for `join/1 -> leave/2 ->
  join/2 -> leave/3`. It proves exact operation UUIDs
  `574bc7af-74aa-5a35-9ae2-ae47f44668ff`,
  `6bfc9bff-6ef8-5f55-9fd3-90e768a4ac84`, and
  `350bf27f-a7e2-5983-b5ba-850f2e311c97`; each admitted transition advances
  the sole EventJournal authority before the next gateway decision.
- The cross-repo crash/retry proof is closed without duplicating nio's existing
  matrix. MindRoom proves that the same journal position recreates the same
  operation UUID; nio then proves durable intent before HTTP, reopen-before-
  source replay, duplicate same-operation collapse to one HTTP, ordered leave
  then rejoin behind Work acknowledgement, terminal/auth fates, retry-backoff
  cancellation, response/intent commit uncertainty, and source fencing until
  lifecycle Work acknowledgement. The exact 12-node selector expands to 25/25
  GREEN, including the new idle-wait intent/Work parameterization.
- Empty-frame readiness is closed compositionally at the real boundaries:
  nio's real empty Classic frame commits its cursor, claims completion,
  retires the Frame, and reaches the second HTTP with zero Work; MindRoom's
  completion test supplies an ingestion session whose `next_batch()` is
  explicitly `None` and proves the authenticated completion still marks first
  ready exactly once without consulting Work. Both exact nodes are GREEN.
- The remaining structured-lifecycle acceptance is closed by composition of
  the exact owner and existing supervisor tests. Six Bot rows prove
  runner/pump child failure, parent cancellation, ingestion-before-HTTP close,
  and later HTTP release even when ingestion close raises or is cancelled.
  Eleven supervisor rows prove watchdog restart, repeated single-owner
  replacement, config-reload/process cancellation, no orphan sync/response
  tasks, stop-entity shutdown intent, and real-supervisor cancellation order.
  nio's terminal/auth and retry-backoff-close rows add 3/3. Fresh focused
  evidence is therefore 6/6 + 11/11 + 3/3 GREEN.
- Final-gate checkpoint: changed MindRoom production passes Ruff, Ruff-format,
  py_compile, and ty; Tach reports every module valid. Privata correctly found
  the now-dead public `run_matrix_sync_forever` wrapper after the sole-engine
  cutover, so it was deleted rather than suppressed; Privata is now GREEN.
  The broad MindRoom lifecycle/journal/config/watchdog selection and the
  durable-admission/client/session/appservice/manager selection both exit zero.
  nio's full coordinator + delivery + source-journal cluster is 603/603 GREEN
  in 94.70 seconds, and nio changed-source Ruff/mypy/Black/compile/diff gates
  are GREEN.
- During verification, invoking MindRoom's `uv run black` was discovered to
  use the Python-3.14 runtime and a formatter not used by this repository; it
  temporarily rewrote pre-existing Python-3.12-compatible formatting and
  replaced the editable nio install. The rewrite was mechanically reversed
  while preserving the semantic diff (back to a focused 835/357 before the
  dead-wrapper deletion), syntax/Ruff/Ruff-format were rerun, and the local nio
  editable install was restored and path-verified. Do not run Black in the
  MindRoom repository; its authoritative formatter is the configured
  `.venv/bin/ruff format`, and its lint check must be invoked with `--no-fix`
  during read-only verification.
- Older config/invite policy tests that intentionally construct no owned nio
  session use explicit test-only membership adapters; they do not alter the
  production gateway and the new causal tests replace those adapters with the
  real bound method. The first broader compatibility failures were therefore
  repaired at their fixture boundary, not by adding a production fallback.
  Fresh evidence across `test_multi_agent_bot.py`, `test_config_reload.py`,
  `test_hook_matrix_admin.py`, and `test_bot_ready_hook.py` is 147 passed/1
  skipped.
- nio's complete repository suite is now 2,735 passed/3 skipped with 5 warnings
  in 435.18 seconds. This is the current full-nio Task 5D evidence, not merely
  the earlier 603-test ingestion cluster.
- The first complete MindRoom run found 19 failures. Eighteen were legacy
  lightweight test bots that intentionally open no owned nio session but still
  asserted mocked Matrix join/leave transports; one was an obsolete Classic
  public receive-loop cancellation test. No failure identified a production
  fallback requirement. The dead public-loop test was deleted because the sole
  owned runner is now the explicit Task 5D contract.
- `tests.conftest.install_runtime_journal_support()` now installs a test-only
  direct membership adapter for those lightweight bots. It resolves the
  monkeypatched join/leave transports at call time, treats a cache-visible room
  as already joined without HTTP, accepts both the current `RoomJoinOutcome`
  and old test doubles returning literal `True`, and also binds cleanup's
  direct gateway call. Production still raises when an owned session is absent.
  The cache check deliberately iterates keys: `_AutoRoomCache.__getitem__`
  lazily creates rooms, so ordinary `room_id in rooms` is not side-effect-free.
- The exact prior-failure rerun is now GREEN: all 18 surviving failures pass,
  and the deleted public-loop test no longer collects. The immediate next gate
  is the complete affected-file cluster followed by a second full MindRoom
  repository run; do not treat the first full-run failure count as unresolved.
- The complete affected-file compatibility cluster is now GREEN: 188 collected,
  185 passed, and 3 skipped across DM behavior/preservation, Matrix continuity,
  multi-agent E2E, router dispatch/rooms, scheduled-task restoration, and team
  invitations.
- The second full MindRoom run reached 100% with only two failures, both in the
  legacy invite fixture. The first distinguished a stale cache entry after a
  local departure from a genuinely current cached membership; the shared
  adapter now short-circuits only when no local source-echo fence is pending.
  The second still expected the deleted direct EventJournal fence write and now
  asserts the surviving `_local_departures_awaiting_sync` fence instead; real
  gateway durability remains covered by the causal membership tests above.
  The exact two nodes and the expanded nine-file cluster are GREEN: 229
  collected, 226 passed, and 3 skipped. A third full MindRoom run is the
  immediate active gate.
- The third full MindRoom repository run is GREEN: all 13,828 collected nodes
  completed with exit status zero. Its warnings are the existing byte-order
  pytest marker warnings, third-party OpenBB/Pydantic deprecations, and the
  existing fork warning in the PostgreSQL harness; no Task 5D failure remains.
  Final static/hooks, budgets, diff review, and the ordinary Task 5D commit are
  now the active boundary.
- The all-files MindRoom pre-commit stack is GREEN on the current tracked
  bytes: whitespace/EOF/YAML, Ruff lint and format, ty, Vulture, generated docs
  and model defaults, tool-extra sync, Tach boundaries, Privata, pyproject-fmt,
  Prettier, ESLint, Bun lock consistency, TypeScript checking, and JSON format.
  Direct scoped Ruff/Ruff-format/pycompile/diff checks are also GREEN.
- Vulture identified code made dormant by the cutover. The Classic rebuild
  attempt counter/backoff was deleted. The old direct
  `MembershipFence.fence_local_departure()` production seam was deleted;
  membership-fence tests now seed the same durable `DepartureSource.LOCAL`
  fact through their real store because production obtains it from admitted nio
  lifecycle Work. The directly tested Classic loop-exit recovery helper is
  temporarily named in `vulture_whitelist.py` and retained only for the Task 6
  compatibility/removal boundary; the whitelist comment states that intent.
  Two type-only imports in `_owned_session.py` moved under `TYPE_CHECKING`.
- Task 5D's committed MindRoom delta from `ba0da04...` is +147 net
  runtime lines and +486 net test lines. The final Task 10 whole-branch hard
  limits are not yet satisfied: from `0acaea2...` the branch is currently
  +1,505 runtime and +3,029 tests, and the combined delta versus `925df7f...`
  is +1,687 rather than negative. This debt is explicit and must be removed in
  Task 10; no baseline or threshold was moved.
- The mandatory read-only Task 5D review completed against the exact
  working-tree diff from `ba0da04...`. It found no Critical issue and approved
  the runner ownership, fail-closed session requirements, transport selection,
  one-record delivery, deterministic local-operation identity, shutdown
  ordering, and dead-code cleanup. It did find one Important commit blocker:
  source-reported `leave -> join` lifecycle Work currently does not rearm the
  EventJournal membership row. `_apply_ingestion_disposition()` only calls
  `_claim_membership_state()` / `note_membership_restarted()` for `LOCAL`
  joins. A server-reported initial join or rejoin can therefore leave the
  journal at `leave`, causing the next managed leave to be misclassified as an
  already-complete no-op and skip Matrix HTTP. Do not commit Task 5D until the
  source-reported join authority is causally RED/GREEN and the review is rerun.
- The frozen repair rule is: a reported join at the
  journal's current membership epoch rearms that epoch; a lower-epoch reported
  join is stale and cannot undo a newer durable local departure; a
  higher-epoch reported join is impossible missing history and fails closed.
  Local joins require exact epoch equality because their carrier is derived
  from this journal authority. An absent row projects `leave/0`, so a reported
  initial join at epoch zero creates/rearms `join/0`. Blindly rearming every
  reported join would resurrect membership after a newer local leave, which is
  why the code and causal matrix preserve this epoch comparison.
- The isolated Claude Fable consultation is complete and approved that rule.
  It ran read-only through `agent-cli dev` on temporary worktree/branch
  `task5d-reported-join-fable-20260815`; both were removed after the verdict.
  The exact frozen semantics are: equal REPORT epoch rearms; lower REPORT epoch
  is consumed as a benign no-op so it cannot wedge later FIFO Work; higher
  REPORT epoch raises a loud integrity failure; any LOCAL epoch mismatch raises
  because LOCAL carriers derive from this journal. An initial REPORT join is
  just the equal `leave/0 -> join/0` case, not a separate branch. This relies on
  the current invariant that membership rows/epochs are never deleted or reset;
  any future row GC must reopen this decision. Histories A-D resolve to
  `join/0 -> leave/1`, reported rejoin at epoch one followed by distinct
  `leave/2`, stale reported join-one leaving `leave/2` untouched, and
  higher-than-stored REPORT rejection respectively.
- The review also requested one Minor cleanup: remove the production-only
  `join_room` re-export/F401 suppression from `bot_room_lifecycle.py` and point
  test adapters and monkeypatches at `mindroom.matrix.client_room_admin`
  directly. This is secondary to the membership-authority fix and must not
  broaden runtime behavior.
- The source-reported-join repair is now causal RED/GREEN on SQLite and
  PostgreSQL. Before the production edit, the three exact journal nodes yielded
  six intended failures: initial REPORT join still projected `leave/0`, the
  lifecycle matrix had no row/rearm, and a REPORT join ahead of stored authority
  did not raise. `_apply_ingestion_disposition()` now claims every lifecycle
  join, consumes only lower-epoch REPORT joins as stale, requires exact epoch
  equality otherwise, and rearms the matching row. Departures, receipt
  sequencing, and semantic-event disposition were not changed.
- The final matrix proves initial reported join -> `join/0`; report departure ->
  `leave/1`; equal-epoch report rejoin -> `join/1`; local departure ->
  `leave/2`; stale report join-one cannot undo `leave/2`; exact local rejoin can
  rearm epoch two; and ahead REPORT plus behind/ahead LOCAL rejoins roll back
  without advancing the graph. A real Bot/EventJournal gateway test now starts
  with REPORT `None -> join/0`, issues one deterministic durable leave at epoch
  zero, admits REPORT `leave/1 -> join/1`, and issues the next leave with a
  distinct deterministic UUID, ending at `leave/2`.
- The first final re-review found one adjacent Important gap before commit:
  LOCAL `join -> leave` carriers were internally epoch-consistent but were not
  compared with the claimed journal state. Behind/ahead LOCAL-departure tests
  then produced four causal `DID NOT RAISE` failures across SQLite/PostgreSQL.
  The lifecycle departure branch now requires the LOCAL carrier's
  `previous_membership_epoch` to equal the stored epoch before fencing; both
  mismatch arms roll back their receipt/frontier and graph. REPORT departures
  retain their existing owed-echo semantics. The complete membership-authority
  selector is now 14/14 GREEN. The final read-only review approved exact
  MindRoom delta SHA-256
  `5e2ed2ebedbdb6fe85e327129948ee2710530f3b572edeb46511b2dc389d3d1b`
  with no Critical, Important, or Minor findings. It independently confirmed
  that the LOCAL guard precedes fence/receipt mutation, mismatch rollback is
  complete, REPORT owed-echo handling is unchanged, and REPORT join
  equal/lower/higher plus LOCAL exactness match the frozen rule. Review status:
  READY YES.
- The review's Minor cleanup is complete: `bot_room_lifecycle.py` no longer
  imports/re-exports `join_room` for tests, and the shared adapter plus all
  affected monkeypatches target `mindroom.matrix.client_room_admin.join_room`
  directly. No product fallback was added. The complete ten-file affected
  cluster (durable admission, EventJournal store, Bot gateway, config reload,
  invites, DM, continuity, router, scheduled restoration, and team invites) is
  GREEN with the same three pre-existing byte-order marker warnings and three
  intended skips.
- The final post-cleanup full MindRoom repository suite is GREEN on the exact
  commit-candidate bytes after both review repairs: all 13,838 collected nodes
  completed with exit status zero. Its warnings are the existing three
  byte-order pytest markers, PostgreSQL fork warning, and third-party
  OpenBB/Pydantic deprecations. The all-files pre-commit stack is also GREEN on
  these bytes with `UV_NO_SYNC=1`: whitespace/EOF/docstring/YAML, Ruff
  lint/format, ty, Vulture, generated docs/defaults, extras, Tach, Privata,
  pyproject-fmt, Prettier, ESLint, Bun, TypeScript, and JSON. The linked nio
  checkout remains path-verified at `/work/dev/mindroom-nio/src/nio`.
- One attempted final suite never reached collection because a changed-file
  pre-commit command omitted `UV_NO_SYNC=1` and its internal `uv run` restored
  released `mindroom-nio==0.37.0`. No source changed. The documented editable
  install was restored with
  `uv pip install --python .venv/bin/python --no-deps -e /work/dev/mindroom-nio`,
  import paths were verified, all hooks were rerun with the no-sync guard, and
  the successful 13,838-node full run above used the restored local checkout.
  Preserve this exact environment boundary on restart.
- nio's pre-commit hooks pass for all three changed files. An all-files nio hook
  invocation reaches only two unchanged diagnostics in
  `scripts/live_sliding_sync_check.py` (`_wait_for` and `_wait_server_up` use a
  `timeout` parameter); those scripts are outside the Task 5D diff and were not
  modified. The changed-file Black/Ruff/whitespace/EOF gates and earlier full
  nio suite remain GREEN. Do not misreport the repository-wide hook baseline as
  a Task 5D regression or silently edit those unrelated scripts in this slice.

Expected state after committing this final ledger checkpoint:

- nio and MindRoom tracked trees are clean at the Task 5D commits recorded
  above. Any tracked modification means the restart boundary has changed and
  must be inspected rather than reset;
- preserve the unrelated untracked paths listed at the top of this document.

Resume commands after a lost session:

```bash
cd /work/dev/mindroom
uv pip install --python .venv/bin/python --no-deps -e /work/dev/mindroom-nio
UV_NO_SYNC=1 .venv/bin/python -m pytest -q -n 0
UV_NO_SYNC=1 .venv/bin/pre-commit run --all-files

# Fast restart slice if the completed full evidence does not need repetition.
.venv/bin/pytest -q -n0 tests/test_multi_agent_bot.py tests/test_config_reload.py \
  tests/test_hook_matrix_admin.py tests/test_bot_ready_hook.py
.venv/bin/pytest -q -n0 tests/test_room_invites.py

# Revalidate every legacy fixture repaired after the first full run.
UV_NO_SYNC=1 .venv/bin/pytest -q \
  tests/test_dm_functionality.py tests/test_dm_room_preservation.py \
  tests/test_matrix_sync_continuity.py tests/test_multi_agent_e2e.py \
  tests/test_router_dispatch.py tests/test_router_rooms.py \
  tests/test_scheduled_task_restoration.py tests/test_team_invitations.py

cd /work/dev/mindroom-nio
.venv/bin/pytest -q tests/ingest/coordinator_test.py \
  -k 'local_membership_idle or local_membership_success_waits or work_wait'
```

Immediate remaining Task 5D order:

1. [x] Add one real EventJournal-backed leave -> join -> leave gateway chain,
   proving epoch advancement and distinct deterministic UUIDs after admitted
   lifecycle transitions rather than mocked journal positions.
2. [x] Bind crash/retry evidence to nio's existing intent/HTTP/Work tests. The
   gateway UUID and real-journal chain supply the MindRoom boundary; no
   duplicate integration row is needed after the 25/25 exact nio matrix.
3. [x] Prove empty-frame completion reaches first-ready without Work.
4. [x] Close the remaining runner/pump restart/watchdog/cleanup acceptance
   rows.
5. [x] Run full cross-repo lifecycle/parity suites and exact Ruff/Black/ty/Tach/
   Privata/pre-commit gates, then review budgets and commit Task 5D. Every
   correctness/static gate and the final independent review are complete:
   14/14 authority cases, the ten-file affected cluster, all 13,838 full-suite
   nodes, and the all-files hook stack are GREEN; final review is READY YES.
   The exact Task 5D delta is +147 runtime/+486 tests and the larger Task 10
   consolidation debt is recorded above. The linked ordinary commits are nio
   `1ea9e4aae108c0f1fd2d1bec1ba81f188af408c4` and MindRoom
   `47180cc07e7d0177ff4981bd7ccf1f1ec65eb047`; this ledger checkpoint records
   their completion and makes Task 6 the restart target.

Task 4F is complete and committed. Its closed checklist is retained below as
historical evidence, not as the current work queue:

- [x] Re-read the controlling Task4F design/plan sections and inspect the
      current retained prepared-Frame, owned runner, Work ack, outbound-plan,
      local-intent, close, poison, and wake seams before editing.
- [x] Freeze the smallest private progress-machine carrier/API boundary. Keep
      it non-exported; do not add a public `IngestionSession` method, a sixth
      table, or live MindRoom wiring.
- [x] Add the first real causal RED for a retained prepared Frame whose Work is
      fully acknowledged: it must progress into exact persisted outbound
      maintenance rather than remain generically BLOCKED, and the next source
      HTTP must remain fenced.
- [x] Review that RED for authenticated ownership, FIFO, crash/restart,
      transaction, cancellation, poison, and no-next-source false greens.
- [x] Implement only the first exact progress transition, run focused GREEN and
      static gates, then continue the Task4F maintenance/claim/retire matrix one
      behavior slice at a time under the Way of working below.
- [x] Keep the living handoff updated after each reviewed GREEN, with exact
      HEAD/dirty hash, tests, blockers, and the next unchecked item.
- [x] Add a real claimed-completion sink raise/cancel/restart matrix. Prove the
      claimed Frame remains authenticated and a fresh session retires it
      without invoking the sink again.
- [x] Add completion-claim and retirement statement rollback plus post-COMMIT
      uncertainty cases, with all-old/all-new reopen fates.
- [x] Replace terminal BLOCKED while Work is outstanding with read-only
      `WAIT_FOR_RECORD_ACK` yield/wake behavior; prove no no-op owner write
      transaction and no next-source HTTP.
- [x] Dispatch and atomically apply one exact persisted key-query response,
      callback-free and under the same Frame settlement transaction.
- [x] Add key-query response rollback/post-COMMIT poison/reopen coverage before
      widening to key claim.
- [x] Add the first restart-safe exact key-claim path: authenticate one persisted
      wedged-device claim context, rebuild the required Olm input after reopen,
      apply the successful response, persist the Olm session, settle the claim,
      and append the generated dummy to-device operation in the same owner
      transaction.
- [x] Add key-claim rollback/post-COMMIT poison/reopen coverage, including the
      exact session, settled claim, and appended dummy-operation fates.
- [x] Preserve a wedged claim's authenticated Megolm rerequest through restart:
      rebuild the exact live bucket and transfer durable ownership to the
      generated dummy operation in the same claim settlement transaction.
- [x] Reconstruct one authenticated own-device waiting request after restart,
      collect it only after the claim creates its Olm session, and atomically
      append the resulting encrypted forwarded-key message as a generic pending
      to-device operation before clearing claim ownership.
- [x] Dispatch a generic Frame-owned to-device operation with its exact event
      type, deterministic transaction ID, and body; apply the response and
      settle it atomically without callback/crypto replay.
- [x] Add generic to-device rollback/post-COMMIT poison/reopen coverage. The
      durable Frame fate, not the already-consumed live outgoing queue, controls
      retry.
- [x] Reconstruct a persisted dummy and exact Megolm rerequest bucket after
      restart; when the dummy is marked sent, append the resulting pending
      room-key-request operation atomically with dummy settlement.
- [x] Add dummy settlement rollback/post-COMMIT poison/reopen coverage before
      dispatching the appended room-key-request operation.
- [x] Reconstruct, send, and atomically settle the appended
      `RoomKeyRequestMessage`, persisting its `OutgoingKeyRequest` under the
      same owner transaction.
- [x] Add room-key-request rollback/post-COMMIT poison/reopen coverage so the
      outgoing-key-request row and Frame settlement have one exact fate.
- [x] Add per-operation retryable Matrix/transport errors, bounded cancellable
      backoff, permanent-auth classification, and close/cancel fencing. No
      error may settle or fan out.
- [x] Add multiple-claim reconstruction after the single-operation transport
      lifecycle is complete; do not combine it with the first generic
      to-device dispatch slice.
- [x] Freeze the exact no-new-table local-membership-intent carrier and private
      owned-session seam. The stable operation identity and desired transition
      must commit before Matrix HTTP, survive restart, serialize ahead of source
      polling, publish the existing LOCAL lifecycle Work exactly once after the
      Matrix fate is reconciled, wait for its ack, and retire without a lost wake.
- [x] Add local-intent HTTP success/error/restart, duplicate/reconcile,
      Work-ack/retirement, rollback/post-COMMIT, cancellation/close, and
      source-poll fencing slices one causal RED at a time.

The completed Task4F implementation commit is
`ec9fe0da38378e7a7d5d100737787fb7584d8662` and contains exactly:

```text
docs/superpowers/plans/2026-08-13-durable-ingestion-lean-replacement.md
src/nio/ingest/coordinator.py
src/nio/store/_sync_journal.py
src/nio/store/_sync_journal_preflight.py
src/nio/store/_sync_journal_rows.py
src/nio/store/_sync_journal_values.py
src/nio/store/sync_journal_schema.py
tests/ingest/coordinator_test.py
tests/ingest/source_journal_test.py
```

After this living handoff update, the expected tracked dirty list is only this
plan file; all `src/` and `tests/` paths must be clean. The exact committed
binary source/test patch has SHA-256
`1ca31931088369a02520a28e1d61280592bef3940531ba5dbc88d6403e7eb1bc`.
The first RED uses one real empty Classic Frame on a fresh owned account. Task4C
produces zero Work and one authenticated pending key-upload operation. Before
production the owned runner stopped solely with `IngestionBlockedError`. It now
loads an authenticated oldest prepared Frame, proves no Work still references
it, exposes the first pending operation through a private carrier, and enters
the exact `POST /_matrix/client/v3/keys/upload` request with the persisted body
before any source poll. Cancellation preserves source, Frame, plan, and the
entire SQLite graph byte-for-byte.

The next causal RED proved a returned 200 response was ignored. The narrow
success path now parses exact `KeysUploadResponse` outside the owner transaction,
recaptures the same authenticated oldest Frame/operation after HTTP, invokes
Olm response application inside one owner transaction, and commits the account,
Meta revision, and pending→settled authenticated Frame envelope together. No
callback or public `receive_response()` path runs. The two exact nodes pass;
Black, production Ruff `--no-fix`, targeted mypy, `py_compile`, and diff-check
are GREEN.

Key-upload response application now also has exact causal uncertainty coverage.
A failure after the authenticated Frame update rolls back account, Meta, and
Frame to the byte-identical pending graph and poisons that client; a post-COMMIT
hook failure reopens the all-new settled graph with the account shared. Both
paths require a fresh client before more owned work.

The private frozen `_FrameCompletion` carrier now binds exact Frame ID,
transport, source epoch, request ID, staged revision, and claimed revision. Once
the oldest authenticated prepared Frame has no Work and every maintenance
operation is settled, one owner transaction advances Meta and durably writes
its claim. The optional private owned-session sink runs after that transaction;
retirement then advances Meta and deletes the exact claimed Frame in a second
transaction. A real empty Classic Frame proves key upload → claim → sink →
retire → next source HTTP, with the Frame present and claimed during the sink
and absent before the source poll. The older retained-head runner
characterization now blocks inside the first sink and proves private
materializer dispatch and two-Frame FIFO without an early second prepare or
HTTP call. The private factory alone gained the underscored completion-sink
parameter; public `open_ingestion()` and exports remain unchanged.

The sink raise/cancellation matrix is also GREEN without a further production
change. Both exact failures propagate after the authenticated claim, retain the
claimed Frame and settled plan, leave the client unpoisoned, and close cleanly.
A fresh client authenticates and retires that Frame without invoking even an
installed sink again. This is the required at-most-once fanout boundary.

The four exact completion statement fates are GREEN: claim rollback is all-old
and invokes no sink; a claim post-COMMIT failure reopens claimed and therefore
retires without fanout; retirement rollback preserves the already-claimed
Frame after one sink invocation; retirement post-COMMIT reopens with the Frame
absent. Every path authenticates its precise reopen state and no path poisons
the client.

The Work boundary is now a resumable wait rather than terminal BLOCKED. Before
attempting a completion transaction, the journal authenticates the oldest
prepared Frame and its Work inventory under a read scope. The owned session
uses a generation plus `asyncio.Event` signal, so an ack between classification
and suspension cannot be lost. Public `acknowledge_batch()` and private
`_settle_batch()` both signal only after their journal ack commits; close still
cancels/drains the waiting runner. A real one-record GLOBAL_ACCOUNT_DATA Frame
proves both ack authorities, zero `BEGIN IMMEDIATE` and zero HTTP while waiting,
then completion/retirement before the next source poll. Completion claiming now
also performs a read-only eligibility preflight and therefore opens no no-op
write transaction for Work-owned or already-claimed Frames.

Key-query is now the second exact maintenance operation. Its authenticated
Frame-owned body is sent only to `POST /_matrix/client/v3/keys/query`; an exact
`KeysQueryResponse` is parsed outside SQLite, then ordinary Olm/device-store
application and pending→settled Frame replacement commit together through the
same generic journal settlement. No callback or public response handler runs.

The response crash matrix now runs upload and query through identical
rollback/post-COMMIT boundaries. Query rollback is deliberately causal: Olm has
already removed the user from its live query queue before the Frame write hook
raises, yet SQLite reopens byte-identical and the client is unusable until a
fresh reconstruction sees the pending operation. Query post-COMMIT reopens the
settled plan; neither fate relies on reconstructing the old in-memory queue.

The first exact key-claim path is now GREEN. A real remote Olm account produces
one signed one-time key; Task4C freezes one claim for a persisted wedged device,
including the exact `was_wedged`, `was_waiting`, waiting-request, and rerequest
context. The test closes and reopens before dispatch, proving the live Olm queues
are empty and the response cannot be applied correctly without the authenticated
Frame context. The coordinator rebuilds the one supported wedged claim input,
parses `KeysClaimResponse` outside SQLite, and applies it inside the owner
transaction. The journal atomically persists the new Olm session, marks the
claim settled, and appends the exact generated dummy as a pending to-device
operation with deterministic Frame/index/body-derived transaction identity.

That first claim checkpoint intentionally accepted only one wedged target. It is
now superseded by the multi-target checkpoint below; the historical statement
remains useful only for understanding the order in which the causal coverage
was built.

Key-claim response uncertainty now runs through the same exact statement matrix
as upload and query. A failure after the Frame write occurs after Olm has created
the live session and dummy, yet SQLite rolls back to the byte-identical pending
claim with no session or follow-up; the current client is poisoned. A post-COMMIT
failure reopens with the Olm session, settled claim, and exact deterministic
pending dummy all durable. In both cases a fresh client authenticates the Frame
and session fate, and the transient outgoing-message queue is empty after reopen
because the Frame is its durable owner.

Wedged-claim rerequests now survive the same restart boundary. Task4C freezes the
exact canonical Megolm source and bound room/event/device/session fields. A fresh
client begins with no rerequest bucket; claim application reconstructs and
revalidates the exact `MegolmEvent` before Olm handles the response. If a live
bucket already exists, it must match exactly. The generated dummy operation then
owns the copied rerequest context durably, so later to-device settlement can
produce the required room-key rerequest without relying on transient Olm state.
Conflicting live state fails closed.

The first waiting-device claim is also restart-safe. A real own-device
`RoomKeyRequest` and inbound Megolm session are prepared through Task4C. After a
fresh open, the coordinator rebuilds the exact waiting device/map entry, applies
the successful claim, and invokes ordinary key-request collection inside the
same owner transaction. The supported case must produce exactly one encrypted
forwarded-key `ToDeviceMessage` and no callback Work. That message is appended
as a generic pending to-device operation before the claim context is cleared.
A second fresh client has an empty transient outgoing queue but authenticates
the Olm session and exact Frame-owned share. Missing sessions, changed waiting
state, callback-producing requests, and other result shapes fail closed.

Each individual claim remains deliberately narrow: its preparation reason must
be either wedged or waiting, not both. The waiting path accepts exactly one
request and the wedged path at most one rerequest. A canonical nonempty claim
list is now supported; combined reasons and wider per-target evidence remain
fail-closed until their own causal coverage.

Generic Frame-owned to-device delivery is now exact and restart-safe. The
coordinator reconstructs one authenticated generic `ToDeviceMessage`, binds its
event type and deterministic transaction ID into
`PUT /_matrix/client/v3/sendToDevice/{type}/{txn}`, parses `ToDeviceResponse`
outside SQLite, and applies it with the pending→settled Frame update inside one
owner transaction. No public response/callback path runs. Its statement matrix
proves rollback reopens pending even though the failed live client has already
consumed its outgoing queue, while post-COMMIT reopens settled; both failure
paths require a fresh client.

Dummy delivery now has its distinct durable side-effect path. On a fresh
client, the coordinator reconstructs the exact `DummyMessage`, restores its
authenticated Megolm rerequest bucket, and places only that dummy in the live
queue before applying `ToDeviceResponse`. Ordinary Olm removal produces the
exact `RoomKeyRequestMessage`; the journal settles the dummy and appends that
request as a pending Frame-owned operation with deterministic identity and
authenticated request/session/room/algorithm context in the same transaction.
A dummy without rerequests settles without a follow-up. Other live queue or
bucket shapes fail closed.

Dummy and room-key-request uncertainty are now closed. A dummy failure after the
Frame write occurs after the live client has generated its room-key request;
rollback nevertheless reopens the pending dummy with no appended request, while
post-COMMIT reopens the settled dummy and exact pending request. Sending that
persisted request reconstructs the exact `RoomKeyRequestMessage`, applies
`ToDeviceResponse`, persists `OutgoingKeyRequest`, and settles the Frame
operation in one transaction. Its rollback removes both the ordinary E2EE row
and Frame settlement; post-COMMIT reopens both. All failed in-memory clients are
poisoned and fresh clients authenticate the durable fate.

Maintenance retry and permanent-auth handling now preserve the same durable
owner. Retryable transport failures, 408/429/5xx, and `M_LIMIT_EXCEEDED` reuse
the exact authenticated `NetworkRequest`; the retry delay is zero initially,
honors bounded `Retry-After`, and then uses the client-configured capped
exponential backoff. After each sleep the pending carrier is reauthenticated
before another send. Upload and deterministic generic to-device tests prove no
SQLite mutation between attempts and exact request equality. `M_UNKNOWN_TOKEN`
and `M_FORBIDDEN` (including status-derived 401/403) raise one sticky
`IngestionSourceError`, leave the Frame byte-identical and the client unpoisoned,
and issue no second request. Closing during a held backoff cancels/drains the
runner; a fresh client authenticates the unchanged pending operation.

Multi-target key claiming is now restart-safe. The causal case freezes devices
in reverse live order but authenticates the canonical `(user_id, device_id)`
order, including one target-owned Megolm rerequest. A fresh client reconstructs
both targets, applies one successful `KeysClaimResponse` once, persists both Olm
sessions, settles the claim, and appends two exact dummy operations with globally
monotone Frame operation indices and target-specific rerequest ownership. The
reducer rejects duplicate targets, missing devices, changed live waiting or
rerequest state, foreign/duplicate generated follow-ups, and unsupported
per-target reason shapes. The shared response crash matrix now includes this
two-device claim: an `outbound_frame_settle` failure reopens the byte-identical
all-old Frame with neither session, while a post-COMMIT failure reopens both
sessions plus the settled claim and both pending dummies.

Fresh checkpoint evidence is fifty-two focused maintenance/completion/intent
cases passing. The current whole coordinator + source-journal + delivery gate
is **600 passed** (**288 + 264 + 48**). The crash-recovery,
owned-materializer-crash, Classic/Sliding parity, and preparation gate is an
additional **186 passed**. Black, production Ruff `--no-fix`,
targeted changed-source mypy, `py_compile`, and diff-check are GREEN. The only
broader mypy output is the four unchanged diagnostics at
`_sync_journal_preflight.py:812`, blame-owned by `c0a259d7`; the Task4F change in
that file is one exact Aggregate-state predicate at the local Work frontier.
The local-intent carrier, pre-HTTP runner, and first success/ack checkpoint are
GREEN. A canonical absent-room
`leave/0 -> join` operation now commits one `_LocalMembershipIntent` inside its
room Aggregate under authenticated `intent_kind='local_membership'`, while
continuity remains `leave/0` and no Work, Frame, source mutation, or ordinary
store mutation occurs. The Aggregate codec accepts the new mutually-exclusive
private intent without changing canonical bytes for ordinary/hydration rows;
the unchanged schema-v1/five-table topology extends only the existing CHECK
discriminator. Fresh owned reopen authenticates and returns the exact carrier
read-only. Existing local-publication and Aggregate/preflight selectors remain
GREEN. The owned runner now persists that intent before Matrix HTTP, blocks the
next source poll, reconstructs the exact request after close/reopen without the
original caller, and consumes a successful `JoinResponse` in one owner
transaction that clears the intent, advances continuity, and inserts the exact
operation-keyed READY LOCAL lifecycle Work. Source HTTP stays fenced while that
Work is outstanding and begins only after settlement wakes the runner. Exact
concurrent callers share one request; conflicting concurrent
commands reject before I/O; replay after Work publication resolves from the
authenticated operation-keyed Work without another request. An existing-room
`join/0 -> leave/1 -> join/1` chain is ordered behind each prior Work ack, and
source remains fenced through the second ack. Retryable 429/5xx and
`M_LIMIT_EXCEEDED` replay the byte-identical request after reauthenticating the
intent; 401/403 retain it under one sticky auth error; a terminal rejection
atomically clears it without Work and resumes source. Close during backoff
leaves one restartable intent. Intent insert/update, success publication, and
terminal clear now have all-old rollback and all-new post-COMMIT reopen proofs.
Transport failure, 408, and malformed-200 cases are now explicit. A committed
local Work survives fresh reopen, issues no HTTP until ack, and never
resurrects the membership request after an ack/reopen. Preflight rejects two
individually authenticated pending local intents and rejects a pending intent
coexisting with Frame or Work queues. The current exact source/test patch
SHA-256 is
`1ca31931088369a02520a28e1d61280592bef3940531ba5dbc88d6403e7eb1bc`.
The complete repository gate is **2,731 passed, 3 skipped** with five unchanged
platform/deprecation warnings. Exact touched-file pre-commit hooks, public
signature/export checks, compile checks, and diff-check are GREEN. The Task4F
slice is +1,946 net runtime lines and +3,972 net test lines; the larger fixed
branch budgets remain intentionally deferred to the Tasks 6/9 deletion and
consolidation work. These exact bytes are committed as
`ec9fe0da38378e7a7d5d100737787fb7584d8662`.

Historical transition satisfied: the Task 5C factory, login lifecycle,
completion sink, and controller-identity resolver are implemented and
full-suite GREEN. The live handoff above owns current finalization. Do not wire
`Bot.sync_forever()` yet; Task 5D owns candidate-engine activation after the
ordinary Task 5C commits.

#### Approved local-membership-intent design and execution plan

The user approved the following design on 2026-08-14. Continue autonomously;
do not pause the RED/review/GREEN loop for intermediate approval. Use best
engineering judgment. If a genuinely material ambiguity remains after reading
the design, code, and tests, consult Claude Fable through `agent-cli-dev`, record
the conclusion here, and continue. Do not delegate ordinary implementation or
review work merely because that consultation path exists.

Use `NioIngestRoomAggregate`, not Work or Frame, as the no-new-table intent
owner. Add private `_LocalMembershipIntent(operation_id, previous_membership,
previous_epoch, current_membership)` and authenticate it under
`intent_kind = 'local_membership'`; the Aggregate row supplies `room_id`.
`RoomAggregateValue` carries at most one of `pending_hydration` and
`pending_local_membership`. A missing-room `leave/0 -> join` intent creates the
same canonical predecessor Aggregate that direct local publication already
uses. Persist no access token or fabricated source identity. The runner rebuilds
the simple current join/leave endpoint from the current authenticated client and
room ID.

The owned runner is the sole ordering authority. A non-exported
`_run_local_membership_transition(*, operation_id: UUID, room_id: str,
previous_membership: str, previous_epoch: int, current_membership: str) -> bool`
queues at most one in-memory command and wakes the runner. The runner first
finishes any older retained Frame/hydration/Work, then records the Aggregate
intent before Matrix HTTP. It serializes and retries the exact persisted intent;
after restart it discovers that intent without the former caller. Successful
join/leave application consumes the matching intent while atomically invoking
the existing operation-keyed LOCAL lifecycle publisher. The caller may receive
success after Work publication, but source HTTP remains fenced until that Work
is acknowledged. A terminal Matrix rejection clears the matching intent without
publishing lifecycle Work and returns false to a live caller. Retryable errors
retain it under bounded cancellable backoff; permanent auth follows the existing
sticky source-error path. Close/cancellation leaves either no intent (before its
commit) or one authenticated restartable intent (after commit).

Implement the approved boundary in these independently reviewable TDD tasks:

- [x] **Carrier/codec RED:** in `tests/ingest/coordinator_test.py`, record a
      canonical missing-room join intent through the journal seam and require
      one authenticated Aggregate with unchanged `leave/0` continuity, no Work,
      no Frame, and `intent_kind='local_membership'`; reopen must return the same
      exact private carrier. Current first failure must be the missing record/load
      seam. Implement only `_LocalMembershipIntent`, Aggregate canonical codec,
      exclusivity validation, preflight authentication, and journal
      `_record_local_membership_intent()` / `_load_pending_local_membership_intents()`.
- [x] **Pre-HTTP runner RED:** start the owned runner with one queued rejoin,
      block the request hook, and prove the intent committed before the hook,
      zero LOCAL Work exists, and no source request can start. Cancel/close after
      the hook, reopen with a fresh client, and require the exact same join request
      to be reconstructed from the authenticated intent. Implement the private
      command/future seam and pending-intent runner branch only.
- [x] **Success/ack fence RED:** return a real `JoinResponse`, require one owner
      transaction to clear intent, advance Aggregate continuity, and insert the
      exact operation-keyed READY LOCAL lifecycle Work. Hold its batch outstanding
      and prove zero source HTTP; settle it, require the runner wake, then allow
      exactly one source request. Modify the existing local publisher to consume
      only an exact matching intent while preserving its direct idempotent path.
- [x] **Leave/rejoin and duplicate matrix:** cover existing-room `join -> leave`
      epoch advance, `leave -> join` same epoch, same-operation replay before and
      after Work publication, conflicting operation/transition rejection,
      multiple queued callers, and older Frame/Work ordering. Do not add knock,
      invite, ban, reason bodies, or public AsyncClient behavior in Task4F.
- [x] **HTTP/error/restart matrix:** cover retryable transport/408/429/5xx and
      bounded close-cancellable backoff; 401/403 and Matrix auth errors; terminal
      rejection clearing without Work; response loss/retry; restart before HTTP,
      after HTTP before local transaction, after Work commit, and after ack.
- [x] **Statement uncertainty matrix:** inject rollback and post-COMMIT failures
      at intent insert/update, intent clear + Aggregate/Work publication, terminal
      clear, and ack/retirement. Every reopen must be exactly all-old or all-new;
      no source HTTP may cross a retained intent or LOCAL Work.
- [x] **Final local-intent gate:** run the exact selector, coordinator,
      source-journal, delivery, crash-recovery, Classic/Sliding parity, and nio
      statics. Update the patch hash, test counts, remaining Task4F checklist, and
      restart commands here before moving to final lost-wakeup/concurrent
      runner+pump completion coverage.

### Way of working

For every remaining behavior slice, use this exact gate:

1. Read the controlling design and this live handoff before touching code.
2. Add a real, semantically valid, test-only causal RED using production
   normalization/preparation where possible.
3. Prove the first and sole failure is the missing behavior, not fixture setup.
4. Review the RED independently and close every P0–P2 test-contract gap.
5. Add the smallest production behavior; do not widen adjacent kinds.
6. Run the exact GREEN, focused regressions, and static checks.
7. Review production independently for semantics, trust boundaries, rollback,
   cancellation, restart, and static regressions.
8. Commit only a complete plan slice; never commit an intentional RED as the
   advertised completed feature.

Operational rules:

- use `apply_patch` for hand edits and preserve all unrelated dirty/untracked
  user files;
- never use destructive reset/checkout commands;
- in nio, do not force Ruff over excluded test files: repository Ruff has
  `fix=true` and twice rewrote shared `coordinator_test.py` during a supposedly
  read-only forced lint. Use the recorded Black command and `git diff --check`
  for those tests;
- in MindRoom, format only exact touched paths with `.venv/bin/ruff format`
  (line length 120), and lint production only with
  `.venv/bin/ruff check --no-fix`; never use system Black there;
- keep callbacks, parsers, and compatibility state application outside owner
  and SQLite transactions;
- authenticate the outstanding batch, Work metadata, and Aggregate before any
  client mutation or callback;
- do not call sync/response/decrypt/live handlers during settlement;
- callbacks finish before ack; errors/cancellation leave Work replayable and
  never poison the ingestion client;
- preparation/crypto transaction failures do poison the current client and
  require a fresh client/Olm reconstruction;
- keep `max_records=1`, exact FIFO, one retained Frame completion owner, schema
  v1, and exactly five ingestion tables;
- send concise progress updates during long work and stop only for a real
  safety, correctness, or scope blocker;
- do not pause the implementation/review loop merely to request approval.
  Exercise best judgment and continue through ordinary TDD/review checkpoints.
  If a material ambiguity remains after local evidence, consult Claude Fable
  through the documented `agent-cli-dev`/`agent-cli dev` isolated-worktree
  workflow, record the question and decision in this live handoff, then
  continue.

### Restart verification commands

For the clean Task 8 entry checkpoint, run these after a new session. They are
read-only except for pytest caches. Expected state is tracked-clean in both
repositories plus only the unrelated untracked paths listed near the top of
this document.

```bash
cd /work/dev/mindroom-nio
test "$(git branch --show-current)" = docs/durable-ingestion-rewrite-plan
test "$(git rev-parse HEAD^)" = 6e4f14aa2b438ba431d8f7c7ab2ff176f4e11a94
test "$(git log -1 --format=%s)" = "docs: record Task 7 completion"
test -z "$(git status --porcelain --untracked-files=no)"
git status --short
git diff --check
uv run pytest -q --tb=short \
  tests/ingest/source_journal_test.py::test_stage_crash_boundary_reopens_to_exact_old_or_new_graph \
  tests/ingest/source_journal_test.py::test_sliding_stage_failure_is_atomic_before_rotated_reopen
# expected at this checkpoint: 10 passed

cd /work/dev/mindroom
test "$(git branch --show-current)" = wip/matrix-journal-ingress-cutover
test "$(git rev-parse HEAD)" = 2cbeecb6f8e68f380a7fabb3cbe28cd88f3e1a2f
test -z "$(git status --porcelain --untracked-files=no)"
test -f tests/test_durable_ingestion_cross_database_crash.py
git status --short
git diff --check
UV_NO_SYNC=1 .venv/bin/python -m pytest -q -n0 \
  tests/test_durable_ingestion_cross_database_crash.py --tb=short
# expected at this checkpoint: 6 passed
```

Then inventory Task 8 without editing:

```bash
cd /work/dev/mindroom-nio
rg -n "canary|Synapse|wheel|docker|compose|Sliding|Classic|M_UNKNOWN_POS" \
  . --glob '!src/mindroom_nio.egg-info/**'
cd /work/dev/mindroom
rg -n "canary|Synapse|wheel|docker|compose|Sliding|Classic|M_UNKNOWN_POS" \
  . --glob '!.claude/worktrees/**'
```

Continue with the canary-harness inventory described in the Task 8 active
checkpoint. Update that checkpoint after every material discovery, build,
canary attempt, blocker, or evidence review so this file remains sufficient
restart authority.

Historical pre-Task6 restart commands (reference only; do not execute against
the current Task 7 boundary):

```bash
cd /work/dev/mindroom-nio
test "$(git branch --show-current)" = docs/durable-ingestion-rewrite-plan
test "$(git rev-parse HEAD^)" = b61e186b2b63e2b81149c40ca67c0435a60ede60
test "$(git log -1 --format=%s)" = "docs: record Task 5C completion"
test -z "$(git status --porcelain --untracked-files=no)"
git status --short
git diff --check
uv run pytest -q \
  tests/ingest/journal_test.py::test_marked_configured_default_probe_raises_typed_no_dml_signal
# expected: 1 passed
uv run pytest -q \
  tests/ingest/coordinator_test.py::test_owned_runner_enters_frame_owned_key_upload_before_source_poll \
  tests/ingest/coordinator_test.py::test_owned_key_upload_response_commits_with_settled_plan \
  tests/ingest/coordinator_test.py::test_owned_key_query_response_commits_with_settled_plan \
  tests/ingest/coordinator_test.py::test_owned_key_claim_reconstructs_context_and_appends_dummy \
  tests/ingest/coordinator_test.py::test_owned_key_claim_reconstructs_multiple_targets_in_canonical_order \
  tests/ingest/coordinator_test.py::test_owned_maintenance_response_crash_boundary_requires_fresh_client \
  tests/ingest/coordinator_test.py::test_owned_local_membership_intent_is_durable_before_http \
  tests/ingest/coordinator_test.py::test_owned_reopen_rejects_multiple_authenticated_local_membership_intents \
  tests/ingest/coordinator_test.py::test_owned_runner_restarts_local_membership_intent_before_source_http \
  tests/ingest/coordinator_test.py::test_owned_local_membership_success_waits_for_work_ack_before_source \
  tests/ingest/coordinator_test.py::test_owned_runner_orders_duplicate_leave_and_rejoin_behind_local_work \
  tests/ingest/coordinator_test.py::test_owned_local_membership_http_fate_preserves_exact_restart_state \
  tests/ingest/coordinator_test.py::test_owned_runner_reports_local_membership_terminal_and_auth_fates \
  tests/ingest/coordinator_test.py::test_owned_close_cancels_local_membership_retry_backoff \
  tests/ingest/coordinator_test.py::test_owned_local_membership_response_uncertainty_reopens_exact_fate \
  tests/ingest/coordinator_test.py::test_owned_local_membership_intent_uncertainty_reopens_exact_fate \
  tests/ingest/coordinator_test.py::test_owned_local_membership_work_reopen_fences_source_until_ack \
  tests/ingest/coordinator_test.py::test_owned_runner_claims_fans_out_retires_then_polls_source \
  tests/ingest/coordinator_test.py::test_owned_claimed_completion_reopens_without_second_sink_call \
  tests/ingest/coordinator_test.py::test_owned_completion_statement_boundary_reopens_exact_fate \
  tests/ingest/coordinator_test.py::test_owned_runner_waits_read_only_for_record_ack_then_resumes
# expected: 52 passed

cd /work/dev/mindroom
test "$(git branch --show-current)" = wip/matrix-journal-ingress-cutover
test "$(git rev-parse HEAD)" = ba0da04b5553bac9ce36b92cb1f0133211b1c3aa
test -z "$(git status --porcelain --untracked-files=no)"
git status --short
uv pip install --python .venv/bin/python --no-deps -e /work/dev/mindroom-nio
.venv/bin/pytest -q \
  tests/test_matrix_client_session.py \
  tests/test_matrix_appservice.py \
  tests/test_matrix_agent_manager.py
# expected: 82 passed
.venv/bin/ruff check --no-fix \
  src/mindroom/bot.py \
  src/mindroom/desktop/identity.py \
  src/mindroom/matrix/_owned_session.py \
  src/mindroom/matrix/client_session.py \
  src/mindroom/matrix/appservice.py \
  src/mindroom/matrix/sync_loop.py \
  src/mindroom/matrix/users.py
.venv/bin/ty check \
  src/mindroom/bot.py \
  src/mindroom/desktop/identity.py \
  src/mindroom/matrix/_owned_session.py \
  src/mindroom/matrix/client_session.py \
  src/mindroom/matrix/appservice.py \
  src/mindroom/matrix/sync_loop.py \
  src/mindroom/matrix/users.py
.venv/bin/tach check --dependencies --interfaces
.venv/bin/privata --methods .
.venv/bin/pytest -q -n 0 \
  tests/test_multi_agent_bot.py \
  tests/test_bot_sync_lifecycle.py \
  tests/test_bot_scheduling.py
# expected: 89 passed
.venv/bin/pytest -q -n 0 \
  tests/test_desktop_identity.py \
  tests/test_desktop_pairing.py \
  tests/test_commands.py \
  tests/test_turn_dispatch_pipeline.py \
  tests/test_turn_controller_focused.py
# expected: 154 passed
.venv/bin/python -m pytest -q -n 0 \
  tests/test_durable_ingestion_admission.py
# expected: 319 passed
.venv/bin/pytest
# expected: 13,704 passed, 123 skipped
UV_NO_SYNC=1 .venv/bin/pre-commit run --all-files
# expected: all configured hooks pass
```

To reproduce the final Task 5B full gate, use the same editable installation
and run `.venv/bin/python -m pytest -q` without a global `PYTHONPATH`, followed
by `.venv/bin/pre-commit run --all-files`.

The current non-RED nio regression baseline is:

```bash
cd /work/dev/mindroom-nio
uv run pytest -q tests/ingest/coordinator_test.py \
# expected: 288 passed
uv run pytest -q tests/ingest/delivery_test.py
# expected: 48 passed
uv run pytest -q tests/ingest/source_journal_test.py
# expected: 264 passed
uv run pytest -q
# expected: 2731 passed, 3 skipped
```

Current static gates are green with:

```bash
uv run black --check --fast --target-version py314 \
  src/nio/client/base_client.py src/nio/ingest/coordinator.py \
  src/nio/ingest/reducer.py src/nio/store/_sync_journal.py \
  src/nio/store/_sync_journal_preflight.py \
  src/nio/store/_sync_journal_rows.py src/nio/store/_sync_journal_values.py \
  src/nio/store/sync_journal_schema.py tests/ingest/coordinator_test.py \
  tests/ingest/delivery_test.py tests/ingest/source_journal_test.py
uv run ruff check --no-fix src/nio/client/base_client.py \
  src/nio/ingest/coordinator.py src/nio/ingest/reducer.py \
  src/nio/store/_sync_journal.py src/nio/store/_sync_journal_preflight.py \
  src/nio/store/_sync_journal_rows.py src/nio/store/_sync_journal_values.py \
  src/nio/store/sync_journal_schema.py
uv run mypy src/nio/ingest/coordinator.py src/nio/ingest/reducer.py \
  src/nio/store/_sync_journal.py src/nio/store/_sync_journal_rows.py \
  src/nio/store/_sync_journal_values.py \
  --follow-imports=skip --ignore-missing-imports
python -m py_compile src/nio/client/base_client.py \
  src/nio/ingest/coordinator.py src/nio/ingest/reducer.py \
  src/nio/store/_sync_journal.py src/nio/store/_sync_journal_preflight.py \
  src/nio/store/_sync_journal_rows.py src/nio/store/_sync_journal_values.py \
  src/nio/store/sync_journal_schema.py tests/ingest/coordinator_test.py \
  tests/ingest/source_journal_test.py
git diff --check
```

The completed Task 5B MindRoom static gate is:

```bash
cd /work/dev/mindroom
.venv/bin/ruff format --check \
  src/mindroom/event_journal/journal.py \
  src/mindroom/matrix/client_session.py \
  src/mindroom/matrix/durable_ingestion.py \
  src/mindroom/matrix/journal_ingress.py \
  tests/test_durable_ingestion_admission.py
.venv/bin/ruff check --no-fix \
  src/mindroom/event_journal/journal.py \
  src/mindroom/matrix/client_session.py \
  src/mindroom/matrix/durable_ingestion.py \
  src/mindroom/matrix/journal_ingress.py
.venv/bin/ty check \
  src/mindroom/event_journal/journal.py \
  src/mindroom/matrix/client_session.py \
  src/mindroom/matrix/durable_ingestion.py \
  src/mindroom/matrix/journal_ingress.py \
  tests/test_durable_ingestion_admission.py
.venv/bin/python -m compileall -q \
  src/mindroom/event_journal/journal.py \
  src/mindroom/matrix/client_session.py \
  src/mindroom/matrix/durable_ingestion.py \
  src/mindroom/matrix/journal_ingress.py \
  tests/test_durable_ingestion_admission.py
git diff --check
```

### Provisional budget status

The final fixed budgets are not yet met because fork-recovery deletion and test
consolidation occur later in Tasks 6 and 9. At the final Task4E checkpoint,
measured from the fixed baselines:

```text
nio src:            +18,234 net lines  (final limit +4,500)
nio tests:          +51,343 net lines  (final limit +15,000)
nio docs/scripts:    +1,680 net lines  (final limit +1,000)
nio scripts:              0 net lines
MindRoom src:          +418 net lines  (final limit +350)
MindRoom tests:        +990 net lines  (final limit +1,000)
MindRoom vs canary:  -1,439 net lines
```

The living handoff currently contributes to the 680-line docs/scripts excess.
Task 10 must consolidate completed implementation detail from this plan after
the code is frozen; do not move the baseline or budget.

## Task 1: Freeze the lean authority and prune the canary architecture

**Complete.** The diagnostic/schema-v2 chain was reverted through ordinary
commits, `origin/main@6ed2b98` was merged normally, MindRoom's canary commit was
ordinarily reverted, and nio's remaining schema-v1 diagnostic materializer was
removed at `0b07c510`. Current authority is schema v1/five ingestion tables,
`secure_delete="fast"`, generic materialization, and no canary production
types/configuration. Exact revert SHAs, paths, and verification are frozen in
the progress ledger and Task-1 report; do not replay or squash them.

## Task 2: Prune documentation, benchmarks, and redundant tests

**Complete.** `fe8a868`, `bdd5d7c`, and `5709f34` removed 2,435 net test lines
without production changes and retained stronger behavior/crash/public-API
coverage. Full verification was 2,056 passed/3 skipped. Exact mappings,
commands, deltas, and review approval are frozen in the ledger/report.

## Task 3: Implement the minimal Sliding source reset

**Complete.** `febbe9a5` implements one authenticated Meta+Source transaction
for Sliding reopen and exact positioned `M_UNKNOWN_POS`, preserving all other
durable state and Classic behavior. Full nio verification was 2,088 passed/3
skipped; exact causal/crash evidence and review are frozen in the ledger/report.

## Desktop compatibility slice: select the upstream sync profile

**Complete.** MindRoom `1fd376583` selects the exact Desktop no-recovery
profile, and `a5d3edc33` supplies the query-only identity reader. The later 5C
live-bot resolver replacement remains pending.

Implement this MindRoom-only slice before Task 4. It is part of the same linked
change set and adds no user-visible option.

Extend the internal `MatrixSyncStorage` construction policy with one exact
limited-timeline-recovery boolean. The default bot profile remains unchanged
until durable activation: recovery on, recovery persistence selected by the
existing field, and sync-token storage selected by the existing field. Define
one private Desktop profile with limited-timeline recovery off, recovery
persistence off, and sync-token storage on.

Pass that exact profile through every Desktop client-construction path:

- password login;
- SSO login-token exchange, including the temporary exchange client and the
  returned authenticated client;
- restored login.

Retain `_MindRoomAsyncClient`, custom headers, SSL, the existing DefaultStore
and pickle key, `replace_rotated_device_keys`, key upload, `sync()` and
`sync_forever()`, to-device and response callbacks, room/client projection,
presence handling, and permanent-authentication errors. Do not introduce a
Desktop sync loop, durable-ingestion adapter, public setting, or nio change.

Write causal tests first. Prove the Desktop profile yields
`backfill_limited_timelines=False`, `backfill_persist_recovery=False`, and
`store_sync_tokens=True`; default bot and application-owned profiles retain
their prior values. Prove password, SSO-token, and restore paths pass the exact
profile into the real internal client factory, and that Desktop startup sync,
pairing sync, bridge `sync_forever()`, to-device callbacks, key upload, headers,
and permanent-auth handling remain behavior-compatible. Run the focused
Matrix-client/Desktop session, pairing, CLI, identity, Cloudflare-access, and
to-device suites, then the full MindRoom suite and statics.

**Commit:** `refactor: isolate desktop from sync recovery`

Before Task 4A, also remove Desktop controller-identity lookup's mutating
`SqliteStore` construction. Read the expected account row through a raw
query-only SQLite connection, authenticate exact store version, user/device,
pickle bytes and shared flag, and decode only the Olm account needed for its
Ed25519 pin. It must not create a table, run a migration, or rewrite an old
pickle. Prove a real DefaultStore-authored database stays byte/schema/row
identical and never gains `DeviceTrustState`; malformed/cardinality/version
cases fail without mutation. This reader is the pre-activation compatibility
path. Task 5C replaces it with the already-owned live-bot resolver before the
exclusive durable session becomes the sole engine.

**Commit:** `fix: read desktop identity without store mutation`

## Task 4: Prove one shared reducer and materializer

Use test-driven development. First capture each causal failure below against the generic path; do not implement production preparation or callback claims before its failing behavior test exists.

Task 4 and Task 5 are one interleaved dependency chain. Execute and review the
following ordinary commits in this exact order inside the same linked PRs:

```text
4A in-place adoption
-> 4B store/session lease transfer
-> 5A MindRoom receipt/idempotency kernel
-> 4C callback-free preparation
-> 4D atomic all-kind materialization and snapshots
-> 4E receipt-gated compatibility apply/ack
-> 5B MindRoom all-record adapter and pump
-> 4F Frame completion state machine
-> 5C MindRoom durable login/session factory
-> 5D sole MindRoom engine activation
```

Do not collapse these into one implementation/review gate. They remain commits
in one PR per repository, not phase PRs.

### Task 4A: exclusive in-place v1 adoption

**Complete.** Implemented and reviewed in nio `044a74e`.

Adopt or reopen an exact populated store-v10 per-device database before Olm
initialization through a non-exported `_open_configured_ingestion_store()`
entrypoint.
It requires an explicit source class and owned runtime class. The only approved
pairings are existing `DefaultStore` source to `SqliteStore` owner, or existing
`SqliteStore` source to `SqliteStore` owner. A DefaultStore source must have the
ordinary MatrixStore tables and may legitimately have no `DeviceTrustState` or
the exact ordinary table with legacy rows: historical v2 upgrade and a pre-fix
Desktop reader could create it even while DefaultStore sidecars remained
authoritative. A SqliteStore source must have that table. Reject malformed table
topology, but the table's presence, absence, emptiness, or contents never infer
the source class. MindRoom explicitly supplies `DefaultStore`: in the same
adoption transaction, create the table when absent or replace its rows when
present, copying each current effective sidecar fact for an exact `DeviceKeys`
identity. Preserve DefaultStore's existing priority—verified, then blacklisted,
then ignored—and its parsing behavior; unmatched/stale entries remain inert.
Preserve all three
sidecar files byte-for-byte as rollback/legacy artifacts, and never use or
mutate them from the durable-owned session. Carry `SqliteStore` immutably as the
validated runtime class on the returned `StoreBootstrap` for Task 4B, without a
new ingestion table or persisted class marker. Keep the public
`open_ingestion_store()` signature unchanged; it remains fail-closed for a
populated unmarked database because it has no private configured-class input.
Retain schema v1, exactly five ingestion tables, and only the authenticated nullable Frame
`callbacks_claimed_revision` column. Preserve all MatrixStore, Olm/Megolm,
token, and inert recovery rows byte-for-byte; the sole ordinary-store mutation
is the explicit DefaultStore-to-SqliteStore trust conversion above. Inject every
bootstrap failure—including trust-table create, legacy-row delete,
post-delete/pre-first-insert, every trust insert, and ingestion schema/row
boundaries—and prove all-old/all-new recovery across both trust conversion and
ingestion bootstrap. Reject partial/mixed topology, wrong identity/version
or pickle key, a live legacy-store lease, and repeat adoption before DML.
Authenticate the exact account pickle without calling a loader that could
upgrade/rewrite it.

The same private entrypoint is the only configured-class restart path for an
already marked database. On every process reopen, the caller explicitly
supplies `SqliteStore` as the owned runtime class; reauthenticate that topology,
account/pickle, identity, foreign keys, and all ingestion state before Task-3
writer/source rotation DML, then return a newly bound `StoreBootstrap` without
re-running adoption or rereading sidecars. A second live or concurrent adoption
rejects; a clean marked reopen does not. Test both source-class adoption paths
and the SqliteStore marked restart.

Every exact built-in filesystem `DefaultStore` and `SqliteStore`, plus a
subclass inheriting the built-in `_create_database()` unchanged, must hold an
internal shared lifetime sidecar/file-identity lease. Ingestion adoption takes
the exclusive form and holds it through session close. Multiple participating
ordinary same-file stores therefore retain upstream behavior; exclusive
adoption rejects any live participating built-in-factory store in any process.
The privately borrowed ingestion store reuses the exclusive owner and opens no
second connection. Release follows the existing database-connection/finalizer
path for ordinary stores and the one ingestion close owner for adopted stores.
A subclass overriding `_create_database()` retains its pre-candidate extension
behavior and public API but is an uncoordinated legacy extension: it is
ineligible for `_open_configured_ingestion_store()` and must not open, or have a
live connection to, any path selected for configured ingestion. The private
opener accepts only exact `DefaultStore`/`SqliteStore` classes before lock, SQL,
sidecar access, or filesystem mutation. This is an explicit deployment
precondition, like stopping a pre-candidate binary before adoption.
Every ordinary `DefaultStore` trust-sidecar read or write additionally enters
one shared ownership guard for the whole semantic method—even multi-file trust
mutations and `load_device_keys`—with per-I/O assertions, including after its
physical database connection closes. Exclusive adoption therefore cannot
snapshot a transient partial trust update.
The durable-owned store is always `SqliteStore` and has no sidecar operations.
Deployment must stop the old binary before candidate adoption; a dormant
pre-candidate connection cannot retroactively participate in the new sidecar
protocol. Cover two ordinary same-file stores, shared/exclusive cross-process
rejection, process/thread close, cancellation, and stale-file identity in
`store_owner_test.py`, `journal_test.py`, and `crash_recovery_test.py`.

**Commit:** `feat: adopt populated matrix stores durably`

### Task 4B: exact store-to-session lease transfer

**Complete.** Contract documentation is nio `c06b88a`; implementation and
review are `f27f5e3c149d3e5215f9641f9daa9b291502c179`.

Preserve the existing public state machine and exact `open_ingestion()`
signature: public session-first remains successful, and a prior direct public
`StoreBootstrap.open_matrix_store()` continues to make public session claim
fail without consuming the bootstrap. Add a non-exported
`_open_owned_ingestion()` factory for MindRoom. It privately creates the one
borrowed `SqliteStore` that Task 4A validated and bound immutably on the
bootstrap from the bootstrap's exclusive owner, attaches that exact object to
a storeless client before Olm initialization, and atomically exchanges a
private exact-object creation capability for the sole journal/store ownership
token held by a minimal `_OwnedIngestionSession`. Task 4F extends that private
session with completion behavior; it does not redefine ownership. Never use
the public store-first claim or reverse `_claim_session()`. Reject a foreign
store, unbound bootstrap, second store/session, use after transfer,
wrong-thread close, and sequential double close; concurrent close waiters share
one cleanup and a physical-close failure remains retryable while exclusion is
held. Cancellation and close detach client Olm/store, revoke the store, close
the shared connection, then release the exclusive lease; HTTP remains owned by
the later MindRoom lifecycle. Add export and `inspect.signature` tests proving
the public factory/state machine is unchanged and the owned factory/protocol is
not exported from `nio` or `nio.ingest`.

Task 4B also owns a non-exported fresh-store preflight used after password or
SSO returns credentials but before any sync, key upload, or ordinary store
open. It hardwires exact `SqliteStore` and returns the same immutable
SqliteStore/pickle-key binding as configured adoption. Under the same exclusive
owner, generate one canonical native unshared `OlmAccount`, pickle it exactly
once, direct-insert it, and atomically create exact full `SqliteStore` v10
topology, its one StoreVersion/account row, and the five ingestion
tables/owner/source rows.
The borrowed store loads that committed account rather than generating a
second one. Before commit, “all absent” means no committed SQLite user objects
or rows; an owner-created zero-length database and unlocked `.ingest.lock` may
remain after process death and must be safely retryable. After commit, the graph
is complete and reopens through
the configured marked path. Cover the fresh SqliteStore branch,
no-account/no-second-account cases, and prove nio's private factory performs no
network, sync, key upload, public `load_store()`, or configured-store
construction. Task 4B characterizes nio's existing no-store/no-sync login
prerequisite; Task 5C owns the real temporary login client's construction,
HTTP lifecycle, and cleanup.

**Commit:** `refactor: transfer matrix store ownership to ingestion`

### Task 5A: MindRoom record receipt and semantic-idempotency kernel

**Complete.** Implemented and reviewed in MindRoom
`da6ac44e44f6ea65a131dbee1bfc7bdd63ff0fbc`.

This MindRoom slice executes before Task 4C. Replace the undeployed receipt
table's mandatory `event_id` foreign key with nonempty `record_id`; add no table
or migration. Every record gets a receipt. Return separate `receipt_new` and
`semantic_event_new` facts for fresh semantic events, later-batch matching event
identity, and exact receipt replay. Compare retained room/thread/kind/sender and
origin timestamp; conflict rolls back event, projection, receipt, and frontier.
Define literal transaction dispositions for event, lifecycle, history loss, and
compatibility-only records, and prove concurrent admission dispatches once.
Keep MindRoom's existing tenure epoch: a departure advances it; only a durably
admitted `LOCAL` transition into join clears the fence without advancing, and
process restart never re-arms a room by itself. Lifecycle input carries exact
previous and current memberships and epochs; disagreement rolls back the whole
admission.
nio epoch numbers are relative transition proofs, not MindRoom's absolute
counter. Lifecycle also names exact local/reported provenance. Only a `LOCAL`
join re-arms MindRoom; a `REPORTED` join never changes its fence. While fenced,
turn-backed/loss Work cannot recreate application state; a suppressed semantic
event retains only its settled immutable identity so it cannot resurrect.

**Commit:** `fix: deduplicate durable matrix events semantically`

### Task 4C: callback-free shared compatibility preparation

**Complete.** Implemented and reviewed in nio
`19587c4cf195e62d4265a03a70d061e407dceec4`.

Parse Classic and private durable Sliding Frames into one private preparation
contract. Preserve the pre-fork callback-free mutation order exactly: token;
to-device decrypt/mutation; invited/joined room state, timeline (including
Megolm), ephemeral, and account-data projection; presence/global projection;
expired-verification mutation; device-list/OTK/fallback handling; then key
request collection. Callback fanout is a later, separate phase. A same-Frame
room key must decrypt its later timeline event, and a same-response newly
projected encrypted room/member plus device-list change must queue that member
for key query. Preserve every own-membership transition in its source order,
including multiple transitions in one Frame; preparation must not collapse
them into only the final `RoomSnapshot` claim. Produce stable event IDs,
decrypted `clear_json` or explicit failure, `RoomSnapshot` values, the ordered
transition sequence, and fully authenticated retained-Frame inputs sufficient
for Task 4F to reconstruct completion after it has a claimed revision. Do not
create `_FrameCompletion` in this commit. Run no callbacks and expose no public
Sliding response API.
Ordinary public response callbacks remain solely on Desktop's pre-fork sync
path.

Create `tests/ingest/preparation_test.py` and cover equivalent Classic/Sliding
output, both causal crypto-order cases above, a same-Frame multi-transition
sequence, explicit failure, and zero callbacks.

**Commit:** `refactor: prepare durable frames without callbacks`

### Task 4D: atomic all-kind materialization and snapshots

**Complete.** The pure prepared planner is nio
`96ff99bf36135e7eef66355953a9c1b4c8c9b2e8`; owned all-kind materialization,
outbound freezing, local lifecycle publication, crash/capacity/parity proof,
and runner FIFO blocking are `eb77f5d0e1c17658ea322016ef04a16bac6e40e9`.

In one owner transaction persist crypto writes, clear events, stable IDs,
aggregates, room snapshots, every `RecordKind` including `TO_DEVICE`, every
`LossRecord`, Work, and preparation revision. In that same transaction, make
the retained Frame envelope the authenticated durable owner of one canonical,
ordered outbound-maintenance plan: exact key-upload/query/claim and queued
to-device request bodies, deterministic domain-separated per-message to-device
transaction IDs, canonical operation kind plus minimal typed local response-
apply context, and an explicit pending/settled state for every operation.
Persist the exact encrypted to-device bodies and the Olm/session mutations that
produced them together; do not reconstruct ciphertext from an in-memory queue.
Wire JSON alone is insufficient because dummy and room-key-request subtypes
have distinct local settlement effects after restart.
This is canonical Frame payload, not a new table or column. Retain the Frame as
completion owner. Any failure after in-memory Olm mutation rolls back SQLite,
poisons that session object, and requires clean reconstruction before replay.
Prove literal fates for all record kinds, canonical plan/header authentication,
identical body/transaction-ID replay, all statement crash boundaries,
empty/nonempty Frame retention, and equal Classic/Sliding durable graphs.
Emit every ordered membership transition rather than only its final claim;
advance the tenure epoch only for a joined-to-nonjoined departure.
Provide the same private durable transition path for MindRoom-local membership
changes so queued Work and lifecycle fences share one order. Give each logical
local transition one stable operation identity and make retries reuse it.

**Commit:** `refactor: materialize all durable record kinds atomically`

### Task 4E: snapshot restore and receipt-gated apply/ack

**Complete.** Implemented, independently reviewed, fully verified, and
committed in nio as `be24cf315c27250d3c2b95a5862278420ee8d270`. The exact
behavior, verification evidence, and restart boundary are recorded in the live
handoff at the top of this plan.

Restore `AsyncClient.rooms` before incremental delivery. Reapply client/room
state idempotently; publish auxiliary callbacks only when `receipt_new` and
semantic event callbacks only when `semantic_event_new`. Duplicate receipts run
no callback and proceed to ack. Callback error/cancellation leaves Work
replayable; after the committed receipt, replay suppresses callback and acks.
No callback runs in either database transaction. Add only the underscored
`session._settle_batch(batch, receipt_new=..., semantic_event_new=...)` method
to the non-exported owned-session protocol; do not add a non-underscored method
to the public `IngestionSession` contract. MindRoom obtains that protocol only
from `_open_owned_ingestion()`.

**Commit:** `feat: settle durable records through compatibility callbacks`

### Task 5B: MindRoom all-record adapter and pump

**Complete.** Implemented in MindRoom commit
`3a80947593bbf6fe508ca2e7499f19e951f416d3`. The live handoff above records the
exact verification, linked-checkout requirement, budget state, and next task.

Accept both origins and map every record/loss to Task 5A's disposition. Timeline
uses the existing classifier; history/context settles without a turn; lifecycle
uses membership/departure fences; loss uses room-history recovery; state,
ephemeral, account data, presence, and to-device are compatibility receipts.
Call `session._settle_batch()` with the two facts. Keep `max_records=1`; malformed
or unknown input never acks and no known record can block the FIFO forever.

**Commit:** `feat: consume every durable ingestion record`

### Task 4F: outbound maintenance, Frame completion, and coordinator progress

Only after Tasks 5A, 4E, and 5B exist, implement
`PREPARE -> HYDRATE_IF_REQUIRED -> WAIT_FOR_RECORD_ACK ->
MAINTAIN_OUTBOUND_CRYPTO -> CLAIM_AND_FANOUT_COMPLETION -> RETIRE ->
NEXT_SOURCE_HTTP`. After all Frame Work is acked but before claim/retirement,
run the callback-free upstream key upload, key query, key claim, and queued
to-device send maintenance in its exact persisted order. Replay only the exact
authenticated pending request body owned by the Frame. Parse each response
without callbacks, then apply its ordinary MatrixStore/Olm effects and mark that
operation settled in one owner transaction. That transaction also appends or
replaces any causally generated follow-up operation in the same authenticated
Frame plan before commit. Run a fixed loop to quiescence in upstream order;
notably key-claim may create a dummy to-device send, and successful
dummy/to-device settlement may create room-key re-requests. Every follow-up
stores exact body/ciphertext and a deterministic transaction ID before it can be
sent. Each also stores the authenticated semantic subtype/fields needed to
reproduce local dummy or room-key-request settlement after restart. Any
exception or rollback after an
in-memory crypto mutation poisons the owned session and requires reconstruction
before replay. Retryable network failure leaves the Frame retained. A queued
to-device resend uses the identical transaction ID and body, so the server
deduplicates the semantic send. Key upload/query replay the identical body.
`/keys/claim` has no Matrix idempotency key: response loss may consume another
remote one-time key on an identical retry, so do not claim remote at-most-once;
claim only locally atomic application of one successful response and no
duplicate downstream event/callback. Auth errors such as `M_UNKNOWN_TOKEN` or
`M_FORBIDDEN` retain the pending Frame/operation and enter the existing
permanent-auth shutdown/health path. Retryable HTTP/Matrix errors retain the
same body with bounded cancellable backoff. No error response settles an
operation or fans out silently. No next source poll can pass this state.

Define a private non-exported `_FrameCompletion` with authenticated
`frame_id`, transport, source epoch, request ID, staged revision, and claimed
revision, reconstructed only from a fully authenticated retained Frame/owner.
Task 4F extends the minimal non-exported `_OwnedIngestionSession` created by
Task 4B so it holds one
`Callable[[_FrameCompletion], Awaitable[None]] | None` sink installed only by
MindRoom's private 5C factory; no registration method is added to public
`IngestionSession` or `open_ingestion()`. Claim completion only after no Work
references the Frame and outbound maintenance succeeds. Persist the claim
before invoking the sink outside transactions. Sink raise, cancellation, or
process death leaves the Frame durably claimed; restart retires it without a
second invocation. Empty Frames use the same path. Hydration may run while the
next source sync is fenced. The same coordinator owns pending local membership
intents: a stable local intent enters this progress machine before the Matrix
operation, restart resumes it, and no source HTTP begins until its ordered
lifecycle Work has been admitted, acknowledged, and the intent retired. Ack,
local-intent publication, and close wake the runner without lost wakeups. Only
malformed, authentication, or conflict states are terminal BLOCKED.

Add causal tests for exact key upload/query/claim/to-device ordering; canonical
Frame-owned bodies and deterministic to-device transaction IDs; response loss
and restart at every operation; byte-identical to-device ciphertext/ID replay;
the explicit `/keys/claim` retry limit above; statement failure/rollback while
applying each maintenance response; maintenance-before-claim; authenticated
completion reconstruction; claim→dummy and dummy-success→key-rerequest chains
with rollback/poison/reopen at every append/replace boundary; sink
success/raise/cancel/crash after claim; per-operation retryable/auth error
responses and close/cancel during backoff; claimed
restart retirement; and absence of `_FrameCompletion`,
`_OwnedIngestionSession`, and the owned factory from public exports/signatures.

**Commit:** `feat: complete durable frames after record settlement`

### Tasks 5C and 5D: session factory, then sole MindRoom engine

5C wires fresh and restored login through the non-exported
`_open_owned_ingestion()` factory, exact per-device adoption/lease, client
attachment using the authenticated SqliteStore runtime binding, session claim,
and private completion sink before Olm/source work; poison rebuilds a new
client/Olm object. In the same 5C commit, replace Desktop pairing's raw
read-only fallback with the internal live-bot controller-identity resolver
before any bot can acquire the exclusive lease. 5D makes
`Bot.sync_forever()` use the durable
runner/pump as MindRoom's only candidate engine, while existing
`matrix_sync.mode` chooses only Classic or Sliding transport. Preserve health,
first-sync, invites/membership, presence, E2EE/to-device pairing, RTC,
permanent-auth error, cancellation, and close behavior. Add no environment
latch, control room, `probe`, YAML engine toggle, or public nio API. The 5C
resolver is injected by the command/orchestrator boundary. It
must resolve the selected target entity through the live bot registry and read
the exact user ID, device ID, and Ed25519 key from that bot's already-owned
client/Olm account; it never opens the store. Missing/not-running/mismatched
targets fail closed. The pre-5C raw read-only compatibility path is removed or
made unreachable in 5C before exclusive ownership exists. Cover setup and
confirm when the serving bot is
or is not the target, target restart/replacement, and absence of any second
MatrixStore/SQLite open under the exclusive lease.
Local departure/rejoin awaits the private durable lifecycle path with `LOCAL`
provenance; it does not mutate MindRoom outside nio's ordered Work FIFO.
Persist or derive its durable desired-state intent before the Matrix operation,
serialize it against source ingestion, and publish its Work before another
source request may start. Restart reconciles an unfinished intent; ordinary
startup never fabricates a new transition for an already-recorded membership.

For password/SSO credentials, prove the temporary no-store/no-sync client's
HTTP session closes on success, login error, owned-factory failure, and
cancellation before exclusive construction. The appservice direct-credential
path creates no temporary MatrixStore or client before the owned factory.
Bot owns the ingestion session separately from its existing journal handle.
Shutdown/config reload joins runner and pump, revokes client/store, closes the
owned session to release the exclusive lease, then closes HTTP; every cleanup
lane runs despite another's error, and replacement registration/start waits for
confirmed lease release. Cover startup failure and bot replacement/reload.

**Commits:** `feat: open durable matrix sessions in place`; then
`feat: activate durable matrix ingestion`

**Production files**

- `src/nio/client/async_client.py` and `base_client.py` for a reusable no-recovery response-preparation boundary
- `src/nio/responses.py` for private durable Sliding parsing without a public AsyncClient Sliding response API
- `src/nio/ingest/reducer.py`
- `src/nio/ingest/model.py` and `serialization.py`
- `src/nio/ingest/coordinator.py`
- `src/nio/store/sync_journal.py` and `_ingestion_store_owner.py`
- `src/nio/store/_sync_journal_preflight.py`
- `src/nio/store/sync_journal_schema.py` and `_sync_journal_rows.py`
- `src/nio/store/_sync_journal.py`
- `src/nio/store/_sync_journal_port.py`
- `src/nio/store/_sync_journal_plan.py`
- transport adapters only where normalization lacks parity data

**Tests**

- `tests/ingest/reducer_test.py`
- `tests/ingest/materializer_test.py`
- shared Classic/Sliding response fixtures

Build a differential behavior matrix covering at least:

- empty sync;
- room hydration and ordinary timeline events;
- invite, join, knock, leave, and membership transitions;
- encryption state and encrypted events;
- to-device, device-list, fallback-key, account-data, presence, and ephemeral traffic;
- limited history, gaps, eviction, and re-entry;
- malformed/conflicting records and repeated BLOCKED behavior;
- callbacks and response-state projection.
- same-Frame Olm room-key arrival followed by Megolm timeline decryption;
- rollback after each crypto/store/Work/marker statement, session poisoning, and clean reopen;
- Frame completion and one private `_FrameCompletion` hook for empty and nonempty Frames;
- `AsyncClient.rooms` restoration from persisted snapshots before incremental response delivery.

For equivalent normalized input, both transports must produce the same record fates and durable graph. A transport adapter may normalize wire differences; it may not choose a different fate engine.

Add one frame-scope compatibility preparation phase before Work becomes application-visible. The MindRoom AsyncClient must adopt the ingestion-owned MatrixStore so ordinary E2EE rows and the five ingestion tables share one SQLite owner/transaction. Preserve the exact pre-fork callback-free mutation order: token; to-device; invited/joined room state, timeline, ephemeral, and account data; presence/global state; expired verifications; device-list/key-count/fallback controls; then key-request collection. This ordering must causally prove both same-Frame room-key decryption and same-response encrypted-room membership before device-list handling; callback fanout remains separate. Persist stable event IDs, decrypted `clear_json`, `RoomSnapshot`, every `RecordKind` including `TO_DEVICE`, aggregates/Work, and the preparation revision atomically. On rollback after Olm memory mutation, poison/close the session and reconstruct it from the rolled-back database; never retry the same object.

Implement ownership through the non-exported owned-session factory, not two independent opens and not a public state-machine reversal. The factory privately creates the borrowed `SqliteStore` from the bootstrap's exclusive owner, consumes the immutable runtime-class binding established by the private configured opener, accepts only that object and authenticated topology, transfers journal/store to the minimal `_OwnedIngestionSession`, and revokes/closes them once; Task 4F later extends that private session. Existing public `open_ingestion()` remains session-first; direct public store-first continues to reject a later public session. Add causal lifecycle and public signature/export tests for successful transfer, cancellation, close ordering, and clean reopen. Prove `replace_rotated_device_keys=True` rolls back `DeviceKeys` and `DeviceTrustState` together, and prove durable preparation/maintenance performs no legacy trust-sidecar I/O. Do not weaken the single-process/thread/file-identity/writer-epoch fences. For a true new device, after credentials are obtained without opening a store, the private fresh-store preflight atomically creates exact full `SqliteStore` v10 topology, one canonical unshared Olm account row, and the five ingestion tables/initial rows under one exclusive owner before constructing the borrowed store. Killed precommit attempts leave no SQLite objects or rows and are retryable; committed state is complete and loads the same account. Task 5C owns the temporary login client's HTTP lifecycle.

Before that lease exists, add a non-exported exclusive configured opener for the populated MindRoom per-device database. Its caller must supply the exact source class, and it authenticates exact account/device/store-v10 ownership plus topology against that class. A supplied-Default database may lack `DeviceTrustState` or contain its exact ordinary topology and legacy rows because historical v2 upgrade and a pre-fix Desktop identity reader could create it while sidecars remained authoritative; malformed topology rejects, and presence/content never infers source class. A SqliteStore source requires the table and is adopted without class conversion. A DefaultStore source is converted in that same transaction by creating the table when absent or deleting/replacing its inert rows when present, then copying effective current sidecar facts for exact `DeviceKeys` identities using existing DefaultStore parsing and verified→blacklisted→ignored priority; unmatched stale entries stay inert, and all three source sidecars remain byte-for-byte unchanged. Return immutable `SqliteStore` runtime binding on `StoreBootstrap` for private Task-4B use, but persist no ingestion class marker and leave the public `open_ingestion_store()` signature and populated-unmarked rejection unchanged. The same transaction adds the five v1 ingestion tables/initial rows and otherwise leaves every existing Olm account pickle, Megolm/session/key row, sync token, and inert historical recovery table byte-identical. Every injected statement failure reopens with the exact prior ordinary shape and no ingestion marker; success reopens as complete SqliteStore plus exactly one marker/source. On later marked process reopen, require `SqliteStore`, reauthenticate the entire ordinary and ingestion state before writer/source DML, and return a new binding without adoption or sidecar reads. Reject mixed/partial topology, source-class/topology mismatch, wrong identity/pickle, unsupported store version, a simultaneously open client/store, or a concurrent/re-entrant adoption without mutation. Do not create a parallel ingestion database for the same device.

Keep the Frame after materialization as the completion owner. Add only one authenticated nullable `callbacks_claimed_revision` column to `NioIngestFrame` while retaining schema version 1 and exactly five tables. One-record receipts suppress record callback replay; when the Frame has no Work left, finish ordered outbound crypto maintenance, claim its private transport-neutral `_FrameCompletion` sink before best-effort fanout, then retire it. Empty Frames use the same path. Persist/restore the existing `RoomSnapshot` carrier before incremental delivery. No callback runs in either database transaction.

Replace the current “any remaining Frame is blocked” coordinator branch with the explicit private progress machine bound in Task 4F, including `MAINTAIN_OUTBOUND_CRYPTO`. A prepared retained Frame yields/wakes the concurrent MindRoom pump instead of raising; first-room HELD Work can complete its hydration HTTP; the next source-sync HTTP remains fenced until every record receipt/ack, outbound maintenance, private completion claim, and retirement settles. Only malformed/auth/conflict states are `BLOCKED`. Add causal concurrent runner+pump tests for empty and nonempty Classic/Sliding Frames and first-room hydration, proving the second source poll occurs only after completion and leaves zero Frame/Work; add crash, cancellation, close, and lost-wakeup coverage. Do not add a progress table.

A retained Frame is retry state, not a successful terminal fate. For both transports, successful encrypted, to-device, device-list, OTK, fallback-key, state, account-data, presence, ephemeral, lifecycle, and loss responses must end with zero permanently retained Frames and zero unacknowledgeable Work. Each known input must resolve to exactly one explicit owner: MindRoom event/lifecycle/loss Work or compatibility-owned MatrixStore/callback state. Malformed input remains durably blocked and visible.

Task 1 must already have removed every dedicated diagnostic entrypoint. Preserve every record-fate invariant: successful input has Work or a committed compatibility owner; only malformed, failed, or in-progress input may retain its Frame—never silent discard.

Run both full adapter suites plus the shared reducer/materializer suite and statics.

The preceding cross-cutting matrix governs Tasks 4C through 4F. Each subtask runs its
focused tests and statics before commit; after 4F, run both full adapter suites,
the shared reducer/materializer/coordinator/delivery/crash suites, and all nio
statics.

## Task 5: Complete the interleaved MindRoom slices

Work in `/work/dev/mindroom` with test-driven development.

Task 5A executes after Task 4B; Task 5B after Task 4E; Tasks 5C and 5D after
Task 4F, as specified above. The following files and acceptance matrix apply
across all four MindRoom slices; they are not authority to reorder them.

**Production files**

- `src/mindroom/event_journal/journal.py`
- `src/mindroom/event_journal/schema.py`
- `src/mindroom/event_journal/models.py`
- `src/mindroom/event_journal/store.py`
- `src/mindroom/event_journal/views.py`
- `src/mindroom/bot.py`
- `src/mindroom/matrix/durable_ingestion.py`
- `src/mindroom/matrix/sync_loop.py`
- `src/mindroom/matrix/journal_ingress.py`
- `src/mindroom/matrix/client_session.py`
- `src/mindroom/matrix/users.py`
- `src/mindroom/commands/handler.py`
- `src/mindroom/commands/desktop_commands.py`
- `src/mindroom/desktop/identity.py`
- `src/mindroom/orchestrator.py` only for the internal live controller-identity resolver
- the appservice login helper if it independently constructs/restores agent clients
- the existing sync-certification/checkpoint modules only to remove old-engine dependencies

**Tests**

- `tests/test_durable_ingestion_admission.py`
- `tests/test_event_journal_crash_matrix.py`
- configuration/runner tests that already cover Matrix mode selection

Write causal failures for:

1. Exact batch sequence/digest replay remains `DUPLICATE`.
2. The same existing `(principal_id, event_id)` in a later Frame/batch, with matching retained room/thread/kind/sender/origin-timestamp headers, advances the new batch receipt/frontier but creates no second application dispatch.
3. The same event ID with a conflicting retained header rolls back the event, projection, receipt, and frontier transaction.
4. Concurrent attempts cannot dispatch twice.
5. Classic behavior is unchanged.
6. Existing `matrix_sync.mode` selects Classic or Sliding for the durable runner without a control room, `probe`, or one-message production scope.
7. `Bot.sync_forever()` uses the durable runner as the candidate's only MindRoom sync engine; `matrix_sync.mode` selects transport, not engine. Activation is deployment of the reviewed candidate cohort, not a canary environment variable or a new YAML option.
8. Every nio `RecordKind` and `LossRecord` has one literal disposition: semantic timeline admission, room-lifecycle fencing, room-history loss obligation, or compatibility-owned state/callback settlement. No known record can occupy the FIFO without an acknowledgement path.
9. Invite/membership, E2EE/to-device, RTC/pairing, response health, first-sync, presence, permanent-auth-error, cancellation, and close behavior remain available through the durable runner and compatibility owner.
10. Restored-login and fresh-login paths locate/adopt the existing per-device MatrixStore before Olm initialization; its account pickle, inbound/outbound sessions, device keys, and sync-token rows survive adoption and restart byte-for-byte.

Keep `max_records=1`. Reuse the existing `journal_events` unique identity, immutable clear columns, and admission transaction. Correct the unmerged `matrix_ingestion_receipts` definition by replacing its mandatory `event_id` foreign key with nonempty `record_id`. Every EventRecord/LossRecord batch receives a receipt; semantic Matrix events independently bind `journal_events`. Extend `IngestionBatchAdmission` with `record_id`, optional membership/event/projection fields, and return separate `receipt_new`/`semantic_event_new` facts so callbacks and application dispatch are gated correctly. This is an in-PR schema-definition correction, not a deployed migration. Do not add a table, event-identity database to nio, retain raw settled payload for a second digest, or add a canary-only receipt path.

Run focused RED/GREEN, full journal/admission/crash suites, statics, and diff checks.

Use the four commit subjects bound in Tasks 5A through 5D above. Run each focused
RED/GREEN gate before its commit, then the full MindRoom suite and statics after
5D.

## Task 6: Restore the pre-fork public sync path and verify compatibility

Use `69aa99c51f354980bf5306a85b4c7b7d71ff3217` as the exact local upstream boundary. Before observation, remove every normal import, export, registration, and call from `sync()`, `sync_forever()`, response handling, and store opening into fork-only recovery/checkpoint machinery while leaving those source modules present for the zero-use gate. Restore the smallest boundary implementation of request, response application, room projection, token persistence, and callback dispatch. Do not route desktop through the durable batch engine and do not replace unrelated later E2EE/cross-signing/security work with a whole-file checkout.

Use test-driven development against behavior, not deleted helper topology. The retained upstream public contract—not fork-specific recovered/unrecovered response fields or checkpoint outcomes—is authoritative.

Create a retained compatibility matrix for:

- `sync()`, `sync_forever()`, `stop_sync_forever()`, `receive_response()`, `run_response_callbacks()`, `add_response_callback()`, and `close()` signatures and return behavior;
- `add_event_callback()`, `add_ephemeral_callback()`, `add_global_account_data_callback()`, `add_room_account_data_callback()`, `add_to_device_callback()`, and `add_presence_callback()` signatures, ordering, exceptions, and re-entry;
- `AsyncClient.rooms` and store projection timing;
- `synced` set/clear behavior, first-sync-before-auxiliary ordering, callback exception propagation, cancellation, and stop-flag reset;
- desktop's unchanged `sync_forever()` plus to-device callback path;
- ordinary sends and error responses.

Explicitly remove fork-added `AsyncClient.sliding_sync()`, `sliding_sync_forever()`, `SlidingSyncResponse` types, recovery/checkpoint/admission methods, recovery response fields, and their top-level exports from the public compatibility contract. Durable Sliding remains under `nio.ingest`, not `AsyncClient`.

Before removing those nio symbols, require a fail-closed MindRoom source scan proving no production reference remains to `add_event_admission_callback`, `SlidingSyncResponse`, `sliding_sync_forever`, Classic checkpoint acknowledgement/reset methods, recovered/unrecovered response fields, or recovery/token exports. This gate covers `bot.py`, `matrix/sync_loop.py`, `matrix/journal_ingress.py`, `matrix/client_session.py`, and sync certification/checkpoint helpers.

For MindRoom, run the same application-behavior fixtures through the durable runner with Classic and Sliding transport. There is no old-engine/durable-engine product toggle in the candidate. For desktop, compare the retained pre-fork public path before and after fork-recovery source deletion. Differences require either a bug fix or an explicit design amendment; do not bless them as internal changes.

Do not destructively downgrade an existing Matrix store or drop historical recovery tables. Fresh stores create only ordinary active tables; existing higher-version databases may retain inert historical tables, but ordinary open/sync/token/callback operations must neither read nor write recovery state.

For observation, run the candidate under external Python coverage and require zero executed lines in the named fork-recovery modules and the recovery/checkpoint regions of `async_client.py` and `database.py`. Do not add counters, persistence, or evidence hooks to production.

**Commit:** `test: bind durable sync compatibility`

## Task 7: Run cross-database crash and parity verification

Use real nio and MindRoom components. Cover one event through:

```text
HTTP response -> Frame -> shared materializer -> Work -> batch
-> MindRoom event/projection/receipt/frontier -> nio ack
```

Inject deterministic crashes before and after response staging, materialization, batch freeze, MindRoom admission, and nio ack in this test harness. Include Sliding reopen and exact `M_UNKNOWN_POS` after admission-before-ack. This is the exact boundary proof; the live canary does not duplicate the hook.

Assertions:

- response/cursor atomicity;
- deterministic batch replay;
- exactly one application dispatch;
- conflicting identity/digest is an integrity failure;
- no Work or Frame loss;
- frontier advances only with its owning transaction;
- public callbacks remain outside database transactions.

Run complete affected nio and MindRoom suites, Ruff, type checks, Black, compilation, diff checks, and suppression inventories.

No production line may exist solely to expose canary evidence.

## Task 8: Run external Classic and Sliding canaries

Build exact wheels from the candidate commits in a fresh isolated environment. Use an explicitly pinned Synapse and ordinary production configuration. The operator may observe request/response sequence and existing Source/Frame/Work/batch/event-journal/frontier/application outcomes; it may not require product diagnostic tables, a control room, or a `probe` witness.

Classic must remain green on schema v1. If any post-prune schema change appears, stop and rerun Classic before using the earlier PASS.

Sliding must demonstrate:

- normal sync and one visible application result;
- an ordinary process kill/restart while a durable batch is outstanding; the exact admission-before-ack boundary is already proved by Task 7 and no production latch is reintroduced;
- restart with a fresh connection-scoped position;
- replay without a second application effect;
- exact current-position unknown-pos reset;
- two bounded idle observations without lost Work or duplicate dispatch.

Materialize a concise allowlisted evidence record and independently review it. A canary PASS authorizes activation observation, not release or legacy deletion.

Because Task 4 changes the schema-v1 Frame layout and E2EE ownership after the earlier Classic qualification, rebuild and rerun the full Classic canary against this exact candidate before accepting any Sliding result. The old Classic PASS cannot be rebound to the new column or store ownership.

## Task 9: Activate, observe, and only then delete fork recovery

Activation remains inside the linked change set. The candidate's `Bot.sync_forever()` already owns the durable runner, and deployment selects the observation cohort; no product opt-in or canary-agent environment variable is introduced. The following thresholds are fixed:

1. Deploy the reviewed candidate to the MindRoom observation cohort.
2. Observe for one uninterrupted 24-hour interval spanning 1,000 successful source polls and three recorded clean process restarts. Require zero duplicate visible effects, zero unexplained event loss, no permanently growing Frame/Work backlog, and external coverage showing zero fork-recovery execution.
3. Run desktop on the restored upstream-style `sync_forever()` for at least two hours and 100 successful responses, including one process restart and one real to-device callback. Require no callback/order/authentication failure and external coverage showing zero fork-recovery execution.
4. Preserve exact run start/end times, candidate hashes, counts, coverage data, and failure totals in the final report.

If either observation fails, do not delete legacy source. Fix the durable path or restore Task 6's old-engine imports/calls inside the linked branches, then rerun all affected verification and the full observation interval; there is no hidden runtime fallback in the candidate.

Only after both observations pass, remove:

- `src/nio/client/sync_recovery.py`;
- `src/nio/client/sync_reset_fence.py`;
- `src/nio/client/sync_response_ordering.py`;
- `src/nio/client/sliding_membership.py`;
- `src/nio/recovery_abandonment.py`;
- `src/nio/sliding_sync_tokens.py` where no longer required;
- recovery/checkpoint code in `async_client.py`, `database.py`, `responses.py`, `base_client.py`, and `store/models.py`;
- implementation-detail tests for those internals.

Retain the upstream public sync/callback implementation and port every still-relevant parity test before deletion. Verify desktop and bot again after deletion; desktop never changes ingestion engine.

**Commit:** `refactor: remove superseded sync recovery machinery`

## Task 10: Final consolidation and linked-PR handoff

Run:

- the complete nio test suite;
- the complete affected MindRoom test suite;
- Classic and Sliding compatibility matrices;
- cross-database crash matrix;
- desktop smoke/soak;
- Ruff, type checking, Black, compilation, diff checks, and suppression scans;
- independent code/evidence review.

Measure net logical lines against `origin/main@6ed2b98`:

```text
runtime src      <= +4,500
tests            <= +15,000
docs and scripts <= +1,000
```

If any limit is exceeded, simplify or delete before handoff. Do not move the baseline or limit.

Also measure MindRoom against `0acaea2baf05a4c41cce7497cfcdf4880afc6d04`: runtime ≤ +350 and tests ≤ +1,000, and require it to be net smaller than `925df7f3cbcc39d4904140efd9d16b93c77238ea`.

Use this fail-closed gate from clean tracked worktrees; unrelated untracked user files are reported but are not part of either diff:

```bash
set -euo pipefail
test "$(git rev-parse 6ed2b9817d2bc9de30dc72942f9cb867d829283b^{commit})" = \
  6ed2b9817d2bc9de30dc72942f9cb867d829283b
test -z "$(git status --porcelain --untracked-files=no)"
net_lines() {
  git diff --numstat 6ed2b9817d2bc9de30dc72942f9cb867d829283b -- "$@" |
    awk '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ { n += $1 - $2 } END { print n + 0 }'
}
test "$(net_lines src)" -le 4500
test "$(net_lines tests)" -le 15000
test "$(net_lines docs scripts)" -le 1000

cd /work/dev/mindroom
test "$(git rev-parse 0acaea2baf05a4c41cce7497cfcdf4880afc6d04^{commit})" = \
  0acaea2baf05a4c41cce7497cfcdf4880afc6d04
test -z "$(git status --porcelain --untracked-files=no)"
mr_net() {
  git diff --numstat 0acaea2baf05a4c41cce7497cfcdf4880afc6d04 -- "$1" |
    awk '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ { n += $1 - $2 } END { print n + 0 }'
}
test "$(mr_net src)" -le 350
test "$(mr_net tests)" -le 1000
test "$(git diff --numstat 925df7f3cbcc39d4904140efd9d16b93c77238ea -- src tests |
  awk '$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ { n += $1 - $2 } END { print n + 0 }')" -lt 0
```

The final PR report must distinguish:

- upstream public compatibility retained;
- fork recovery implementation deleted;
- schema v1 and five-table topology;
- Classic and Sliding parity evidence;
- bot and desktop observation windows;
- exact commits, wheels, tests, canary evidence, and budgets;
- remaining limitations and rollback procedure.

Push only ordinary commits to the existing feature branches. Create one linked PR per repository and present them as one accept/reject decision; do not split either into phase PRs. No amend, rebase, force push, merge, release, or cutover claim is implied by completing this plan.
