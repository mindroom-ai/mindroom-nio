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

## Live execution status and restart handoff — 2026-08-14

This section is the authoritative restart point. Read it together with the
controlling design and the remaining task descriptions below. If conversation
state is lost, do not infer progress from unchecked prose elsewhere in the
plan: verify this checkpoint first, then continue from **Immediate next
action**.

### Repository and commit boundary

| Repository | Branch | Required HEAD | State |
| --- | --- | --- | --- |
| `/work/dev/mindroom-nio` | `docs/durable-ingestion-rewrite-plan` | `59bbb3b05c993488d1e81a35e6b743658ac4c87b` | Task 4F in progress at the exact dirty checkpoint below; preserve it |
| `/work/dev/mindroom` | `wip/matrix-journal-ingress-cutover` | `3a8094759d23b3740119893419cd7eff8b0c785b` | Task 5B committed; no tracked changes expected |

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
| Task 5B MindRoom all-record adapter/pump | Complete | MindRoom `3a8094759d23b3740119893419cd7eff8b0c785b`; owning 319-test file, full repository suite, and all pre-commit hooks GREEN |
| Task 4F coordinator progress/completion | In progress | All implementation/checklist slices, including the complete durable local-membership matrix, are GREEN at dirty source/test patch `1ca31931...`; final whole-repository gate and commit-readiness audit are next |
| Tasks 5C, 5D, 6–10 | Not started | Follow the dependency order in this plan |

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
`3a8094759d23b3740119893419cd7eff8b0c785b`. Its implementation commit contains
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

Immediate next: continue Task 4F in `/work/dev/mindroom-nio` from the live
restart handoff above. The next causal slice is local leave/rejoin plus
duplicate/ordering behavior; the carrier, pre-HTTP restart, successful local
publication, and Work-ack/source fence are already GREEN. Task 5B deliberately
does not wire the pump into `bot.py` or `orchestrator.py`; Tasks 5C/5D own that
activation after Task 4F exists.

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

### Immediate next action

A fresh session can be started with: “Read the controlling design and the live
restart handoff in this plan, verify the recorded branches and dirty hash,
preserve unrelated untracked files, and continue Task4F from the first unchecked
item.”

Task4F is the current slice. Continue it in this order:

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
- [ ] Add local-intent HTTP success/error/restart, duplicate/reconcile,
      Work-ack/retirement, rollback/post-COMMIT, cancellation/close, and
      source-poll fencing slices one causal RED at a time.

Current Task4F dirty checkpoint starts from clean nio HEAD
`59bbb3b05c993488d1e81a35e6b743658ac4c87b` and contains exactly:

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

The exact binary source/test patch has SHA-256
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
Immediate next: run the complete nio suite and final Task4F static/budget/diff
audit, resolve any failure, then update this handoff and create the ordinary
Task4F commit only if every gate is GREEN.

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
  safety, correctness, or scope blocker.

### Restart verification commands

Run these before editing after a new session:

```bash
cd /work/dev/mindroom-nio
test "$(git branch --show-current)" = docs/durable-ingestion-rewrite-plan
test "$(git rev-parse HEAD)" = 59bbb3b05c993488d1e81a35e6b743658ac4c87b
test "$(git diff --name-only)" = "$(printf '%s\n' \
  docs/superpowers/plans/2026-08-13-durable-ingestion-lean-replacement.md \
  src/nio/ingest/coordinator.py \
  src/nio/store/_sync_journal.py \
  src/nio/store/_sync_journal_preflight.py \
  src/nio/store/_sync_journal_rows.py \
  src/nio/store/_sync_journal_values.py \
  src/nio/store/sync_journal_schema.py \
  tests/ingest/coordinator_test.py \
  tests/ingest/source_journal_test.py)"
test "$(git diff --binary -- src tests | sha256sum | cut -d' ' -f1)" = \
  1ca31931088369a02520a28e1d61280592bef3940531ba5dbc88d6403e7eb1bc
git status --short
git diff --check
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
test "$(git rev-parse HEAD)" = 3a8094759d23b3740119893419cd7eff8b0c785b
test -z "$(git status --short --untracked-files=no)"
git status --short
uv pip install --python .venv/bin/python --no-deps -e /work/dev/mindroom-nio
.venv/bin/python -m pytest -q -n 0 \
  tests/test_durable_ingestion_admission.py
# expected: 319 passed
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
`3a8094759d23b3740119893419cd7eff8b0c785b`. The live handoff above records the
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
