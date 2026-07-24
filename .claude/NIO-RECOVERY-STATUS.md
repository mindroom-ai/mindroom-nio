# mindroom-nio PR 20 Recovery Status

## Current Head

Branch `fix/limited-sync-recovery-loss-v2` is published as PR #20.
The last published local, remote, and PR head is `af585b42c590dd7ef9e8bda54bc0647e9c055a3f`.
That head is rejected and must not be merged.
The worktree currently contains the tested recovery-transaction redesign that will become the next candidate commit.
Record the resulting commit SHA here and in the external gate ledger immediately after commit and push.

## Accepted Findings Resolved in the Working Candidate

- A complete non-limited response fetched from the preserved checkpoint now clears a sticky gap, while a response already in flight from a speculative cursor cannot certify it.
- Each room owns its limited-walk completion, so an incomplete room no longer blocks callback delivery for a complete room and the global checkpoint still waits for every limited room.
- A since-less first-response callback failure now restores `next_batch` to `None`, and a retry delivers untouched later events without repeating terminal fan-out.
- Restart loading now initializes the in-memory callback checkpoint from the stored checkpoint, allowing a complete non-limited restart retry to close the durable sticky gap.
- A bounded data prefix is withheld until a membership-only continuation proves that no later own-join boundary exists.
- A membership proof that is absent from a later data page remains uncertified instead of losing post-join events.
- To-device, invite, ephemeral, room-account-data, global-account-data, and presence callbacks now use interval-scoped durable replay identities.
- To-device replay identity is captured before decryption so a decrypt-once replay cannot change its durable identity.
- Interval-scoped callback identities are cleared after checkpoint certification, so an identical legitimate event in a later response is delivered again.
- File-backed store work runs off the event loop under the recovery deadline, while `SqliteMemoryStore` writes stay on their owning connection.
- The v4 `gap_pending` migration is restart-safe after the column add and before the version update.
- Attempt-all fanout remains limited to recovery-enabled clients, and disabled mode keeps upstream short-circuit behavior.
- Ordinary non-limited sync callbacks are not subjected to the backfill deadline.

## Test Gaps Closed in the Working Candidate

- The public sliding-sync call path now verifies the 512-event memory bound.
- The v2-to-v3 migration is exercised directly before unconditional final table creation can mask it.
- Event-journal and checkpoint write failures are injected deterministically and prove the response remains retryable.
- The opposite cross-cache encrypted-state merge order has a mutation-killing regression.
- A direct cross-process live-timeline callback-error regression uses one aggregate callback ledger across both processes.
- Ancillary callback replay is covered across process restart.
- More than 512 durable callback IDs, multiple limited rooms, speculative responses, missing membership boundaries, and in-memory SQLite ownership are covered.

## Fixed or Rejected Stale Claims

- The old overlap-only early return is fixed because the overlap journal is persisted before the empty-recovery return.
- Silent backward-`to` ignore is fixed by forward recovery from the safe `from` token.
- The decrypted-replay event-cap bypass is fixed.
- In-session bounded retry, room rotation, per-event crash-prefix durability, callback attempt-all, sliding decrypt persistence, and forward documentation are present.
- The stale checkpoint-pinning mechanism caused by always requesting from an advancing `next_batch` is gone.
- The specific stale reentrant-token claim is fixed because request provenance is consumed before duplicate handling and sticky recovery chooses the safe checkpoint.

## Remaining Risks and Blind Spots

- A hard kill between an external callback returning and its one-event journal commit can replay that active event because those systems cannot share a transaction.
- A permanently uncloseable gap can retain a growing durable callback journal because no safe eviction exists while the persisted checkpoint can still replay those events.
- Queue-backed custom SQLite stores cannot make multiple queued SQL statements one transaction, although built-in file-backed stores are atomic and the active callback crash window remains one event.
- Real Tuwunel evidence is stale and cannot gate this working candidate.

## Validation at Rejected Head

- Focused `tests/backfill_test.py tests/store_test.py`: `95 passed in 9.18s`.
- Full suite: `545 passed, 3 skipped, 2 warnings in 55.64s`.
- All-file pre-commit: passed.
- Stale-review evidence subset: `12 passed in 1.03s`.
- Independent exact-head subset: `7 passed in 0.89s`.
- Fresh Codex reviews returned changes required.
- Fresh Fable approval is absent.
- No exact-head real-Tuwunel gate is current.

## Working Candidate Validation

- Focused `tests/backfill_test.py tests/store_test.py`: `112 passed in 5.17s`.
- Full suite: `561 passed, 3 skipped, 2 warnings in 34.76s`.
- All-file pre-commit: passed.
- The full suite's known mutation of `tests/data/encryption/example_DEVICEID.db` was restored and no other test artifact was reset.
- Git author is `Bas Nijholt <bas@nijho.lt>`.

## Live Evidence

Earlier real-Tuwunel campaigns predate `af585b42` and are not gating evidence.
No new live campaign may run until the recovery correctness blockers are fixed.
The next live gate must record exact nio module path, version, Git SHA, Tuwunel provenance, operation counts, retries, restarts, missing and duplicate counts, stalls, and runtime.

## Redesign Contract

One sync response is a recoverable delivery transaction.
Non-limited timelines returned from the preserved safe cursor are complete and may close sticky recovery.
Each limited room reports completion independently while global checkpoint advancement waits for every limited room.
Every callback surface replayed by a safe-cursor retry has sequence-safe suppression.
Own-join membership is established before any bounded data prefix is dispatched.
File-backed store work is bounded and off-loop, while in-memory SQLite work stays on its owning connection.
Migrations and store-failure paths are restart-safe.
Flag-disabled behavior remains identical to upstream.
Replay suppression for event types without Matrix event IDs lives only for the uncertified response interval.

## Next Steps

1. Commit and push this tested candidate without amend or force.
2. Record the exact pushed SHA in this handoff and the external gate ledger.
3. Run a current-context review against the exact pushed head and fix every valid blocker.
4. Freeze the corrected candidate and obtain fresh Codex approval, fresh Claude Fable approval, and real-Tuwunel PASS on the same exact head.
5. Resume MindRoom PR #1640 only after nio PR #20 passes those gates.
6. Remove this handoff only immediately before merge, then revalidate the final documentation-only removal head.
