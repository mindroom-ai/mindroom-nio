# Batch Recovery Commits Design

## Goal

Reduce SQLite write amplification during durable limited-timeline recovery without weakening the existing single-event admission contract.

The optimization must remain opt-in because grouping completion commits widens the crash replay window for ordinary callbacks.

## Evidence

The existing 100-stream sustained-capacity workload completed in 85.974 seconds after the `secure_delete=FAST` change.

Instrumenting that existing workload counted 6,126 `accept_recovery_event` commits and 6,286 `finish_recovery` commits.

Those calls consumed 31.954 seconds and 37.139 seconds respectively.

The next useful target is therefore transaction count rather than SQL query construction or encryption.

## Rejected Shortcut

An admission-accepted row cannot be converted directly into a completed marker.

The accepted row intentionally survives a crash between durable admission and ordinary callback fanout.

After restart, nio skips the already durable admission callback but replays the still-owed ordinary callback.

Treating acceptance as completion would lose that callback after a crash.

## Public API

Add an optional `AsyncClient.add_event_batch_admission_callback` API alongside the existing single-event API.

Only one admission owner may be registered, so registering both forms is an error.

The callback receives an ordered tuple of `TimelineAdmissionEntry` values containing the prepared room, event, and provenance.

The callback returns one `TimelineAdmissionDisposition` per entry so an application can preserve an exact per-event handoff to ordinary callbacks.

The dispositions describe application semantics and do not suppress nio room-state updates or ordinary callback fanout.

The existing `add_event_admission_callback` path and its event-by-event durability remain unchanged.

## Recovery Flow

When a batch callback is registered, the recovery pump selects the contiguous timeline prefix of one room-local recovery gap.

Parsing, room-state application, decryption, admission, and ordinary fanout retain event order.

nio invokes the application batch admission callback before any ordinary callback in that batch.

After application admission succeeds, nio records every previously unaccepted recovery row in one SQLite transaction.

nio then fans the prepared events out in order.

After fanout succeeds, nio replaces the completed rows with bounded completion markers in one SQLite transaction.

Ephemeral events, room account data, corrupt rows, and clients without a batch callback continue through the existing individual path.

Already accepted rows skip application admission after restart but still participate in ordered ordinary fanout and grouped completion.

## Failure And Crash Semantics

If batch admission refuses the batch or raises before acceptance, nio leaves the entire selected batch pending and runs no ordinary callback.

If nio cannot commit the acceptance markers, nio leaves its in-memory rows unaccepted and runs no ordinary callback.

If an ordinary callback fails, nio commits the completed prefix according to the existing live-versus-history acknowledgement rules and leaves later obligations pending.

A crash after application admission but before nio acceptance can repeat batch admission, which is the same unavoidable boundary that already exists for one event.

A crash during fanout can replay ordinary callbacks whose grouped completion transaction did not commit.

Batch admission is therefore appropriate for durable, event-ID-idempotent consumers such as MindRoom's journal, while the existing single-event API remains available for consumers requiring the narrower replay window.

## Storage Design

Add atomic `accept_recovery_events` and `finish_recovery_events` store operations.

Each operation preserves the single-event path's crashed-iteration behavior by matching a pending event independently of its stale generation and tolerating a row that another iteration already cleared.

The batch transaction still rolls back every mutation when an actual database operation fails.

The batch completion operation preserves encryption status, provenance, synthetic-event deletion, and the existing 512-marker per-room bound.

The single-event operations remain the source of behavior for the non-batch path.

Shared internal helpers should keep the single and batch SQL semantics aligned without adding a transaction coordinator, timer, background worker, or new dependency.

## Testing

Store tests will prove atomic acceptance, atomic completion, rollback for a missing key, encryption and provenance preservation, synthetic-row deletion, and marker pruning.

Recovery tests will prove that admission commits before fanout, recovered order is unchanged, rejection retries the whole batch, accepted restart rows skip re-admission, partial callback failures settle only the correct prefix, and simultaneous admission owners are rejected.

Existing single-event recovery tests must continue to pass unchanged.

The existing fuzz and stress scripts will provide live validation.

The 100-stream workload will be profiled with the same commit counters used for the baseline, and the 200-stream workload will be the final capacity comparison.

## Success Criteria

The full nio test suite and repository checks pass.

The 100-stream profile shows acceptance and completion commit counts grouped by recovery batch rather than by event.

The 200-stream stress result improves materially or, if environmental variance obscures elapsed time, shows materially lower SQLite commit and filesystem-sync counts without a correctness regression.

The pull request contains no SQLite durability-mode relaxation and no production workload generator.
