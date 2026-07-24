# mindroom-nio PR 20 Recovery Status

## Current Candidate

Branch `fix/limited-sync-recovery-loss-v2` is published as PR #20.
The committed head before this uncommitted rewrite is `aafe34b72a842e39d8c594382900e06d4f44c574`.
The rejected response-transaction architecture ends at that head.
The replacement and this handoff will be committed together.
The external `MERGE-GATES.md` records the exact resulting SHA because a commit cannot contain its own hash.
This candidate is not merge-ready until fresh Codex, Claude Fable, and real-Tuwunel gates pass on one exact head.

## Current Architecture

- Public `/sync` `next_batch` is a monotonic transport cursor and is never rewound for callback recovery.
- One atomic store transaction persists a new transport token plus durable obligations for only the limited rooms in that response.
- Each room obligation owns a generation, private forward cursor, and one ordered queue shared by classic sync, recovered history, and Sliding Sync.
- Complete rooms, to-device events, invites, presence, ephemeral events, account data, and response callbacks are not replayed while another room recovers.
- The pump rotates rooms fairly and bounds pages, events, and network recovery time.
- Recovery dispatch is oldest first, attempts every matching callback, and acknowledges only after full fan-out.
- One membership planner clears leave, ban, invite, and own-join boundaries before callback delivery.
- Only the requested target cursor closes pagination; missing, repeated, bounded, timed-out, and ignored targets leave the gap open.
- Classic and Sliding Sync rows enter the same durable lane, so neither protocol can overtake or replay the other across restart.
- File-backed writes run off-loop through one serialized commit boundary; submitted writes reach a terminal outcome before memory changes or cancellation propagate.
- In-memory SQLite writes remain on their owning thread.
- Bounded completed event IDs are durable and preserve exactly one encrypted-to-decrypted upgrade.
- Disabled mode preserves upstream callback order and short-circuit behavior.

## Accepted Findings Resolved

- Same-token sync responses pump pending obligations.
- Later-generation held IDs no longer deadlock an older generation.
- Recovery callback fan-out attempts all callbacks without changing ordinary callback semantics.
- Sliding sync cannot overtake a pending classic gap.
- Committed cancellation applies the durable outcome to memory before propagating.
- Failed token-plus-plan persistence restores the prior in-process transport cursor.
- Reaching the requested target cursor closes the recovery slice.
- Current timelines suppress rows before the last own-join.
- Legal concurrent-DAG ordering no longer depends on a live-event echo.
- Classic-to-Sliding and Sliding-to-classic restart replay use the same completed-ID source of truth.
- Sliding leave, ban, invite, and own-join transitions clear classic obligations through the common planner.
- Feature-enabled classic and Sliding Sync timelines use one terminal all-callback fan-out path.
- Dead `RecoveryGap.from_token` and `own_join_event_id` metadata were removed from values, schema, store, and tests.
- Duplicate recovery imports and redundant fetch, completion, and shutdown adapters were removed.

## Deterministic Coverage

- Monotonic transport and next-request `since`.
- Ordered gap plus held-live dispatch.
- Complete-room and presence progress beside an incomplete room.
- New same-room live rows joining the held lane.
- Silent ignored-`to` future-row rejection.
- Independent two-room progress.
- Current and paginated own rejoin boundaries.
- Recent overlap and encrypted-to-decrypted replay.
- Process restart without replaying ancillary or response surfaces.
- Same-token pumping.
- Recovery-only attempt-all callback fan-out.
- Sliding/classic ordering.
- Bidirectional classic/Sliding restart deduplication.
- Sliding encrypted-to-classic plaintext upgrade across restart.
- Durable classic leave/invite and Sliding leave/ban/invite reset.
- AsyncClient token-plus-plan store failure injection.
- Target-token closure without live echo.
- Serialized commit cancellation for plan, progress, acknowledgement, and deletion.
- Active-row replay after callback failure.
- Later-generation suffix handling.
- Atomic store rollback and direct v2-to-v3 migration.

## Rejected-Candidate Test Audit

The rejected `aafe34b` candidate collected 564 tests and reported `561 passed, 3 skipped`.
The replacement collects 547 tests, so 17 branch-only items remain intentionally removed.
Production size accounting excludes all tests.

| Rejected test or group | Replacement or reason |
| --- | --- |
| `test_disabled_makes_no_requests`, `test_disabled_callback_error_keeps_upstream_short_circuit` | `test_disabled_preserves_short_circuit` exercises a limited response with recovery disabled; any `/messages` request or altered fan-out would fail it. |
| `test_disabled_keeps_eager_store_token_on_callback_error` | Rewritten directly as `test_disabled_store_token_stays_eager_on_callback_error`. |
| `test_first_sync_is_skipped`, `test_ordinary_sync_is_not_subject_to_backfill_deadline` | Rewritten as `test_first_sync_needs_no_recovery` and `test_ordinary_sync_ignores_recovery_deadline`. |
| `test_limited_timeline_recovers_gap_in_order`, `test_events_in_sync_response_are_not_redispatched`, `test_later_sync_does_not_redispatch_recovered_event` | `test_gap_and_live_window_dispatch_in_order` now also sends a later repeated gap event and proves one callback. |
| `test_default_recovery_progresses_in_bounded_slices`, `test_pagination_stops_at_page_bound`, `test_page_bound_retry_resumes_after_completed_prefix`, `test_event_bound_slices_incomplete_recovery` | Rewritten as `test_empty_page_and_page_bound_resume` and `test_event_bound_resumes_from_persisted_cursor`; both verify the exact private cursor used by the next pump. |
| `test_empty_page_continues_pagination`, `test_repeated_end_token_leaves_gap_open`, `test_room_messages_failure_is_tolerated` | Rewritten directly as empty-page resume, repeated-cursor hold, and room-error hold tests. |
| `test_forward_recovery_does_not_require_prev_batch`, `test_since_bound_closes_without_delivered_event`, `test_explicit_since_reaches_backfill` | Rewritten as `test_missing_prev_batch_uses_response_target`, `test_target_cursor_closes_without_live_echo`, and `test_explicit_since_bounds_first_recovery`. |
| `test_boundary_page_keeps_gap_events_after_sync_overlap`, `test_gap_events_on_pages_after_sync_overlap_are_recovered`, `test_event_bound_after_sync_overlap_keeps_safe_prefix` | `test_ignored_to_future_is_not_dispatched` preserves the stronger safety contract: a concurrent or unknown suffix after a held boundary remains durable and undispatched instead of being guessed safe. |
| `test_backfilled_state_events_do_not_regress_room_state` | Rewritten directly as `test_recovered_state_does_not_regress_live_room_state`. |
| `test_restart_resume_backfills_first_limited_sync`, `test_full_state_resume_still_backfills` | Rewritten as `test_restart_first_limited_sync_uses_loaded_transport` and `test_full_state_join_does_not_cancel_timeline_recovery`. |
| `test_newly_joined_room_is_not_backfilled`, `test_walk_stops_at_own_join`, `test_bounded_prefix_waits_for_later_own_join`, `test_restart_walk_holds_prefix_until_membership_is_known` | Covered by current-timeline suppression, paginated rejoin filtering, and the new two-pump bounded-prefix test; no pre-join row reaches callbacks. |
| `test_incomplete_backfill_keeps_checkpoint_until_recovered`, `test_one_incomplete_room_keeps_multi_room_checkpoint`, `test_incomplete_room_does_not_block_complete_room_retry` | Global checkpoint assertions were replaced by monotonic transport plus private room obligations in `test_incomplete_room_keeps_transport_monotonic`, `test_newer_same_room_event_cannot_overtake_gap`, `test_unrelated_room_and_presence_do_not_wait`, and `test_two_rooms_close_independently`. |
| `test_backfill_budget_is_shared_across_rooms`, `test_incomplete_retries_rotate_past_stalled_room`, `test_backfill_timeout_is_tolerated` | Consolidated into `test_room_rotation_progresses_after_another_room_times_out`, which proves one shared deadline and next-pump rotation. |
| `test_dispatch_respects_backfill_budget`, `test_hanging_callback_cannot_stall_dispatch` | Consolidated into `test_hanging_callback_leaves_active_row_pending`; callback wait is bounded and its row remains retryable. |
| `test_callback_error_skips_only_that_event`, `test_callback_fanout_failure_is_terminal_across_restart` | Covered by recovery-only attempt-all in `test_recovery_attempts_all_callbacks_once` and the rewritten same-name cross-process test. |
| `test_completed_prefix_is_durable_while_next_callback_blocks` | `test_restart_replays_only_active_unacknowledged_row` injects failure on the second acknowledgement and proves the first row is not repeated across process lifetimes. |
| `test_durable_journal_writes_run_off_event_loop`, `test_memory_store_journal_stays_on_owning_connection` | The journal is gone; `test_cancelled_old_commit_stays_ahead_of_newer_commit` proves file-store work is off-loop and serialized, while `test_memory_store_commit_stays_on_event_loop_thread` proves in-memory ownership. |
| `test_store_write_failure_keeps_response_retryable` | Replaced by AsyncClient-level `test_failed_plan_commit_restores_transport_cursor` plus atomic store rollback coverage. |
| `test_straggler_already_delivered_is_not_redispatched`, `test_walk_upgrades_previously_encrypted_event`, `test_event_bound_counts_decrypted_replay` | Covered by `test_recent_overlap_and_encrypted_replay`, the explicit event-bound resume test, and store-level encrypted-to-decrypted row upgrade. |
| `test_cross_cache_plaintext_state_wins` | Rewritten directly as `test_plaintext_overlap_state_wins_over_later_encrypted_copy`. |
| `test_sliding_sync_call_path_keeps_dedup_bounded`, `test_sliding_sync_uses_recent_normal_sync_dedup` | Covered by public-path bounded dedup plus bidirectional cross-process classic/Sliding restart tests. |
| `test_sliding_callback_error_records_prefix_and_attempts_all` | Replaced by `test_sliding_callback_failure_is_terminal`; every callback is attempted once and replay cannot repeat completed side effects. |
| `test_silent_backward_to_ignore_cannot_replay_old_history` | Backward recovery no longer exists; `test_ignored_to_future_is_not_dispatched` tests the forward-walk ignored-`to` hazard. |
| `test_live_callback_error_is_terminal_across_process_restart` | Response-wide live journaling was rejected; the still-relevant held-room contract is covered by the cross-process recovery fan-out test. |
| `test_ancillary_callback_is_terminal_across_process_restart`, `test_to_device_replay_uses_pre_decryption_identity` | Ancillary replay identities were deleted because the monotonic transport response is never replayed; `test_restart_resumes_room_without_replaying_response_surfaces` proves presence and response callbacks remain single-shot while only the room obligation resumes. |
| `test_live_callback_cancellation_restores_safe_cursor`, `test_since_less_callback_error_restores_full_sync_cursor`, `test_explicit_since_is_persisted_before_callback_failure`, `test_sticky_gap_closes_on_non_limited_retry`, `test_public_token_reset_preserves_recovery_bound` | These asserted the rejected public-cursor rewind and response-transaction checkpoint; replacement tests instead prove monotonic transport, atomic plan failure rollback, same-token private pumping, and committed-cancellation consistency. |
| `test_restart_loads_checkpoint_before_incomplete_backfill` | Replaced by durable room-obligation restart coverage; no global callback checkpoint exists. |
| `test_restart_journal_exceeds_memory_dedup_limit`, `test_restart_journal_allows_one_decrypted_upgrade`, `test_sliding_sync_dedup_stays_bounded_with_durable_journal`, `test_live_timeline_commits_each_durable_journal_event`, `test_overlap_only_recovery_is_durable_before_checkpoint_prune`, `test_sliding_decrypted_upgrade_updates_durable_journal` | All targeted the deleted response-wide event journal; durable pending rows, monotonic transport, bounded sliding memory, per-row acknowledgement, and encrypted-row upgrade now own those contracts. |
| Store tests `test_v2_store_migrates_dispatched_event_journal`, `test_upgrade_to_v3_creates_journal_before_final_table_creation`, `test_v3_store_migrates_sync_recovery_marker`, `test_interrupted_v4_migration_is_restart_safe`, `test_sync_recovery_marker_round_trip`, `test_dispatched_event_journal_bulk_upsert` | Branch-only journal/marker schemas were replaced instead of migrated; `test_v2_store_creates_recovery_tables`, atomic round-trip/rollback, and `test_recovery_event_upgrade_and_acknowledgement` cover the shipping v3 schema. |

## Validation

- Focused recovery/store suite: `95 passed in 32.21s`.
- Exact new bidirectional restart, encrypted upgrade, and membership-reset regressions: `7 passed in 0.33s`.
- Full suite: `544 passed, 3 skipped, 2 warnings in 62.77s`.
- The known full-suite mutation of `tests/data/encryption/example_DEVICEID.db` was restored.
- Generated `src/mindroom_nio.egg-info` was moved to Trash and is absent from the worktree.
- Production diff against `origin/main`: `+1073/-533`, net `+540`, including the 696-line recovery module.
- Focused Black, Ruff, Python compilation, and `git diff --check` passed.
- All-file pre-commit passed.
- Same-head live/review gates remain pending.
- Git author is `Bas Nijholt <bas@nijho.lt>`.

## Fixed or Rejected Stale Claims

- Public cursor rewind, sticky global checkpoint state, ancillary content hashes, and response-wide callback journals no longer exist.
- The old overlap-only early return no longer exists.
- The design does not certify recovery from a non-limited speculative response.
- Recovery callback attempt-all is isolated from ordinary upstream fan-out.
- A submitted file-store write is not abandoned behind a cancellable `wait_for`.

## Remaining Risks and Gates

- SQLite and external callback side effects cannot share a transaction, so a hard kill can replay at most the active timeline row.
- Bounded completed event IDs and pending rows are durable; a hard kill can still replay the active unacknowledged row.
- Real Tuwunel evidence predates this rewrite and is not gating evidence.
- Fresh Codex approval is absent.
- Fresh Claude Fable approval is absent.
- Same-head real-Tuwunel PASS is absent.

## Next Steps

1. Commit and push this tested rewrite without amend or force-push.
2. Record exact local, remote, and PR heads in the external gate ledger.
3. Obtain fresh Codex approval, fresh Claude Fable approval, and real-Tuwunel PASS on that same exact head.
4. Resume MindRoom PR #1640 only after nio PR #20 passes those gates.
5. Remove this handoff only immediately before merge, then revalidate the documentation-only removal head.
