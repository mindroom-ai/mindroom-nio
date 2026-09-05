# Classic gap recovery and capacity

This amendment supersedes the Classic actionable-history exclusion in
`durable-ingestion-contract.md`. The user authorized recovery and performance
fixes after the September 5 capacity experiment. Other deliberate limits in
that contract remain in force. Standard-library JSON remains the chosen codec.

## Problem and acceptance criteria

The 200-conversation Tuwunel experiment delivered only 106 requests through
Classic sync. The other 94 became display context through MindRoom history
repair but never became actionable requests. A Synapse experiment with a
5,000-event window later stopped at the 16 MiB source-response limit. These
are failures of the complete request path, despite correct journal internals.

The fix must recover a limited interval for an already joined, hydrated room,
deliver recovered requests through existing admission, and preserve captured
input across restart. It must retry oversized sync responses without advancing
the cursor. Validate both reported server configurations with 200 concurrent
conversations, exact replies, no duplicates, drained queues, and continued
post-load sync. This is not a 200-account capacity claim.

## Ownership and alternatives

Nio owns the active Classic source cursor, missing-interval capture, crypto
preparation, and durable delivery. MindRoom owns application admission and
conversation display/context. Its existing context hydration does not trigger
actions; adding another application history-to-action queue would duplicate
ordering and deduplication responsibilities. Increasing server limits alone
does not address response bytes or servers that clamp the requested limit.

The first implementation captured the whole missing interval in one bounded
Frame. Its Tuwunel rerun delivered all 200 canonical replies and drained the
queues, then repeatedly failed the post-load fence at the 16 MiB capture bound.
The tested server enforces the requested forward bound; pages contained no
duplicated lazy-member state. Retained evidence points to a genuine backlog
of streaming edits, although exact failed page sizes were not retained.

The final design checkpoints bounded recovery pages and drains between them.
This adds a continuation state machine because the measured workload requires
it. It reuses ordinary Frames, preparation, Work, settlement, and retirement;
it adds neither another crypto writer nor another application delivery queue.

## Recovery eligibility and pagination

For a limited Classic JOIN/LEAVE timeline with a trusted prior joined-room
baseline, fetch `/messages` forwards from the request's `since` token to the
new timeline's `prev_batch`. Both tokens are opaque. Eligibility is evaluated
after older Frames have settled, so queued departures cannot leave stale
joined-room authority. If shutdown polling already captured a limited response,
hold that bounded response locally while older Frames drain, retaining its
original request identity. A crash before durable capture leaves the old cursor
available for refetch.

Keep source filters unchanged on sync retries except for the bounded timeline
limit. Recovery supports limit and lazy-member settings; selectors such as
`types`, `not_types`, or sender filters cannot prove intervening state ordering
and fail explicitly before capture. MindRoom's unfiltered timeline is supported.
Initial sync and rooms without a trusted prior baseline do not acquire
actionable cold history.

The Matrix endpoint accepts sync tokens as both bounds. Tuwunel uses exclusive
forward bounds; its `end` usually identifies the last returned event rather
than equalling `to`. Continue until `end` is absent or reaches the target.
An empty chunk with a progressing `end` is not exhaustion. Reject stalled or
cyclic tokens, malformed pages, and mismatched starts. Recovery covers
server-visible events; it cannot restore history hidden by access controls.
See the [Matrix pagination contract](https://spec.matrix.org/v1.16/client-server-api/#get_matrixclientv3roomsroomidmessages).

## Durable phases and replay

Freeze the original bounded canonical sync response once in an authenticated
singleton `NioIngestRecovery` row. Keep a small continuation in the existing
Classic source cursor; its global `next_batch` remains unchanged. The continuation
pins the response digest, eligible rooms, phase, room index, page position,
and pagination progress. Do not put the response body inside the cursor or
refetch a moving sync target.

Drain normal, independently identified child Frames through the existing owner:

1. A prologue processes retained to-device events and device-key updates once.
2. Each page contains recovered timeline events, with no later sync state,
   fresh tail, or fabricated room-section membership. Persist its evidence
   and successor continuation atomically before preparation or callbacks.
3. The final Frame applies retained state and fresh timelines after recovered
   history, with the remaining sync sections. It commits the original
   `next_batch` and removes the pending recovery row atomically.

Each child has a distinct request and Frame identity and is independently
replayable from its authenticated payload. Optional
`StagedSourceResponse.recovery_json` retains the page alongside the original
sync response in that Frame. Disk decoding checks its binding to the request
and room segment. A staged page replays without another HTTP fetch; after a
settled page, restart resumes from the persisted continuation.

Keep one child active at a time. The singleton retains input, not another
delivery queue. Internal children do not emit completed-sync callbacks or
satisfy quiescence; those belong to the final Frame. Completion eligibility
uses the completed Frame's authenticated candidate cursor, so starting another
recovery cannot suppress a previously committed ordinary completion.

## Event order and authority

Recovered events receive `RECOVERED` provenance. Drop duplicate IDs within a
page, across adjacent page overlap, and against the retained fresh tail.
Recovered state changes execute in timeline order before retained sync state
or fresh events. Rooms are recovered in their pinned order.

Recovery eligibility stays pinned across recovered leave/invite/knock/rejoin
events, while membership epochs, permission snapshots, callback routing, and
departure fences follow event order. Existing invited and joined projections
remain the owners of their respective callback routes. A trusted baseline can
survive a complete recovered transition sequence without a future-state
hydration request interrupting that sequence.

Local membership commands wait behind older pending recovery or a locally
captured limited response. They cannot interleave with old history and then
have their departure authority undone by replay.

MindRoom already admits recovered messages. Its lifecycle validator also
accepts recovered provenance; recovery does not grant fresh reply permissions.
Cold context repair remains non-actionable.

An interactive source may reach admission while an attempted outgoing edit is
still awaiting durable projection. Keep the existing atomic admission rollback
and prompt-selection barrier. The companion pump must treat the specific
`DeliveryProjectionPendingError` as a wait for progress from its existing
outbox recovery worker, then retry the same unacknowledged batch. A single
bot-owned event signals each completed recovery pass; the sole admission pump
consumes that signal and rechecks the authoritative admission barrier.
Do not wait for all outbox debt: an unrelated failed delivery must not keep
an already-projectable source parked. Starting a wait must not reset an active
worker's retry backoff.

The pump must not restart transport, acknowledge early, create a second
recovery queue, or repeatedly poll admission. Other admission errors still
propagate. Cancellation stops the pump promptly without cancelling the
independently owned worker. Shutdown wakes the waiter and stops admission.

## Bounds and failure

Owned Classic polling requests at most 100 timeline events, retaining any
smaller user limit and other filter settings. If a successful response exceeds
the wire or canonical byte bound, halve the requested limit and retry from the
same cursor, down to one. Never truncate JSON, skip a sync token, or discard
key traffic. Intrinsically oversized non-timeline data at the minimum window
remains a clear source error requiring intervention.

Recovery requests at most 100 events per page and bounds both the wire page
body and canonical evidence to 2 MiB. Narrow an oversized page at the same position down
to one event. The original response plus one page must fit the existing
16 MiB staged-response bound, and the authenticated Frame must fit its existing
24 MiB bound. Keep the existing prepared-output, Work, and crypto limits.

A 1,000-page watchdog bounds all eligible rooms in one frozen sync response.
Retain only bounded pagination
history and adjacent overlap IDs; total interval bytes are not accumulated
in memory or one Frame. Allow three attempts total per page request, including
the initial attempt, for transient failures.
Unavailable, malformed, stalled, irreducibly oversized, or exhausted-watchdog
recovery stops explicitly. The global source cursor remains unchanged; already
settled pages stay settled and reopening resumes at the retained page position.
Reopening does not reset an exhausted watchdog; that case requires intervention.
A later failure does not roll back callbacks from earlier pages.

This provides recoverable request delivery within stated limits, not unlimited
archival replay. Sliding restart overlap recovery and explicit loss semantics
retain their existing contract; this amendment makes no new Sliding gap claim.

## Performance decisions

Five alternating pairs with 500 messages containing 4,800-character bodies
measured removal of internal reconstruction of already validated frozen source
carriers. Median total delivery time improved 2.64% for one Frame and 1.85%
for 100 Frames of five messages; all pairs improved. Network/disk constructors,
payload authentication, and final identity checks remain. The removed checks
defend mutation through an explicitly unsupported Python escape hatch.

A second experiment removed repeated event canonicalization during callback
settlement and deferred caller-batch validation to acknowledged retries, where
retained Work is unavailable. Outstanding settlement still reconstructs
authenticated Work and requires full batch equality. Five paired runs with
1,000 messages and 4,800-character bodies improved a median 4.67%
(all pairs 3.79–5.75%). Short-message results were noisy.

The new recovery workload also justified deleting a duplicate Frame payload
encoding within the same staging transaction. The final write still encodes,
authenticates, and bounds the identical payload; errors still roll back the
whole transaction. Three paired 1,100-event recovery runs with a 1.2 MiB
retained sync response improved a median 2.63% (all pairs 2.13–3.54%).
This removes six production lines.

The three selected changes remove 57 production lines without a new dependency
or cache. Do not add their percentage gains together or claim an application
throughput improvement. A journal-owned normalized Frame cache improved the
recovery workload by 4.69%, but its additional saving did not justify another
cache and its lifecycle obligations. Keep standard-library JSON; the measured
codec experiment did not justify orjson.

The subsequent error-free live run still missed the post-load fence. Its
backlog makes repeated persisted Frame decoding a separate, material target:
the local 1,100-event recovery profile decoded Frame state 2,644 times.
Permit one decoded Frame entry per journal, following the existing Work and
Aggregate reuse rules. Every read still fetches the actual row and validates
its column types, identity, drain-header authentication, and revision bounds
against the current owner. A hit requires identical complete stored fields,
including payload bytes, digests, and revisions, plus the account, stream,
and transport identity used to authenticate them. Changed input takes the
full authenticated decode path; clear the entry on close.

Cache only deeply immutable staged Frames or prepared Frame state with no
outbound operations. Outbound operation contexts may contain mutable JSON,
so prepared Frames containing those operations stay uncached. This adds no
projection cache or new authority: callbacks, admission, claims, acknowledgements,
SQLite transactions, and recovery ordering remain unchanged. Keep all recovered
streaming revisions; discarding intermediate edits would change durable event
delivery and is outside this performance correction.

The original 500-event request ceiling still allowed retained sync responses of
8.25 MiB during the Synapse workload. Repeated processing of that large tail
made otherwise bounded recovery expensive. A matched local benchmark with three
owned journals sharing one event loop delivered 3,000 ordered events with
22.79 seconds of recovery time and a 2.236-second maximum heartbeat pause.
Requesting 100 events reduced those measurements to 13.66 and 0.723 seconds.
Select the smaller fixed request ceiling; existing pages already use 100.
Keep oversize halving and all persistence, ownership, and replay checks.

Ordinary CPU throughput was approximately unchanged, with more sync requests;
real network latency can add cost. This is a measured workload tradeoff, not
a universal event-loop latency bound. Large individual events, state/global
sections, and other application tasks remain separate limits. The local pause
reproduction does not by itself establish the live health timeout's cause.

## Verification and remaining limits

Use real owned SQLite sessions with mocked HTTP only at the network boundary.
Cover intervals larger than one Frame, empty advancing pages, staged-page and
settled-page restart, corruption, encrypted recovered messages, state and
membership transitions, local departures, multiple rooms, cold sync,
completion timing, and oversized retry at the unchanged cursor.

Run full tests, repository hooks, and zero-error mypy, then the real Tuwunel
and Synapse capacity harnesses without instrumentation. Use current local Nio
source and preserve the user's companion dependency edits. Record actual
counts, production size, benchmarks, integration results, and remaining limits
in the implementation plan. Internal tests alone do not establish cutover
readiness. The separately documented interactive key-share restart gap remains
a separate task.
