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
deliver recovered requests through the existing admission path, and preserve
the recovered input across restart. It must retry oversized sync responses
without advancing the cursor. Validate both reported server configurations with
200 concurrent conversations, exact replies, no duplicates, drained queues,
and continued post-load sync. This is not a 200-account capacity claim.

## Ownership and alternatives

Nio owns the active Classic source cursor, missing-interval capture, crypto
preparation, and durable delivery. MindRoom owns application admission and
conversation display/context. Its existing context hydration does not trigger
actions; adding a second application history-to-action queue would duplicate
ordering and deduplication responsibilities. Increasing server limits alone
does not address response bytes or servers that clamp the requested limit.

Use bounded capture before committing the source cursor. Keep the original
canonical sync response and canonical recovery pages in the same authenticated
Frame envelope. The existing preparation transaction then consumes one ordered
normalized view. This avoids a second durable recovery scheduler or a second
crypto writer. If capture is cancelled or crashes before staging, the old
cursor remains; after staging, restart uses the retained pages without HTTP.

## Capture and replay

For a limited Classic JOIN/LEAVE timeline with a trusted prior joined-room
baseline, fetch `/messages` forwards from the request's `since` token to the
new timeline's `prev_batch`. Both tokens are opaque. Preserve the source
filter's event-selection semantics. Initial sync and rooms without a trusted
prior baseline do not acquire actionable cold history through this path.

The Matrix endpoint accepts sync tokens as both bounds. Tuwunel uses exclusive
forward bounds; its `end` usually identifies the last returned event rather
than equalling `to`. Continue until `end` is absent or reaches the target.
An empty chunk with a progressing `end` is not exhaustion. Reject stalled or
cyclic tokens, malformed pages, and mismatched page starts. See the
[Matrix pagination contract](https://spec.matrix.org/v1.16/client-server-api/#get_matrixclientv3roomsroomidmessages).

Store recovery evidence separately from the original wire response in optional
`StagedSourceResponse.recovery_json`. It records room IDs, exact bounds, and
canonical pages. The Frame payload authentication covers it; the existing
source digest continues to identify the original sync response. Disk decoding
checks the evidence and its binding to the retained request and room segment.

Recovered events precede the fresh timeline and receive `RECOVERED` provenance.
Drop duplicate timeline IDs at the join boundary. State changes present in the
recovered interval must execute in timeline order: omit their later copies
from the sync state block rather than applying future state before old events.
Keep existing membership transitions, authorization snapshots, departure
fences, crypto preparation, admission identities, and callback settlement.
MindRoom already admits recovered messages. Its lifecycle validator must also
accept recovered provenance; recovery does not grant fresh reply permissions.

## Bounds and failure

Owned Classic polling requests at most 500 timeline events, retaining any
smaller user limit and other filter settings. If a successful response exceeds
the wire/canonical byte bound, halve that requested limit and retry from the
same cursor. Never truncate JSON, skip a sync token, or discard key traffic.
At a one-event request, intrinsically oversized non-timeline data remains a
clear source error requiring intervention.

Recovery pages request at most 100 events. Bound one capture to 100 pages,
10,000 events, and 16 MiB for the original response plus recovery evidence.
Use bounded retries for transient page failures. An unavailable, malformed,
stalled, or over-budget interval fails explicitly before cursor commit;
the source can be reopened at its old cursor. This provides recoverable
request delivery within stated bounds, not unlimited archival replay.
Keep all existing prepared-output, Work, and crypto transaction limits.

This amendment addresses the measured Classic failure. Sliding restart overlap
recovery and explicit loss semantics retain their existing contract; no new
claim of complete Sliding gap replay is made.

## Performance decision

Measured five alternating pairs with 500 messages containing 4,800-character
bodies. Removing internal reconstruction of already validated frozen source
carriers reduced total delivery time by a median 2.64% for one frame and 1.85%
for 100 frames of five messages. All pairs improved. Network constructors,
disk constructors, payload authentication, and final identity checks remain.
The removed checks defend object mutation through an explicitly unsupported
Python escape hatch. Remove that obsolete test instead of preserving the
implementation for its error-string assertion.

A Frame-cache experiment measured 0.24%/3.38% median gains across those shapes;
duplicate stage-encoding removal measured 0.71%/0.36%. Neither warrants adding
cache machinery. These microbenchmarks establish modest local improvements,
not application throughput gains. Re-run the complete workload after recovery;
record observed costs and any remaining bottleneck without attributing HTTP
wait entirely to Nio or summing overlapping asynchronous durations.

## Verification and remaining limits

Use real owned SQLite sessions with mocked HTTP only at the network boundary.
Cover the missing middle interval, multiple/empty pages, restart after staging,
duplicates, cold sync, membership/state ordering, unavailable history, and
oversized retry at the unchanged cursor. Keep corruption/replay tests and the
zero-error mypy gate. Then run the real Tuwunel and Synapse capacity harnesses
without instrumentation, using current local Nio source and preserving the
user's companion dependency edits.

Record actual suite counts, benchmarks, production size, integration results,
and unresolved limits in the accompanying implementation plan. Do not call the
cutover complete solely because internal tests pass. The separately documented
interactive key-share restart gap remains a separate task.
