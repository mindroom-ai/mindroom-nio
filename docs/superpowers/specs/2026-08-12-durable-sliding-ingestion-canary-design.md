# Durable Sliding Ingestion Canary Design

**Date:** 2026-08-12

**Status:** Approved for autonomous execution

**Scope:** Restart-safe diagnostic ingestion for the pinned Synapse 1.148.0rc1-mindroom.2 Simplified Sliding Sync compatibility dialect and its mandatory live canary

## Authority

The tracked 2026-08-10 durable-ingestion scope correction controls wherever documents conflict.

This design refines the Sliding implementation and evidence path only; it does not supersede the durable record-fate, ownership, crash, cutover, or release requirements.

The user separately approved this Sliding phase and instructed autonomous continuation without routine approval pauses.

Classic evidence remains historical evidence for Classic and is not evidence for Sliding.

The implementation starts from nio push head `b32ebf8953215bcf946b75094cbc2529f20e640f`, nio code ancestor `b313ae284be5dcc7797a7f7de83c70ad80520beb`, MindRoom head `925df7f3cbcc39d4904140efd9d16b93c77238ea`, Classic evidence file SHA-256 `5a1e2e03778832301c3cf7ede223b1744edb1a656711a79e4778bf8c5f7fb838`, and Classic payload SHA-256 `9ea59ceaffbe73b3b0a2eed0a116020dba7ee8b78adfafde3272752222073fdd`.

## Decision Summary

The durable runner gains an explicitly selected `matrix_sync.mode: sliding` branch while Classic remains the default.

The canary validates the pinned Synapse `org.matrix.simplified_msc3575` compatibility dialect, not stable or revision-complete MSC4186 conformance.

The implementation makes six coordinated changes.

1. Sliding responses normalize authenticated list operations into the same `SyncFrame` that owns the room and event evidence.
2. A fresh schema-v2 journal persists the immutable diagnostic scope, committed list observations, stable event receipts and occurrences, and source-rotation receipts.
3. Existing-store reopen and a positioned current `M_UNKNOWN_POS` atomically rotate the Sliding source epoch and connection before another request can escape, clear only connection-scoped `pos`, and preserve every other durable owner.
4. The diagnostic materializer accepts exactly one delivery room and one control room, turns validated state, fixture activity, and authenticated self-sender echoes into explicit zero-Work occurrences, and creates Work only for the sole eligible external-sender delivery-room LIVE event.
5. Initial-history retires without Work only under the three exact event-fate cases below; every other unseen or conflicting history remains in its Frame and blocks.
6. MindRoom constructs the pinned-dialect source, validates the configured transport, and reuses its existing same-sequence admission receipt to close the admission-post/ack crash window.

The live gate proves committed one-slot occupancy `target → control → target`, a crash after MindRoom admission and before nio acknowledgement, a fresh positionless request after restart, one stable measured-event Work/batch/receipt/frontier effect, an `ADMITTED → DUPLICATE` replay result, one application response, and two duplicate-free durable idles.

## Dialect Boundary

The pinned homeserver serves `POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync` and advertises `org.matrix.simplified_msc3575`.

It places `pos` and `timeout` in query parameters, accepts the deployed plural `ranges` and tuple-list `required_state` shapes, and emits list results with full `SYNC` snapshots for each requested range.

The implementation records actual operation kinds but does not require `DELETE` or `INSERT`, because this Synapse version uses successive `SYNC` snapshots.

The current MSC4186 draft uses `/v4/sync`, body `pos` and `timeout`, singular `range`, structured required state, and count-only list results.

Migration to that wire protocol is a separate adapter project and is not smuggled into this canary.

## Non-Negotiable Durable Contract

For every accepted response, the source successor and canonical raw Frame commit atomically.

Every resulting record is retained in nio Work, admitted by MindRoom, or represented by an explicit durable context, control, duplicate, or terminal fact.

Delivery remains deterministic FIFO by stream, sequence, and digest.

MindRoom admits the complete unit and its receipt in one transaction.

nio advances acknowledgement only after that MindRoom commit.

No MindRoom commit means byte-identical nio redelivery.

MindRoom commit without nio acknowledgement means the same batch reference and digest is replayed and MindRoom returns `DUPLICATE`.

A same consumer, stream, and sequence with different bytes fails before either side advances.

Deferred means absent or fail-closed, not silently ignored.

## Deliberately Narrow Diagnostic Scope

The persisted diagnostic scope contains exactly one delivery room, one distinct control room, one observation-list name, and the inclusive range `(0, 0)`.

The stream rejects a different scope on reopen.

The account used by the live gate has exactly those two joined rooms and no invite or knock rooms.

The delivery room is the only room eligible to create application Work.

The control room exists only to move the one-slot recency window, and its one exact bot-authored fixture event becomes an explicit durable control occurrence with zero Work.

Any other room, a third room, or a scope mismatch blocks the oldest Frame and stops later source HTTP.

This checkpoint does not provide general Sliding gap recovery, encrypted-event recovery, membership operations, desktop parity, or release cutover.

Unexplained restart history is not discarded or guessed; outside the exact receipt-duplicate and sole eligible outbox-correlated self-response cases, it stays durably staged and blocks.

## Normalized List Evidence

`SyncFrame` gains an immutable tuple of validated Sliding list results.

Each result names a requested list, its reported count, and the ordered operations returned by the server.

Each operation records its kind, inclusive range, and ordered room IDs.

The Sliding normalizer validates requested list names, counts, range bounds, operation cardinality, room-ID uniqueness, and operation-specific fields.

Unknown operations, contradictory ranges, malformed IDs, duplicate list results, or a response that cannot be deterministically applied is a malformed source result.

The normalizer applies the operations to the request cursor's prior list state and stores the resulting state in the candidate cursor.

A new connection begins with unknown list state and its first exact `[0,0]` snapshot seeds the state rather than fabricating an entry transition.

Re-normalizing a frozen staged response must reproduce the same list results and candidate cursor byte-for-byte.

At materialization, the journal derives `seed`, `enter`, `evict`, `reenter`, or `replace` from the frozen request and candidate states and persists only a semantic observation tied to the same Frame and source digest.

The qualified window witness is a committed target-present observation, a later target-absent/control-present observation, and a later target-present observation on one source epoch and connection.

No raw HTTP observer or second Sliding connection contributes to PASS.

## Schema-v2 Journal

The ingestion journal schema advances from 1 to 2 while the cross-repository `SyncBatch` wire schema remains 1.

There is no in-place v1 migration.

The Sliding gate creates a fresh v2 database and reopens that exact database after the crash.

Classic also uses a fresh v2 journal with unchanged behavioral semantics and unchanged batch schema.

All ingestion rows remain canonical plaintext BLOBs with SHA-256 digests and digest-bound clear-column headers.

No ingestion row is encrypted or re-encrypted.

### Diagnostic scope

`NioIngestDiagnosticScope` stores the canonical scope payload, payload SHA-256, and created revision.

Its clear columns bind account, delivery room, control room, list name, and range.

The row is immutable for the lifetime of the stream.

### List observations

`NioIngestListObservation` stores an observation ID, Frame ID, source epoch, request ID, list name, semantic transition, target/control presence booleans, created revision, canonical payload, and payload SHA-256.

Its payload binds normalized operations, resulting `[0,0]` occupant role, source SHA-256, connection digest, proof kind, JOIN-anchor Frame/source digests, membership-event digest, continuity result, and the exact timeline-boundary kind without persisting an unhashed connection identifier.

The table has a small diagnostic cap; exceeding it blocks rather than growing without bound.

### Event receipts and occurrences

`NioIngestEventReceipt` is keyed by a stable record identity derived from stream ID, room ID, and nonempty Matrix event ID.

It stores room ID, event ID, an application-stable event SHA-256, first Frame/source digests, optional Work identity, fate, optional delivery sequence/batch digest, acknowledgement revision, and revision fields.

The stable event digest excludes volatile `unsigned` data and includes room, event ID, type, sender, origin timestamp, state key, redaction target, and canonical content.

The same room/event ID with a different stable digest is terminal corruption.

Fates are `context`, `control`, `self_control`, `ready`, `outstanding`, and `acknowledged`.

`NioIngestEventOccurrence` ties each accepted source occurrence to its Frame, source SHA-256, provenance, and disposition `application`, `context`, `control`, `self_control`, or `duplicate`.

An occurrence never creates a second Work when its receipt already proves the event is ready, outstanding, or acknowledged.

The diagnostic occurrence table is bounded; capacity exhaustion blocks the Frame.

### Source rotations

`NioIngestSourceRotation` stores a fixed reason `reopen` or `unknown_pos`, predecessor and successor epochs/request IDs, digests of predecessor and successor cursors and connection identifiers, predecessor/successor `pos_present` booleans, created revision, canonical payload, and payload SHA-256.

It never stores an opaque position or connection identifier in clear text.

The first successor Frame later binds the exact frozen request with no `pos` query parameter.

## Stable Event Identity and Work Fate

Eligible timeline records require a nonempty Matrix event ID.

Their nio record and Work identity is `UUIDv5(stream_id, "timeline:<room_id>:<event_id>")` rather than Frame-scoped identity.

The first eligible LIVE occurrence atomically inserts its event receipt, occurrence, and exactly one Work row.

Claiming the batch atomically changes that receipt from `ready` to `outstanding` and binds sequence and batch SHA-256.

Acknowledgement atomically deletes Work, advances the existing delivery frontier, and changes the receipt to `acknowledged` with the same sequence and digest.

The receipt therefore preserves source → Frame → Work → batch → acknowledgement correlation after Frame and Work retirement.

Cold HISTORY is not itself proof of prior admission and cannot silently remove a resulting record from the durable ownership chain.

An initial-history occurrence may retire without Work only in three event-fate cases.

First, source epoch zero may classify exactly one canonical bot-authored target-room warm-up `m.room.message` as explicit `context` before the measured-event boundary.

Second, any epoch may classify it as `duplicate` only when room ID, event ID, and stable digest match an existing receipt.

Third, the first cold Frame after rotation may classify exactly one previously unseen authenticated self-sender target-room response echo as `self_control` when the measured application receipt already exists; it atomically creates its stable receipt and occurrence with zero Work and remains subject to the uniqueness and outbox-correlation gates below.

The pinned server's bounded timeline may report `limited:true` even when its sole returned event is the exact diagnostic event and older room-creation history is outside the window.

A source-epoch-zero initial limited Frame may retire only when it has the exact own-JOIN required-state proof, is not expanded, adopts no gap or loss, and every returned timeline event is either the one target warm-up, the one control-room fixture, or an exact receipt/digest duplicate.

The first cold Frame after a rotation may retire with `limited:true` only when it has the exact own-JOIN required-state proof, is not expanded, adopts no gap or loss, and every returned timeline event either exactly matches a durable receipt or is the sole eligible authenticated self-sender response echo described below.

Those two exceptions persist timeline-boundary kind `bootstrap_truncated` or `rotation_truncated` in the same committed list observation and make no continuity claim across omitted history.

Every other initial or continuation HISTORY event, gap, loss, expanded timeline, limited timeline, or ambiguous boundary remains `BLOCKED` with the Frame unchanged.

## Membership and State Control

Entry on a seed or new connection requires an exact same-Frame own-member `m.room.member` required-state event whose state key is the immutable canary account, whose content membership is `join`, and whose event ID is nonempty.

The event ID and stable digest become the durable JOIN baseline.

The `probe` list uses exact defense-in-depth filters `is_encrypted:false` and `is_invite:false`, but `is_invite:false` is not described or trusted as a joined-only filter.

Same-connection re-entry may inherit the durable JOIN baseline only when the target is present in the committed `probe` transition from control to target, the source epoch and connection are unchanged, the applied request/opaque-position chain is contiguous, and the canonical list/subscription configuration requesting exact own-member and encryption state is unchanged.

After the JOIN-anchor Frame, the source chain through re-entry must have no reset or rotation and must retain contiguous request, opaque-position, and applied-list state.

Every target-room segment after that anchor through and including re-entry must have no initial, limited, expanded, hydration, loss, gap, stripped-state, membership-epoch, or contradictory own-membership evidence.

If a same-Frame own-member event is present, it must exactly repeat the durable baseline; any different own-member event, including a join-to-join replacement, blocks this narrow diagnostic witness.

List presence by itself is never membership proof.

Required-state descriptors are classified before Work planning.

The exact own-member proof and bounded non-encryption room state become explicit control occurrences and never become MindRoom Work.

Any encryption state, malformed or duplicate state key, live membership transition, invite, knock, leave, ban, or unexplained state blocks.

## Diagnostic Materialization Matrix

Every accepted frame requires no to-device events, no device-list change, no fallback-key change, no encrypted event, no global account data, no presence, no ephemeral event, and no unsupported room account data.

One-time-key counts are accepted only when the canonical object uses a subset of `curve25519` and `signed_curve25519` and every present value has exact integer type and value zero.

Booleans, negative or positive values, nonintegers, or unknown keys block.

An empty safe frame retires with no Work and may advance only source and journal revision.

The first source-epoch-zero target bootstrap frame may commit joined continuity and explicit context/control occurrences with no Work.

An exact control-room frame may commit joined continuity, list evidence, and exactly one canonical bot-authored LIVE `m.room.message` fixture as a control occurrence with no Work.

Exactly one delivery-room LIVE `m.room.message` whose authenticated sender differs from the immutable canary account creates one application occurrence, receipt, and ready Work.

A delivery-room LIVE `m.room.message`, or the sole eligible rotation-cold HISTORY response echo, whose authenticated sender equals the canary account creates a `self_control` receipt and occurrence with zero Work.

Exactly one distinct post-boundary self-control event is permitted and must be the visible application response; an exact repeat is a duplicate, while an unmatched or second distinct self event blocks.

The self-control transition is eligible only after the measured application receipt exists, and final evidence must match its role-bound event digest to MindRoom's acknowledged response-outbox event digest.

A second distinct external-sender delivery-room application event also blocks.

A replay of an existing receipt creates exactly one duplicate occurrence and no Work.

Mixed target/control frames are accepted only when every descriptor independently fits those exact roles and the list operation explains the occupant transition.

Any other shape returns stable `BLOCKED` twice, preserves the identical Frame and durable graph, performs zero materializer DML, and emits no transition callback.

## Sliding Connection Rotation

A fresh Sliding store starts at source epoch zero, request ID zero, a new connection instance, and `pos=None` without a rotation receipt.

Opening an existing Sliding store performs one bootstrap transaction before activation or return.

The transaction authenticates the owner, source, diagnostic scope, Frames, Work, receipts, and delivery state.

It installs the new writer epoch, advances source epoch from `next_source_epoch`, sets request ID zero, generates a new connection instance, clears `pos`, resets range acknowledgement and connection list state, preserves `to_device_since`, preserves connection name/page/range configuration, advances owner revision and `next_source_epoch`, rewrites the canonical source payload/digest/header, and inserts one rotation receipt.

It leaves every Frame, Aggregate, Work, event receipt, occurrence, list observation, delivery field, and E2EE row unchanged.

The transaction is all-old or all-new at every statement boundary.

Classic reopen changes only writer epoch exactly as before.

The journal exposes the same transition for a current frozen Sliding request rejected with HTTP 400 `M_UNKNOWN_POS` when that request carried nonempty `pos`.

Positionless, stale, malformed, wrong-stream, wrong-epoch, wrong-request, wrong-cursor, repeatedly cold, or exhausted reset attempts are terminal.

No reset failure falls back to Classic, reuses the rejected position, or discards staged or delivery-owned work.

## Coordinator Ordering

The coordinator drains old Frames before a new source request.

It completes exact hydration required by the two-room diagnostic scope before polling.

It stages successful source responses atomically with successor state.

It reloads committed state after `M_UNKNOWN_POS` rotation instead of mutating an in-memory cursor.

Delivery may run concurrently because event receipts make repeated source occurrences idempotent.

The live latch nevertheless crashes at `admission.post`, so the outstanding batch remains byte-identical for replay.

On restart, MindRoom receives the same batch reference and digest and returns `DUPLICATE`; nio then acknowledges it once.

Any restart HISTORY event other than an exact receipt/digest duplicate or the sole eligible rotation-cold self-response echo blocks before another source poll and prevents a qualified PASS.

## MindRoom Runner

`run_durable_ingestion()` remains the sole production canary runner.

It validates the canary agent, SQLite journal, nio client, one approval/delivery room, and one distinct control-room environment value before filesystem or HTTP work.

Classic constructs its existing filter unchanged.

Sliding constructs one fixed connection name, one caller list named `probe` with `ranges:[[0,0]]`, recency sort, bounded timeline, filters `is_encrypted:false` and `is_invite:false`, exact own-member and encryption-blocker required state, no second delivery source, exact reserved-list page size one, and the required E2EE/room extensions.

nio continues to inject its reserved all-room safety list.

The reserved list is validated and carried in cursor state but never substitutes for the `probe` observation or membership proof.

The internal `DiagnosticIngestionScope` carries the approval room and control room through open/session/materialization.

`validate_ingestion_batch()` takes the configured expected transport rather than hard-coding Classic.

The measured record remains exactly one unencrypted canonical `m.room.message` with LIVE provenance.

Every existing digest, batch, principal, room, membership, projection, and acknowledgement check remains.

Classic runner behavior and existing Classic test IDs remain green.

## Live Canary Order

The live gate creates one fresh disposable account and exactly two unencrypted joined rooms: target `T` and control `C`.

The bot sends a pre-boundary warm-up event in `T` so the first one-slot snapshot is `SYNC[T]`.

The production Sliding runner starts and commits the seed/entry evidence plus bootstrap context.

The canary-only transition latch stops the child immediately after that materialization commit and before the coordinator can plan the expanded reserved-list request.

While the child is stopped, the external operator sends one bot-authored fixture control event in `C`, then resumes the child.

The next request expands only the reserved safety range from `[0,0]` to `[0,1]`; production commits `SYNC[C]` in `probe` as target eviction plus the control occurrence with zero Work.

The external sender sends the sole measured application event in `T`, and production commits `SYNC[T]` as target re-entry with the uninterrupted durable JOIN proof and the measured event in the exact `num_live` suffix.

The event creates one Work and one batch.

The process is stopped at `admission.post`, after MindRoom commits `ADMITTED` and before nio acknowledgement.

The whole process group is killed and both databases remain.

Restart atomically rotates source and connection before HTTP.

The first successor request has source request ID zero and no `pos` query parameter.

The first successful response negotiates a new nonempty opaque position and any replayed measured event matches the durable receipt as a duplicate occurrence with no new Work.

The original outstanding batch is replayed, MindRoom returns `DUPLICATE`, and nio acknowledges it once.

Exactly one journal-driven application response becomes visible, and its authenticated self-sender Matrix echo is durably correlated as the sole post-boundary `self_control` occurrence with zero Work.

Two later ten-second idle observations create no new event occurrence of any disposition for any receipt and no Work, batch, admission receipt, projection, response, acknowledgement, or frontier movement.

Source request count and opaque position may advance during idle.

## Evidence and Secrecy

The final evidence binds commits, wheel hashes, Synapse version/image digest, dialect label, schema versions, scope digest, canonical request-configuration digest, rotation receipt, semantic list observations, JOIN-anchor digest, zero contradictory-membership-delta count, event receipt/occurrence chain, batch identity/digest, admission results, acknowledgement, application counts, self-control response digest, idle comparisons, and cleanup.

Room, event, position, and connection identities appear only as role-bound SHA-256 digests.

Evidence never contains raw positions, connection IDs, room IDs, event IDs, access tokens, event bodies, configuration, environment, HTTP bodies, logs, SQL, PIDs, or tracebacks.

Exactly one complete evidence object is materialized and independently audited before a qualified Sliding PASS is claimed.

## TDD and Review Boundaries

Production edits begin only after causal RED tests reach their intended barrier.

The implementation is divided into reviewable source/list, schema/scope, rotation, receipt/materializer, coordinator, MindRoom, cross-repository crash, and live-evidence checkpoints.

Every negative materializer test calls twice and requires stable `BLOCKED`, the identical Frame and graph, zero DML, and no transition callback.

Every transaction change has all-old/all-new statement-failure tests.

Every checkpoint runs focused tests, named broader regressions, Ruff, ty where configured, Black, `git diff --check`, exact path/numstat/suppression inventories, and independent review.

Each reviewed checkpoint is committed with targeted staging and pushed non-force without amend.

No implementation checkpoint waits for another routine user approval.

## Acceptance Matrix

| Property | Required PASS evidence |
| --- | --- |
| Dialect | Pinned Synapse 1.148.0rc1-mindroom.2 compatibility endpoint and actual `SYNC` operations, with no stable/current MSC4186 claim |
| Scope | One immutable delivery room, one immutable control room, one `probe` `[0,0]` list, no other rooms |
| Window | Committed target present → absent/control present → present observations on one production connection |
| Membership | Entry has exact own JOIN state; same-connection re-entry has the unchanged durable JOIN baseline plus an uninterrupted applied cursor/list chain and zero contradictory membership evidence |
| Event | One stable measured-event receipt/record identity; repeat source occurrences are durable duplicates |
| Work/batch | One Work, one sequence, one batch ID and digest |
| Crash | MindRoom result sequence exactly `ADMITTED`, then same-batch `DUPLICATE` |
| Restart | Rotation receipt advances epoch, changes connection digest, clears position, and first successor request omits `pos` |
| Acknowledgement | One original digest advances the frontier and marks the event receipt acknowledged |
| Application | One journal event, one projection, one visible response |
| History | Only the exact epoch-zero bot-authored warm-up becomes context, an exact receipt/digest match becomes duplicate, and the sole rotation-cold authenticated response echo becomes self control; the two explicit limited-boundary cases persist no-continuity facts, and every other history shape blocks |
| Idle | No new event occurrence of any disposition for any receipt and no application/delivery change across two ten-second observations |
| Cleanup | Clients, process group, instance, container resources, registry, unit, and lab are removed; repositories remain clean |

## Release Gate

Qualified Sliding diagnostic canary PASS applies only to the reviewed pinned-Synapse, one-account, one-delivery-room plus one-control-room, one-plaintext-LIVE-event path.

This satisfies the Sliding live-canary prerequisite for a Sliding-enabled release only.

It does not enable Sliding by default, remove Classic or the legacy consumer, authorize cutover, or authorize or publish a release.

Release remains blocked on crypto and missing-key behavior, deployment-required general recovery and membership routes, cross-repository crash breadth, legacy-consumer removal, final parity/evidence review, release soaks, artifact pinning, and clean suites.

The qualified diagnostic canary is not full parity, cutover readiness, release readiness, or whole-project completion.
