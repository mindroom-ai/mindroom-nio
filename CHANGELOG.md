# Changelog

All notable changes to this project will be documented in this file.

## 0.38.0

### Breaking Changes

These only apply when `backfill_limited_timelines=True`.

- Classic Sync token and recovery state must now have one owner.
  `store_sync_tokens` and the resolved `backfill_persist_recovery` setting must
  agree; mixed configurations raise `LocalProtocolError` before a Classic
  request is sent or a supplied response mutates state. Application-owned
  Classic recovery stays in memory and cannot acknowledge an open gap, while
  nio-owned Classic recovery persists its cursor and room obligations together.
- Custom `MatrixStore` subclasses used for persisted recovery must explicitly
  declare `supports_atomic_recovery = True` in their own class body. The
  built-in `DefaultStore`, `SqliteStore`, and `SqliteMemoryStore` opt in;
  backends that cannot provide multi-statement recovery transactions are
  rejected before network or state mutation.

### Features

- Add `AsyncClient.acknowledge_unrecovered_rooms()` and store schema v9's
  `SyncRecoveryAbandonedRooms`. A room whose history gap was abandoned remains
  in `unrecovered_room_ids` across later responses and restarts until the
  application explicitly records and acknowledges the loss.

### Bug Fixes

- Clearing a real recovery gap during a recovery reset, successful
  `room_leave()`, or successful `room_forget()` now records sticky abandonment
  atomically with deleting the gap, so permanent history loss cannot silently
  make the room appear healthy.
- Reject recovery-enabled sync processing, membership cleanup, and abandonment
  settlement before mutation when the configured store has not explicitly
  promised atomic recovery writes.

## 0.37.0

### Features

- Support recursive event relations, as described by the Matrix 1.10 recursive relations extension.
  `Api.room_get_event_relations()` and `AsyncClient.room_get_event_relations()` take `recurse`, and `RoomEventRelationsResponse` exposes the server-reported `recursion_depth`.
  A caller that depends on a traversal depth passes `minimum_recursion_depth`, and every non-empty page that omits `recursion_depth` or reports less than the requirement raises `InsufficientRecursionDepthError` before any of that page's events are yielded.
  A server's advertised Matrix version does not imply the depth it actually traverses, so a caller that needs a guaranteed depth must require it explicitly rather than infer it.
  Note that servers disagree about what the number means: some report the depth they are willing to traverse, others the depth of the deepest event they actually returned, which for a shallow relation tree is legitimately `0`.
  An empty page is never rejected, since it has no depth to report and nothing that could have been truncated.

### Bug Fixes

- Classic limited-sync gaps bounded by the prior `since` token now treat `/messages`
  exhaustion without an `end` token as proven continuity and classify recovered events
  as `RECOVERED`.

## 0.36.0

### Bug Fixes

- Classify timeline events as `RECOVERED` when a bounded `/messages` walk proves they follow the held room baseline, distinguishing them from both direct `LIVE` activity and unverified cold `HISTORY`.
  A Sliding Sync walk bound to one exact own-membership identity treats the servers' normal empty response without an `end` token as successful bounded exhaustion, while unbound history remains fenced.
  Store schema v8 persists that narrow authority across interrupted recovery.
  The exact callback task draining such a recovery gap may reply to the same room while child tasks and sends to other recovering rooms remain blocked.

## 0.35.0

### Features

- Add `AsyncClient.reset_classic_sync_state()` for applications that disable nio sync persistence and durably own the Classic Sync checkpoint.
  The operation drains started callbacks and active or queued room-state users before clearing transient response, room, recovery, and replay-suppression state so the application can replay from its committed cursor.
  It waits behind non-sync membership cleanup while continuing to reject active sync-family requests and response callbacks.
  The first response after reset is applied as a rebuild even when its opaque token equals the restored checkpoint.
- Add `AsyncClient.has_uncommitted_classic_sync_state` and `AsyncClient.acknowledge_classic_sync()` so an application can distinguish a clean transport restart from partially applied Classic state and acknowledge only the exact token it durably committed.
  Acknowledgement remains unavailable until nio finishes applying the response and no retained recovery callback or failure remains, so failed processing cannot clear the dirty state through an older or partially advanced cursor.
- Add `AsyncClient.clear_persisted_sync_recovery()` for removing legacy cursor, recovery, and Sliding Sync window rows when migrating to application-owned Classic Sync.
  Its first response also bypasses same-token suppression so startup full state always rebuilds the in-memory room model.

### Bug Fixes

- Wait for cancelled Classic and Sliding Sync sibling tasks to finish before their outer loop exits, making loop completion a quiescence boundary for application-owned resets.
- Retain Classic first-sync filters, cursors, and full-state requests across transient sync errors until a successful response rebuilds the room model.
- Raise `SendRetryError` for encrypted room sends while a rejected Classic response is rebuilding the room cache, allowing callers to use their existing bounded recovery retry.

## 0.34.1

### Bug Fixes

- Make persisted sync recovery admission and completion tolerate missing, generation-divergent, and already-completed event rows after a crashed sync iteration, preventing permanent sync-loop crashes and duplicate-row constraint failures.

## 0.34.0

### Features

- Add typed `TimelineEventProvenance` metadata to the single durable timeline admission callback.
  Classic Sync distinguishes initial history from `since` continuations, and `/messages` recovery stays historical.
  Sliding Sync uses the validated `num_live` tail when present, clamps server counts to the returned timeline, treats ordinary continuations without the optional count as live, and treats initial or expanded responses without it as history.
  Historical sync rows retain their independent sync-origin durability and room-state behavior instead of overloading provenance.
  Store schema v7 preserves legacy recovery obligations and transport tokens, classifies their previously unrecorded provenance conservatively as history, and derives room-state application from their existing sync origin.
  Existing two-argument admission callbacks remain supported; new callbacks can accept provenance as a third argument.

### Bug Fixes

- Keep newly received timelines after every queued recovery row, including a recovered-only prefix, and use insertion order to break legacy sequence ties after restart.

## 0.33.0

### Breaking Changes

These only apply when `backfill_limited_timelines=True`; default behaviour is unchanged.

- `room_leave()` and `room_forget()` raise `LocalProtocolError` when called from
  any event callback — to-device, presence and account-data callbacks included,
  not only timeline ones. Those callbacks run while the sync response lock is
  held and the membership reset needs that lock, so the call would deadlock.
  Leave the room from the task that owns the sync loop instead.
- `close()` can now raise, and can block. It waits for the sync response lock
  and every started recovery callback, re-raising the first callback or
  finalization failure. A callback that never returns prevents `close()` from
  returning. It also raises `LocalProtocolError` if called from a serialized
  sync-loop timeline callback.
- Classic Sync and a to-device-enabled Sliding Sync connection cannot both
  consume to-device messages in one client generation, because their cursor
  formats are incompatible. A single `sync()` call claims that stream for the
  generation, so a later to-device Sliding Sync raises `LocalProtocolError`.

### Features

- Persist each room's sliding window token, so a restarted client can walk the gap its downtime left behind instead of dropping it.
  0.31.0 held the walk baseline in memory, which left the first limited or initial window after a restart unrecoverable — the one case the `/v3/sync` transport had always covered through its stored sync token.
  Live, a sliding reader torn down mid-flood and rebuilt from its store now loses nothing where it previously lost every event written while it was down.
  Persisted tokens are scoped to the own-membership event that earned them, join-to-join profile changes rotate that proof, and explicit membership loss fails closed.
  Servers without `$ME` support provide no trusted initial membership proof, so nio retains no baseline and reports later known discontinuities as unrecovered.
  Classic Sync serializes its opaque cursor selection and long poll, while Sliding Sync connections overlap except when they share the device to-device cursor; Classic and Sliding Sync cannot both consume to-device messages in one client generation because their cursor formats are incompatible.
  Bounded per-room and type-keyed account-data floors reject stale state and baseline changes without dropping unseen timeline events.
  Sync-family response application remains serialized within one replaceable client generation, so every accepted response slice is delivered once and a late pre-close response cannot mutate reused client state.
  Persistence requires `backfill_limited_timelines=True`, a store, and `backfill_persist_recovery` resolving to `True`; when unset, the latter follows `store_sync_tokens`.
  This migrates the store from schema v3 to v6, safely discards unscoped v4 token rows, records durable event-admission acceptance, and exports `SlidingWindowTokens` from `nio.store`.
- Add `AsyncClientConfig.backfill_persist_recovery`. It defaults to None,
  which follows `store_sync_tokens` as before, and can be set to True to
  persist recovery state while nio never reads or writes `next_batch`
  itself. Clients that decide for themselves when a sync token may be
  advanced — because they only trust a token once their own writes are
  durable — previously had to choose between that ownership and durable
  recovery. Recovery then resumes relative to whatever token the caller
  restores.

### Bug Fixes

- Persist same-generation pending-event resequencing so recovered history stays
  ahead of its held live window after a restart.
- Give every retried sync transport attempt a fresh ordering identity so a
  successful membership reset cannot make a later retry discard current room
  state while exposing its advanced cursor.
- Report a known limited Sliding Sync room as unrecovered when its persisted walk baseline cannot be trusted under missing or mismatched exact current own-membership proof.
  Fresh rooms without a prior baseline are not reported as lost, and historical joins outside `num_live` do not suppress a real gap.
- Add `AsyncClient.add_event_admission_callback()` as a pre-fanout durable admission boundary.
  Exactly one admission owner may be registered so durable side effects cannot be partially accepted by multiple callbacks.
  `CallbackNotAcceptedError` is valid only there and keeps the event pending when raised before acceptance or side effects.
  The same exception from an ordinary callback is too late to reject and has ordinary error behavior.
  Ordinary live callback errors acknowledge the event once, while ordinary recovered-history errors leave it pending for a later pump or restart.
- Drain started room callback work before computing the response plan that replaces room recovery state.
  This prevents a stale pre-drain plan from restoring an event whose callback already finished.
- Continue bounded forward recovery walks until the server exhausts the range instead of stopping when an event overlaps the live window.
- Apply successful leave or forget invalidation under response serialization without waiting for an in-flight sync long poll.
  Recovery-enabled membership changes called from a direct or inherited event-callback context raise before network I/O, while default recovery-disabled behavior remains uncoordinated.
- Serialize implicit Classic Sync cursor selection with its request so concurrent calls cannot send the same stale `since` token.
- Prevent a sync request from retrying or creating a new HTTP session after `close()` replaces its client generation.
- Preserve tuple or list caller Sliding Sync ranges when the initial recovery seed range is added.
- Require every `SlidingWindowToken` to carry a non-empty own-membership event ID matching its non-null database column.
- Document that `close()` cannot run from a sync-loop timeline callback; stop the sync loop there and await `close()` from its owner after the loop exits.
- Retain one keyed task for a recovered callback that outlives its pump deadline, then durably finish it before client shutdown so started callback side effects are neither cancelled nor replayed after restart.

## 0.32.0

### Features

- Add typed recovered and unrecovered room outcomes to classic and Sliding
  Sync responses when limited-timeline recovery is enabled, while preserving
  the server's original limited-timeline flag.

### Bug Fixes

- Report the server's `errcode` and `error` when a response fails its success
  schema. Responses are validated against the success schema before anything
  checks whether the body is an error, so an error body failed that validation
  and the warning named the first missing success field instead of the errcode
  the server sent, leaving the request outcome invisible in logs. Only those
  two fields are logged, since the rest of a body may hold user content.

## 0.31.0

### Features

- Recover limited timelines on the Simplified Sliding Sync transport too.
  `backfill_limited_timelines` previously only planned a walk for `/v3/sync`
  gaps, so `sliding_sync_forever()` dropped the events a limited window left
  behind with no walk and no warning (measured with a one-event window and
  200 concurrent writes: 10/40 lost serially, 170/200 concurrently). A
  sliding `pos` is a connection cursor rather than a `/messages` token, but
  each room's `prev_batch` is one, so consecutive windows now bound an
  ordinary forward walk: from the token held for the room to the one this
  window carries. The overlap that leaves is dropped by the existing
  de-duplication. `initial` rooms the client already tracks are walked the
  same way, which covers a room re-entering a list window and a connection
  the server expired. The token is per-process, so the first discontinuity
  after a client restart still has no baseline to walk from and is not
  recovered; the `/v3/sync` transport keeps recovering across restarts
  through its stored sync token.
- Widen the list ranges of a sliding connection's first request to
  `AsyncClientConfig.backfill_sliding_seed_rooms` (default 1000, 0 disables)
  while `backfill_limited_timelines` is on. A room outside the configured
  window is never sent, so the client holds no token for it and cannot
  recover the limited window it eventually arrives with: everything written
  to it before its first delivery is unreachable. Collecting a token for
  every room up front costs one larger response per connection and closes
  that hole — under a chaos pass with 20 rooms behind an 8-room window,
  4320 events, eight writers and a connection reset every round, the two
  sliding readers went from 267 and 269 events lost to none. The configured
  ranges apply from the second request on, so the steady-state window is
  unchanged.

## 0.30.1

### Bug Fixes

- Keep the recovered history of a limited-timeline walk that runs out of
  events. A forward `/messages` walk bounded by the window's token ends on
  a page with no `end` token — the spec's way of saying no further events
  are available — and both Synapse and Tuwunel stop short of the window's
  own events, so the live-overlap check never got the chance to close the
  gap. The exhausted page was treated as unprovable and every event the
  walk had already collected was discarded, losing the majority of events
  under concurrent writes on both servers (measured with
  `scripts/probe_classic_recovery.py --concurrent`: 173/200 events lost on
  Synapse, 148/200 on Tuwunel; zero after the fix). An unbounded walk with
  no target token still discards such a page, since there it cannot be
  told apart from the live edge.

## 0.30.0

### Features

- Make limited-timeline recovery durable per room with monotonic transport tokens, held live rows, restart resumption when encrypted sync-token storage is active, cross-transport event-ID de-duplication, and one later decrypted replay.
- Migrate the encrypted client store from schema v2 to schema v3 for recovery gaps and pending timeline events, encrypt pending event bodies with authenticated row identity under the existing store secret, and export `SyncRecoveryGaps` and `PendingTimelineEvents` from `nio.store`.

### Bug Fixes

- Preserve normal live callback exception propagation while recovered-history dispatch attempts every matching callback before acknowledgement.
- Keep room sends fail closed while that room has an unresolved recovery lane, and serialize concurrent classic and Sliding Sync recovery pumps.
- Treat HTTP 408, HTTP 429, and server errors as retryable recovery failures while terminal client errors release held live rows.
- Scan every accepted `/messages` page before using held live-window overlap as a boundary, cap durable recovered history across all open room generations, and atomically abandon only an unverified historical prefix that cannot fit before a complete boundary.
- Apply every joined room's state before fallible callbacks, preserve the full current timeline across own-join resets, retry Sliding Sync decryption after same-response room keys, enforce held-live limits on the first limited window, and preserve per-event to-device callback ordering.
- Remove delivered event bodies from durable de-duplication markers while retaining the event ID and encrypted-state bit required for replay suppression.

## 0.29.0

### Upstream Sync

- Rebase the fork onto matrix-nio 0.26.0 and incorporate subsequent upstream
  changes through room version 12 support. This brings in the vodozemac
  migration, a Python 3.10 minimum, the password-change API, parsing of
  unencrypted media in encrypted containers, flattened event helpers, and
  corrected room-redaction transaction IDs. See the 0.26.0 section below for
  its breaking changes and store-migration details.

## 0.28.0

### Features

- Add `AsyncClient.sliding_sync_forever()`, the MSC4186 Simplified Sliding
  Sync counterpart of `sync_forever()`. The loop threads the connection
  position and the to_device extension's `since` token between requests,
  restarts the connection transparently on `M_UNKNOWN_POS` (keeping the
  to-device token, which is independent of `pos`), sends outgoing to-device
  messages, uploads/queries/claims encryption keys, runs response callbacks
  for every response, and honours `stop_sync_forever()`, the `synced` event
  and `loop_sleep_time` exactly like the /v3/sync loop. Re-sent timeline
  windows (connection expiry, rooms re-entering a list window) are
  de-duplicated against a bounded per-room memory of dispatched event ids,
  so event callbacks never see the same event twice mid-run; the memory
  records whether an event could only be dispatched encrypted, letting
  exactly one decryptable replay through once the missing room key
  arrives (the same upgrade rule now applies to limited-timeline backfill
  recovery walks). Rooms flagged ``initial`` that the client already
  tracks are rebuilt from the snapshot rather than patched, so members and
  metadata removed while no connection was live cannot linger — the
  outbound group session is rotated so departed members stop receiving
  room keys, while data owned by other channels (tags, read markers,
  receipts, typing) survives the rebuild. Consecutive error responses back
  off exponentially (reusing
  the transport retry curve) instead of hot-looping, while the position is
  kept so a recovered server resumes where it left off. A failure in one
  of the loop's parallel requests cancels its siblings, so no orphaned
  long-poll can apply state after the loop has died.
- `AsyncClient.sliding_sync()` responses now update client state via
  `receive_response()`, mirroring `sync()`: rooms are created and updated
  from `required_state` (state events, membership, encryption flag) plus the
  server-computed summary fields (heroes, joined/invited counts,
  notification counts), heroes unknown to lazy member loading are seeded as
  members so display names and avatars resolve (marked invited when nobody
  else has joined; skipped for otherwise-empty rooms, whose heroes are
  members who left; an explicit empty heroes list clears stale ones —
  `SlidingSyncRoom.heroes` is now `None` when the field was absent), the
  server-computed top-level `name`/`avatar` are deliberately not applied to
  room state (deployed servers disagree on their meaning: Synapse mirrors
  the state events, Tuwunel sends calculated display values such as the DM
  partner's profile avatar), MSC4186 state deletion stubs are applied (room
  name,
  canonical alias, topic, avatar, join rules, guest access, history
  visibility, tombstone, power levels, space parent/child links, and member
  deletions — which also rotate the outbound session), invites appear in
  `client.invited_rooms` from stripped state, timelines are decrypted and
  dispatched through the registered event callbacks, and left/banned rooms
  are skipped like in /v3/sync.
- Parse the sliding sync `to_device`, `e2ee` and `account_data` extension
  payloads into typed fields on `SlidingSyncResponse` (`to_device_events`,
  `to_device_next_batch`, `device_key_count`, `device_list`,
  `account_data_events`, `room_account_data`) and feed them through the
  same handling as their /v3/sync counterparts: to-device decryption (room
  keys land before the timelines that need them), one-time-key counts
  (including an explicit zero for drained pools), device-list based key
  query invalidation, and global/per-room account data callbacks. Account
  data for rooms outside the sliding window is buffered (newest event per
  type, merged across responses) until the room appears. The raw
  `extensions` dict remains available byte-for-byte untouched (event
  parsers only ever see copies).
- Extend `scripts/live_sliding_sync_check.py` with `sliding_sync_forever`
  checks: invites and live messages arriving through the loop, clean
  shutdown, the `M_UNKNOWN_POS` rejection for unknown positions, a full
  encrypted round trip (room key over the to_device extension plus megolm
  timeline decryption) between two store-backed clients, and an opt-in
  `--restart-cmd` mode that restarts the homeserver under the running loop
  and asserts recovery without duplicate dispatch. All checks pass against
  Synapse 1.156 and Tuwunel, including the `--slam` stress pass with state
  processing active.

## 0.27.4

### Features

- Add opt-in recovery of events dropped by limited sync timelines
  (`AsyncClientConfig.backfill_limited_timelines`). When a room's sync
  timeline arrives with `limited: true`, the client pages `/messages`
  forwards from the token the sync continued from and dispatches the
  recovered gap through the normal event callbacks — oldest first, before
  the sync response's own events, decrypted like live events but never
  applied to room state. Gaps spanning a client restart are recovered when
  resuming from a stored or explicit since token; freshly joined rooms are
  never backfilled past our own join. Recovery dispatches only when the
  walk verifiably reaches the sync window; anything less (bounds, errors,
  stalls, the live edge) is discarded with a warning, so failure is always
  loud loss, never duplicates. All backfill for one sync response shares a
  single time budget (`backfill_timeout`) covering pagination and dispatch,
  including hanging callbacks. Disabled by default; behaviour with the
  flag off is identical to upstream nio.

### Bug Fixes

- Match sliding sync (simplified MSC4186) wire format to deployed servers:
  `pos`/`timeout`/`set_presence` as query parameters, unstable endpoint by
  default, `invite_state`/`unstable_expanded_timeline` response keys, and
  per-room notification counts. Renames `SlidingSyncRoom.timeline_events`
  to `timeline` (the wire name; nothing consumed the old attribute).

## 0.27.3

### Bug Fixes

- Apply `AsyncClientConfig.custom_headers` in the low-level `send()` transport
  so direct request paths, including cross-signing uploads, receive the same
  configured headers as high-level Matrix client methods.

## 0.27.2

### Features

- Add an opt-in `ClientConfig.replace_rotated_device_keys` policy: when a
  device re-uploads different, validly self-signed identity keys under an
  existing device id (e.g. a client that kept its access token but lost its
  crypto store), replace the stored identity and reset its earned trust
  instead of ignoring the new keys forever. Blacklisted devices stay
  blacklisted. Default off, preserving upstream behavior.

## 0.27.1

### Bug Fixes

- Preserve an explicit zero signed Curve25519 one-time-key count from `/sync`
  so clients replenish a fully drained Olm key pool.

## 0.27.0

### Features

- Surface unknown decrypted olm to-device events to client callbacks as
  `UnknownToDeviceEvent` instead of silently dropping them, so clients can
  receive custom encrypted to-device messages such as Element Call's
  `io.element.call.encryption_keys` frame keys. `DecryptedOlmT` and
  `Olm.decrypt_event` now include `UnknownToDeviceEvent` in their return
  types.

## [0.26.0] - 2026-07-23

### Breaking Changes
- [[#555]] Replace `libolm`/`python-olm` with `vodozemac` for end-to-end encryption
  - The `e2e` extra now depends on `vodozemac` instead of `python-olm`, and `libolm` is no longer required as a system dependency.
  - Existing encryption stores are migrated automatically on load. Stores created with a `libolm` pickle version older than `4` (roughly, pre-December 2021) can only be migrated if the optional `python-olm >= 3.2.7` package is installed at upgrade time; without it, those pickles cannot be read.
  - `hmac-sha256` is no longer offered as a SAS message authentication code; only `hkdf-hmac-sha256` is supported.
  - `Account.remove_one_time_keys()` has been removed, as it is no longer supported by the underlying library.
- [[#558]] Drop support for end-of-life `python3.8` and `python3.9`; the minimum supported version is now `python3.10`

### Features
- [[#540]] Add `unread_thread_notifications` to `SyncResponse`
- Add self-managed cross-signing for bot-style clients: `AsyncClient.ensure_cross_signing()` creates and persists master and self-signing keys next to the encryption store, uploads them (with an MSC3967-first flow and password-based UIA retry), and signs the account's own device so MSC4153-era clients keep sharing room keys with it. Requires pycryptodome >= 3.15 for Ed25519 signing.

### Bug Fixes
- [[#531]] Fix `get_openid_token`, the endpoint needs an empty JSON body
- [[#542]] Fix print for `FileResponse` when download is saved to file

### Miscellaneous Tasks
- [[#558]] Adopt `uv` for project management, apply `pyupgrade` and linting, and bump dependencies
- [[#556]] Unpin dependencies

### Dependencies

- Drop the unmaintained `atomicwrites` dependency in favor of a small internal
  stdlib-based atomic write helper with the same semantics
  (matrix-nio/matrix-nio#566).
- Allow peewee 4.x by relaxing the `e2e` extra constraint to
  `peewee>=3.14,<5`; the test suite passes against peewee 4.1.1
  (matrix-nio/matrix-nio#566).

[#555]: https://github.com/matrix-nio/matrix-nio/pull/555
[#558]: https://github.com/matrix-nio/matrix-nio/pull/558
[#540]: https://github.com/matrix-nio/matrix-nio/pull/540
[#531]: https://github.com/matrix-nio/matrix-nio/pull/531
[#542]: https://github.com/matrix-nio/matrix-nio/pull/542
[#556]: https://github.com/matrix-nio/matrix-nio/pull/556

## [0.25.4] - 2026-05-20

### Bug Fixes
- [[#2]] Persist vodozemac account state after inbound session creation consumes
  one-time keys, and handle unknown one-time-key session creation failures as
  recoverable decryption failures.

[#2]: https://github.com/mindroom-ai/mindroom-nio/pull/2

## [0.25.3] - 2026-05-04

### Features
- [[#1]] Add low-level MSC4186 Simplified Sliding Sync support, including request
  builders, async and HTTP client wrappers, typed response parsing, and stable
  plus unstable endpoint coverage.

### Bug Fixes
- [[#1]] Convert malformed nested sliding sync list and room payloads into
  `SlidingSyncError` instead of leaking parser exceptions.

### Notes
- Sliding sync responses are parsed as a separate low-level API and do not update
  the existing `/v3/sync` room state loop or `sync_forever()`.

[#1]: https://github.com/mindroom-ai/mindroom-nio/pull/1

## [0.25.2] - 2024-10-04

### Bug Fixes
- [[#523]] Utilize old media path for uploads (fix [[#520]])
- [[#521]] Fix type of call event version

### Miscellaneous Tasks
- [[#522]] Replace m2r2 with sphinx_mdinclude

[#523]: https://github.com/matrix-nio/matrix-nio/pull/523
[#521]: https://github.com/matrix-nio/matrix-nio/pull/521
[#522]: https://github.com/matrix-nio/matrix-nio/pull/522

## [0.25.1] - 2024-09-08

### Features
- [[#516]] Improve dependency resolution + tidy up dependencies
- [[#520]] Use authenticated media + Authorization header
  - This restores support for use on the popular [matrix.org](https://matrix.org) homeserver, which has recently [disabled unauthenticated media access](https://matrix.org/blog/2024/06/26/sunsetting-unauthenticated-media/).
  - Your homeserver MUST be compliant with matrix `v1.11`.

### Miscellaneous Tasks
- Fix `pytest-asyncio` warning during unit tests

[#516]: https://github.com/matrix-nio/matrix-nio/pull/516
[#520]: https://github.com/matrix-nio/matrix-nio/pull/520

## [0.25.0] - 2024-08-13

### Features
- [[#449]] Aggregated Event Relations + Threading + Threaded/Private Read Receipts
- [[#489]] Compliance with MSC2844. BREAKING CHANGE.
- [[#490]] Moved callback execution to separate methods.
- [[#499]] Add stop_sync_forever method, to gracefully exit sync_forever loop.

### Bug Fixes
- [[#471]] Changing room_messages to conform to MSC3567
- [[#482]] [[#483]] [[#486]] Spec-compliant bugfixes for joined_member, e2ee, RoomContext
- [[#495]] remove creator property from event
- [[#498]] Properly pass JSON content to DownloadResponse
- [[#508]] Add room_read_markers type hints

### Miscellaneous Tasks
- Tagged releases will automatically be published to PyPI
- Many dependency bumps

[#449]: https://github.com/matrix-nio/matrix-nio/pull/449
[#471]: https://github.com/matrix-nio/matrix-nio/pull/471
[#482]: https://github.com/matrix-nio/matrix-nio/pull/482
[#483]: https://github.com/matrix-nio/matrix-nio/pull/483
[#486]: https://github.com/matrix-nio/matrix-nio/pull/486
[#489]: https://github.com/matrix-nio/matrix-nio/pull/489
[#498]: https://github.com/matrix-nio/matrix-nio/pull/498
[#499]: https://github.com/matrix-nio/matrix-nio/pull/499
[#490]: https://github.com/matrix-nio/matrix-nio/pull/490
[#495]: https://github.com/matrix-nio/matrix-nio/pull/495
[#508]: https://github.com/matrix-nio/matrix-nio/pull/508


## [0.24.0] - 2024-01-18

### Miscellaneous Tasks

- [[#473]] Update pre-commit hooks, fix issues with sphinx-lint
- [[#472]] [[#475]] Add content to `built-with-nio`
- [[#468]] Bump `aiohttp` from 3.8.6 to 3.9.0
- [[#461]] Support `python3.12`
- [[#478]] Bump `pycryptodome` from 3.19.0 to 3.19.1

[#461]: https://github.com/poljar/matrix-nio/pull/461
[#468]: https://github.com/poljar/matrix-nio/pull/468
[#472]: https://github.com/poljar/matrix-nio/pull/472
[#473]: https://github.com/poljar/matrix-nio/pull/473
[#475]: https://github.com/poljar/matrix-nio/pull/475
[#478]: https://github.com/poljar/matrix-nio/pull/478

## [0.23.0] - 2023-11-17

### Bug Fixes

- [[#460]] Allow custom `ToDeviceEvent`s via `UnknownToDeviceEvent`
- [[#463]] Remove callback execution boilerplate + allow arbitrary callable/awaitable objects
- [[#457]] Fix schemas for `m.room.avatar` and `m.room.canonical_alias`
- [[#403]] Propagate `asyncio.CancelledError` in `sync_forever`

### Features

- [[#451]] Introduce the DM room account data (`m.direct`)

### Miscellaneous Tasks

- [[#458]] Update the `nio-bot` description
- [[#462]] Don't manually build `libolm` during tests + `pre-commit autoupdate`
- [[#464]] Bump `aiohttp` from 3.8.5 to 3.8.6

[#460]: https://github.com/poljar/matrix-nio/pull/460
[#458]: https://github.com/poljar/matrix-nio/pull/458
[#462]: https://github.com/poljar/matrix-nio/pull/462
[#451]: https://github.com/poljar/matrix-nio/pull/451
[#463]: https://github.com/poljar/matrix-nio/pull/463
[#464]: https://github.com/poljar/matrix-nio/pull/464
[#457]: https://github.com/poljar/matrix-nio/pull/457
[#403]: https://github.com/poljar/matrix-nio/pull/403


## [0.22.1] - 2023-10-9

### Bug Fixes
- [[#453]] Fix `ImportError` from when e2e is not installed

[#453]: https://github.com/poljar/matrix-nio/pull/453


## [0.22.0] - 2023-10-6

### Bug Fixes

- [[#434]] Fix space handling to account for Matrix spec ambiguities.

### Features

- [[#426]] Add a simple streamed response to download to files
- [[#436]] Add get space hierarchy capability
- [[#437]] Support for Token-Authenticated Registration
- [[#330]] Add `room_type` to `room_create` API function to allow for custom room types
- [[#351]] Add support for `m.reaction` events (Closes [[#174]])

### Miscellaneous Tasks

- [[#427]], [[#446]] Add `.readthedocs.yaml` v2 to support ReadTheDocs migration
- [[#440]] Remove `future` dependency
- [[#438]] Fix `jsonschema` deprecations
- [[#439]] Replace `cgi.parse_header()`
- [[#441]] Run `pre-commit autoupdate` to fix deprecation
- [[#442]] Introduce `ruff` as a `pre-commit` hook + run on whole codebase
- [[#445]] Update `pre-commit` hooks
- [[#447]] Replace ALL type comments with type hints
- [[#448]] Add `pyupgrade`, `async`, various `flake8`, `Perflint`, and more `ruff` linting rules

[#174]: https://github.com/poljar/matrix-nio/issues/174
[#434]: https://github.com/poljar/matrix-nio/pull/434
[#426]: https://github.com/poljar/matrix-nio/pull/426
[#436]: https://github.com/poljar/matrix-nio/pull/436
[#437]: https://github.com/poljar/matrix-nio/pull/437
[#330]: https://github.com/poljar/matrix-nio/pull/330
[#351]: https://github.com/poljar/matrix-nio/pull/351
[#427]: https://github.com/poljar/matrix-nio/pull/427
[#446]: https://github.com/poljar/matrix-nio/pull/446
[#440]: https://github.com/poljar/matrix-nio/pull/440
[#438]: https://github.com/poljar/matrix-nio/pull/438
[#439]: https://github.com/poljar/matrix-nio/pull/439
[#441]: https://github.com/poljar/matrix-nio/pull/441
[#442]: https://github.com/poljar/matrix-nio/pull/442
[#445]: https://github.com/poljar/matrix-nio/pull/445
[#447]: https://github.com/poljar/matrix-nio/pull/447
[#448]: https://github.com/poljar/matrix-nio/pull/448


## [0.21.2] - 2023-7-17

### Bug Fixes

- [[#423]] Revert [[#411]] due to backwards-incompatibilities.

[#423]: https://github.com/poljar/matrix-nio/pull/423

## [0.21.1] - 2023-7-16

### Bug Fixes

- [[#422]] `async_client.whoami` will alter the state of `async_client` correctly, and accept all spec-compliant fields.

### Miscellaneous Tasks

- [[#420]] Add `python3.8` tests to workflow.

[#422]: https://github.com/poljar/matrix-nio/pull/422
[#420]: https://github.com/poljar/matrix-nio/pull/420

## [0.21.0] - 2023-7-14

### Breaking Changes

- [[#416]] Drop support for end-of-life `python3.7`
- [[#413]] Drop usage of `logbook` in favor of standard library `logging`
  - This fixes an issue where logging was effectively disabled by default.

### Features

- [[#409]] Support m.space.parent and m.space.child events
- [[#418]] Add ability to knock on a room, and enable knocking for a room

### Documentation

- Add documentation on how to configure `logging`
- Note in `README` that room upgrades/tombstone events *are* supported

### Miscellaneous Tasks

- [[#401]] Removing skip for passing test
- [[#417]] Add type hints
- [[#406]] [[#407]] [[#414]] Add content to `built-with-nio`

### Bug Fixes

- [[#408]] Properly generate code coverage
- [[#411]] Fixed bug in Event Callbacks

[#416]: https://github.com/poljar/matrix-nio/pull/416
[#413]: https://github.com/poljar/matrix-nio/pull/413
[#409]: https://github.com/poljar/matrix-nio/pull/409
[#418]: https://github.com/poljar/matrix-nio/pull/418
[#401]: https://github.com/poljar/matrix-nio/pull/401
[#417]: https://github.com/poljar/matrix-nio/pull/417
[#406]: https://github.com/poljar/matrix-nio/pull/406
[#407]: https://github.com/poljar/matrix-nio/pull/407
[#414]: https://github.com/poljar/matrix-nio/pull/414
[#408]: https://github.com/poljar/matrix-nio/pull/408
[#411]: https://github.com/poljar/matrix-nio/pull/411

## [0.20.2] - 2023-3-26

### Miscellaneous Tasks

- Upgrade dependencies
- Various test, formatting, type hinting fixes
- Update GitHub Workflow Actions versions for CI
- [[#384]] Add content to `built-with-nio`

### Bug Fixes

- [[#335]] Default to the configured request timeout when syncing
- [[#354]] Fix `first_sync_filter` parameter of `AsyncClient.sync_forever`
- [[#357]] Element exports keys without required fields
- [[#396]] Fix `timeline->limited` being required

[#384]: https://github.com/poljar/matrix-nio/pull/384
[#335]: https://github.com/poljar/matrix-nio/pull/335
[#354]: https://github.com/poljar/matrix-nio/pull/354
[#357]: https://github.com/poljar/matrix-nio/pull/357
[#396]: https://github.com/poljar/matrix-nio/pull/396

## [0.20.1] - 2022-11-09

### Bug Fixes

- Fix Python 3.11 compatibility

## [0.20.0] - 2022-09-28

### Bug Fixes

- Fix import sequence errors.
- Exclude `tests/data/` from pre-commit workflow.
- Only accept forwarded room keys from our own trusted devices

### Documentation

- Mention that room key backups are unsupported.
- Add matrix-webhook to built-with-nio
- Add matrix-asgi to built-with-nio

### Features

- Add `mxc` URI parameter to `AsyncClient.download` and deprecate `server_name` and `media_id`.

### Miscellaneous Tasks

- Remove the usage of the imp module
- Fix our import order
- Fix a bunch of typos
- Remove key re-sharing
- Remove some unnecessary test code
- Add poetry to the test requirements
- Style fixes
- Sort our imports

### Refactor

- Clean up and make a bunch of tests more consistent

### Styling

- Add config for `pre-commit`.
- Fix formatting using `black` and `isort`.
- Convert from `str.format` to f-strings.

### Testing

- Update test for `AsyncClient.download`.
- Fix our async tests

### Ci

- Add `black` and `isort`.

## 0.19.0 - 2022-02-04

- [[#296]] Allow creating spaces
- [[#293]] Add special check for "room_id" in PushEventMatch
- [[#291]] Send empty object with m.read receipt
- [[#288]] Update aiohttp-socks dependency
- [[#286]] Fix type annotation for async callbacks in add_event_callback
- [[#285]] Remove chain_index field when sending room keys
- [[#281]] Add support for room upgrades

[#296]: https://github.com/poljar/matrix-nio/pull/296
[#293]: https://github.com/poljar/matrix-nio/pull/293
[#291]: https://github.com/poljar/matrix-nio/pull/291
[#288]: https://github.com/poljar/matrix-nio/pull/288
[#286]: https://github.com/poljar/matrix-nio/pull/286
[#285]: https://github.com/poljar/matrix-nio/pull/285
[#281]: https://github.com/poljar/matrix-nio/pull/281

## 0.18.7 - 2021-09-27

- [[#277]] Allow setting custom headers with the client.
- [[#276]] Allow logging in using an email.
- [[#273]] Use the correct json format for login requests.

[#277]: https://github.com/poljar/matrix-nio/pull/277
[#276]: https://github.com/poljar/matrix-nio/pull/276
[#273]: https://github.com/poljar/matrix-nio/pull/273

## 0.18.6 - 2021-07-28

- [[#272]] Allow the mimetype to be in the info for encrypted images

[#272]: https://github.com/poljar/matrix-nio/pull/272

## 0.18.5 - 2021-07-26

- [[1f17a20]] Fix errors due to missing keys in syncs

[1f17a20]: https://github.com/poljar/matrix-nio/commit/1f17a20ca818c1c3a0c2e75fdc64da9c629eb5f9

## 0.18.4 - 2021-07-14

- [[#265]] Fix parsing syncs missing invite/join/leave rooms

[#265]: https://github.com/poljar/matrix-nio/pull/265

## 0.18.3 - 2021-06-21

- [[#264]] Allow for devices in keys query that have no signatures

[#264]: https://github.com/poljar/matrix-nio/pull/264

## 0.18.2 - 2021-06-03

- [[#261]] Use the IV as is when decrypting attachments
- [[#260]] Always load the crypto data, even if a new account was made

[#260]: https://github.com/poljar/matrix-nio/pull/260
[#261]: https://github.com/poljar/matrix-nio/pull/261

## 0.18.1 - 2021-05-07

- [[#258]] Fix sticker event parsing

[#258]: https://github.com/poljar/matrix-nio/pull/256

## 0.18.0 - 2021-05-06

- [[#256]] Upgrade our dependencies
- [[#255]] Relax the sync response json schema
- [[#253]] Support the BytesIO type for uploads
- [[#252]] Add a sticker events type

[#256]: https://github.com/poljar/matrix-nio/pull/256
[#255]: https://github.com/poljar/matrix-nio/pull/255
[#253]: https://github.com/poljar/matrix-nio/pull/253
[#252]: https://github.com/poljar/matrix-nio/pull/252

## 0.17.0 - 2021-03-01

- [[#228]] Add support for global account data
- [[#222]] Add support for push rules events and API
- [[#233]] Treat `device_lists` in `SyncResponse` as optional
- [[#239]] Add support for authenticated `/profile` requests
- [[#246]] Add support for SOCKS5 proxies

[#228]: https://github.com/poljar/matrix-nio/pull/228
[#222]: https://github.com/poljar/matrix-nio/pull/222
[#233]: https://github.com/poljar/matrix-nio/pull/233
[#239]: https://github.com/poljar/matrix-nio/pull/239
[#246]: https://github.com/poljar/matrix-nio/pull/246

## 0.16.0 - 2021-01-18

- [[#235]] Expose the whoami API endpoint in the AsyncClient.
- [[#233]] Treat device lists as optional in the Sync response class.
- [[#228]] Add support for account data in the AsyncClient.
- [[#223]] Percent encode user IDs when they appear in an URL.

[#235]: https://github.com/poljar/matrix-nio/pull/235
[#233]: https://github.com/poljar/matrix-nio/pull/233
[#228]: https://github.com/poljar/matrix-nio/pull/228
[#223]: https://github.com/poljar/matrix-nio/pull/223

## 0.15.2 - 2020-10-29

### Fixed

- [[#220]] Copy the unencrypted `m.relates_to` part of an encrypted event into the
  decrypted event.

[#220]: https://github.com/poljar/matrix-nio/pull/220

## 0.15.1 - 2020-08-28

### Fixed

- [[#216]] `AsyncClient.room_get_state_event()`: return a
  `RoomGetStateEventError` if the server returns a 404 error for the request
- [[ffc4228]] When fetching the full list of room members, discard the members
  we previously had that are absent from the full list
- [[c123e24]] `MatrixRoom.members_synced`: instead of depending on the
  potentially outdated room summary member count, become `True` when the
  full member list has been fetched for the room.

[#216]: https://github.com/poljar/matrix-nio/pull/216
[ffc4228]: https://github.com/poljar/matrix-nio/commit/ffc42287c22a1179a9be7d4e47555693417f715d
[c123e24]: https://github.com/poljar/matrix-nio/commit/c123e24c8df81c55d40973470b825e78fd2f92a2

## 0.15.0 - 2020-08-21

### Added

- [[#194]] Add server discovery info (.well-known API) support to AsyncClient
- [[#206]] Add support for uploading sync filters to AsyncClient
- New [examples] and documentation improvements

### Fixed

- [[#206]] Fix `AsyncClient.room_messages()` to not accept filter IDs, using
  one results in a server error
- [[4b6ea92]] Fix the `SqliteMemoryStore` constructor
- [[4654c7a]] Wait for current session sharing operation to finish before
  starting a new one
- [[fc9f5e3]] Fix `OverflowError` occurring in
  `AsyncClient.get_timeout_retry_wait_time()` after a thousand retries

[#194]: https://github.com/poljar/matrix-nio/pull/194
[#206]: https://github.com/poljar/matrix-nio/pull/206
[4b6ea92]: https://github.com/poljar/matrix-nio/commit/4b6ea92cb69e445bb39bbfd83948b40adb8a23a5
[4654c7a]: https://github.com/poljar/matrix-nio/commit/4654c7a1a7e39b496b107337977421aeb5953974
[fc9f5e3]: https://github.com/poljar/matrix-nio/commit/fc9f5e3eda25ad65936aeb95412a26af73cedf6a
[examples]: https://matrix-nio.readthedocs.io/en/latest/examples.html

## 0.14.1 - 2020-06-26

### Fixed

- [[238b6ad]] Fix the schema for the devices response.

[238b6ad]: https://github.com/poljar/matrix-nio/commit/238b6addaaa85b994552e00007638b0170c47c43

## 0.14.0 - 2020-06-21

### Added

- [[#166]] Add a method to restore the login with an access token.

### Changed

- [[#159]] Allow whitespace in HTTP headers in the HttpClient.
- [[42e70de]] Fix the creation of PresenceGetError responses.
- [[bf60bd1]] Split out the bulk of the key verification events into a common module.
- [[9a01396]] Don't require the presence dict to be in the sync response.


### Removed

- [[cc789f6]] Remove the PartialSyncResponse. This is a breaking change, but
  hopefully nobody used this.

[#166]: https://github.com/poljar/matrix-nio/pull/166
[#159]: https://github.com/poljar/matrix-nio/pull/159
[42e70de]: https://github.com/poljar/matrix-nio/commit/42e70dea945ae97b69b41d49cb57f64c3b6bd1c4
[cc789f6]: https://github.com/poljar/matrix-nio/commit/cc789f665063b38be5b4146855e5204e9bc5bdb6
[bf60bd1]: https://github.com/poljar/matrix-nio/commit/bf60bd19a15429dc03616b9be11c3a205768e5ad
[9a01396]: https://github.com/poljar/matrix-nio/commit/9a0139673329fb82abc59496025d78a34b419b77

## 0.13.0 - 2020-06-05

### Added

- [[#145]] Added the `room_get_event()` method to `AsyncClient`.
- [[#151]] Added the `add_presence_callback` method to base `Client`.
- [[#151]] Added the `get_presence()` and `set_presence()` methods
  to `AsyncClient`.
- [[#151]] Added the `presence`, `last_active_ago`, `currently_active` and
  `status_msg` attributes to `MatrixUser`
- [[#152]] Added a docker container with E2E dependencies pre-installed.
- [[#153]] Added the `add_room_account_data_callback` method to base `Client`.
- [[#153]] Added the `fully_read_marker` and `tags` attributes to `MatrixRoom`.
- [[#156]] Added the `update_receipt_marker()` method to `AsyncClient`.
- [[#156]] Added the `unread_notifications` and `unread_highlights` attributes
  to `MatrixRoom`.

### Changed

- [[#141]] Improved the upload method to accept file objects directly.

[#141]: https://github.com/poljar/matrix-nio/pull/141
[#145]: https://github.com/poljar/matrix-nio/pull/145
[#151]: https://github.com/poljar/matrix-nio/pull/151
[#152]: https://github.com/poljar/matrix-nio/pull/152
[#153]: https://github.com/poljar/matrix-nio/pull/153
[#156]: https://github.com/poljar/matrix-nio/pull/156

## 0.12.0 - 2020-05-21

### Added

- [[#140]] Added the `update_device()` method to the `AsyncClient`.
- [[#143]] Added the `login_info()` method to the `AsyncClient`.
- [[c4f460f]] Added support for the new SAS key agreement protocol.

### Fixed

- [[#146]] Fix room summary updates when new summary doesn't have any
  attributes.
- [[#147]] Added missing requirements to the test requirements file.

[#140]: https://github.com/poljar/matrix-nio/pull/140
[#143]: https://github.com/poljar/matrix-nio/pull/143
[#146]: https://github.com/poljar/matrix-nio/pull/146
[#147]: https://github.com/poljar/matrix-nio/pull/147
[c4f460f]: https://github.com/poljar/matrix-nio/commit/c4f460f62c9543a76eaf1dad4be8ff5ae9312243

## 0.11.2 - 2020-05-11

### Fixed

- Fixed support to run nio without python-olm.
- Fixed an incorrect raise in the group sessions sharing logic.
- Handle 429 errors correctly even if they don't contain a json response.

## 0.11.1 - 2020-05-10

### Fixed

- Fix a wrong assertion resulting in errors when trying to send a message.

## 0.11.0 - 2020-05-10

### Added

- Kick, ban, unban support to the AsyncClient.
- Read receipt sending support in the AsyncClient.
- Read receipt parsing and emitting.
- Support token login in the AsyncClient login method.
- Support for user registration in the BaseClient and AsyncClient.
- Support for ID based filters for the sync and room_messages methods.
- Support filter uploading.

### Changed

- Convert attrs classes to dataclasses.
- Fire the `synced` asyncio event only in the sync forever loop.

### Fixed

- Don't encrypt reactions.
- Properly put event relationships into the unencrypted content.
- Catch Too Many Requests errors more reliably.
- Better room name calculation, now using the room summary.

### Removed

- Removed the legacy store.
