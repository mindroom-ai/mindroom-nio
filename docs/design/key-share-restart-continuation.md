# Interactive key sharing across owned restart

## Failure and required behavior

Owned preparation already publishes an unverified own-device room-key request
as durable callback Work. Olm keeps the corresponding approval only in
`key_request_from_untrusted`, which starts empty after reconstruction. Replaying
the callback consequently cannot continue the request. The approval also
disappears if its callback was acknowledged and its Frame retired before restart.

The real owned-store regression covers both boundaries. It retains actual Olm
and Megolm sessions, prepares an incoming request, settles its source record,
optionally acknowledges its callback and retires its Frame, then reconstructs
the owned client. Both cases currently fail through `continue_key_share` with an
unknown pending request.

Continuation has a second boundary: the callback runs after the Frame's outbound
plan was frozen. A successful continuation can advance an Olm session and append
ciphertext only to the live queue. A crash before another Frame captures that
queue loses the approved share. Both boundaries belong to this fix.

## Ownership and lifetime

Add a narrow account-owned key-share carrier independent of callback Work and
Frame lifetime. Its stored envelope binds the existing account, stream and
transport identity, request identity and committed revision. Validate canonical
payloads, digests, request fields and target identities at the disk boundary.
Use the existing SQLite owner and transaction; no worker, synthetic source Frame,
human-held Frame, generic crypto queue, or second semantic-event ledger.

Each retained entry contains the original canonical request and one state:

- Awaiting approval. Reconstruct this into Olm's pending approval map before any
  callback can run. It remains discoverable through `get_active_key_requests`
  after callback acknowledgment and Frame retirement. It has no automatic expiry.
- Waiting for an Olm session after explicit continuation. Retain that request's
  claim context until the next ordinary preparation takes ownership.
- Encrypted message after explicit continuation. Retain the exact generated
  ciphertext until ordinary preparation transfers it into outbound maintenance.

Only this interaction's context and message are retained. Unrelated verification,
key-query, unwedging and application send queues are not snapshotted.

The public Client approval methods keep their synchronous signatures. During
owned ingestion they delegate to the owned transaction. Ordinary clients retain
their existing path. A stale or poisoned owned client must fail before mutation.
Direct mutation of Olm's private queues is not an approval interface.

## Trust, cancellation and retry

Persisting or replaying a callback does not authorize sharing. Verification alone
does not continue a request. Explicit continuation checks the currently stored
request and current device trust through the existing Olm sharing policy.
An unverified request stays pending and returns false without generating a key.
Missing sessions use the existing claim and response path. Reconstructed claim
work rechecks trust when it can encrypt; it never turns an unverified device into
an approved recipient merely because a prior process accepted continuation.

Local cancellation durably removes an awaiting approval. Incoming cancellation
is applied during ordered preparation and removes its matching pending request
in the same transaction as its callback Work. Callback replay does not recreate
a cancelled request. Once continuation has created an encrypted message, local
cancellation retains the existing meaning: it does not recall an already
continued share. A repeated continuation while its durable handoff remains must
reuse that handoff, not encrypt another message. Unknown or conflicting requests
must not gain approval through replay.

## Atomic capture and transfer

During source preparation, capture pending approvals in the same transaction as
the crypto changes and prepared Work. A preparation failure rolls back all three
and poisons any client whose crypto memory changed.

During explicit continuation, authenticate the retained request, run the existing
Olm decision and persist its resulting claim or ciphertext in the same owner
transaction as ratchet changes and approval removal. A failure after crypto
mutation poisons the client and requires fresh reconstruction. Callback
acknowledgment is a later independent operation and cannot discard this state.
Keeping an encrypted message visible in the ordinary live message queue does not
replace its retained handoff.

Claim handoffs must not alter a still-running older Frame's frozen claim context.
They join the live collection path only when the next ordinary Frame can own
them. At that point its source cancellations and current trust policy apply in
the normal event order. Encrypted handoffs keep their exact bytes.

The next ordinary preparation consumes retained handoffs and freezes their
existing outbound maintenance plan atomically. Before commit, restart sees the
handoff. After commit, restart sees the prepared Frame and its stable send body
and transaction ID. Network errors and lost responses retain that existing plan;
maintenance response application and Frame retirement keep their current rules.
No network call occurs inside the preparation or approval transaction.

Callbacks remain outside SQLite transactions. Callback failure, cancellation,
receipt deduplication and acknowledgment retry retain their existing delivery
semantics. Retained approval state does not depend on whether a callback is
invoked again. Owned sends continue through the coordinator's existing outbound
maintenance path; this change does not introduce a second sender.

## Implementation and verification plan

1. Keep the two real owned restart cases red before production changes.
2. Add the authenticated key-share carrier and load pending approvals on owned
   attachment. Capture collection/cancellation with normal preparation.
3. Route owned Client continuation/cancellation through the owner transaction;
   retain only the resulting interaction's claim or encrypted message.
4. Transfer handoffs through ordinary preparation and existing outbound
   maintenance. Preserve old-Frame claim isolation and exact encrypted retry.
5. Extend owned tests for acknowledgment before restart, callback replay without
   duplicate encryption, explicit and incoming cancellation, unverified and
   changed trust, missing-session claim, crash before/after continuation commit,
   crash at handoff transfer, and lost send response followed by restart.
6. Assert real persisted crypto and decrypt the actual outgoing key on the
   requesting device. Exercise malformed/corrupted retained rows and ownership
   refusal before callback, network or crypto effects. Keep ordinary-client
   approval behavior covered.
7. Run focused crypto/owned tests, type checking, repository hooks and the full
   suite. Remove the known-gap paragraph only after those boundaries pass and
   independent review accepts the final diff.

Earlier unmerged ingestion schemas need no compatibility path. Adoption of
supported ordinary stores and all existing ownership/corruption checks remain.
