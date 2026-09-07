# Encrypted-room persistence after the durable sync cutover

PR #58 moves ordinary asynchronous encrypted-room saves off the event loop.
The shared Classic/Sliding interpreter identifies the set to persist and yields
one internal persistence item at the existing save boundary. The host executes
it: synchronous clients save directly, asynchronous clients await their store
policy, and the durable collector saves inside its existing preparation
transaction. This item is not a user callback or a persisted durable record.

Built-in disk stores opt into worker writes. Memory stores and custom subclasses
stay on their owning thread unless they explicitly declare
`supports_threaded_encrypted_room_writes = True`. An existing outer
transaction also keeps the write inline. The durable store is a non-opted-in
subclass, and its collector uses the synchronous store operation directly.
Moving a durable write onto another connection would break its transaction and
is not part of this change.

The worker uses one connection context and awaits completion through the existing
cancellation-draining helper. It closes its thread-local connection afterward.
Encrypted-room queries execute against the owning database explicitly; they do
not mutate shared Peewee model bindings while other clients process responses.
Empty sets need no save. No long-lived writer, queue or new retry policy is added.

Potentially mutating store transactions use `BEGIN IMMEDIATE`, including the
worker save. SQLite reserves the writer before any account lookup, avoiding a
read-to-write lock upgrade racing with concurrent key maintenance on the same
store. The existing SQLite timeout bounds contention; nested operations remain
savepoints in their caller's transaction. A synchronous store operation can still
wait for an active worker write and block the event loop during that wait. This
change does not promise nonblocking behavior for all database operations.

The original reported 6.5-second stall did not reproduce on the merged PR #55
tree. In the retained three-sample local probe, 200-room initial saves took
5.73/6.25 ms for ordinary Classic/Sliding and 1.71/1.65 ms inside durable
preparation. A controlled 200 ms exclusive SQLite lock delayed the ordinary
event loop by 233 ms. This change addresses responsiveness during slow ordinary
saves; it does not claim faster durable preparation or concurrent replies.
Evidence is indexed by the existing capacity workspace under
`durable-sync-kernel/review-invalidation-20260906/native-loop`.

Verification covers Classic and Sliding async responsiveness, cancellation,
worker connection cleanup, memory/custom stores, concurrent database isolation,
and preservation of ordinary outer-transaction and durable restart rollback.

Qualification: 935 tests passed, three skipped; mypy reports zero errors across
60 source files; repository hooks pass. Self-review found and fixed the same-store
writer race; the subsequent review found no further actionable issues. Regression
tests cover concurrent sync and key-upload/key-query responses; removing either
writer reservation restores a reproduced failure. The final PR adds 59 net
production lines, compared with 55 in the original patch; published branch
history is preserved.
