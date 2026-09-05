# Type Correctness and Work Reuse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> baspowers:subagent-driven-development to implement the production tasks in
> sequence. The controller owns design updates, final verification, and push.

**Goal:** Reach zero mypy errors and remove measured repeated Work decoding.
**Architecture:** Keep existing runtime boundaries and correct their types;
repair demonstrated error handling, then reuse one authenticated immutable Work
value when its complete stored input is unchanged.
**Tech Stack:** Python >=3.12, mypy, pytest, SQLite/Peewee, asyncio, uv, pre-commit.
**Spec:** [Type correctness and measured delivery cost](type-correctness-and-delivery-cost.md).

## Global Constraints

- The durable ingestion contract remains binding, including exact validation,
  ownership, atomic crypto publication, stable replay, and deliberate limits.
- No runtime dependency, backend upgrade, storage format, delivery API, public
  ingestion configuration, batching mode, or callback timing change.
- Development stub packages and narrow verified declarations are permitted.
  Do not replace precise types with Any, remove annotations, exclude checked
  source, ignore whole modules, or suppress errors to reach zero.
- Preserve MindRoom factory, batch, and settlement names/signatures.
- Reproduce behavior defects before changing expectations. Annotation-only
  corrections use existing behavioral coverage; do not invent private-state
  failures or add tests that merely mirror annotations.
- Use uv run or uv sync; prefer repository pre-commit hooks. No Claude consultation,
  amendments, new branches, or docs/baspowers commits. Use the existing worktree.
- Scan proposed content/metadata before repository writes, stage explicit paths,
  and inspect staged scope before each commit. No live cutover or deployment.
- Final canonical check: `MYPYPATH=src uv run --no-sync mypy -p nio
  --warn-redundant-casts --no-incremental`, with zero errors across the package.

## Task 1: Type dependency boundaries and crypto wrappers

Files: pyproject.toml, uv.lock; narrow declarations under stubs/ for olm and
playhouse.sqliteq; src/nio/schemas.py, store/_ingestion_store_owner.py,
store/_sync_journal_preflight.py, crypto/sessions.py, crypto/async_attachments.py,
crypto/cross_signing.py, crypto/olm_machine.py. Existing tests:
tests/async_attachment_test.py, sessions_test.py, encryption_test.py,
store_test.py, store_binding_test.py, and ingest/store_owner_test.py.

Interfaces: preserve installed library behavior and optional legacy imports.
The native inbound-session result is tuple[vodozemac.Session, bytes]. Existing
AsyncDataT remains the shared accepted attachment-input type. Native/private
methods absent from upstream stubs keep their existing signatures and effects.

- [ ] Record current canonical diagnostics, then add types-jsonschema,
  types-aiofiles, and types-peewee as development dependencies. Refresh only the
  necessary lock entries and verify locked installation. Configure a portable
  local stub search path if declarations need it; retain all 70 source files.
- [ ] Inspect actual optional/private/native API definitions before writing
  declarations. A local optional Olm declaration covers only used Account
  pickle methods; the queue-store declaration subclasses Peewee's SqliteDatabase.
  Do not install a legacy runtime backend solely for checking.

```python
# Optional legacy import declaration; no runtime implementation.
class Account:
    @classmethod
    def from_pickle(cls, pickle: bytes, passphrase: str = "") -> Account: ...
    def pickle(self, passphrase: str = "") -> bytes: ...
```

- [ ] Correct LeasedSqliteDatabase constructor forwarding types and declare its
  existing timeout/connection hook interface truthfully. Bind omitted
  peewee.sort_models through a precise verified callable declaration if needed.
  Preserve leases, connection arguments, hooks, epoch fences, and all four
  explicit runtime model bindings. Correct preflight path narrowing exposed by
  published stubs without widening supported input kinds.
- [ ] Type jsonschema format callbacks for arbitrary JSON input. Add a focused
  failing case for applying the string format to a non-string JSON value, using
  the library format contract; preserve schema type rejection separately.

```python
assert Checker.check(42, "user_id") is None  # schema owns non-string type checks
with pytest.raises(ValidationError):
    validate_json(42, {"type": "string", "format": "user_id"})
```

- [ ] Correct attachment type aliases, inbound-session tuple return, concrete
  message-index cache type, and verified ECC/native declaration gaps. Preserve
  deferred first decryption, exact pickle conversions, and cryptographic bytes.
  For dynamic decrypted-event room_id assignment, express the existing dynamic
  field accurately; change bad-event behavior only after a supported reproducer.

```python
def create_inbound_session(
    self, identity_key: str,
    message: vodozemac.PreKeyMessage | vodozemac.AnyOlmMessage,
) -> tuple[vodozemac.Session, bytes]:
    if isinstance(message, vodozemac.AnyOlmMessage):
        pre_key_message = message.to_pre_key()
        assert pre_key_message
        message = pre_key_message
    return self._account.create_inbound_session(
        vodozemac.Curve25519PublicKey.from_base64(identity_key), message,
    )
```

- [ ] Run focused dependency/crypto/store tests and canonical mypy; record the
  remaining diagnostic inventory for later tasks. Run hooks on changed files,
  inspect runtime changes and production size, then commit with rationale.

## Task 2: Correct request and response contracts

Files: src/nio/api.py, responses.py, rooms.py, events/invite_events.py.
Tests: tests/api_test.py, responses_test.py, room_test.py and existing event tests.

Interfaces: public_rooms returns tuple[str, str, str | None], with serialized
JSON body. Optional fields permitted by wire schemas stay optional. File-response
factories accept their existing success bodies and error dictionaries.

- [ ] Correct heterogeneous query dictionary types and nested request payload
  declarations using their real JSON shapes. Fix implicit optional defaults and
  make GET/POST control flow exhaustive without adding a fallback response.

```python
# Both actual branches return; POST already serializes its body.
filter_room_types: list[str | None] | None = None
# public_rooms result annotation:
# tuple[str, str, str | None]
```

- [ ] Align InviteMemberEvent.prev_membership, interactive registration fields,
  hierarchy next_batch, and room-message end with schema-permitted absence.
  Fix response-factory input/return declarations around existing construction;
  do not substitute a different error class just to satisfy a narrow annotation.
- [ ] Resolve known-member sorting's optional lookup type from its actual
  invariant: the synchronous comprehension iterates self.users, and known
  MatrixUser names fall back to user IDs. Preserve name/avatar behavior; no
  new unnamed-member failure test is justified by this diagnostic.
- [ ] Run API/response/room/event tests and canonical mypy. Verify wire payloads
  and public return values are unchanged, run hooks, review diff, then commit.

## Task 3: Correct client and transport contracts and real error paths

Files: src/nio/client/base_client.py, client/async_client.py, http.py;
only directly required shared declarations if Task 2 exposes a precise mismatch.
Tests: tests/async_client_test.py, client_test.py, http_test.py, http2_test.py,
public_sync_compatibility_test.py and existing attachment tests.

Interfaces: callbacks are functions returning None or Awaitable[None]; preserve
callback order and re-entry. Upload input providers keep existing synchronous
and asynchronous input support; the internal _send provider is awaited. Existing
error-result classes remain the error channel for permission/upgrade operations.

- [ ] Correct callback callable/filter declarations, explicit filtered None
  return, type aliases, receipt thread-ID narrowing, response-class selection,
  and separate factory argument/body/path/JSON locals. Preserve dynamic event
  metadata using accurate fields or existing setattr behavior, not blanket Any.
- [ ] Express awaited internal providers and aiohttp request values correctly.
  Preserve configured timeout and SSL semantics, including default verification
  and explicit ssl=False. Remove obsolete suppression comments when types now
  describe the actual call.

```python
# Internal provider contract, distinct from public upload inputs:
data_provider: Callable[[int, int], Awaitable[AsyncDataT]] | None
# Callback contract:
callback: Callable[..., Awaitable[None] | None]
```

- [ ] Add failing whoami-error cases for has_permission, has_event_permission,
  and the v12 upgrade path. Use existing client/HTTP fixtures and assert the
  appropriate error result and no dependent remote writes. Handle absent v12
  power-level state clearly before accessing its users map.

```python
result = await client.has_permission(room_id, "kick")
assert isinstance(result, ErrorResponse)
# HTTP fixture supplies WhoamiError; no following state-changing request occurs.
```

- [ ] Add a real download response with no content-disposition filename and
  save_to pointing to a directory. Require ValueError before any file opens;
  an explicit destination file remains usable for the same unnamed response.
  Preserve existing named downloads and error-body parsing.

```python
with pytest.raises(ValueError, match="filename"):
    await client.download(mxc, save_to=directory)
assert list(directory.iterdir()) == []
```

- [ ] Correct HTTP/2 header/event iterable types, request-header method/instance
  field typing, and numeric reset codes. Fix ConnectionTerminated's loop-variable
  typo while retaining its existing logging/pass behavior. Existing transport
  tests suffice for annotation and diagnostic-only changes; add no reset policy.
- [ ] Run new regressions plus existing client/transport/callback suites. Recheck
  mypy, hooks, and exact runtime/error behavior before committing.

## Task 4: Expose ingestion invariants to the checker

Files: src/nio/ingest/state.py, source.py, sliding.py, coordinator.py;
src/nio/store/_sync_journal.py, _sync_journal_rows.py,
_sync_journal_preflight.py; .github/workflows/tests.yml. Task 1 already owns
dependency-facing changes in preflight; preserve those when fixing row/control-flow
typing.
Tests: existing source, journal, delivery, ownership, hydration, coordinator,
and crash/recovery suites under tests/ingest/.

Interfaces: keep all record types, exact accepted values, validation failures,
source/owner identities, batch APIs, transaction boundaries, and status enums.
Expected runtime behavior is unchanged.

- [ ] Narrow integer/tuple/object values after existing exact checks. Preserve
  bool rejection and other intentional exact-type requirements. Use precise
  cast targets for previously validated SQLite fields, not a new validator layer.
- [ ] Give distinct names to staged and prepared frames and narrow discriminated
  results before using optional frames. Remove redundant casts and duplicate
  scoped-list declarations only where established control flow proves them.

```python
frame = result.frame
assert frame is not None  # inside the existing FRAME-discriminated branch
# Existing frame logic uses this narrowed local.
```

- [ ] Type maintenance response-class alternatives with their common precise
  response interface and validated room-key fields with their existing type.
  Construct typed OwnerView, SourceState, and DeliveryState values from checked
  columns, preserving the authoritative decode at each boundary.
- [ ] Run affected ingestion suites, canonical mypy, and hooks. Resolve all
  remaining package diagnostics attributable to Tasks 1–4 through their owning
  implementer before this task is complete; no diagnostic budget remains.
- [ ] Confirm zero errors across all source files, then add the type-check CI
  job using the existing checkout/uv setup versions and a locked environment.
  No baseline or exclusions; new errors fail CI.

```yaml
  type-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - name: Install uv
        uses: astral-sh/setup-uv@v9.0.0
        with:
          enable-cache: true
          python-version: "3.14"
      - name: Check types
        env:
          MYPYPATH: src
        run: uv run --locked mypy -p nio --warn-redundant-casts --no-incremental
```

- [ ] Run the exact CI command locally, inspect behavior-preserving scope and
  production delta, validate workflow hooks, then commit.

## Task 5: Reuse the unchanged authenticated Work value

Files: src/nio/store/_sync_journal_rows.py, _sync_journal.py;
focused existing delivery/journal/cache tests or a small adjacent test module.
Interfaces: existing JournalRows and AuthenticatedWork; one additional private
cache entry, no change to next_batch, _settle_batch, acknowledgement, or schema.
Task 4's exact column/revision typing stays intact.

- [ ] Add behavioral regressions through real owned delivery: one decode per
  unchanged Work over claim/settlement/ack; same callback order, batch identity,
  and frame retirement. Use decode call count only alongside those outcomes.
- [ ] Cover payload mutation with unchanged digest/revision, a different valid
  row, account/stream/transport identity change, lowered owner revision, HELD
  promotion, deletion/ack, and close/reopen. Keep corruption failures observable.
- [ ] Add a single-entry cache after _validate_task3_work_row, following existing
  Aggregate authentication keys. Every read still fetches SQLite and validates
  clear columns before consulting the cached value.

```python
stored = self._validate_task3_work_row(owner, row)
authentication = (owner.account_id, owner.stream_id, owner.transport_kind)
cached = self._work_cache
if cached is not None and cached[:2] == (authentication, stored):
    return cached[2]
# Existing complete authenticated decode; on success only:
self._work_cache = authentication, stored, value
return value
```

- [ ] Clear Work and Aggregate caches on close. Do not cache mutable projection,
  parsed callback events, admission facts, operations, or effects.
- [ ] Run focused delivery/corruption/crypto/recovery tests, zero-error mypy,
  hooks, and the same paired real-store benchmark. Record actual production
  delta and measured speedup; no machine-specific timing assertion in tests.
  Inspect cache bounds/invalidation and commit.

## Task 6: Enforce, verify, review, and publish

Controller owns tracked results and integration; no unreviewed production or CI
edits in this task. Task 4 provides the CI type-check gate.
- [ ] Review task reports and covering checks. Run final complete suites on
  Python 3.12/3.13/3.14, repository pre-commit hooks, locked install/check, and
  canonical mypy. Re-run broader tests only for actual subsequent behavior changes.
- [ ] Independently review the entire follow-up delta from bd320ab against this
  spec and the existing durable contract. The previously completed PR-wide review
  remains recorded in the hardening plan; do not silently expand this follow-up
  into a new architecture or unrelated repair campaign.
- [ ] Record zero-error command/result, meaningful test changes, dependency and
  production-line deltas, benchmark boundaries/results, and unchanged cutover
  limitations. Commit results, scan outgoing content/metadata, check remote
  drift, push non-forcibly to PR #55, and verify remote HEAD/CI.

## Plan self-review

Tasks 1–4 resolve the inspected dependency, crypto, API/model, client/transport,
and ingestion diagnostic groups. Task 5 changes only measured Work reuse after
Task 4 has made its validation types explicit. Task 6 prevents another undeclared
baseline and verifies the combined behavior. Files shared by tasks are edited
sequentially; no two implementers own them concurrently. Real error paths have
focused regressions, while annotation-only paths retain existing assertions.
No new guarantee or delivery representation is required.
