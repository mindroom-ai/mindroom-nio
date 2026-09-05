"""Real prepared-engine fixtures for journal and source boundary tests."""

import asyncio

from dataclasses import replace
from functools import wraps

from nio import AsyncClient, AsyncClientConfig
from nio.crypto import OlmAccount
from nio.ingest.coordinator import _open_owned_ingestion, _initial_outbound_maintenance
from nio.store import SqliteStore
from nio.store.sync_journal import StoreBootstrap
from nio.store.sync_journal import open_ingestion_store as _open_journal
from nio.store._sync_journal_values import MaterializerLimits


@wraps(_open_journal)
def open_ingestion_store(*args, **kwargs):
    raw = _open_journal(*args, **kwargs)
    return StoreBootstrap(
        raw._journal,
        owned_store_class=SqliteStore,
        authenticated_pickle_key=kwargs.get("pickle_key", ""),
    )._bind_owned_config(kwargs["source"], kwargs.get("sqlite_busy_timeout_ms", 2000))


def open_test_session(client, bootstrap, *, config, consumer_generation, stream_id):
    """Use the owned factory for source tests with an existing fake HTTP client."""
    client.store_path = str(bootstrap.database_path.parent)
    client.config = replace(
        client.config,
        encryption_enabled=True,
        store=SqliteStore,
        pickle_key=bootstrap._authenticated_pickle_key,
        store_name=bootstrap.database_path.name,
    )
    session = _open_owned_ingestion(
        client,
        bootstrap,
        config=config,
        consumer_generation=consumer_generation,
        stream_id=stream_id,
    )
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    return session


def materialize_journal(journal, *, limits=None):
    """Prepare through the real client and journal under the journal test's lease.

    These tests exercise source/journal transactions with their own database
    lifetime. Owned-session tests separately cover sync-token persistence,
    outbound network maintenance, close, and process-crash recovery.
    """
    bootstrap = StoreBootstrap(
        journal,
        owned_store_class=SqliteStore,
        authenticated_pickle_key=journal.pickle_key,
    )
    store = bootstrap._open_owned_store_candidate()._store_for_attachment()
    if store.load_account() is None:
        store.save_account(OlmAccount())
    client = AsyncClient(
        "https://example.org",
        journal.account_id,
        journal.device_id,
        config=AsyncClientConfig(store=SqliteStore, pickle_key=journal.pickle_key),
    )
    client.restore_login(journal.account_id, journal.device_id, "test-token")
    client._attach_ingestion_store(store)
    try:
        return journal._prepare_and_materialize_oldest_frame(
            prepare=client._prepare_ingestion_frame,
            freeze_outbound=lambda frame, prepared: _initial_outbound_maintenance(
                frame_id=frame.frame_id,
                delta=prepared.crypto_delta,
                upload_body=None,
            ),
            limits=limits or MaterializerLimits(),
        )
    finally:
        client._detach_ingestion_store(store)
        store._revoke_ingestion_lease()


def retire_completed_frame(journal):
    """Complete a journal fixture frame after all its Work was acknowledged."""
    completion = journal._claim_frame_completion()
    assert completion is not None
    journal._retire_claimed_frame(completion)


async def run_with_acknowledgements(session, received=None):
    """A minimal accepting consumer for source scheduling and hydration tests."""
    runner = asyncio.create_task(session.run())
    try:
        while not runner.done():
            batch = session.next_batch(max_records=1)
            if batch is not None:
                if received is not None:
                    received.extend(batch.records)
                session.acknowledge_batch(batch.ref)
                continue
            waiter = asyncio.create_task(session._wait_for_work())
            try:
                await asyncio.wait(
                    {runner, waiter}, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
        await runner
    finally:
        if not runner.done():
            runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
