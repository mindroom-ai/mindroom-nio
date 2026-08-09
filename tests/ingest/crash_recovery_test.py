"""On-disk restart and schema kill-point coverage for ingestion v1."""

import multiprocessing
import os
from pathlib import Path
from typing import Callable

import pytest

from nio.ingest.config import ClassicSourceConfig
from nio.ingest.model import TransportKind
from nio.ingest.state import SourceState
from nio.store.sync_journal import open_ingestion_store
from nio.store.sync_journal_schema import SCHEMA_SQL

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
CLASSIC_SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")
CRASH_EXIT_CODE = 86


def _exit_at_statement(kill_after: int) -> Callable[[str], None]:
    observed = 0

    def exit_process(_label: str) -> None:
        nonlocal observed
        observed += 1
        if observed == kill_after:
            os._exit(CRASH_EXIT_CODE)

    return exit_process


def _assert_process_crashed(
    target: Callable[..., None],
    *args: object,
) -> None:
    process = multiprocessing.get_context("spawn").Process(target=target, args=args)
    process.start()
    process.join(timeout=15)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("crash-injection child did not exit")
    assert process.exitcode == CRASH_EXIT_CODE


def _kill_during_schema(store_path: Path, kill_after: int) -> None:
    open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
        schema_statement_hook=_exit_at_statement(kill_after),
    )


@pytest.mark.parametrize("kill_after", range(1, len(SCHEMA_SQL) + 4))
def test_fresh_schema_creation_is_atomic_at_every_statement(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"schema-kill-{kill_after}"
    _assert_process_crashed(_kill_during_schema, store_path, kill_after)

    database_path = store_path / "journal.db"
    if database_path.exists():
        import sqlite3

        with sqlite3.connect(database_path) as connection:
            tables = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        assert tables == []

    reopened = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        assert reopened.schema_version == 1
        assert reopened._journal.load_source() == SourceState(
            0,
            TransportKind.CLASSIC,
            b'{"next_batch":null}',
            0,
            True,
        )
    finally:
        reopened.close()
