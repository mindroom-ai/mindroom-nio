import sqlite3
from pathlib import Path

from nio.ingest.config import ClassicSourceConfig
from nio.store import SqliteStore
from nio.store.sync_journal import open_ingestion_store

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
CLASSIC_SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")


def _table_names(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }


def test_store_public_surface_exposes_only_ingestion_bootstrap() -> None:
    import nio.store as store_api
    import nio.store.sync_journal as bootstrap_api

    assert store_api.StoreBootstrap is not None
    assert store_api.open_ingestion_store is not None
    assert not hasattr(store_api, "EncryptedRowCodec")
    assert not hasattr(store_api, "IngestionJournal")
    assert not hasattr(store_api, "SqliteIngestionJournal")
    assert not hasattr(bootstrap_api, "SqliteIngestionJournal")


def test_bootstrap_opens_only_the_matrix_store_e2ee_subset(tmp_path: Path) -> None:
    database_path = tmp_path / "journal.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name=database_path.name,
    )

    store = bootstrap.open_matrix_store(SqliteStore)
    try:
        tables = _table_names(database_path)
        assert "accounts" in tables
        assert "NioIngestMeta" in tables
        assert (
            not {
                "synctokens",
                "syncrecoverygaps",
                "syncrecoveryabandonedrooms",
                "pendingtimelineevents",
                "slidingwindowtokens",
            }
            & tables
        )
    finally:
        bootstrap.close()

    assert store.database.is_closed()
