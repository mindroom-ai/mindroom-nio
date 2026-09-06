from uuid import uuid4

import pytest

from nio.crypto import OlmAccount
from nio.durable.store import DurableStore
from nio.store import (
    Accounts,
    SqliteMemoryStore,
    use_database,
    use_database_atomic,
)


class ExpectedStoreFailure(RuntimeError):
    pass


@use_database
def _read_store_then_fail(store, expected_token, expected_identity_keys):
    assert Accounts._meta.database is store.database
    if expected_token is not None:
        assert store.load_sync_token() == expected_token
    assert store.load_account().identity_keys == expected_identity_keys
    raise ExpectedStoreFailure("injected after reads")


@use_database_atomic
def _write_account_then_fail(store):
    assert Accounts._meta.database is store.database
    account = Accounts.get(
        Accounts.user_id == store.user_id,
        Accounts.device_id == store.device_id,
    )
    Accounts.update(shared=True).where(Accounts.id == account.id).execute()
    assert store.load_account().shared is True
    raise ExpectedStoreFailure("injected after write")


def _seed_store(store, token):
    account = OlmAccount()
    store.save_account(account)
    if token is not None:
        store.save_sync_token(token)
    return account.identity_keys


@pytest.fixture
def ordinary_binding_stores():
    store_a = SqliteMemoryStore("@binding-a:example.org", "DEVICE-A", "pickle-a")
    store_b = SqliteMemoryStore("@binding-b:example.org", "DEVICE-B", "pickle-b")
    try:
        yield (
            (store_a, "token-a", _seed_store(store_a, "token-a")),
            (store_b, "token-b", _seed_store(store_b, "token-b")),
        )
    finally:
        store_a.database.close()
        store_b.database.close()


@pytest.fixture
def durable_binding_stores(tmp_path):
    owners = []
    stores = []
    try:
        for label in ("a", "b"):
            store_path = tmp_path / label
            store_path.mkdir()
            owner = DurableStore(
                store_path,
                user_id=f"@binding-{label}:example.org",
                device_id=f"DEVICE-{label.upper()}",
                consumer_id=uuid4(),
                database_name="journal.db",
                pickle_key=f"pickle-{label}",
            )
            owners.append(owner)
            store = owner.matrix
            with owner.transaction():
                stores.append((store, None, _seed_store(store, None)))
        yield tuple(stores)
    finally:
        for owner in reversed(owners):
            owner.close()


def _assert_bindings(models, database):
    assert all(model._meta.database is database for model in models)


def _exercise_nested_store_bindings(store_values):
    (store_a, token_a, keys_a), (store_b, token_b, keys_b) = store_values
    models = tuple(store_a.models)
    original_bindings = {model: model._meta.database for model in models}
    assert keys_a != keys_b

    with store_a.database.bind_ctx(
        store_a.models, bind_refs=False, bind_backrefs=False
    ):
        _assert_bindings(models, store_a.database)
        if token_a is not None:
            assert store_a.load_sync_token() == token_a
        assert store_a.load_account().identity_keys == keys_a
        _assert_bindings(models, store_a.database)

        if token_b is not None:
            assert store_b.load_sync_token() == token_b
        assert store_b.load_account().identity_keys == keys_b
        _assert_bindings(models, store_a.database)

        with pytest.raises(ExpectedStoreFailure, match="injected after reads"):
            _read_store_then_fail(store_b, token_b, keys_b)
        _assert_bindings(models, store_a.database)

        with pytest.raises(ExpectedStoreFailure, match="injected after write"):
            _write_account_then_fail(store_b)
        assert store_b.load_account().shared is False
        if token_b is not None:
            assert store_b.load_sync_token() == token_b
        _assert_bindings(models, store_a.database)

    assert all(model._meta.database is original_bindings[model] for model in models)


def test_nested_ordinary_store_bindings_restore_and_roll_back(
    ordinary_binding_stores,
):
    _exercise_nested_store_bindings(ordinary_binding_stores)


def test_nested_durable_store_bindings_restore_and_roll_back(durable_binding_stores):
    with durable_binding_stores[0][0].database.atomic():
        with durable_binding_stores[1][0].database.atomic():
            _exercise_nested_store_bindings(durable_binding_stores)
