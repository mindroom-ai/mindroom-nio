"""Health progress follows committed publication and backlog consumption."""

import pytest

from nio.durable import DurableSyncConfig
from nio.exceptions import LocalProtocolError

from .client_test import open_session, response
from .store_test import message


@pytest.mark.asyncio
async def test_progress_advances_while_prepared_backlog_drains(tmp_path):
    session = open_session(tmp_path, config=DurableSyncConfig(max_batch_records=1))
    try:
        initial = session.progress_generation
        await session._accept_response(response(messages=3))
        published = session.progress_generation
        assert published > initial
        previous = published
        acknowledged = 0
        while batch := await session.next_batch():
            assert session.progress_generation == previous
            await session.ack(batch)
            current = session.progress_generation
            assert current > previous
            assert session.cursor == "s1"
            await session.ack(batch)
            assert session.progress_generation == current
            previous = current
            acknowledged += 1
        assert acknowledged > 1
        assert session.progress_generation == previous
        with session._store.transaction():
            session._store.publish((message("$later"),))
        assert session.progress_generation > previous
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("acknowledged", [1, 2], ids=["partial", "empty"])
async def test_progress_survives_reopening_after_acknowledgement(
    tmp_path, acknowledged
):
    session = open_session(tmp_path)
    try:
        with session._store.transaction():
            batches = [session._store.publish((message(),)) for _ in range(2)]
        for batch in batches[:acknowledged]:
            await session.ack(batch)
        progress = session.progress_generation
    finally:
        await session.close()

    reopened = open_session(tmp_path)
    try:
        assert reopened.progress_generation == progress
        await reopened.ack(batches[acknowledged - 1])
        assert reopened.progress_generation == progress
        if batch := await reopened.next_batch():
            await reopened.ack(batch)
            assert reopened.progress_generation > progress
    finally:
        await reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["publish", "ack"])
async def test_rejected_or_rolled_back_work_does_not_advance_progress(
    tmp_path, operation
):
    session = open_session(tmp_path)
    try:
        with session._store.transaction():
            first = session._store.publish((message(),))
            second = session._store.publish((message("$two"),))
        progress = session.progress_generation
        with pytest.raises(LocalProtocolError, match="oldest"):
            await session.ack(second)
        assert session.progress_generation == progress
        with pytest.raises(RuntimeError, match="rollback"):
            with session._store.transaction():
                if operation == "publish":
                    session._store.publish((message("$three"),))
                else:
                    await session.ack(first)
                raise RuntimeError("rollback")
        assert session.progress_generation == progress
        assert await session.next_batch() == first
    finally:
        await session.close()
