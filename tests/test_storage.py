"""
Unit tests for coordinator/storage.py.

Tests:
- Term and voted_for persist across close/reopen
- voted_for is None by default (no implicit vote on cold start)
- Log append, retrieval, truncation
- Job CRUD roundtrip
- Idempotency index
- Execution-level dedupe
"""
from __future__ import annotations

import os
import pytest
import pytest_asyncio

from coordinator.storage import RaftStorage
from common.models import Job, JobStatus, LogEntry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def db(tmp_path):
    path = str(tmp_path / "test.db")
    store = RaftStorage(path)
    await store.initialize()
    yield store
    await store.close()


# ---------------------------------------------------------------------------
# Term / vote persistence
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_initial_term_is_zero(db: RaftStorage):
    assert await db.get_term() == 0


@pytest.mark.asyncio
async def test_initial_voted_for_is_none(db: RaftStorage):
    assert await db.get_voted_for() is None


@pytest.mark.asyncio
async def test_term_and_vote_persist_across_reopen(tmp_path):
    path = str(tmp_path / "persist.db")
    store = RaftStorage(path)
    await store.initialize()
    await store.save_term_and_vote(7, "coordinator-2")
    await store.close()

    store2 = RaftStorage(path)
    await store2.initialize()
    assert await store2.get_term() == 7
    assert await store2.get_voted_for() == "coordinator-2"
    await store2.close()


@pytest.mark.asyncio
async def test_vote_can_be_cleared_with_none(tmp_path):
    path = str(tmp_path / "clearvote.db")
    store = RaftStorage(path)
    await store.initialize()
    await store.save_term_and_vote(3, "coordinator-1")
    await store.save_term_and_vote(4, None)
    await store.close()

    store2 = RaftStorage(path)
    await store2.initialize()
    assert await store2.get_term() == 4
    assert await store2.get_voted_for() is None
    await store2.close()


# ---------------------------------------------------------------------------
# Raft log
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_log_returns_zero_zero(db: RaftStorage):
    idx, term = await db.get_last_log_index_and_term()
    assert idx == 0
    assert term == 0


@pytest.mark.asyncio
async def test_append_and_retrieve_log_entry(db: RaftStorage):
    entry = LogEntry(term=1, index=1, command={"type": "submit_job", "payload": {"job_id": "abc"}})
    await db.append_log_entry(entry)
    fetched = await db.get_log_entry(1)
    assert fetched is not None
    assert fetched.term == 1
    assert fetched.index == 1
    assert fetched.command["type"] == "submit_job"


@pytest.mark.asyncio
async def test_last_log_index_and_term(db: RaftStorage):
    for i in range(1, 4):
        await db.append_log_entry(LogEntry(term=2, index=i, command={"type": "noop"}))
    idx, term = await db.get_last_log_index_and_term()
    assert idx == 3
    assert term == 2


@pytest.mark.asyncio
async def test_get_log_entries_from(db: RaftStorage):
    for i in range(1, 6):
        await db.append_log_entry(LogEntry(term=1, index=i, command={"type": "noop"}))
    entries = await db.get_log_entries_from(3)
    assert [e.index for e in entries] == [3, 4, 5]


@pytest.mark.asyncio
async def test_truncate_log(db: RaftStorage):
    for i in range(1, 6):
        await db.append_log_entry(LogEntry(term=1, index=i, command={"type": "noop"}))
    await db.truncate_log_from(3)
    entries = await db.get_log_entries_from(1)
    assert [e.index for e in entries] == [1, 2]
    idx, _ = await db.get_last_log_index_and_term()
    assert idx == 2


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_job_upsert_and_get(db: RaftStorage):
    job = Job(
        job_id="job-1",
        idempotency_key="key-1",
        payload={"task": "test"},
        status=JobStatus.PENDING,
        created_at=1000.0,
        updated_at=1000.0,
    )
    await db.upsert_job(job)
    fetched = await db.get_job("job-1")
    assert fetched is not None
    assert fetched.job_id == "job-1"
    assert fetched.status == JobStatus.PENDING


@pytest.mark.asyncio
async def test_job_update(db: RaftStorage):
    job = Job(
        job_id="job-1",
        idempotency_key="key-1",
        payload={},
        created_at=1000.0,
        updated_at=1000.0,
    )
    await db.upsert_job(job)
    job.status = JobStatus.COMPLETED
    job.result = {"output": "done"}
    await db.upsert_job(job)
    fetched = await db.get_job("job-1")
    assert fetched is not None
    assert fetched.status == JobStatus.COMPLETED
    assert fetched.result == {"output": "done"}


@pytest.mark.asyncio
async def test_get_nonexistent_job_returns_none(db: RaftStorage):
    assert await db.get_job("nonexistent") is None


# ---------------------------------------------------------------------------
# Idempotency index
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_idempotency_set_and_get(db: RaftStorage):
    await db.set_idempotency_key("my-key", "job-xyz")
    result = await db.get_job_id_by_idempotency_key("my-key")
    assert result == "job-xyz"


@pytest.mark.asyncio
async def test_idempotency_missing_key_returns_none(db: RaftStorage):
    result = await db.get_job_id_by_idempotency_key("nonexistent-key")
    assert result is None


@pytest.mark.asyncio
async def test_idempotency_insert_ignore(db: RaftStorage):
    """Second set_idempotency_key for same key must NOT overwrite."""
    await db.set_idempotency_key("my-key", "job-1")
    await db.set_idempotency_key("my-key", "job-2")  # should be ignored
    result = await db.get_job_id_by_idempotency_key("my-key")
    assert result == "job-1"


# ---------------------------------------------------------------------------
# Execution-level dedupe
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_completed_execution_not_present(db: RaftStorage):
    assert not await db.has_completed_execution("job-1", "worker-1")


@pytest.mark.asyncio
async def test_completed_execution_add_and_check(db: RaftStorage):
    await db.add_completed_execution("job-1", "worker-1")
    assert await db.has_completed_execution("job-1", "worker-1")


@pytest.mark.asyncio
async def test_completed_execution_different_worker(db: RaftStorage):
    """Dedupe is scoped to (job_id, worker_id) pair."""
    await db.add_completed_execution("job-1", "worker-1")
    # Same job, different worker — should NOT be marked as completed
    assert not await db.has_completed_execution("job-1", "worker-2")


@pytest.mark.asyncio
async def test_completed_execution_idempotent_insert(db: RaftStorage):
    """Adding same pair twice must not raise."""
    await db.add_completed_execution("job-1", "worker-1")
    await db.add_completed_execution("job-1", "worker-1")  # no error
    assert await db.has_completed_execution("job-1", "worker-1")
