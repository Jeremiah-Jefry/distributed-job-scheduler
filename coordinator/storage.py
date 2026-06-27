"""
Durable per-node storage for Raft state.

Each coordinator owns exactly one RaftStorage instance backed by its own
SQLite file.  No RaftStorage instance is ever shared between nodes — that
invariant is enforced by the caller (coordinator/main.py) passing its own
db_path via env var.

Tables
------
raft_meta         : single-row KV for current_term and voted_for
raft_log          : append-only Raft log (index, term, command JSON)
jobs              : full Job records serialised as JSON
idempotency_index : idempotency_key → job_id mapping
completed_executions : (job_id, worker_id) pairs for execution-level dedupe
"""
from __future__ import annotations

import json
import logging
from typing import Optional

import aiosqlite

from common.models import Job, LogEntry

logger = logging.getLogger(__name__)


class RaftStorage:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Open the SQLite connection and create tables if they don't exist."""
        self._db = await aiosqlite.connect(self._db_path)
        # WAL mode: better concurrent read performance; still fully durable.
        await self._db.execute("PRAGMA journal_mode=WAL")
        # synchronous=FULL ensures fsync on commit — required for Raft safety.
        await self._db.execute("PRAGMA synchronous=FULL")
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS raft_meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS raft_log (
                idx     INTEGER PRIMARY KEY,
                term    INTEGER NOT NULL,
                command TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                data   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS idempotency_index (
                idempotency_key TEXT PRIMARY KEY,
                job_id          TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS completed_executions (
                job_id    TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                PRIMARY KEY (job_id, worker_id)
            );
        """)
        await self._db.commit()
        logger.info("RaftStorage initialised: %s", self._db_path)

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    # ------------------------------------------------------------------
    # Term / vote  (must be fsync'd before responding to any vote request)
    # ------------------------------------------------------------------

    async def get_term(self) -> int:
        row = await self._fetch_meta("current_term")
        return int(row) if row is not None else 0

    async def get_voted_for(self) -> Optional[str]:
        return await self._fetch_meta("voted_for")

    async def save_term_and_vote(
        self, term: int, voted_for: Optional[str]
    ) -> None:
        """Atomically persist term + voted_for, then fsync."""
        assert self._db is not None
        async with self._db.execute("BEGIN"):
            await self._db.execute(
                "INSERT OR REPLACE INTO raft_meta (key, value) VALUES (?, ?)",
                ("current_term", str(term)),
            )
            await self._db.execute(
                "INSERT OR REPLACE INTO raft_meta (key, value) VALUES (?, ?)",
                ("voted_for", voted_for),
            )
        await self._db.commit()

    # ------------------------------------------------------------------
    # Raft log
    # ------------------------------------------------------------------

    async def append_log_entry(self, entry: LogEntry) -> None:
        assert self._db is not None
        await self._db.execute(
            "INSERT OR REPLACE INTO raft_log (idx, term, command) VALUES (?, ?, ?)",
            (entry.index, entry.term, json.dumps(entry.command)),
        )
        await self._db.commit()

    async def get_log_entry(self, index: int) -> Optional[LogEntry]:
        assert self._db is not None
        async with self._db.execute(
            "SELECT idx, term, command FROM raft_log WHERE idx = ?", (index,)
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return None
        return LogEntry(index=row[0], term=row[1], command=json.loads(row[2]))

    async def get_log_entries_from(self, from_index: int) -> list[LogEntry]:
        """Return all entries with index >= from_index, ordered ascending."""
        assert self._db is not None
        async with self._db.execute(
            "SELECT idx, term, command FROM raft_log WHERE idx >= ? ORDER BY idx ASC",
            (from_index,),
        ) as cur:
            rows = await cur.fetchall()
        return [
            LogEntry(index=r[0], term=r[1], command=json.loads(r[2])) for r in rows
        ]

    async def get_last_log_index_and_term(self) -> tuple[int, int]:
        """Return (last_index, last_term). Returns (0, 0) for empty log."""
        assert self._db is not None
        async with self._db.execute(
            "SELECT idx, term FROM raft_log ORDER BY idx DESC LIMIT 1"
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return 0, 0
        return row[0], row[1]

    async def truncate_log_from(self, from_index: int) -> None:
        """Delete all entries with index >= from_index (conflict resolution)."""
        assert self._db is not None
        await self._db.execute(
            "DELETE FROM raft_log WHERE idx >= ?", (from_index,)
        )
        await self._db.commit()

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------

    async def upsert_job(self, job: Job) -> None:
        assert self._db is not None
        await self._db.execute(
            "INSERT OR REPLACE INTO jobs (job_id, data) VALUES (?, ?)",
            (job.job_id, job.model_dump_json()),
        )
        await self._db.commit()

    async def get_job(self, job_id: str) -> Optional[Job]:
        assert self._db is not None
        async with self._db.execute(
            "SELECT data FROM jobs WHERE job_id = ?", (job_id,)
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return None
        return Job.model_validate_json(row[0])

    async def get_all_jobs(self) -> list[Job]:
        assert self._db is not None
        async with self._db.execute("SELECT data FROM jobs") as cur:
            rows = await cur.fetchall()
        return [Job.model_validate_json(r[0]) for r in rows]

    # ------------------------------------------------------------------
    # Idempotency index
    # ------------------------------------------------------------------

    async def get_job_id_by_idempotency_key(
        self, key: str
    ) -> Optional[str]:
        assert self._db is not None
        async with self._db.execute(
            "SELECT job_id FROM idempotency_index WHERE idempotency_key = ?",
            (key,),
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else None

    async def set_idempotency_key(self, key: str, job_id: str) -> None:
        assert self._db is not None
        await self._db.execute(
            "INSERT OR IGNORE INTO idempotency_index (idempotency_key, job_id)"
            " VALUES (?, ?)",
            (key, job_id),
        )
        await self._db.commit()

    # ------------------------------------------------------------------
    # Execution-level dedupe
    # ------------------------------------------------------------------

    async def has_completed_execution(
        self, job_id: str, worker_id: str
    ) -> bool:
        assert self._db is not None
        async with self._db.execute(
            "SELECT 1 FROM completed_executions WHERE job_id = ? AND worker_id = ?",
            (job_id, worker_id),
        ) as cur:
            row = await cur.fetchone()
        return row is not None

    async def add_completed_execution(
        self, job_id: str, worker_id: str
    ) -> None:
        assert self._db is not None
        await self._db.execute(
            "INSERT OR IGNORE INTO completed_executions (job_id, worker_id)"
            " VALUES (?, ?)",
            (job_id, worker_id),
        )
        await self._db.commit()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_meta(self, key: str) -> Optional[str]:
        assert self._db is not None
        async with self._db.execute(
            "SELECT value FROM raft_meta WHERE key = ?", (key,)
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else None
