"""
Raft consensus node state machine.

Key design points:
- Election timer uses a asyncio.Lock to prevent concurrent elections.
- After stepping down (seeing a higher term from a peer), the node waits a
  FULL new randomised timeout before re-trying — it does not immediately fire.
- RPC timeouts are set to 100ms (connect) / 200ms (read) — well within the
  150ms minimum election timeout, so a slow peer does not extend our window.
- The election_reset_event is an asyncio.Event that is set when any valid
  AppendEntries or vote-grant arrives; the timer loop re-randomises after each
  event and only fires _start_election after a quiet period.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import time
import uuid
from typing import Optional

from common.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    Job,
    JobStatus,
    LeasedJobPayload,
    LogEntry,
    NodeRole,
    RequestVoteRequest,
    RequestVoteResponse,
)
from coordinator.storage import RaftStorage
from common.rpc_client import RaftRPCClient

logger = logging.getLogger(__name__)

# Timing constants (seconds)
# Election timeout: widened to 500-1500ms so Windows timer resolution
# doesn't cause all 3 nodes to fire in lockstep.
ELECTION_TIMEOUT_MIN = 0.500
ELECTION_TIMEOUT_MAX = 1.500
HEARTBEAT_INTERVAL = 0.050

# RPC timeout: must be well under ELECTION_TIMEOUT_MIN
RPC_TIMEOUT = 0.200  # connect+read combined

LEASE_DURATION_SECS = float(os.getenv("LEASE_DURATION_SECS", "30"))
LEASE_CHECK_INTERVAL = 5.0


class RaftNode:
    """One coordinator node. Instantiated once per process; never shared."""

    def __init__(
        self,
        node_id: str,
        peer_urls: list[str],
        storage: RaftStorage,
        self_url: Optional[str] = None,
    ) -> None:
        self.node_id = node_id
        self.peer_urls = peer_urls
        self._storage = storage
        self.self_url = self_url  # This node's own HTTP URL (for redirect hints)

        # Volatile Raft state
        self.role: NodeRole = NodeRole.FOLLOWER
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        self._leader_url: Optional[str] = None  # URL of the known leader (from AppendEntries)
        self.commit_index: int = 0
        self.last_applied: int = 0

        # Leader-only
        self._next_index: dict[str, int] = {}
        self._match_index: dict[str, int] = {}

        # In-memory job state (rebuilt from committed log on startup)
        self.job_table: dict[str, Job] = {}
        self.idempotency_index: dict[str, str] = {}

        # Synchronisation primitives
        # Timestamp of last valid leader contact (AppendEntries) or vote grant.
        # The election timer fires if now() - _last_heartbeat > timeout.
        # Step-down does NOT update this — only genuine leader contact does.
        self._last_heartbeat: float = time.monotonic()
        self._election_lock = asyncio.Lock()

        # Background tasks
        self._election_timer_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._lease_watcher_task: Optional[asyncio.Task] = None
        self._rpc: Optional[RaftRPCClient] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self.current_term = await self._storage.get_term()
        self.voted_for = await self._storage.get_voted_for()

        # Rebuild in-memory state from all persisted log entries
        entries = await self._storage.get_log_entries_from(1)
        for entry in entries:
            self._apply_command(entry.command)
            self.last_applied = entry.index
        # commit_index will be confirmed by first AppendEntries from leader
        self.commit_index = self.last_applied

        self._rpc = RaftRPCClient(timeout=RPC_TIMEOUT)
        await self._rpc.__aenter__()

        # Stagger start: each node waits a small initial delay based on a
        # random offset so they don't ALL fire elections simultaneously.
        self._last_heartbeat = time.monotonic()
        self._election_timer_task = asyncio.create_task(
            self._election_timer_loop(), name=f"{self.node_id}-election"
        )
        self._lease_watcher_task = asyncio.create_task(
            self._lease_expiry_watcher(), name=f"{self.node_id}-lease-watcher"
        )
        logger.info(
            "[%s] started (term=%d, voted_for=%s)", self.node_id, self.current_term, self.voted_for
        )

    async def stop(self) -> None:
        for task in (self._election_timer_task, self._heartbeat_task, self._lease_watcher_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._rpc:
            await self._rpc.__aexit__(None, None, None)

    # ------------------------------------------------------------------
    # Election timer loop
    # ------------------------------------------------------------------

    async def _election_timer_loop(self) -> None:
        """
        Polls monotonic time to detect election timeout.
        Each cycle picks ONE random timeout duration, then checks every 50ms
        whether that duration has elapsed since the last leader contact.
        When it fires, launches _run_election() and resets the timestamp.
        """
        # Initial stagger: prevents all 3 nodes firing in lockstep at startup.
        await asyncio.sleep(random.uniform(0, ELECTION_TIMEOUT_MIN))
        while True:
            # Pick a new timeout for this cycle
            timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
            # Wait until elapsed >= timeout, checking every 50ms
            while True:
                await asyncio.sleep(0.05)
                if self.role == NodeRole.LEADER:
                    self._last_heartbeat = time.monotonic()
                    break  # restart cycle (leader keeps timer fresh)
                elapsed = time.monotonic() - self._last_heartbeat
                if elapsed >= timeout:
                    break
            if self.role == NodeRole.LEADER:
                continue
            # Timeout fired — start election if one isn't already running
            if not self._election_lock.locked():
                asyncio.create_task(self._run_election())
            # Reset timestamp so we don't immediately fire another election
            self._last_heartbeat = time.monotonic()


    def _reset_election_timer(self) -> None:
        """Called ONLY on AppendEntries from a valid leader or when we grant a vote."""
        self._last_heartbeat = time.monotonic()

    # ------------------------------------------------------------------
    # Election
    # ------------------------------------------------------------------

    async def _run_election(self) -> None:
        async with self._election_lock:
            if self.role == NodeRole.LEADER:
                return
            await self._start_election()

    async def _start_election(self) -> None:
        self.role = NodeRole.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        await self._storage.save_term_and_vote(self.current_term, self.voted_for)

        term_at_start = self.current_term
        logger.info("[%s] election term=%d", self.node_id, term_at_start)

        last_idx, last_term = await self._storage.get_last_log_index_and_term()
        req = RequestVoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=last_idx,
            last_log_term=last_term,
        )

        votes = 1  # self

        async def ask_peer(url: str) -> bool:
            assert self._rpc is not None
            resp = await self._rpc.request_vote(url, req)
            if resp is None:
                logger.info("[%s] vote from %s: NO RESPONSE (timeout/error)", self.node_id, url)
                return False
            logger.info(
                "[%s] vote from %s: granted=%s resp_term=%d (our_term=%d)",
                self.node_id, url, resp.vote_granted, resp.term, self.current_term,
            )
            if resp.term > self.current_term:
                await self._step_down(resp.term)
                return False
            return resp.vote_granted and self.role == NodeRole.CANDIDATE

        results = await asyncio.gather(*[ask_peer(u) for u in self.peer_urls], return_exceptions=True)
        for r in results:
            if r is True:
                votes += 1


        cluster_size = len(self.peer_urls) + 1
        quorum = cluster_size // 2 + 1
        if (
            votes >= quorum
            and self.role == NodeRole.CANDIDATE
            and self.current_term == term_at_start
        ):
            await self._become_leader()

    # ------------------------------------------------------------------
    # Become leader
    # ------------------------------------------------------------------

    async def _become_leader(self) -> None:
        self.role = NodeRole.LEADER
        self.leader_id = self.node_id
        last_idx, _ = await self._storage.get_last_log_index_and_term()
        for url in self.peer_urls:
            self._next_index[url] = last_idx + 1
            self._match_index[url] = 0

        logger.info("[%s] became LEADER (term=%d)", self.node_id, self.current_term)

        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.node_id}-heartbeat"
        )

    # ------------------------------------------------------------------
    # Heartbeat / replication loop
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        while self.role == NodeRole.LEADER:
            await asyncio.gather(
                *[self._send_append_entries(url) for url in self.peer_urls],
                return_exceptions=True,
            )
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _send_append_entries(self, peer_url: str) -> None:
        if self.role != NodeRole.LEADER:
            return
        assert self._rpc is not None

        next_idx = self._next_index.get(peer_url, 1)
        prev_idx = next_idx - 1
        prev_term = 0
        if prev_idx > 0:
            prev_entry = await self._storage.get_log_entry(prev_idx)
            if prev_entry:
                prev_term = prev_entry.term

        entries = await self._storage.get_log_entries_from(next_idx)
        req = AppendEntriesRequest(
            term=self.current_term,
            leader_id=self.node_id,
            leader_url=self.self_url,
            prev_log_index=prev_idx,
            prev_log_term=prev_term,
            entries=entries,
            leader_commit=self.commit_index,
        )

        resp = await self._rpc.append_entries(peer_url, req)
        if resp is None:
            return

        if resp.term > self.current_term:
            await self._step_down(resp.term)
            return

        if resp.success:
            if entries:
                new_match = entries[-1].index
                self._match_index[peer_url] = new_match
                self._next_index[peer_url] = new_match + 1
                await self._maybe_advance_commit_index()
        else:
            if resp.conflict_index is not None:
                self._next_index[peer_url] = max(1, resp.conflict_index)
            else:
                self._next_index[peer_url] = max(1, next_idx - 1)

    async def _maybe_advance_commit_index(self) -> None:
        last_idx, _ = await self._storage.get_last_log_index_and_term()
        cluster_size = len(self.peer_urls) + 1
        quorum = cluster_size // 2 + 1
        for n in range(last_idx, self.commit_index, -1):
            entry = await self._storage.get_log_entry(n)
            if entry is None or entry.term != self.current_term:
                continue
            count = 1 + sum(1 for m in self._match_index.values() if m >= n)
            if count >= quorum:
                self.commit_index = n
                await self._apply_up_to_commit()
                break

    # ------------------------------------------------------------------
    # Step down
    # ------------------------------------------------------------------

    async def _step_down(self, new_term: int) -> None:
        if new_term <= self.current_term and self.role == NodeRole.FOLLOWER:
            return
        logger.info("[%s] step down term %d→%d", self.node_id, self.current_term, new_term)
        self.current_term = new_term
        self.voted_for = None
        self.role = NodeRole.FOLLOWER
        self.leader_id = None
        await self._storage.save_term_and_vote(new_term, None)
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        # NOTE: do NOT call _reset_election_timer here.
        # Step-down is not equivalent to receiving a leader heartbeat.
        # The election timer will fire naturally after its next timeout.

    # ------------------------------------------------------------------
    # Handle RequestVote (incoming)
    # ------------------------------------------------------------------

    async def handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        if req.term > self.current_term:
            await self._step_down(req.term)

        vote_granted = False
        if req.term < self.current_term:
            pass
        elif self.voted_for in (None, req.candidate_id):
            last_idx, last_term = await self._storage.get_last_log_index_and_term()
            up_to_date = (
                req.last_log_term > last_term
                or (req.last_log_term == last_term and req.last_log_index >= last_idx)
            )
            if up_to_date:
                vote_granted = True
                self.voted_for = req.candidate_id
                await self._storage.save_term_and_vote(self.current_term, self.voted_for)
                self._reset_election_timer()

        logger.debug(
            "[%s] RequestVote from %s term=%d granted=%s",
            self.node_id, req.candidate_id, req.term, vote_granted
        )
        return RequestVoteResponse(term=self.current_term, vote_granted=vote_granted)

    # ------------------------------------------------------------------
    # Handle AppendEntries (incoming)
    # ------------------------------------------------------------------

    async def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        if req.term < self.current_term:
            return AppendEntriesResponse(term=self.current_term, success=False)

        self._reset_election_timer()

        if req.term > self.current_term:
            await self._step_down(req.term)

        self.role = NodeRole.FOLLOWER
        self.leader_id = req.leader_id
        if req.leader_url:
            self._leader_url = req.leader_url

        # Consistency check on prev_log
        if req.prev_log_index > 0:
            prev = await self._storage.get_log_entry(req.prev_log_index)
            if prev is None or prev.term != req.prev_log_term:
                return AppendEntriesResponse(
                    term=self.current_term,
                    success=False,
                    conflict_index=req.prev_log_index,
                )

        # Append / overwrite entries
        for entry in req.entries:
            existing = await self._storage.get_log_entry(entry.index)
            if existing and existing.term != entry.term:
                await self._storage.truncate_log_from(entry.index)
            await self._storage.append_log_entry(entry)

        if req.leader_commit > self.commit_index:
            last_idx, _ = await self._storage.get_last_log_index_and_term()
            self.commit_index = min(req.leader_commit, last_idx)
            await self._apply_up_to_commit()

        return AppendEntriesResponse(term=self.current_term, success=True)

    # ------------------------------------------------------------------
    # Apply committed entries
    # ------------------------------------------------------------------

    async def _apply_up_to_commit(self) -> None:
        while self.last_applied < self.commit_index:
            nxt = self.last_applied + 1
            entry = await self._storage.get_log_entry(nxt)
            if entry is None:
                break
            self._apply_command(entry.command)
            self.last_applied = nxt

    def _apply_command(self, command: dict) -> None:
        cmd_type: str = command.get("type", "")
        payload: dict = command.get("payload", {})

        if cmd_type == "submit_job":
            job = Job.model_validate(payload)
            self.job_table[job.job_id] = job
            self.idempotency_index[job.idempotency_key] = job.job_id

        elif cmd_type == "lease_job":
            job = self.job_table.get(payload["job_id"])
            if job and job.status == JobStatus.PENDING:
                job.status = JobStatus.LEASED
                job.lease_term = payload["lease_term"]
                job.lease_holder = payload["lease_holder"]
                job.lease_expiry = payload["lease_expiry"]
                job.attempts += 1
                job.updated_at = time.time()

        elif cmd_type == "complete_job":
            job = self.job_table.get(payload["job_id"])
            if job and job.status == JobStatus.LEASED:
                job.status = JobStatus.COMPLETED
                job.result = payload.get("result")
                job.lease_holder = None
                job.lease_expiry = None
                job.updated_at = time.time()

        elif cmd_type == "fail_job":
            job = self.job_table.get(payload["job_id"])
            if job and job.status == JobStatus.LEASED:
                job.lease_holder = None
                job.lease_expiry = None
                job.lease_term = None
                job.updated_at = time.time()
                if job.attempts >= job.max_attempts:
                    job.status = JobStatus.DEAD_LETTER
                else:
                    job.status = JobStatus.PENDING

        else:
            logger.warning("[%s] unknown command type: %s", self.node_id, cmd_type)

    # ------------------------------------------------------------------
    # Replicate a new entry (called by HTTP handlers on the leader)
    # ------------------------------------------------------------------

    async def replicate_entry(self, command: dict) -> bool:
        if self.role != NodeRole.LEADER:
            raise RuntimeError("not leader")

        last_idx, _ = await self._storage.get_last_log_index_and_term()
        new_idx = last_idx + 1
        entry = LogEntry(term=self.current_term, index=new_idx, command=command)
        await self._storage.append_log_entry(entry)

        cluster_size = len(self.peer_urls) + 1
        quorum = cluster_size // 2 + 1
        ack_count = 1  # leader itself

        async def replicate_to(url: str) -> bool:
            for _ in range(8):
                await self._send_append_entries(url)
                if self._match_index.get(url, 0) >= new_idx:
                    return True
                await asyncio.sleep(0.02)
            return False

        results = await asyncio.gather(
            *[replicate_to(u) for u in self.peer_urls], return_exceptions=True
        )
        for r in results:
            if r is True:
                ack_count += 1

        if ack_count >= quorum:
            self.commit_index = max(self.commit_index, new_idx)
            await self._apply_up_to_commit()
            return True
        return False

    # ------------------------------------------------------------------
    # Lease expiry watcher
    # ------------------------------------------------------------------

    async def _lease_expiry_watcher(self) -> None:
        while True:
            await asyncio.sleep(LEASE_CHECK_INTERVAL)
            if self.role != NodeRole.LEADER:
                continue
            now = time.time()
            expired = [
                j for j in self.job_table.values()
                if j.status == JobStatus.LEASED
                and j.lease_expiry is not None
                and j.lease_expiry < now
            ]
            for job in expired:
                logger.info("[%s] lease expired for job %s", self.node_id, job.job_id)
                try:
                    await self.replicate_entry({
                        "type": "fail_job",
                        "payload": {
                            "job_id": job.job_id,
                            "worker_id": job.lease_holder,
                            "error": "lease_expired",
                        },
                    })
                except RuntimeError:
                    pass  # stepped down between check and replicate

    # ------------------------------------------------------------------
    # Helpers for HTTP handlers
    # ------------------------------------------------------------------

    def get_leader_url(self) -> Optional[str]:
        """Return URL of known leader, for redirect purposes."""
        return self._leader_url

    def queue_depth(self) -> int:
        return sum(
            1 for j in self.job_table.values()
            if j.status in (JobStatus.PENDING, JobStatus.LEASED)
        )

    def dead_letter_count(self) -> int:
        return sum(1 for j in self.job_table.values() if j.status == JobStatus.DEAD_LETTER)

    def next_pending_job(self, worker_id: str) -> Optional[LeasedJobPayload]:
        pending = [j for j in self.job_table.values() if j.status == JobStatus.PENDING]
        if not pending:
            return None
        job = min(pending, key=lambda j: j.created_at)
        return LeasedJobPayload(
            job_id=job.job_id,
            payload=job.payload,
            lease_term=self.current_term,
            lease_expiry=time.time() + LEASE_DURATION_SECS,
        )
