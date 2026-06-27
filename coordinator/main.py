"""
Coordinator FastAPI application.

One process per coordinator node.  Required env vars:
  NODE_ID        e.g. "coordinator-1"
  PEER_URLS      comma-separated URLs of the OTHER coordinators
                 e.g. "http://127.0.0.1:8002,http://127.0.0.1:8003"
  SELF_URL       this node's own URL (e.g. "http://127.0.0.1:8001")
                 used in redirect hints sent to workers/clients
  DB_PATH        path to this node's SQLite file (default: {NODE_ID}.db)
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from common.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    CompleteJobRequest,
    CompleteJobResponse,
    FailJobRequest,
    FailJobResponse,
    Job,
    JobStatus,
    JobStatusResponse,
    NodeRole,
    PollJobRequest,
    PollJobResponse,
    RequestVoteRequest,
    RequestVoteResponse,
    StatusResponse,
    SubmitJobRequest,
    SubmitJobResponse,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
)
from coordinator.raft_state import RaftNode
from coordinator.storage import RaftStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

NODE_ID: str = os.getenv("NODE_ID", "coordinator-unknown")
_raw_peers: str = os.getenv("PEER_URLS", "")
PEER_URLS: list[str] = [u.strip() for u in _raw_peers.split(",") if u.strip()]
DB_PATH: str = os.getenv("DB_PATH", f"{NODE_ID}.db")
SELF_URL: Optional[str] = os.getenv("SELF_URL")

# Populated during lifespan
_node: Optional[RaftNode] = None
# Registered workers: worker_id → registration timestamp
_workers: dict[str, float] = {}


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _node
    storage = RaftStorage(DB_PATH)
    await storage.initialize()
    _node = RaftNode(node_id=NODE_ID, peer_urls=PEER_URLS, storage=storage, self_url=SELF_URL)
    await _node.start()
    logger.info("[%s] coordinator ready on peers=%s db=%s", NODE_ID, PEER_URLS, DB_PATH)
    yield
    await _node.stop()
    await storage.close()


app = FastAPI(title=f"Coordinator {NODE_ID}", lifespan=lifespan)


def _get_node() -> RaftNode:
    assert _node is not None, "node not initialised"
    return _node


def _leader_redirect_response() -> JSONResponse:
    """Return a JSON redirect envelope when this node is not the leader."""
    node = _get_node()
    leader_url = node.get_leader_url()
    return JSONResponse(
        status_code=200,
        content={
            "leader_url": leader_url,
            "leader_id": node.leader_id,
            "detail": "Not the leader — redirect to leader_url",
        },
    )


# ---------------------------------------------------------------------------
# Operational endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict:
    return {"node_id": NODE_ID, "status": "ok"}


@app.get("/status", response_model=StatusResponse)
async def status() -> StatusResponse:
    node = _get_node()
    return StatusResponse(
        node_id=NODE_ID,
        role=node.role,
        term=node.current_term,
        commit_index=node.commit_index,
        leader_id=node.leader_id,
        queue_depth=node.queue_depth(),
        dead_letter_count=node.dead_letter_count(),
    )


# ---------------------------------------------------------------------------
# Raft internal RPCs (node-to-node only)
# ---------------------------------------------------------------------------

@app.post("/raft/request_vote", response_model=RequestVoteResponse)
async def request_vote(req: RequestVoteRequest) -> RequestVoteResponse:
    node = _get_node()
    return await node.handle_request_vote(req)


@app.post("/raft/append_entries", response_model=AppendEntriesResponse)
async def append_entries(req: AppendEntriesRequest) -> AppendEntriesResponse:
    node = _get_node()
    return await node.handle_append_entries(req)


# ---------------------------------------------------------------------------
# Client-facing endpoints
# ---------------------------------------------------------------------------

@app.post("/submit_job")
async def submit_job(req: SubmitJobRequest) -> JSONResponse:
    node = _get_node()

    if node.role != NodeRole.LEADER:
        return _leader_redirect_response()

    # Idempotency check (in-memory, rebuilt from committed log)
    existing_job_id = node.idempotency_index.get(req.idempotency_key)
    if existing_job_id:
        existing_job = node.job_table.get(existing_job_id)
        if existing_job:
            return JSONResponse(
                content=SubmitJobResponse(
                    job_id=existing_job.job_id,
                    status=existing_job.status,
                    idempotency_key=existing_job.idempotency_key,
                ).model_dump()
            )

    now = time.time()
    job = Job(
        job_id=str(uuid.uuid4()),
        idempotency_key=req.idempotency_key,
        payload=req.payload,
        status=JobStatus.PENDING,
        max_attempts=req.max_attempts,
        created_at=now,
        updated_at=now,
    )

    success = await node.replicate_entry({
        "type": "submit_job",
        "payload": job.model_dump(),
    })
    if not success:
        raise HTTPException(status_code=503, detail="Failed to replicate to majority")

    return JSONResponse(
        content=SubmitJobResponse(
            job_id=job.job_id,
            status=job.status,
            idempotency_key=job.idempotency_key,
        ).model_dump()
    )


@app.get("/job/{job_id}", response_model=JobStatusResponse)
async def get_job(job_id: str) -> JobStatusResponse:
    node = _get_node()
    job = node.job_table.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobStatusResponse(
        job_id=job.job_id,
        status=job.status,
        attempts=job.attempts,
        result=job.result,
        idempotency_key=job.idempotency_key,
        lease_holder=job.lease_holder,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


# ---------------------------------------------------------------------------
# Worker-facing endpoints
# ---------------------------------------------------------------------------

@app.post("/worker/register", response_model=WorkerRegisterResponse)
async def worker_register(req: WorkerRegisterRequest) -> JSONResponse:
    node = _get_node()
    if node.role != NodeRole.LEADER:
        return _leader_redirect_response()

    _workers[req.worker_id] = time.time()
    logger.info("[%s] worker registered: %s", NODE_ID, req.worker_id)
    return JSONResponse(
        content=WorkerRegisterResponse(
            acknowledged=True, leader_id=NODE_ID
        ).model_dump()
    )


@app.post("/worker/poll_job", response_model=PollJobResponse)
async def worker_poll_job(req: PollJobRequest) -> JSONResponse:
    node = _get_node()
    if node.role != NodeRole.LEADER:
        return _leader_redirect_response()

    leased = node.next_pending_job(req.worker_id)
    if leased is None:
        return JSONResponse(content=PollJobResponse(job=None).model_dump())

    # Replicate the lease command
    success = await node.replicate_entry({
        "type": "lease_job",
        "payload": {
            "job_id": leased.job_id,
            "lease_term": leased.lease_term,
            "lease_holder": req.worker_id,
            "lease_expiry": leased.lease_expiry,
        },
    })
    if not success:
        return JSONResponse(content=PollJobResponse(job=None).model_dump())

    return JSONResponse(content=PollJobResponse(job=leased).model_dump())


@app.post("/worker/complete_job", response_model=CompleteJobResponse)
async def worker_complete_job(req: CompleteJobRequest) -> JSONResponse:
    node = _get_node()
    if node.role != NodeRole.LEADER:
        return _leader_redirect_response()

    # Fencing check: lease_term must match the current leader's term
    if req.lease_term != node.current_term:
        logger.warning(
            "[%s] complete_job rejected: stale lease_term=%d current_term=%d",
            NODE_ID, req.lease_term, node.current_term,
        )
        return JSONResponse(
            content=CompleteJobResponse(
                accepted=False, reason="stale_term"
            ).model_dump()
        )

    job = node.job_table.get(req.job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.LEASED:
        # Already completed or failed — idempotent no-op
        return JSONResponse(
            content=CompleteJobResponse(accepted=True, reason="already_terminal").model_dump()
        )

    # Execution-level dedupe: check via storage (replicated across nodes)
    if await node._storage.has_completed_execution(req.job_id, req.worker_id):
        logger.info(
            "[%s] duplicate complete_job dropped: job=%s worker=%s",
            NODE_ID, req.job_id, req.worker_id,
        )
        return JSONResponse(
            content=CompleteJobResponse(accepted=True, reason="duplicate_execution").model_dump()
        )

    # Record the execution before replicating (idempotent INSERT OR IGNORE)
    await node._storage.add_completed_execution(req.job_id, req.worker_id)

    success = await node.replicate_entry({
        "type": "complete_job",
        "payload": {
            "job_id": req.job_id,
            "worker_id": req.worker_id,
            "result": req.result,
        },
    })
    if not success:
        raise HTTPException(status_code=503, detail="Failed to replicate to majority")

    return JSONResponse(content=CompleteJobResponse(accepted=True).model_dump())


@app.post("/worker/fail_job", response_model=FailJobResponse)
async def worker_fail_job(req: FailJobRequest) -> JSONResponse:
    node = _get_node()
    if node.role != NodeRole.LEADER:
        return _leader_redirect_response()

    # Fencing check
    if req.lease_term != node.current_term:
        return JSONResponse(
            content=FailJobResponse(
                accepted=False, reason="stale_term"
            ).model_dump()
        )

    job = node.job_table.get(req.job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    success = await node.replicate_entry({
        "type": "fail_job",
        "payload": {
            "job_id": req.job_id,
            "worker_id": req.worker_id,
            "error": req.error,
        },
    })
    if not success:
        raise HTTPException(status_code=503, detail="Failed to replicate to majority")

    # Determine next status after apply
    updated_job = node.job_table.get(req.job_id)
    next_status = updated_job.status.value if updated_job else "UNKNOWN"

    return JSONResponse(
        content=FailJobResponse(accepted=True, next_status=next_status).model_dump()
    )