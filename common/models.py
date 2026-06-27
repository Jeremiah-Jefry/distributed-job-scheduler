from __future__ import annotations

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
import time


# ---------------------------------------------------------------------------
# Core enums
# ---------------------------------------------------------------------------

class NodeRole(str, Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    LEASED = "LEASED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


# ---------------------------------------------------------------------------
# Core data models
# ---------------------------------------------------------------------------

class Job(BaseModel):
    job_id: str
    idempotency_key: str
    payload: dict
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    lease_term: Optional[int] = None        # Raft term when lease was issued
    lease_holder: Optional[str] = None      # worker_id holding current lease
    lease_expiry: Optional[float] = None    # unix timestamp
    result: Optional[dict] = None
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)


class LogEntry(BaseModel):
    term: int
    index: int
    # command type: "submit_job" | "lease_job" | "complete_job" | "fail_job"
    command: dict


# ---------------------------------------------------------------------------
# Raft internal RPC models (node-to-node only)
# ---------------------------------------------------------------------------

class RequestVoteRequest(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


class RequestVoteResponse(BaseModel):
    term: int
    vote_granted: bool


class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


class AppendEntriesResponse(BaseModel):
    term: int
    success: bool
    # Hint so leader can fast-decrement next_index on conflict
    conflict_index: Optional[int] = None


# ---------------------------------------------------------------------------
# Client-facing request/response models
# ---------------------------------------------------------------------------

class SubmitJobRequest(BaseModel):
    idempotency_key: str
    payload: dict
    max_attempts: int = 3


class SubmitJobResponse(BaseModel):
    job_id: str
    status: JobStatus
    idempotency_key: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    attempts: int
    result: Optional[dict] = None
    idempotency_key: str
    lease_holder: Optional[str] = None
    created_at: float
    updated_at: float


# Non-leader redirect envelope
class RedirectResponse(BaseModel):
    leader_url: Optional[str] = None
    leader_id: Optional[str] = None
    detail: str = "Not the leader"


# ---------------------------------------------------------------------------
# Worker-facing request/response models
# ---------------------------------------------------------------------------

class WorkerRegisterRequest(BaseModel):
    worker_id: str


class WorkerRegisterResponse(BaseModel):
    acknowledged: bool
    leader_id: Optional[str] = None


class PollJobRequest(BaseModel):
    worker_id: str


class LeasedJobPayload(BaseModel):
    job_id: str
    payload: dict
    lease_term: int
    lease_expiry: float


class PollJobResponse(BaseModel):
    job: Optional[LeasedJobPayload] = None


class CompleteJobRequest(BaseModel):
    job_id: str
    worker_id: str
    lease_term: int
    result: dict


class CompleteJobResponse(BaseModel):
    accepted: bool
    reason: Optional[str] = None


class FailJobRequest(BaseModel):
    job_id: str
    worker_id: str
    lease_term: int
    error: str


class FailJobResponse(BaseModel):
    accepted: bool
    next_status: Optional[str] = None
    reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Operational
# ---------------------------------------------------------------------------

class StatusResponse(BaseModel):
    node_id: str
    role: NodeRole
    term: int
    commit_index: int
    leader_id: Optional[str] = None
    queue_depth: int
    dead_letter_count: int