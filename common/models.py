from enum import Enum
from pydantic import BaseModel
from typing import Optional

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

class Job(BaseModel):
    job_id: str
    idempotency_key: str
    payload: dict
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    lease_term: Optional[int] = None
    lease_expiry: Optional[float] = None