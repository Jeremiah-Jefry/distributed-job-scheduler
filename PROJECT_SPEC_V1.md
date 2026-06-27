# Distributed Job Scheduler — Version 1 Specification

This is the authoritative spec for V1. If anything in conversation or comments contradicts this document, this document wins. V1 is feature-complete for the scope below — there is no "V1.5"; everything here must work, or it is explicitly listed in Non-Goals.

---

## 1. Goal

A fault-tolerant distributed job scheduler: a 3-node coordinator cluster running simplified Raft for leader election and log replication, dispatching jobs to a pool of worker processes with lease-based at-least-once delivery, idempotent execution, and fencing-token protection against stale leaders.

## 2. Non-Goals (explicitly out of scope for V1 — do not implement)

- Raft log compaction / snapshotting
- Dynamic cluster membership changes (adding/removing coordinator nodes at runtime)
- gRPC or any binary protocol — HTTP/JSON only
- Multi-shard / multi-Raft-group scaling
- Persistent worker state across worker restarts (workers re-register from empty state)
- Authentication/authorization on any endpoint
- A UI of any kind — JSON APIs only, curl-able

If you (the agent) believe something on this list is required for correctness, stop and flag it rather than silently implementing it.

---

## 3. Components

### 3.1 Coordinator (3 processes: `coordinator-1`, `coordinator-2`, `coordinator-3`)

Each coordinator is a FastAPI app holding:
- Raft role: `FOLLOWER | CANDIDATE | LEADER`
- Persisted state (SQLite, one file per node, must survive process restart): `current_term: int`, `voted_for: Optional[str]`, `log: list[LogEntry]`
- In-memory (rebuilt from log on startup): `commit_index`, `job_table: dict[job_id, Job]`, `idempotency_index: dict[idempotency_key, job_id]`

### 3.2 Worker (N processes, N ≥ 2 for testing)

Stateless. On startup: registers with whichever coordinator it's pointed at. If that node isn't the leader, it must receive a redirect to the current leader and register there instead. Polls for jobs, executes, reports back.

### 3.3 Client

A simple CLI or script (not a service) that submits jobs with a caller-supplied `idempotency_key` and polls `/job/{job_id}` for status. Must handle leader redirects.

---

## 4. Data Models

```python
class JobStatus(str, Enum):
    PENDING = "PENDING"
    LEASED = "LEASED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"

class Job(BaseModel):
    job_id: str                      # server-generated UUID
    idempotency_key: str              # client-supplied, unique per logical job
    payload: dict
    status: JobStatus
    attempts: int = 0
    max_attempts: int = 3
    lease_term: Optional[int] = None  # Raft term active when lease was issued
    lease_holder: Optional[str] = None  # worker_id holding current lease
    lease_expiry: Optional[float] = None  # unix timestamp
    result: Optional[dict] = None
    created_at: float
    updated_at: float

class LogEntry(BaseModel):
    term: int
    index: int
    command: dict   # {"type": "submit_job"|"lease_job"|"complete_job"|"fail_job", "payload": {...}}
```

---

## 5. Raft Behavior (simplified — this exact scope, no more, no less)

- **Election timeout**: randomized per node in range 150–300ms, reset on receiving valid `AppendEntries` from current leader or granting a vote.
- **RequestVote**: candidate increments `current_term`, votes for self, sends `RequestVote{term, candidate_id, last_log_index, last_log_term}` to peers. A node grants its vote at most once per term, and only if the candidate's log is at least as up to date as its own.
- **Heartbeats**: leader sends empty `AppendEntries` every 50ms to maintain authority.
- **Log replication**: leader appends new entries locally first, replicates to followers via `AppendEntries{term, leader_id, prev_log_index, prev_log_term, entries, leader_commit}`. An entry is **committed** once replicated to a majority (2 of 3). Only committed entries are applied to `job_table`.
- **Term persistence**: `current_term` and `voted_for` must be fsync'd to SQLite before responding to any vote request — a node must never vote twice in the same term even after a crash/restart.
- **Stale leader detection**: any RPC carrying a term lower than a node's `current_term` is rejected; the responder includes its own term so the sender can step down.

---

## 6. API Contracts

### 6.1 Raft internal (node-to-node only, not for client/worker use)

**`POST /raft/request_vote`**
```json
// Request
{"term": 4, "candidate_id": "coordinator-2", "last_log_index": 12, "last_log_term": 3}
// Response
{"term": 4, "vote_granted": true}
```

**`POST /raft/append_entries`**
```json
// Request
{"term": 4, "leader_id": "coordinator-1", "prev_log_index": 12, "prev_log_term": 3,
 "entries": [{"term": 4, "index": 13, "command": {...}}], "leader_commit": 12}
// Response
{"term": 4, "success": true}
```

### 6.2 Client-facing

**`POST /submit_job`** — must redirect (HTTP 307 or a JSON `{"redirect_to": "<leader_url>"}`, agent's choice, but be consistent) if called on a non-leader.
```json
// Request
{"idempotency_key": "client-key-123", "payload": {"task": "resize_image", "url": "..."}}
// Response (new job)
{"job_id": "uuid", "status": "PENDING", "idempotency_key": "client-key-123"}
// Response (duplicate idempotency_key) — same job_id returned, no new job created
{"job_id": "uuid", "status": "LEASED", "idempotency_key": "client-key-123"}
```

**`GET /job/{job_id}`**
```json
{"job_id": "uuid", "status": "COMPLETED", "attempts": 1, "result": {...}}
```

### 6.3 Worker-facing

**`POST /worker/register`**
```json
// Request: {"worker_id": "worker-1"}
// Response: {"acknowledged": true, "leader_id": "coordinator-2"}
```

**`POST /worker/poll_job`** — long-poll or short-poll with client-side retry, agent's choice; document which.
```json
// Request: {"worker_id": "worker-1"}
// Response (job available)
{"job": {"job_id": "uuid", "payload": {...}, "lease_term": 4, "lease_expiry": 1719500000.0}}
// Response (no job available)
{"job": null}
```

**`POST /worker/complete_job`**
```json
// Request: {"job_id": "uuid", "worker_id": "worker-1", "lease_term": 4, "result": {...}}
// Response: {"accepted": true}
// OR if lease_term no longer matches current leader's term:
// Response: {"accepted": false, "reason": "stale_term"}
```

**`POST /worker/fail_job`**
```json
// Request: {"job_id": "uuid", "worker_id": "worker-1", "lease_term": 4, "error": "..."}
// Response: {"accepted": true, "next_status": "PENDING"}  // or "DEAD_LETTER" if attempts exhausted
```

### 6.4 Operational

**`GET /health`** — already implemented, do not change its contract.

**`GET /status`**
```json
{"node_id": "coordinator-1", "role": "LEADER", "term": 4, "commit_index": 12,
 "queue_depth": 3, "dead_letter_count": 1}
```

---

## 7. Core Behaviors That MUST Work (this is the actual test of "flawless")

1. **Leader crash mid-operation**: kill the leader process. Within ~1 second, the remaining 2 nodes elect a new leader. No committed job is lost.
2. **Worker crash mid-job**: kill a worker process after it has leased a job but before it reports completion. The lease expires (background watcher checks `lease_expiry` against wall clock); job returns to `PENDING`; a different worker picks it up.
3. **Duplicate submission**: submitting the same `idempotency_key` twice returns the same `job_id` both times — does not create a second job.
4. **Duplicate execution**: if a job gets redelivered (case 2) and the original worker's completion report arrives late, only one `COMPLETED` transition takes effect (use an execution-dedupe check, not just trusting the client).
5. **Stale leader fencing**: simulate an old leader (deposed but not yet aware) attempting to accept a `complete_job` for a lease it issued in an earlier term. It must be rejected by checking that `lease_term` matches the *current* leader's term — not just any past term.
6. **Dead-letter**: a job that fails `max_attempts` times moves to `DEAD_LETTER` and stops being retried.
7. **Follower redirect**: submitting a job to a follower node returns a redirect to the current leader, not a silent failure or an incorrect local write.

These six behaviors are not optional and not "nice to have" — they are the definition of this project. If time runs short, cut polish (logging format, CLI niceties), never cut these.

---

## 8. Non-Functional Requirements

- Python 3.12, fully type-hinted (functions and class attributes — no bare `Any` unless genuinely justified)
- Fully async (`asyncio`/FastAPI/`httpx.AsyncClient`) — no blocking I/O in request handlers
- Each coordinator's persisted state lives in its own SQLite file (`coordinator_1.db`, etc.) — never shared across nodes
- No global mutable state shared across "nodes" — each node, even running on localhost, must be a fully independent process with its own memory and its own state file. This is a hard invariant: if the project would still work after replacing `asyncio.sleep` network calls with direct Python function calls between nodes, the isolation is wrong.
- pytest test suite covering: Raft term/vote correctness, all six behaviors in Section 7, and submission/idempotency edge cases

---

## 9. Definition of Done

V1 is complete when, from a clean checkout:

- [ ] 5 processes (3 coordinators + 2 workers) start from documented commands and all hit `/health`
- [ ] Killing the leader process results in a new leader within the election timeout, verifiable via `/status` on the survivors
- [ ] A submitted job completes successfully end-to-end via `/submit_job` → `/job/{id}` polling
- [ ] Killing a worker mid-job results in the job being picked up and completed by a different worker, verifiable via `/job/{id}`
- [ ] Resubmitting the same `idempotency_key` does not create a duplicate job (verify via `/job/{id}` showing identical `job_id`)
- [ ] `pytest tests/ -v` passes, covering all items in Section 7
- [ ] `README.md`'s API Reference table accurately reflects implemented vs. planned for every endpoint (no endpoint marked `(planned)` once it's actually implemented and tested)
- [ ] Each meaningful unit of work is a separate git commit with a descriptive message — not one giant commit at the end
