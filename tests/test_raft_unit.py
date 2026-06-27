"""
Unit tests for Raft term/vote correctness.

These tests directly exercise RaftNode logic without running real HTTP servers.
They verify the correctness of vote granting rules, term persistence,
log up-to-date checks, and step-down behavior.
"""
from __future__ import annotations

import asyncio
import pytest
import pytest_asyncio

from common.models import LogEntry, RequestVoteRequest
from common.rpc_client import RaftRPCClient
from coordinator.storage import RaftStorage
from coordinator.raft_state import RaftNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def make_node(tmp_path, node_id: str = "coordinator-1", peers: list[str] | None = None) -> RaftNode:
    db_path = str(tmp_path / f"{node_id}.db")
    storage = RaftStorage(db_path)
    await storage.initialize()
    node = RaftNode(
        node_id=node_id,
        peer_urls=peers or [],
        storage=storage,
    )
    # Manually init without starting background tasks (we test logic directly)
    node.current_term = await storage.get_term()
    node.voted_for = await storage.get_voted_for()
    # Provide a real RPC client (unused in unit tests — no peers)
    node._rpc_client = RaftRPCClient()
    await node._rpc_client.__aenter__()
    return node


async def cleanup_node(node: RaftNode) -> None:
    if node._rpc_client:
        await node._rpc_client.__aexit__(None, None, None)
    await node._storage.close()


# ---------------------------------------------------------------------------
# Term and vote persistence
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_node_starts_with_term_zero(tmp_path):
    node = await make_node(tmp_path)
    assert node.current_term == 0
    await cleanup_node(node)


@pytest.mark.asyncio
async def test_vote_persists_after_grant(tmp_path):
    node = await make_node(tmp_path)
    req = RequestVoteRequest(
        term=1, candidate_id="coordinator-2", last_log_index=0, last_log_term=0
    )
    resp = await node.handle_request_vote(req)
    assert resp.vote_granted is True
    assert resp.term == 1

    # Verify it's actually written to storage
    stored_term = await node._storage.get_term()
    stored_vote = await node._storage.get_voted_for()
    assert stored_term == 1
    assert stored_vote == "coordinator-2"
    await cleanup_node(node)


@pytest.mark.asyncio
async def test_vote_not_granted_for_stale_term(tmp_path):
    node = await make_node(tmp_path)
    # Advance node to term 5
    await node._step_down(5)

    req = RequestVoteRequest(
        term=3,  # lower than node's term 5
        candidate_id="coordinator-2",
        last_log_index=0,
        last_log_term=0,
    )
    resp = await node.handle_request_vote(req)
    assert resp.vote_granted is False
    assert resp.term == 5
    await cleanup_node(node)


@pytest.mark.asyncio
async def test_vote_not_granted_twice_in_same_term(tmp_path):
    node = await make_node(tmp_path)

    req1 = RequestVoteRequest(
        term=2, candidate_id="coordinator-2", last_log_index=0, last_log_term=0
    )
    resp1 = await node.handle_request_vote(req1)
    assert resp1.vote_granted is True

    # Same term, different candidate
    req2 = RequestVoteRequest(
        term=2, candidate_id="coordinator-3", last_log_index=0, last_log_term=0
    )
    resp2 = await node.handle_request_vote(req2)
    assert resp2.vote_granted is False
    await cleanup_node(node)


@pytest.mark.asyncio
async def test_vote_granted_again_to_same_candidate_same_term(tmp_path):
    """Idempotent: already voted for this candidate → grant again."""
    node = await make_node(tmp_path)
    req = RequestVoteRequest(
        term=2, candidate_id="coordinator-2", last_log_index=0, last_log_term=0
    )
    resp1 = await node.handle_request_vote(req)
    resp2 = await node.handle_request_vote(req)
    assert resp1.vote_granted is True
    assert resp2.vote_granted is True
    await cleanup_node(node)


# ---------------------------------------------------------------------------
# Log up-to-date check
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_vote_denied_if_candidate_log_stale(tmp_path):
    """Candidate with lower last_log_index than node → no vote."""
    node = await make_node(tmp_path)
    # Give node a log entry at index=5, term=2
    await node._storage.append_log_entry(
        LogEntry(term=2, index=5, command={"type": "noop"})
    )
    # Advance term so node is at term 2
    await node._step_down(2)

    req = RequestVoteRequest(
        term=3,
        candidate_id="coordinator-2",
        last_log_index=3,   # stale: our log goes to 5
        last_log_term=2,
    )
    resp = await node.handle_request_vote(req)
    assert resp.vote_granted is False


@pytest.mark.asyncio
async def test_vote_granted_if_candidate_log_at_least_as_uptodate(tmp_path):
    """Candidate with same last_log_index and term → vote granted."""
    node = await make_node(tmp_path)
    await node._storage.append_log_entry(
        LogEntry(term=2, index=5, command={"type": "noop"})
    )
    await node._step_down(2)

    req = RequestVoteRequest(
        term=3,
        candidate_id="coordinator-2",
        last_log_index=5,   # equal to ours
        last_log_term=2,
    )
    resp = await node.handle_request_vote(req)
    assert resp.vote_granted is True


@pytest.mark.asyncio
async def test_vote_denied_if_candidate_log_term_stale(tmp_path):
    """Candidate whose last_log_term is lower than ours → no vote."""
    node = await make_node(tmp_path)
    await node._storage.append_log_entry(
        LogEntry(term=4, index=1, command={"type": "noop"})
    )
    await node._step_down(4)

    req = RequestVoteRequest(
        term=5,
        candidate_id="coordinator-2",
        last_log_index=1,
        last_log_term=3,   # stale term
    )
    resp = await node.handle_request_vote(req)
    assert resp.vote_granted is False


# ---------------------------------------------------------------------------
# Step down behavior
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_higher_term_in_request_causes_stepdown(tmp_path):
    node = await make_node(tmp_path)
    await node._step_down(3)   # currently at term 3

    # RequestVote with higher term should cause stepdown first, then evaluate
    req = RequestVoteRequest(
        term=10,
        candidate_id="coordinator-2",
        last_log_index=0,
        last_log_term=0,
    )
    resp = await node.handle_request_vote(req)
    assert node.current_term == 10
    assert resp.vote_granted is True
    await cleanup_node(node)


@pytest.mark.asyncio
async def test_step_down_clears_voted_for(tmp_path):
    node = await make_node(tmp_path)
    await node._storage.save_term_and_vote(3, "coordinator-2")
    node.current_term = 3
    node.voted_for = "coordinator-2"

    await node._step_down(4)
    assert node.voted_for is None
    assert await node._storage.get_voted_for() is None
    await cleanup_node(node)
