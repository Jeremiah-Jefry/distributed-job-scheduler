"""
Thin async HTTP client used by coordinators (peer Raft RPCs)
and by workers (calls to the leader).

Wraps httpx.AsyncClient with a shared timeout and JSON helpers.
Each caller owns its own RaftRPCClient instance — no global state.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from common.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)

logger = logging.getLogger(__name__)

# Default timeouts (seconds). Keep them tighter than election timeout.
_CONNECT_TIMEOUT = 0.2
_READ_TIMEOUT = 0.5


class RaftRPCClient:
    """
    Async HTTP client for Raft internal RPCs (coordinator → coordinator).

    Usage:
        async with RaftRPCClient(peer_urls=["http://localhost:8002"]) as client:
            resp = await client.request_vote("http://localhost:8002", req)
    """

    def __init__(self, timeout: float = _READ_TIMEOUT) -> None:
        self._timeout = httpx.Timeout(timeout, connect=_CONNECT_TIMEOUT)
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "RaftRPCClient":
        self._client = httpx.AsyncClient(timeout=self._timeout)
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Raft internal RPCs
    # ------------------------------------------------------------------

    async def request_vote(
        self, peer_url: str, req: RequestVoteRequest
    ) -> Optional[RequestVoteResponse]:
        """Send RequestVote to a peer. Returns None on network error."""
        try:
            resp = await self._client.post(  # type: ignore[union-attr]
                f"{peer_url}/raft/request_vote",
                json=req.model_dump(),
            )
            resp.raise_for_status()
            return RequestVoteResponse.model_validate(resp.json())
        except Exception as exc:
            logger.debug("request_vote to %s failed: %s", peer_url, exc)
            return None

    async def append_entries(
        self, peer_url: str, req: AppendEntriesRequest
    ) -> Optional[AppendEntriesResponse]:
        """Send AppendEntries to a peer. Returns None on network error."""
        try:
            resp = await self._client.post(  # type: ignore[union-attr]
                f"{peer_url}/raft/append_entries",
                json=req.model_dump(),
            )
            resp.raise_for_status()
            return AppendEntriesResponse.model_validate(resp.json())
        except Exception as exc:
            logger.debug("append_entries to %s failed: %s", peer_url, exc)
            return None

    # ------------------------------------------------------------------
    # Generic helper for worker → coordinator calls
    # ------------------------------------------------------------------

    async def post_json(
        self,
        url: str,
        body: dict,
        *,
        timeout: Optional[float] = None,
    ) -> Optional[dict]:
        """
        Generic POST with JSON body. Used by workers to call coordinator
        endpoints (/worker/register, /worker/poll_job, etc.).
        Returns parsed JSON dict, or None on error.
        """
        effective_timeout = (
            httpx.Timeout(timeout, connect=_CONNECT_TIMEOUT)
            if timeout is not None
            else self._timeout
        )
        try:
            resp = await self._client.post(  # type: ignore[union-attr]
                url,
                json=body,
                timeout=effective_timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.debug("post_json to %s failed: %s", url, exc)
            return None

    async def get_json(self, url: str) -> Optional[dict]:
        """Generic GET returning parsed JSON dict, or None on error."""
        try:
            resp = await self._client.get(url)  # type: ignore[union-attr]
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.debug("get_json to %s failed: %s", url, exc)
            return None
