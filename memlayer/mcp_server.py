from typing import Any, Dict, List, Optional
from mcp.server.fastmcp import FastMCP
import json
import datetime
from memlayer.models import Scope, EventPayload, EpisodePayload, L2DraftPayload, ArtifactRef
from memlayer.core.ingestion import upsert_event, commit_episode, promote_to_l2, link_memories
from memlayer.core.retrieval import search_memory, expand_memory
from memlayer.core.governance import forget_memory, deprecate_memory, gc_sweep
from memlayer.db import DB_PATH_DEFAULT

mcp = FastMCP("memlayer")

# Utility to helper construct scope from flat args
def _make_scope(tenant_id: str, workspace_id: str, repo_id: Optional[str] = None, 
                module: Optional[str] = None, environment: Optional[str] = None, 
                user_id: Optional[str] = None, session_id: Optional[str] = None, 
                task_id: Optional[str] = None) -> Scope:
    return Scope(
        tenant_id=tenant_id, workspace_id=workspace_id, repo_id=repo_id,
        module=module, environment=environment, user_id=user_id,
        session_id=session_id, task_id=task_id
    )

@mcp.tool()
def mem_upsert(tenant_id: str, workspace_id: str, content: str, idempotency_key: str,
               repo_id: Optional[str] = None, session_id: Optional[str] = None, 
               task_id: Optional[str] = None, metadata: Optional[Dict] = None, 
               distill: bool = False) -> Dict:
    """Upsert an event (L0) to working memory. Optionally distill to L1."""
    scope = _make_scope(tenant_id, workspace_id, repo_id=repo_id, session_id=session_id, task_id=task_id)
    payload = EventPayload(content=content, metadata=metadata or {})
    return upsert_event(scope, payload, idempotency_key, distill, db_path=DB_PATH_DEFAULT)

@mcp.tool()
def mem_commit(tenant_id: str, workspace_id: str, title: str, summary: str, idempotency_key: str,
               repo_id: Optional[str] = None, session_id: Optional[str] = None, task_id: Optional[str] = None,
               tags: List[str] = [], entities: List[str] = [], claims: List[str] = [], 
               applicability: Dict = {}) -> Dict:
    """Commit an episode summary (L1) to episodic memory."""
    scope = _make_scope(tenant_id, workspace_id, repo_id=repo_id, session_id=session_id, task_id=task_id)
    payload = EpisodePayload(title=title, summary=summary, tags=tags, entities=entities, claims=claims, applicability=applicability)
    return commit_episode(scope, payload, idempotency_key, db_path=DB_PATH_DEFAULT)

@mcp.tool()
def mem_promote(tenant_id: str, workspace_id: str, type: str, title: str, summary: str, 
                artifact_locator: str, idempotency_key: str,
                repo_id: Optional[str] = None, module: Optional[str] = None, environment: Optional[str] = None,
                claims: List[str] = [], tags: List[str] = [], entities: List[str] = [], applicability: Dict = {},
                artifact_kind: str = "file", artifact_hash: Optional[str] = None,
                classification: str = "internal", snippet_policy: str = "allowed") -> Dict:
    """Promote a stable fact/decision (L2) to canonical memory."""
    scope = _make_scope(tenant_id, workspace_id, repo_id=repo_id, module=module, environment=environment)
    draft = L2DraftPayload(type=type, title=title, summary=summary, tags=tags, entities=entities, claims=claims, applicability=applicability)
    # Note: memory_id in ArtifactRef is placeholder until created, but we need to pass a valid ArtifactRef
    artifact = ArtifactRef(memory_id="placeholder", layer="L2", kind=artifact_kind, locator=artifact_locator, 
                           hash=artifact_hash, classification=classification, snippet_policy=snippet_policy)
    return promote_to_l2(scope, draft, artifact, idempotency_key, db_path=DB_PATH_DEFAULT)

@mcp.tool()
def mem_link(tenant_id: str, workspace_id: str, from_id: str, to_id: str, rel: str, idempotency_key: str) -> Dict:
    """Link two L2 memory nodes."""
    scope = _make_scope(tenant_id, workspace_id)
    return link_memories(scope, from_id, to_id, rel, 1.0, idempotency_key, db_path=DB_PATH_DEFAULT)

@mcp.tool()
def mem_search(tenant_id: str, workspace_id: str, query: str, 
               repo_id: Optional[str] = None, view: str = "index", budget: int = 1000, top_k: int = 8,
               filters: Optional[Dict] = None) -> Dict:
    """Search memory (L1/L2) by text query."""
    scope = _make_scope(tenant_id, workspace_id, repo_id=repo_id)
    return search_memory(scope, query, view, budget, top_k, filters, db_path=DB_PATH_DEFAULT)

@mcp.tool()
def mem_expand(tenant_id: str, workspace_id: str, seed_id: str, 
               hops: int = 1, view: str = "detail", budget: int = 1000) -> Dict:
    """Expand memory relations (L2) from a seed ID."""
    scope = _make_scope(tenant_id, workspace_id)
    return expand_memory(scope, seed_id, hops, view, budget, db_path=DB_PATH_DEFAULT)

@mcp.tool()
def mem_forget(tenant_id: str, workspace_id: str, user_id: Optional[str] = None,
               start_time: Optional[str] = None, end_time: Optional[str] = None) -> Dict:
    """Forget memories by selector (Tombstone)."""
    scope = _make_scope(tenant_id, workspace_id)
    selector = {}
    if user_id: selector["user_id"] = user_id
    if start_time: selector["start_time"] = start_time
    if end_time: selector["end_time"] = end_time
    return forget_memory(scope, selector, db_path=DB_PATH_DEFAULT)

@mcp.tool()
def mem_deprecate(tenant_id: str, workspace_id: str, memory_id: str, reason: str, superseded_by: Optional[str] = None) -> Dict:
    """Deprecate a memory node."""
    scope = _make_scope(tenant_id, workspace_id)
    return deprecate_memory(scope, memory_id, reason, superseded_by, db_path=DB_PATH_DEFAULT)

if __name__ == "__main__":
    mcp.run()
