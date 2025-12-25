import pytest
import json
import os
import sqlite3
from memlayer.db import init_db, get_db_connection, apply_migrations
from memlayer.models import Scope, EventPayload, EpisodePayload, L2DraftPayload, ArtifactRef
from memlayer.core.ingestion import upsert_event, commit_episode, promote_to_l2, link_memories
from pydantic import ValidationError

@pytest.fixture
def db_connection():
    # Create an in-memory connection
    with get_db_connection(":memory:") as conn:
        # Apply schema manually to the open connection
        apply_migrations(conn)
        yield conn

@pytest.fixture
def scope():
    return Scope(tenant_id="t1", workspace_id="w1", repo_id="r1", session_id="s1", module="mod1")

def test_upsert_event(db_connection, scope):
    payload = EventPayload(content="Test content", metadata={"k": "v"})
    
    # 1. Normal Upsert
    res = upsert_event(scope, payload, "k1", distill_to_l1=False, connection=db_connection)
    assert res["layer"] == "L0"
    assert "l1_id" not in res
    
    # Verify DB
    row = db_connection.execute("SELECT * FROM memory_l0 WHERE id=?", (res["id"],)).fetchone()
    assert row is not None
        
    # 2. Idempotency
    res2 = upsert_event(scope, payload, "k1", distill_to_l1=False, connection=db_connection)
    assert res2["id"] == res["id"]
    
    # 3. Distill to L1
    res3 = upsert_event(scope, payload, "k2", distill_to_l1=True, connection=db_connection)
    assert "l1_id" in res3
    
    row = db_connection.execute("SELECT * FROM memory_l1 WHERE id=?", (res3["l1_id"],)).fetchone()
    assert row["type"] == "Observation"

def test_commit_episode(db_connection, scope):
    episode = EpisodePayload(title="Ep1", summary="Sum1", tags=["t1"])
    
    # 1. Create
    res = commit_episode(scope, episode, "k_ep1", connection=db_connection)
    assert res["action"] == "created"
    id1 = res["id"]
    
    # 2. Update (same session)
    episode2 = EpisodePayload(title="Ep1 Updated", summary="Sum1", tags=["t1"])
    res2 = commit_episode(scope, episode2, "k_ep2", connection=db_connection)
    assert res2["action"] == "updated"
    assert res2["id"] == id1
    
    row = db_connection.execute("SELECT title FROM memory_l1 WHERE id=?", (id1,)).fetchone()
    assert row["title"] == "Ep1 Updated"

def test_promote_to_l2_validation(db_connection, scope):
    # Invalid Type - Pydantic raises ValidationError
    with pytest.raises(ValidationError):
        L2DraftPayload(type="Invalid", title="T", summary="S", claims=["C"])
    
    # Missing Claims
    art = ArtifactRef(memory_id="x", layer="L2", kind="f", locator="l", classification="public", snippet_policy="allowed")
    draft = L2DraftPayload(type="VerifiedFact", title="T", summary="S", claims=[])
    
    res = promote_to_l2(scope, draft, art, "k_p2", connection=db_connection)
    assert res["status"] == "error"
    assert res["message"] == "No claims provided"

def test_promote_to_l2_success(db_connection, scope):
    draft = L2DraftPayload(type="VerifiedFact", title="Fact", summary="Sum", claims=["Claim1"], applicability={"repo_id": "r1"})
    art = ArtifactRef(memory_id="x", layer="L2", kind="f", locator="l", classification="public", snippet_policy="allowed")
    
    res = promote_to_l2(scope, draft, art, "k_p3", connection=db_connection)
    assert res["status"] == "ok"
    
    # Verify FTS
    row = db_connection.execute("SELECT * FROM memory_l2_fts WHERE id=?", (res["id"],)).fetchone()
    assert row is not None
    assert row["title"] == "Fact"

def test_link_memories(db_connection, scope):
    # Setup 2 nodes
    draft = L2DraftPayload(type="VerifiedFact", title="N1", summary="S", claims=["C"], applicability={"repo_id": "r1"})
    art = ArtifactRef(memory_id="x", layer="L2", kind="f", locator="l", classification="public", snippet_policy="allowed")
    id1 = promote_to_l2(scope, draft, art, "k_n1", connection=db_connection)["id"]
    id2 = promote_to_l2(scope, draft, art, "k_n2", connection=db_connection)["id"]
    
    # Link
    res = link_memories(scope, id1, id2, "RELATED", idempotency_key="k_link1", connection=db_connection)
    assert res["status"] == "ok"
    
    # Verify
    row = db_connection.execute("SELECT * FROM memory_l2_edges WHERE from_id=? AND to_id=?", (id1, id2)).fetchone()
    assert row is not None
    assert row["rel"] == "RELATED"
        
    # Missing Node
    res = link_memories(scope, id1, "missing_id", "RELATED", connection=db_connection)
    assert res["status"] == "error"
    assert res["error_code"] == "NOT_FOUND"
