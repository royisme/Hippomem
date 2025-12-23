import pytest
import json
import os
import sqlite3
from memlayer.db import init_db, get_db_connection
from memlayer.models import Scope, EventPayload, EpisodePayload, L2DraftPayload, ArtifactRef
from memlayer.core.ingestion import upsert_event, commit_episode, promote_to_l2, link_memories

TEST_DB = "test_unit_ingestion.db"

@pytest.fixture
def db_path():
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    init_db(TEST_DB)
    yield TEST_DB
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

@pytest.fixture
def scope():
    return Scope(tenant_id="t1", workspace_id="w1", repo_id="r1", session_id="s1", module="mod1")

def test_upsert_event(db_path, scope):
    payload = EventPayload(content="Test content", metadata={"k": "v"})
    
    # 1. Normal Upsert
    res = upsert_event(scope, payload, "k1", distill_to_l1=False, db_path=db_path)
    assert res["layer"] == "L0"
    assert "l1_id" not in res
    
    # Verify DB
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM memory_l0 WHERE id=?", (res["id"],)).fetchone()
        assert row is not None
        
    # 2. Idempotency
    res2 = upsert_event(scope, payload, "k1", distill_to_l1=False, db_path=db_path)
    assert res2["id"] == res["id"]
    
    # 3. Distill to L1
    res3 = upsert_event(scope, payload, "k2", distill_to_l1=True, db_path=db_path)
    assert "l1_id" in res3
    
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM memory_l1 WHERE id=?", (res3["l1_id"],)).fetchone()
        assert row["type"] == "Observation"

def test_commit_episode(db_path, scope):
    episode = EpisodePayload(title="Ep1", summary="Sum1", tags=["t1"])
    
    # 1. Create
    res = commit_episode(scope, episode, "k_ep1", db_path=db_path)
    assert res["action"] == "created"
    id1 = res["id"]
    
    # 2. Update (same session)
    episode2 = EpisodePayload(title="Ep1 Updated", summary="Sum1", tags=["t1"])
    res2 = commit_episode(scope, episode2, "k_ep2", db_path=db_path)
    assert res2["action"] == "updated"
    assert res2["id"] == id1
    
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT title FROM memory_l1 WHERE id=?", (id1,)).fetchone()
        assert row["title"] == "Ep1 Updated"

from pydantic import ValidationError

def test_promote_to_l2_validation(db_path, scope):
    # Invalid Type - Pydantic raises ValidationError before it hits our logic if we construct payload directly
    # So we should catch ValidationError if testing construction, or bypass construction if testing logic.
    # But `promote_to_l2` expects a payload OBJECT.
    # If we pass an object, it must be valid Pydantic object.
    # So `draft.type` must be valid.
    # To test logic inside `promote_to_l2` that checks type (if any), we need to bypass Pydantic check or use a mock object?
    # Our code says: `if draft.type not in ...`. But Pydantic ensures it IS in ...
    # So that check is redundant if using Pydantic, but defensive.
    # Let's verify ValidationError is raised during object creation, which validates the model definition.
    
    with pytest.raises(ValidationError):
        L2DraftPayload(type="Invalid", title="T", summary="S", claims=["C"])
    
    # Check other logic that Pydantic allows but we reject?
    # e.g. Missing claims is checked by us explicitly: `if not draft.claims:`
    
    # Missing Claims
    art = ArtifactRef(memory_id="x", layer="L2", kind="f", locator="l", classification="public", snippet_policy="allowed")
    draft = L2DraftPayload(type="VerifiedFact", title="T", summary="S", claims=[])
    # The default value for claims is []? No, we check if not draft.claims.
    # L2DraftPayload claims is List[str] = [].
    
    res = promote_to_l2(scope, draft, art, "k_p2", db_path=db_path)
    assert res["status"] == "error"
    assert res["message"] == "No claims provided"

def test_promote_to_l2_success(db_path, scope):
    draft = L2DraftPayload(type="VerifiedFact", title="Fact", summary="Sum", claims=["Claim1"], applicability={"repo_id": "r1"})
    art = ArtifactRef(memory_id="x", layer="L2", kind="f", locator="l", classification="public", snippet_policy="allowed")
    
    res = promote_to_l2(scope, draft, art, "k_p3", db_path=db_path)
    assert res["status"] == "ok"
    
    # Verify FTS
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM memory_l2_fts WHERE id=?", (res["id"],)).fetchone()
        assert row is not None
        assert row["title"] == "Fact"

def test_link_memories(db_path, scope):
    # Setup 2 nodes
    draft = L2DraftPayload(type="VerifiedFact", title="N1", summary="S", claims=["C"], applicability={"repo_id": "r1"})
    art = ArtifactRef(memory_id="x", layer="L2", kind="f", locator="l", classification="public", snippet_policy="allowed")
    id1 = promote_to_l2(scope, draft, art, "k_n1", db_path=db_path)["id"]
    id2 = promote_to_l2(scope, draft, art, "k_n2", db_path=db_path)["id"]
    
    # Link
    res = link_memories(scope, id1, id2, "RELATED", idempotency_key="k_link1", db_path=db_path)
    assert res["status"] == "ok"
    
    # Verify
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM memory_l2_edges WHERE from_id=? AND to_id=?", (id1, id2)).fetchone()
        assert row is not None
        assert row["rel"] == "RELATED"
        
    # Missing Node
    res = link_memories(scope, id1, "missing_id", "RELATED", db_path=db_path)
    assert res["status"] == "error"
    assert res["error_code"] == "NOT_FOUND"
