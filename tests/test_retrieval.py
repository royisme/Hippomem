import pytest
import json
import os
import sqlite3
from memlayer.db import init_db, get_db_connection
from memlayer.models import Scope, L2DraftPayload, ArtifactRef, EpisodePayload
from memlayer.core.ingestion import promote_to_l2, link_memories, commit_episode
from memlayer.core.retrieval import search_memory, expand_memory

TEST_DB = "test_unit_retrieval.db"

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
    return Scope(tenant_id="t1", workspace_id="w1", repo_id="r1", session_id="s1")

def seed_data(db_path, scope):
    # L1
    commit_episode(scope, EpisodePayload(title="L1 Item", summary="Summary L1", tags=["tag1"]), "k_l1", db_path=db_path)
    
    # L2
    draft = L2DraftPayload(type="VerifiedFact", title="L2 Item", summary="Summary L2", claims=["C"], applicability={"repo_id": "r1"})
    art = ArtifactRef(memory_id="x", layer="L2", kind="file", locator="test_file.txt", classification="public", snippet_policy="allowed")
    res = promote_to_l2(scope, draft, art, "k_l2", db_path=db_path)
    return res["id"]

def test_search_memory_unified(db_path, scope):
    seed_data(db_path, scope)
    
    # Search common term "Item"
    res = search_memory(scope, "Item", db_path=db_path)
    assert len(res["items"]) == 2
    types = [item["type"] for item in res["items"]]
    assert "EpisodeSummary" in types
    assert "VerifiedFact" in types

def test_search_memory_filtering(db_path, scope):
    seed_data(db_path, scope)
    
    # Filter by type
    res = search_memory(scope, "Item", filters={"type": "VerifiedFact"}, db_path=db_path)
    assert len(res["items"]) == 1
    assert res["items"][0]["type"] == "VerifiedFact"

def test_expand_memory(db_path, scope):
    id1 = seed_data(db_path, scope)
    
    # Create another node and link
    draft = L2DraftPayload(type="VerifiedFact", title="L2 Linked", summary="Sum", claims=["C"], applicability={"repo_id": "r1"})
    art = ArtifactRef(memory_id="x", layer="L2", kind="file", locator="l", classification="public", snippet_policy="allowed")
    id2 = promote_to_l2(scope, draft, art, "k_l2_2", db_path=db_path)["id"]
    
    link_memories(scope, id1, id2, "LINKS", db_path=db_path)
    
    # Expand
    res = expand_memory(scope, id1, hops=1, db_path=db_path)
    assert len(res["items"]) == 1
    assert res["items"][0]["id"] == id2
    assert res["paths"][0]["to"] == id2

def test_package_results_budget_and_evidence(db_path, scope):
    # Create a real file for evidence
    with open("test_file.txt", "w") as f:
        f.write("This is the content of the artifact.")
        
    try:
        id1 = seed_data(db_path, scope)
        
        # 1. Evidence View
        res = search_memory(scope, "L2 Item", view="evidence", db_path=db_path)
        item = res["items"][0]
        assert "artifacts" in item
        assert item["artifacts"][0]["snippet"] == "This is the content of the artifact."
        
        # 2. Budget Truncation
        # Set very low budget
        res = search_memory(scope, "L2 Item", view="evidence", budget=10, db_path=db_path)
        assert res["truncation"]["truncated"] is True
        # Should return empty or truncated list?
        # Logic: `if used_tokens + item_cost > budget: break`
        # Item cost includes JSON dump. 10 chars is very small.
        # It might return empty list if first item fails.
        assert len(res["items"]) == 0
        
    finally:
        if os.path.exists("test_file.txt"):
            os.remove("test_file.txt")
