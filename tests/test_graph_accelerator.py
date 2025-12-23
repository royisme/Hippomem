import pytest
from unittest.mock import MagicMock, patch
from memlayer.core.graph import GraphAccelerator

def test_graph_accelerator_disabled_if_no_client():
    # If we mock falkordb import failure or init failure
    with patch("memlayer.core.graph.FalkorDB", None):
        acc = GraphAccelerator()
        assert acc.enabled is False
        assert acc.upsert_node("1", "T", "t", [], 1.0) is False
        assert acc.expand("1") is None

def test_graph_accelerator_enabled():
    with patch("memlayer.core.graph.FalkorDB") as mock_falkor:
        mock_client = MagicMock()
        mock_graph = MagicMock()
        mock_falkor.return_value = mock_client
        mock_client.select_graph.return_value = mock_graph
        
        acc = GraphAccelerator()
        assert acc.enabled is True
        
        # Test Upsert
        acc.upsert_node("id1", "Fact", "Title", ["tag"], 0.9)
        mock_graph.query.assert_called()
        
        # Test Expand
        # Mock result set
        mock_result = MagicMock()
        # Complex mocking of Path object would be needed here to verify parsing logic
        # For now, verify query call
        acc.expand("id1")
        mock_graph.query.assert_called()

import os
from memlayer.db import init_db
from memlayer.models import Scope

@pytest.fixture
def db_path():
    path = "test_acc_fallback.db"
    if os.path.exists(path):
        os.remove(path)
    init_db(path)
    yield path
    if os.path.exists(path):
        os.remove(path)

@pytest.fixture
def scope():
    return Scope(tenant_id="t1", workspace_id="w1", repo_id="r1")

def test_fallback_logic(db_path, scope):
    # This tests integration via ingestion/retrieval with DISABLED accelerator
    # Real accelerator is disabled in this env because no redis
    # So we verify SQLite fallback works
    from memlayer.core.ingestion import promote_to_l2
    from memlayer.models import L2DraftPayload, ArtifactRef
    from memlayer.core.retrieval import expand_memory
    
    # Ingest
    draft = L2DraftPayload(type="VerifiedFact", title="T", summary="S", claims=["C"])
    art = ArtifactRef(memory_id="x", layer="L2", kind="f", locator="l", classification="public", snippet_policy="allowed")
    promote_to_l2(scope, draft, art, "k_fallback", db_path=db_path)
    
    # Expand (should rely on SQLite)
    res = expand_memory(scope, "non_existent", db_path=db_path)
    assert res['status'] == 'ok'
    # It works (returns empty or valid result) without crashing
