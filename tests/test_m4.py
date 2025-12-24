import pytest
import sqlite3
import os
import json
from memlayer.models import Scope, EventPayload
from memlayer.core.ingestion import upsert_event
from memlayer.core.retrieval import search_memory
from memlayer.db import init_db

# Use a temporary file
DB_PATH = "test_m4.db"

@pytest.fixture
def db_path():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db(DB_PATH)
    yield DB_PATH
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

@pytest.fixture
def scope():
    return Scope(
        tenant_id="t1",
        workspace_id="w1",
        repo_id="r1",
        module="core",
        environment="dev",
        user_id="u1"
    )

def test_hybrid_search_fallback(scope, db_path):
    # Test that hybrid search works (falls back gracefully) even without embeddings in DB
    upsert_event(scope, EventPayload(content="test content alpha", source="test"), "id-1", distill_to_l1=True, db_path=db_path)
    upsert_event(scope, EventPayload(content="test content beta", source="test"), "id-2", distill_to_l1=True, db_path=db_path)

    # Search with embedding (mock)
    # Since we can't easily populate embeddings without sqlite-vec logic in upsert (which we skipped for now),
    # we mainly test the retrieval logic's ability to handle the "query_embedding" filter and generate SQL.

    # Note: If sqlite-vec is NOT installed/loaded in this environment, it should fallback.
    # If it IS installed, it might run but all distances will be 0 or null if data is null.

    # We pass a dummy embedding
    embedding = [0.1] * 1536
    filters = {"query_embedding": embedding}

    res = search_memory(scope, "alpha", filters=filters, db_path=db_path)

    assert res["status"] == "ok"
    assert len(res["items"]) > 0
    assert res["items"][0]["title"] == "Observation: test content alpha"

def test_schema_has_embedding_column(db_path):
    # Verify migration worked
    conn = sqlite3.connect(db_path)
    # Check L1
    cursor = conn.execute("PRAGMA table_info(memory_l1)")
    cols = [row[1] for row in cursor.fetchall()]
    assert "embedding" in cols

    # Check L2
    cursor = conn.execute("PRAGMA table_info(memory_l2_nodes)")
    cols = [row[1] for row in cursor.fetchall()]
    assert "embedding" in cols
    conn.close()
