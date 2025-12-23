import pytest
import json
import os
import sqlite3
import datetime
from memlayer.db import init_db, get_db_connection
from memlayer.models import Scope, EpisodePayload, L2DraftPayload, ArtifactRef
from memlayer.core.ingestion import commit_episode, promote_to_l2
from memlayer.core.governance import forget_memory, deprecate_memory, gc_sweep

TEST_DB = "test_unit_governance.db"

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
    return Scope(tenant_id="t1", workspace_id="w1", repo_id="r1", session_id="s1", user_id="u1")

def test_forget_memory(db_path, scope):
    # Setup L1 with user_id matching scope
    # Note: `commit_episode` uses `scope` to set fields. 
    # `scope` has `user_id="u1"`.
    # `commit_episode` inserts into `memory_l1` using `scope.user_id`.
    commit_episode(scope, EpisodePayload(title="User Ep", summary="S", tags=[]), "k1", db_path=db_path)
    
    # Forget by user_id
    # `forget_memory` uses `selector={"user_id": "u1"}`.
    # It queries: `DELETE FROM memory_l1 WHERE tenant_id=? AND workspace_id=? AND user_id=?`.
    # This should match.
    # Why did it fail with 0?
    # Let's verify DB state before forget.
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT user_id FROM memory_l1").fetchone()
        assert row["user_id"] == "u1"

    res = forget_memory(scope, {"user_id": "u1"}, db_path=db_path)
    assert res["status"] == "ok"
    assert res["deleted_l1"] == 1
    
    # Verify Tombstone
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM tombstones WHERE selector_hash=?", (res["tombstone_hash"],)).fetchone()
        assert row is not None

def test_deprecate_memory(db_path, scope):
    # Setup L2
    draft = L2DraftPayload(type="VerifiedFact", title="L2", summary="S", claims=["C"], applicability={"repo_id": "r1"})
    art = ArtifactRef(memory_id="x", layer="L2", kind="f", locator="l", classification="public", snippet_policy="allowed")
    id1 = promote_to_l2(scope, draft, art, "k2", db_path=db_path)["id"]
    
    # Deprecate
    res = deprecate_memory(scope, id1, "Old", db_path=db_path)
    assert res["status"] == "ok"
    assert res["action"] == "deprecated"
    
    # Verify DB
    with get_db_connection(db_path) as conn:
        # Commit needed? Ingestion/Governance functions usually commit.
        # check ingestion.py / governance.py
        # Yes, they call conn.commit().
        row = conn.execute("SELECT status FROM memory_l2_nodes WHERE id=?", (id1,)).fetchone()
        # Why did it fail? "active" == "deprecated"
        # Maybe `deprecate_memory` logic bug?
        # Check `deprecate_memory` in `memlayer/core/governance.py`.
        # It executes UPDATE but maybe filter is wrong?
        # `WHERE id = ?` -> id1
        # It calls `get_db_connection` which commits on exit? 
        # `get_db_connection` is a context manager but does NOT auto-commit!
        # It yields conn.
        # The function using it must commit.
        # Let's check `deprecate_memory`.
        assert row["status"] == "deprecated"

def test_gc_sweep(db_path, scope):
    with get_db_connection(db_path) as conn:
        # Insert expired L0
        conn.execute(
            "INSERT INTO memory_l0 (id, tenant_id, workspace_id, payload_json, expires_at) VALUES (?, ?, ?, ?, ?)",
            ("exp", "t1", "w1", "{}", "2000-01-01T00:00:00")
        )
        # Insert valid L0
        conn.execute(
            "INSERT INTO memory_l0 (id, tenant_id, workspace_id, payload_json, expires_at) VALUES (?, ?, ?, ?, ?)",
            ("valid", "t1", "w1", "{}", "2099-01-01T00:00:00")
        )
        conn.commit()
        
    res = gc_sweep(db_path=db_path)
    assert res["deleted_l0"] == 1
    
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT id FROM memory_l0").fetchall()
        assert len(row) == 1
        assert row[0]["id"] == "valid"
