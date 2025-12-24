import pytest
import sqlite3
import datetime
import os
from memlayer.models import Scope, EventPayload
from memlayer.core.ingestion import upsert_event, commit_episode
from memlayer.core.governance import gc_compact
from memlayer.db import init_db

# Use a temporary file instead of :memory: to persist across calls in the same test
DB_PATH = "test_m3.db"

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

def test_confidence_calculation(scope, db_path):
    # 1. Upsert event -> Confidence should be 0.5
    payload = EventPayload(content="test content", source="test")
    res1 = upsert_event(scope, payload, "id-1", distill_to_l1=True, db_path=db_path)
    l1_id = res1["l1_id"]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT confidence FROM memory_l1 WHERE id=?", (l1_id,)).fetchone()
    assert row['confidence'] == 0.5
    conn.close()

def test_compaction(scope, db_path):
    # Create 3 observations in same scope/day
    for i in range(3):
        payload = EventPayload(content=f"observation {i}", source="test")
        upsert_event(scope, payload, f"id-{i}", distill_to_l1=True, db_path=db_path)

    # Create 1 observation in different module
    scope2 = scope.copy(update={"module": "other"})
    upsert_event(scope2, EventPayload(content="other obs", source="test"), "id-other", distill_to_l1=True, db_path=db_path)

    # Run compact
    res = gc_compact(scope, db_path=db_path)
    assert res["status"] == "ok"
    assert res["compacted_observations"] == 3
    assert res["episodes_created"] == 1

    # Verify DB state
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Check archived
    archived = conn.execute("SELECT count(*) as c FROM memory_l1 WHERE status='archived'").fetchone()['c']
    assert archived == 3

    # Check new episode
    episodes = conn.execute("SELECT * FROM memory_l1 WHERE type='EpisodeSummary'").fetchall()
    assert len(episodes) == 1
    assert "Compacted 3 observations" in episodes[0]['summary']
    assert episodes[0]['module'] == "core"

    conn.close()
