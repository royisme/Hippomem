import os
import json
import pytest
import sqlite3
import datetime
from click.testing import CliRunner
from memlayer.cli import cli
from memlayer.db import init_db

TEST_DB = "test_integration.db"

@pytest.fixture
def runner():
    return CliRunner()

@pytest.fixture
def db_path():
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    init_db(TEST_DB)
    yield TEST_DB
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

def test_full_lifecycle(runner, db_path):
    scope = json.dumps({"tenant_id": "t1", "workspace_id": "w1", "repo_id": "r1", "session_id": "s1", "module": "core"})
    
    # 1. Init
    result = runner.invoke(cli, ['init', '--db-path', db_path])
    assert result.exit_code == 0
    
    # 2. Upsert Event
    payload = json.dumps({"content": "System crash due to memory leak", "metadata": {}})
    result = runner.invoke(cli, ['event', 'upsert', '--scope', scope, '--payload', payload, '--idempotency-key', 'k1', '--distill', '--db-path', db_path])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data['data']['layer'] == 'L0'
    l1_id = data['data'].get('l1_id')
    assert l1_id is not None
    
    # 3. Search (L1)
    result = runner.invoke(cli, ['search', '--scope', scope, '--query', 'memory leak', '--db-path', db_path])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert len(data['data']['items']) >= 1
    # Check that we found the L1 item
    found_l1 = any(item['id'] == l1_id for item in data['data']['items'])
    assert found_l1
    
    # 4. Commit Episode
    episode = json.dumps({
        "title": "Fixing Memory Leak",
        "summary": "Investigated and fixed memory leak.",
        "tags": ["bug", "memory"],
        "entities": ["System"],
        "claims": ["Leak fixed"],
        "applicability": {}
    })
    result = runner.invoke(cli, ['episode', 'commit', '--scope', scope, '--episode', episode, '--idempotency-key', 'k2', '--db-path', db_path])
    assert result.exit_code == 0
    
    # 5. Promote L2
    draft = json.dumps({
        "type": "VerifiedFact",
        "title": "Memory Limit",
        "summary": "System has 4GB limit",
        "claims": ["Limit is 4GB"],
        "applicability": {"repo_id": "r1"}
    })
    artifact = json.dumps({
        "memory_id": "x",
        "layer": "L2",
        "kind": "file",
        "locator": "config.yaml",
        "classification": "internal",
        "snippet_policy": "allowed"
    })
    result = runner.invoke(cli, ['promote', '--scope', scope, '--draft', draft, '--artifact', artifact, '--idempotency-key', 'k3', '--db-path', db_path])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data['data']['status'] == 'ok'
    l2_id = data['data']['id']

    # 5.5 Search (L2) - verify L2 is searchable
    result = runner.invoke(cli, ['search', '--scope', scope, '--query', '4GB limit', '--db-path', db_path])
    assert result.exit_code == 0
    data = json.loads(result.output)
    # Should find the L2 item
    found_l2 = any(item['id'] == l2_id for item in data['data']['items'])
    assert found_l2

    # 5.6 Link L2 Nodes
    # Promote another L2
    draft2 = json.dumps({
        "type": "VerifiedFact",
        "title": "Related Fact",
        "summary": "This is related.",
        "claims": ["Related"],
        "applicability": {"repo_id": "r1"}
    })
    artifact2 = json.dumps({
        "memory_id": "y",
        "layer": "L2",
        "kind": "file",
        "locator": "test_artifact.txt",
        "classification": "internal",
        "snippet_policy": "allowed"
    })
    # Create dummy artifact file
    with open("test_artifact.txt", "w") as f:
        f.write("This is a test artifact content.")
        
    result = runner.invoke(cli, ['promote', '--scope', scope, '--draft', draft2, '--artifact', artifact2, '--idempotency-key', 'k4', '--db-path', db_path])
    l2_id_2 = json.loads(result.output)['data']['id']
    
    # Link them
    result = runner.invoke(cli, ['link', '--scope', scope, '--from-id', l2_id, '--to-id', l2_id_2, '--rel', 'RELATED_TO', '--db-path', db_path])
    assert result.exit_code == 0
    assert json.loads(result.output)['data']['status'] == 'ok'
    
    # 6. Expand (L2) - Check link and Evidence
    result = runner.invoke(cli, ['expand', '--scope', scope, '--seed', l2_id, '--view', 'evidence', '--db-path', db_path])
    assert result.exit_code == 0
    data = json.loads(result.output)
    
    # Check Paths
    assert len(data['data']['paths']) > 0
    assert data['data']['paths'][0]['to'] == l2_id_2
    
    # Check Evidence Content (for l2_id_2 if expanded? Expand expands TO target, so l2_id_2 is target)
    # The `expand` command returns node details for the expanded nodes.
    # So `items` should contain l2_id_2.
    target_item = next((item for item in data['data']['items'] if item['id'] == l2_id_2), None)
    assert target_item is not None
    assert 'artifacts' in target_item
    assert target_item['artifacts'][0]['snippet'] == "This is a test artifact content."
    
    # Clean up
    if os.path.exists("test_artifact.txt"):
        os.remove("test_artifact.txt")
    
    # 7. Deprecate
    result = runner.invoke(cli, ['deprecate', '--scope', scope, '--id', l2_id, '--reason', 'Test', '--db-path', db_path])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data['data']['action'] == 'deprecated'
    
    # 8. GC Sweep
    result = runner.invoke(cli, ['gc', 'sweep', '--db-path', db_path])
    assert result.exit_code == 0

    # 9. Forget
    selector = json.dumps({"user_id": "u1"}) # Scope doesn't have user_id, so this might not find anything, but should run
    result = runner.invoke(cli, ['forget', '--scope', scope, '--selector', selector, '--db-path', db_path])
    assert result.exit_code == 0
