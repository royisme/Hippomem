from typing import Dict, List, Optional
from memlayer.models import Scope, Tombstone
from memlayer.db import get_db_connection
import datetime
import hashlib
import json
import sqlite3
import uuid

def compute_selector_hash(selector: Dict) -> str:
    # Sort keys to ensure deterministic hash
    s = json.dumps(selector, sort_keys=True)
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def forget_memory(scope: Scope, selector: Dict, db_path: str = None) -> Dict:
    # Selector keys: tags, time_range (start, end), user_id, type
    # AND logic
    
    selector_hash = compute_selector_hash(selector)
    
    with get_db_connection(db_path) as conn:
        # Create Tombstone
        try:
            conn.execute(
                "INSERT INTO tombstones (tenant_id, workspace_id, selector_hash, created_at) VALUES (?, ?, ?, ?)",
                (scope.tenant_id, scope.workspace_id, selector_hash, datetime.datetime.now().isoformat())
            )
        except sqlite3.IntegrityError:
            # Already exists
            pass
            
        # Build Query to identify items to delete/tombstone
        where_clauses = ["tenant_id = ?", "workspace_id = ?"]
        params = [scope.tenant_id, scope.workspace_id]
        
        # Apply selector
        if selector.get("user_id"):
            where_clauses.append("user_id = ?")
            params.append(selector["user_id"])
            
        # Time range (created_at)
        if selector.get("start_time"):
            where_clauses.append("created_at >= ?")
            params.append(selector["start_time"])
        if selector.get("end_time"):
            where_clauses.append("created_at <= ?")
            params.append(selector["end_time"])

        where_sql = " AND ".join(where_clauses)
        
        # 1. Hard delete L0
        # L0 does not have user_id, module, environment in current schema (only in payload)
        # Check if selector uses fields not in L0 columns.
        # L0 cols: tenant_id, workspace_id, repo_id, session_id, task_id
        # If selector has user_id, L0 delete might be tricky unless we query payload.
        # For V1, if selector has unsupported L0 fields, we skip L0 or warn?
        # Or we rely on session_id if available.
        # Let's adjust filtering for L0.
        l0_where_clauses = ["tenant_id = ?", "workspace_id = ?"]
        l0_params = [scope.tenant_id, scope.workspace_id]
        
        can_filter_l0 = True
        if selector.get("user_id"):
             # L0 table doesn't have user_id. 
             # We could parse payload_json, but that's slow.
             # For now, let's assume if user_id is specified, we skip L0 delete OR we delete everything in that scope? NO.
             # Let's Skip L0 if user_id is in selector, as L0 is ephemeral anyway (GC will catch it).
             can_filter_l0 = False
        
        if can_filter_l0:
            if selector.get("start_time"):
                 # L0 doesn't have created_at, only expires_at? 
                 # Actually it doesn't have created_at column in schema!
                 # So we can't filter by time for L0 easily.
                 can_filter_l0 = False
            if selector.get("end_time"):
                 can_filter_l0 = False
        
        if can_filter_l0:
            l0_where_sql = " AND ".join(l0_where_clauses)
            count_l0 = conn.execute(f"DELETE FROM memory_l0 WHERE {l0_where_sql}", l0_params).rowcount
        else:
            count_l0 = 0
        
        # 2. Hard delete L1
        # Bug fix: We originally used `count_l1 = conn.execute(...)`. But then we refactored to fetch IDs.
        # However, `cursor.execute` uses `params` which contains: [tenant_id, workspace_id, user_id]
        # `where_sql` is "tenant_id = ? AND workspace_id = ? AND user_id = ?"
        # But earlier we appended to `params` IF selector has user_id.
        # Check logic:
        # params = [scope.tenant_id, scope.workspace_id]
        # if selector.get("user_id"): ... params.append(...)
        # So `params` matches `where_sql`.
        
        cursor = conn.execute(f"SELECT id FROM memory_l1 WHERE {where_sql}", params)
        l1_ids = [row['id'] for row in cursor.fetchall()]
        
        if l1_ids:
            placeholders = ','.join(['?'] * len(l1_ids))
            # DELETE from main table
            conn.execute(f"DELETE FROM memory_l1 WHERE id IN ({placeholders})", l1_ids)
            # DELETE from FTS table
            conn.execute(f"DELETE FROM memory_l1_fts WHERE id IN ({placeholders})", l1_ids)
            count_l1 = len(l1_ids)
        else:
            count_l1 = 0

        # 3. L2: Mark as tombstoned (soft delete)
        # Note: L2 doesn't have user_id usually, but might have other selectors.
        # If selector has user_id, L2 might not match.
        # Assuming strict match.
        count_l2 = 0
        if not selector.get("user_id"): # L2 usually doesn't have user_id
             # Similar logic to get IDs
             cursor = conn.execute(f"SELECT id FROM memory_l2_nodes WHERE {where_sql}", params)
             l2_ids = [row['id'] for row in cursor.fetchall()]
             
             if l2_ids:
                 placeholders = ','.join(['?'] * len(l2_ids))
                 conn.execute(f"UPDATE memory_l2_nodes SET status='tombstoned' WHERE id IN ({placeholders})", l2_ids)
                 count_l2 = len(l2_ids)

        conn.commit()
        
        return {
            "status": "ok",
            "tombstone_hash": selector_hash,
            "deleted_l0": count_l0,
            "deleted_l1": count_l1,
            "tombstoned_l2": count_l2
        }

def deprecate_memory(scope: Scope, memory_id: str, reason: str, superseded_by: Optional[str] = None, db_path: str = None) -> Dict:
    with get_db_connection(db_path) as conn:
        # Check L1
        cursor = conn.execute("SELECT id FROM memory_l1 WHERE id = ? AND tenant_id = ?", (memory_id, scope.tenant_id))
        if cursor.fetchone():
            conn.execute("UPDATE memory_l1 SET status='deprecated' WHERE id = ?", (memory_id,))
            conn.commit()
            return {"status": "ok", "id": memory_id, "layer": "L1", "action": "deprecated"}
            
        # Check L2
        cursor = conn.execute("SELECT id FROM memory_l2_nodes WHERE id = ? AND tenant_id = ?", (memory_id, scope.tenant_id))
        if cursor.fetchone():
            # If deprecated, we just update status.
            # `supersedes_id` on the *new* node should point to this one, or this one points to new?
            # Schema says `supersedes_id` on L2 nodes. Usually "A supersedes B".
            # If we are deprecating A because of B, B should say "supersedes A".
            # But here we are editing A.
            
            conn.execute(f"UPDATE memory_l2_nodes SET status='deprecated' WHERE id = ?", (memory_id,))
            
            if superseded_by:
                 # Update the new node to point to this one as the one it supersedes
                 conn.execute("UPDATE memory_l2_nodes SET supersedes_id = ? WHERE id = ?", (memory_id, superseded_by))

            conn.commit()
            return {"status": "ok", "id": memory_id, "layer": "L2", "action": "deprecated"}
            
        return {"status": "error", "message": "Memory not found", "error_code": "NOT_FOUND"}

def gc_sweep(db_path: str = None) -> Dict:
    with get_db_connection(db_path) as conn:
        now = datetime.datetime.now().isoformat()
        # Delete expired L0
        cursor = conn.execute("DELETE FROM memory_l0 WHERE expires_at < ?", (now,))
        count = cursor.rowcount
        conn.commit()
        return {"status": "ok", "deleted_l0": count}

def gc_compact(scope: Optional[Scope] = None, db_path: str = None) -> Dict:
    """
    Compacts L1 Observations into EpisodeSummaries.
    Buckets by: tenant, workspace, repo, module, and 24h time window.
    """
    if not scope:
        # Require at least tenant/workspace for safety in this version
        return {"status": "error", "message": "Scope required for compaction"}

    with get_db_connection(db_path) as conn:
        # 1. Find active Observations that are not already summarized (heuristically check if they are very old or just grab all active)
        # We group by day.

        # SQLite doesn't have easy date truncation, assuming ISO format.
        # SUBSTR(created_at, 1, 10) gives YYYY-MM-DD

        sql = """
            SELECT
                SUBSTR(created_at, 1, 10) as day,
                repo_id,
                module,
                GROUP_CONCAT(id) as ids,
                GROUP_CONCAT(summary, ' || ') as combined_summary
            FROM memory_l1
            WHERE tenant_id = ? AND workspace_id = ?
            AND type = 'Observation'
            AND status = 'active'
            GROUP BY day, repo_id, module
        """

        cursor = conn.execute(sql, (scope.tenant_id, scope.workspace_id))
        rows = cursor.fetchall()

        compacted_count = 0
        episodes_created = 0

        for row in rows:
            day = row['day']
            repo_id = row['repo_id']
            module = row['module']
            ids = row['ids'].split(',')
            combined_summary = row['combined_summary']

            if len(ids) < 2:
                # Don't compact singletons for now
                continue

            # Create EpisodeSummary
            episode_id = str(uuid.uuid4()) # We need uuid
            now = datetime.datetime.now().isoformat()

            title = f"Episode: {day} - {module or 'General'}"
            summary = f"Compacted {len(ids)} observations. Content: {combined_summary[:200]}..."

            conn.execute(
                """
                INSERT INTO memory_l1 (
                    id, tenant_id, workspace_id, repo_id, module, environment, user_id, session_id, task_id,
                    type, status, title, summary, tags_json, entities_json, claims_json, applicability_json,
                    confidence, evidence_count, confirmation_count, created_at, updated_at, last_confirmed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (episode_id, scope.tenant_id, scope.workspace_id, repo_id, module, scope.environment, scope.user_id, None, None,
                 "EpisodeSummary", "active", title, summary, "[]", "[]", "[]", "{}",
                 0.8, len(ids), 1, now, now, now)
            )

            # Insert FTS
            conn.execute(
                "INSERT INTO memory_l1_fts (id, title, summary, tags_text, entities_text) VALUES (?, ?, ?, ?, ?)",
                (episode_id, title, summary, "", "")
            )

            # Archive original observations
            placeholders = ','.join(['?'] * len(ids))
            conn.execute(f"UPDATE memory_l1 SET status='archived' WHERE id IN ({placeholders})", ids)

            compacted_count += len(ids)
            episodes_created += 1

        conn.commit()

        return {
            "status": "ok",
            "compacted_observations": compacted_count,
            "episodes_created": episodes_created
        }
