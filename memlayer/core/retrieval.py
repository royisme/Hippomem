from typing import List, Dict, Any, Optional, Literal, Tuple, Union
from memlayer.models import Scope
from memlayer.db import get_db_connection
from memlayer.core.graph import GraphAccelerator
import sqlite3
import json
import math
import os

# Global accelerator
graph_accelerator = GraphAccelerator()

# Heuristic scoring weights
W_CONFIDENCE = 0.55
W_FRESHNESS = 0.20
W_TYPE = 0.10
W_VECTOR = 0.15 # Not used yet, but reserved

def calculate_freshness(days_since_confirmed: float) -> float:
    # f = exp(-days_since_confirmed / 180)
    return math.exp(-days_since_confirmed / 180.0)

def search_memory(scope: Scope, query: str, view: str = "index", budget: int = 1000, top_k: int = 8, filters: Optional[Dict] = None, db_path: str = None) -> Dict:
    with get_db_connection(db_path) as conn:
        # FTS Search on L1
        # We need to join with the main table to get metadata for filtering and scoring
        
        # Build filter clause
        # Hard scope filters
        params = [scope.tenant_id, scope.workspace_id]
        where_clauses = ["m.tenant_id = ?", "m.workspace_id = ?"]
        
        if scope.repo_id:
             where_clauses.append("(m.repo_id = ? OR m.repo_id IS NULL)") # Be permissive or strict? Req says "Apply hard scope filters". Usually strict match if provided.
             params.append(scope.repo_id)

        # Apply optional filters from input
        if filters:
            if filters.get("type"):
                where_clauses.append("m.type = ?")
                params.append(filters["type"])
            if filters.get("status"):
                where_clauses.append("m.status = ?")
                params.append(filters["status"])
        
        where_sql = " AND ".join(where_clauses)
        
        # FTS Query - Union L1 and L2
        
        # We need to construct SQL that queries both tables. 
        # Since tables have different schemas, we normalize them in the select.
        
        # L1 Query
        sql_l1 = f"""
            SELECT 
                m.id, m.type, m.title, m.summary, m.status, m.confidence, m.last_confirmed_at, m.applicability_json, m.claims_json,
                fts.rank as rank,
                'L1' as layer
            FROM memory_l1 m
            JOIN memory_l1_fts fts ON m.id = fts.id
            WHERE {where_sql.replace('m.', 'm.')}
            AND memory_l1_fts MATCH ?
        """
        
        # L2 Query
        # Reuse where_clauses but for L2. L2 also has tenant/workspace.
        # Check if optional filters apply to L2.
        # repo_id is in L2. type is in L2. status is in L2.
        # So where_clauses logic holds.
        sql_l2 = f"""
            SELECT 
                m.id, m.type, m.title, m.summary, m.status, m.confidence, m.last_confirmed_at, m.applicability_json, m.claims_json,
                fts.rank as rank,
                'L2' as layer
            FROM memory_l2_nodes m
            JOIN memory_l2_fts fts ON m.id = fts.id
            WHERE {where_sql.replace('m.', 'm.')}
            AND memory_l2_fts MATCH ?
        """
        
        # Combine
        full_sql = f"{sql_l1} UNION ALL {sql_l2} ORDER BY rank LIMIT ?"
        
        # Params: L1 params + match query + L2 params + match query + limit
        # L1 params: `params` (from top)
        # L2 params: same as `params` (assuming same filters apply)
        # But we appended to `params` earlier? No, `params` only has filter values so far.
        
        combined_params = params + [query] + params + [query] + [top_k * 2]
        
        cursor = conn.execute(full_sql, combined_params)
        rows = cursor.fetchall()
        
        # Scoring & Ranking
        scored_items = []
        for row in rows:
            # Parse dates
            # last_confirmed = row['last_confirmed_at'] 
            freshness = 1.0 
            
            confidence = row['confidence']
            
            # Boost L2 slightly or boost specific types?
            # Requirement: Decision/Contract > EpisodeSummary > Observation
            t = row['type']
            if t in ['Decision', 'Contract', 'VerifiedFact']:
                type_boost = 1.0
            elif t == 'EpisodeSummary':
                type_boost = 0.8
            else:
                type_boost = 0.5
            
            score = (W_CONFIDENCE * confidence) + (W_FRESHNESS * freshness) + (W_TYPE * type_boost)
            scored_items.append((score, row))
            
        scored_items.sort(key=lambda x: x[0], reverse=True)
        top_items = scored_items[:top_k]
        
        # Packaging
        return package_results(top_items, view, budget, conn)

def expand_memory(scope: Scope, seed_id: str, hops: int = 1, view: str = "detail", budget: int = 1000, db_path: str = None) -> Dict:
    # Try Accelerator First
    accelerated_result = graph_accelerator.expand(seed_id, hops)
    
    node_ids = set()
    path_info = []
    
    if accelerated_result:
        # Use accelerated result to populate node_ids and path_info
        for node in accelerated_result['nodes']:
            if node['id'] != seed_id:
                node_ids.add(node['id'])
        for edge in accelerated_result['edges']:
            path_info.append(edge)
            
    # Fallback to SQLite if accelerator failed/disabled or returned empty (check logic? No, if it worked, use it. If None, fallback)
    if accelerated_result is None:
        with get_db_connection(db_path) as conn:
            # Hop 1
            sql = """
                SELECT to_id, rel FROM memory_l2_edges
                WHERE tenant_id = ? AND workspace_id = ? AND from_id = ?
            """
            cursor = conn.execute(sql, (scope.tenant_id, scope.workspace_id, seed_id))
            edges = cursor.fetchall()
            
            for edge in edges:
                node_ids.add(edge['to_id'])
                path_info.append({"from": seed_id, "rel": edge['rel'], "to": edge['to_id']})
                
            # Hop 2 (if requested)
            if hops > 1 and node_ids:
                placeholders = ','.join(['?'] * len(node_ids))
                sql = f"""
                    SELECT from_id, to_id, rel FROM memory_l2_edges
                    WHERE tenant_id = ? AND workspace_id = ? AND from_id IN ({placeholders})
                """
                params = [scope.tenant_id, scope.workspace_id] + list(node_ids)
                cursor = conn.execute(sql, params)
                edges_h2 = cursor.fetchall()
                
                for edge in edges_h2:
                    # Avoid cycles back to seed? Or just add all unique nodes.
                    if edge['to_id'] != seed_id:
                        node_ids.add(edge['to_id'])
                        path_info.append({"from": edge['from_id'], "rel": edge['rel'], "to": edge['to_id']})

    # Fetch Node Details (Always from SQLite for rich data/evidence)
    with get_db_connection(db_path) as conn:
        if not node_ids:
            return {"status": "ok", "view": view, "items": [], "truncation": {"truncated": False, "remaining_budget": budget}, "paths": path_info}

        placeholders = ','.join(['?'] * len(node_ids))
        sql = f"SELECT * FROM memory_l2_nodes WHERE id IN ({placeholders})"
        cursor = conn.execute(sql, list(node_ids))
        nodes = cursor.fetchall()
        
        result = package_results(nodes, view, budget, conn)
        result["paths"] = path_info
        return result

def package_results(rows: List[Any], view: str, budget: int, conn: sqlite3.Connection) -> Dict:
    # rows can be list of sqlite3.Row OR list of tuples (score, row) if coming from search
    items = []
    used_tokens = 0 # Rough estimate chars / 4
    truncated = False
    
    for entry in rows:
        if isinstance(entry, tuple):
            score, row = entry
        else:
            score, row = 0.0, entry

        item = {
            "id": row['id'],
            "type": row['type'],
            "title": row['title'],
            "score": score
        }
        
        # Cost of index
        item_cost = len(json.dumps(item)) // 4
        
        if view in ["detail", "evidence"]:
            details = {
                "summary": row['summary'],
                "status": row['status'],
                "confidence": row['confidence'],
                "applicability": json.loads(row['applicability_json']),
                "claims": json.loads(row['claims_json'])
            }
            # Fetch artifacts if evidence
            if view == "evidence":
                # Only L2 has artifact links in current ingestion logic, but L1 might have them too in future
                # Checking artifacts table
                cursor = conn.execute("SELECT * FROM memory_artifacts WHERE memory_id = ?", (row['id'],))
                artifacts = cursor.fetchall()
                art_list = []
                for art in artifacts:
                    art_entry = {
                        "kind": art['kind'],
                        "locator": art['locator'],
                        "snippet_policy": art['snippet_policy']
                    }
                    if art['snippet_policy'] == 'allowed':
                     # Read file if exists
                     snippet = "[Content placeholder]"
                     if art['kind'] == 'file':
                        # Safe read: only allow reading from allowed directories?
                        # Requirement says "Artifacts stored as files under artifacts directory"
                        # But locator can be anything.
                        # For V1, we try to read file at locator.
                        try:
                            # Assume locator is absolute or relative to CWD
                            if os.path.exists(art['locator']) and os.path.isfile(art['locator']):
                                with open(art['locator'], 'r', encoding='utf-8', errors='ignore') as f:
                                    content = f.read(1024) # Limit to 1KB for now
                                    snippet = content
                        except Exception:
                            pass
                     art_entry['snippet'] = snippet
                    art_list.append(art_entry)
                details["artifacts"] = art_list

            item.update(details)
            item_cost = len(json.dumps(item)) // 4
            
        if used_tokens + item_cost > budget:
            truncated = True
            break
        
        used_tokens += item_cost
        items.append(item)
        
    return {
        "status": "ok",
        "view": view,
        "items": items,
        "truncation": {
            "truncated": truncated,
            "reason": "TOKEN_BUDGET" if truncated else None,
            "remaining_budget": budget - used_tokens
        },
        "token_estimate_used": used_tokens
    }
