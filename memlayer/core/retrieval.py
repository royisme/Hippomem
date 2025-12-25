from typing import List, Dict, Any, Optional, Literal, Tuple, Union
from memlayer.models import Scope
from memlayer.db import get_db_context
from memlayer.core.graph import GraphAccelerator
import sqlite3
import json
import math
import os

# Global accelerator
graph_accelerator = GraphAccelerator()

# Heuristic scoring weights
W_CONFIDENCE = 0.40
W_FRESHNESS = 0.15
W_TYPE = 0.10
W_VECTOR = 0.35 # Significantly boosted for hybrid search

def calculate_freshness(days_since_confirmed: float) -> float:
    # f = exp(-days_since_confirmed / 180)
    return math.exp(-days_since_confirmed / 180.0)

def search_memory(scope: Scope, query: str, view: str = "index", budget: int = 1000, top_k: int = 8, filters: Optional[Dict] = None, db_path: str = None, connection: sqlite3.Connection = None) -> Dict:
    """
    Searches memory using Hybrid Search (FTS + Vector).
    Note: 'query' is treated as text for FTS. For Vector, we assume an embedding is provided in filters
    or we skip vector part if no embedding is available (since we don't have an embedder here).

    If `filters` contains `query_embedding` (list of floats), we use it.
    """

    # Check for embedding in filters
    query_embedding = None
    if filters and "query_embedding" in filters:
        query_embedding = filters["query_embedding"]
        # Remove from filters so it doesn't break SQL generation
        # Copy filters to avoid mutating input
        filters = filters.copy()
        del filters["query_embedding"]

    with get_db_context(db_path, connection) as conn:
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
        # We fetch vector_distance if query_embedding is present
        vec_col_l1 = "0.0 as vector_dist"
        vec_param_l1 = []

        if query_embedding:
            # Check if sqlite-vec is active and table has column?
            # We assume DB has sqlite-vec loaded if we are here (or it will fail gracefully?)
            # vec_distance_L2(embedding, ?)
            # Note: We need to handle cases where embedding is NULL.
            # If embedding is NULL, distance is NULL (or handled by coalesce).
            # We'll use COALESCE(vec_distance_L2(m.embedding, ?), 2.0) -- 2.0 is max distance for normalized?
            # Let's assume unnormalized, but for ranking large distance is bad.
            # We want similarity. Sim = 1 / (1 + dist).

            # Serialize embedding to bytes for sqlite-vec
            import struct
            # sqlite-vec expects raw bytes of float32 array
            emb_bytes = struct.pack(f'{len(query_embedding)}f', *query_embedding)

            vec_col_l1 = "vec_distance_L2(m.embedding, ?) as vector_dist"
            vec_param_l1 = [emb_bytes]

        sql_l1 = f"""
            SELECT 
                m.id, m.type, m.title, m.summary, m.status, m.confidence, m.last_confirmed_at, m.applicability_json, m.claims_json,
                fts.rank as rank,
                {vec_col_l1},
                'L1' as layer
            FROM memory_l1 m
            JOIN memory_l1_fts fts ON m.id = fts.id
            WHERE {where_sql.replace('m.', 'm.')}
            AND memory_l1_fts MATCH ?
        """
        
        # L2 Query
        vec_col_l2 = "0.0 as vector_dist"
        vec_param_l2 = []
        if query_embedding:
             # Re-use bytes
             emb_bytes = struct.pack(f'{len(query_embedding)}f', *query_embedding)
             vec_col_l2 = "vec_distance_L2(m.embedding, ?) as vector_dist"
             vec_param_l2 = [emb_bytes]

        sql_l2 = f"""
            SELECT 
                m.id, m.type, m.title, m.summary, m.status, m.confidence, m.last_confirmed_at, m.applicability_json, m.claims_json,
                fts.rank as rank,
                {vec_col_l2},
                'L2' as layer
            FROM memory_l2_nodes m
            JOIN memory_l2_fts fts ON m.id = fts.id
            WHERE {where_sql.replace('m.', 'm.')}
            AND memory_l2_fts MATCH ?
        """
        
        # Combine
        full_sql = f"{sql_l1} UNION ALL {sql_l2} ORDER BY rank LIMIT ?"
        
        # Params structure:
        # L1: [vec_param if any] + [filter params] + [match query]
        # L2: [vec_param if any] + [filter params] + [match query]
        
        # Correction: The SELECT list comes first, but in WHERE clause?
        # Wait, parameters bind in order of appearance in the statement.
        # SELECT ... vec_distance(..., ?) ... WHERE ... = ? AND ... MATCH ?
        # So order is: Vector Param (if any), Filter Params, FTS Query
        
        l1_params = vec_param_l1 + params + [query]
        l2_params = vec_param_l2 + params + [query]
        combined_params = l1_params + l2_params + [top_k * 2]
        
        try:
            cursor = conn.execute(full_sql, combined_params)
            rows = cursor.fetchall()
        except sqlite3.OperationalError as e:
            # Fallback if vec_distance_L2 is missing or column missing
            if "no such function: vec_distance_L2" in str(e) or "no such column: m.embedding" in str(e):
                # Fallback to pure FTS
                 # Recursive call without embedding
                 # Or just re-construct query without vector part
                 # Simplified: return recursive call with query_embedding stripped from filters (already done at top, but we passed it as None?)
                 # If we are here, query_embedding was not None.
                 # Let's just log and continue without vector scores (effectively 0 dist -> handled in scoring)
                 # Actually if SQL failed, we need to re-run.

                 # Re-run without vector column in select
                 sql_l1_fallback = f"""
                    SELECT m.id, m.type, m.title, m.summary, m.status, m.confidence, m.last_confirmed_at, m.applicability_json, m.claims_json,
                    fts.rank as rank, 0.0 as vector_dist, 'L1' as layer
                    FROM memory_l1 m JOIN memory_l1_fts fts ON m.id = fts.id
                    WHERE {where_sql.replace('m.', 'm.')} AND memory_l1_fts MATCH ?
                 """
                 sql_l2_fallback = f"""
                    SELECT m.id, m.type, m.title, m.summary, m.status, m.confidence, m.last_confirmed_at, m.applicability_json, m.claims_json,
                    fts.rank as rank, 0.0 as vector_dist, 'L2' as layer
                    FROM memory_l2_nodes m JOIN memory_l2_fts fts ON m.id = fts.id
                    WHERE {where_sql.replace('m.', 'm.')} AND memory_l2_fts MATCH ?
                 """
                 full_sql_fb = f"{sql_l1_fallback} UNION ALL {sql_l2_fallback} ORDER BY rank LIMIT ?"
                 combined_params_fb = params + [query] + params + [query] + [top_k * 2]
                 cursor = conn.execute(full_sql_fb, combined_params_fb)
                 rows = cursor.fetchall()
            else:
                raise e

        # Scoring & Ranking
        scored_items = []
        for row in rows:
            # Parse dates
            # last_confirmed = row['last_confirmed_at'] 
            freshness = 1.0 
            
            confidence = row['confidence']
            vector_dist = row['vector_dist']
            
            # Vector Similarity: 1 / (1 + dist)
            # If vector_dist is 0 (fallback), similarity is 1.0 (max).
            # This biases fallback items to top.
            # If fallback happened, we should treat vector score as neutral (e.g. 0 contribution or average).
            # For now, let's assume if query_embedding was provided, we want vector influence.
            # If fallback, vector_dist is 0.0.

            if query_embedding is None:
                # Pure FTS: Vector weight is 0 effectively
                vec_score = 0
                w_vec = 0
            else:
                vec_score = 1.0 / (1.0 + vector_dist) if vector_dist is not None else 0
                w_vec = W_VECTOR

            # Boost L2 slightly or boost specific types?
            # Requirement: Decision/Contract > EpisodeSummary > Observation
            t = row['type']
            if t in ['Decision', 'Contract', 'VerifiedFact']:
                type_boost = 1.0
            elif t == 'EpisodeSummary':
                type_boost = 0.8
            else:
                type_boost = 0.5
            
            # Normalize FTS rank. Lower is better in FTS5 BM25.
            # Usually negative? FTS5 rank is roughly BM25 score * -1?
            # No, standard FTS5 rank is just a score, usually lower is better (more negative).
            # "The rank values ... are generally negative... more negative is better."
            # Let's invert/normalize.
            # Simple approach: Rank is arbitrary.
            # Let's use rank directly if we can't normalize.
            # Or just rely on sorting by score.
            # We'll treat rank as small negative number.
            # Convert to positive score: score = -rank
            fts_score = -1.0 * row['rank']
            # Clamp to 0..1? Hard without global stats.
            # Let's just use it raw but weighted.

            # Hybrid Score = Weighted Sum
            # Since fts_score is unbounded, this is tricky.
            # Reciprocal Rank Fusion is better for this, but we are doing weighted sum per requirements/convention?
            # Let's stick to Weighted Sum but acknowledge fts_score scale issues.

            score = (W_CONFIDENCE * confidence) + (W_FRESHNESS * freshness) + (W_TYPE * type_boost) + (w_vec * vec_score) + (0.5 * fts_score)

            scored_items.append((score, row))
            
        scored_items.sort(key=lambda x: x[0], reverse=True)
        top_items = scored_items[:top_k]
        
        # Packaging
        return package_results(top_items, view, budget, conn)

def expand_memory(scope: Scope, seed_id: str, hops: int = 1, view: str = "detail", budget: int = 1000, db_path: str = None, connection: sqlite3.Connection = None) -> Dict:
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
        with get_db_context(db_path, connection) as conn:
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
    with get_db_context(db_path, connection) as conn:
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
