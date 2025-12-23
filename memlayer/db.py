import sqlite3
import contextlib
import os
from typing import Generator

DB_PATH_DEFAULT = os.path.expanduser("~/.local/share/memlayer/memlayer.db")

@contextlib.contextmanager
def get_db_connection(db_path: str = None) -> Generator[sqlite3.Connection, None, None]:
    if db_path is None:
        db_path = DB_PATH_DEFAULT
    
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # Enable WAL mode and foreign keys as recommended
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        yield conn
    finally:
        conn.close()

def init_db(db_path: str = None):
    with get_db_connection(db_path) as conn:
        # Schema version
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL
            );
        """)
        
        # Idempotency
        conn.execute("""
            CREATE TABLE IF NOT EXISTS idempotency (
                tenant_id TEXT NOT NULL,
                key TEXT NOT NULL,
                created_at TEXT NOT NULL,
                result_json TEXT NOT NULL,
                PRIMARY KEY (tenant_id, key)
            );
        """)

        # Memory L0
        conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_l0 (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                repo_id TEXT,
                session_id TEXT,
                task_id TEXT,
                payload_json TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_l0_scope ON memory_l0 (tenant_id, workspace_id, repo_id, session_id, task_id);")

        # Memory L1
        conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_l1 (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                repo_id TEXT,
                module TEXT,
                environment TEXT,
                user_id TEXT,
                session_id TEXT,
                task_id TEXT,
                type TEXT NOT NULL,
                status TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                entities_json TEXT NOT NULL,
                claims_json TEXT NOT NULL,
                applicability_json TEXT NOT NULL,
                confidence REAL NOT NULL,
                evidence_count INTEGER NOT NULL,
                confirmation_count INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_confirmed_at TEXT NOT NULL,
                ttl_seconds INTEGER
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_l1_scope ON memory_l1 (tenant_id, workspace_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_l1_type_status ON memory_l1 (type, status);")

        # Memory L1 FTS
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS memory_l1_fts USING fts5(
                id UNINDEXED,
                title,
                summary,
                tags_text,
                entities_text
            );
        """)

        # Memory L2 FTS
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS memory_l2_fts USING fts5(
                id UNINDEXED,
                title,
                summary,
                tags_text,
                entities_text
            );
        """)

        # Memory Artifacts
        conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_artifacts (
                memory_id TEXT NOT NULL,
                layer TEXT NOT NULL,
                kind TEXT NOT NULL,
                locator TEXT NOT NULL,
                hash TEXT,
                created_at TEXT,
                classification TEXT NOT NULL,
                snippet_policy TEXT NOT NULL,
                PRIMARY KEY (memory_id, kind, locator)
            );
        """)

        # Memory L2 Nodes
        conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_l2_nodes (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                repo_id TEXT,
                module TEXT,
                environment TEXT,
                type TEXT NOT NULL,
                status TEXT NOT NULL,
                version INTEGER NOT NULL,
                supersedes_id TEXT,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                entities_json TEXT NOT NULL,
                claims_json TEXT NOT NULL,
                applicability_json TEXT NOT NULL,
                confidence REAL NOT NULL,
                evidence_count INTEGER NOT NULL,
                confirmation_count INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_confirmed_at TEXT NOT NULL
            );
        """)

        # Memory L2 Edges
        conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_l2_edges (
                tenant_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                from_id TEXT NOT NULL,
                rel TEXT NOT NULL,
                to_id TEXT NOT NULL,
                weight REAL NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (tenant_id, workspace_id, from_id, rel, to_id)
            );
        """)

        # Tombstones
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tombstones (
                tenant_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                selector_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (tenant_id, workspace_id, selector_hash)
            );
        """)
