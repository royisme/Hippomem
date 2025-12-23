from typing import Optional, List, Dict, Any, Union, Literal
from pydantic import BaseModel, Field, field_validator
import datetime

# --- Auxiliary Models ---

class Scope(BaseModel):
    tenant_id: str
    workspace_id: str
    repo_id: Optional[str] = None
    module: Optional[str] = None
    environment: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    task_id: Optional[str] = None

class IdempotencyRecord(BaseModel):
    tenant_id: str
    key: str
    created_at: datetime.datetime
    result_json: str

class Tombstone(BaseModel):
    tenant_id: str
    workspace_id: str
    selector_hash: str
    created_at: datetime.datetime

# --- Artifacts ---

class ArtifactRef(BaseModel):
    memory_id: str
    layer: Literal['L1', 'L2']
    kind: str
    locator: str
    hash: Optional[str] = None
    created_at: Optional[datetime.datetime] = None
    classification: Literal['public', 'internal', 'restricted']
    snippet_policy: Literal['allowed', 'forbidden']

# --- Memory Layers ---

class L0Memory(BaseModel):
    id: str
    tenant_id: str
    workspace_id: str
    repo_id: Optional[str] = None
    session_id: Optional[str] = None
    task_id: Optional[str] = None
    payload_json: str
    expires_at: datetime.datetime

class L1Memory(BaseModel):
    id: str
    tenant_id: str
    workspace_id: str
    repo_id: Optional[str] = None
    module: Optional[str] = None
    environment: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    task_id: Optional[str] = None
    type: Literal['Observation', 'EpisodeSummary']
    status: Literal['active', 'deprecated', 'tombstoned', 'merged']
    title: str
    summary: str
    tags_json: str # List[str] serialized
    entities_json: str # List[str] serialized
    claims_json: str # List[str] serialized
    applicability_json: str # Dict serialized
    confidence: float
    evidence_count: int
    confirmation_count: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    last_confirmed_at: datetime.datetime
    ttl_seconds: Optional[int] = None

class L2Memory(BaseModel):
    id: str
    tenant_id: str
    workspace_id: str
    repo_id: Optional[str] = None
    module: Optional[str] = None
    environment: Optional[str] = None
    type: Literal['Decision', 'Contract', 'VerifiedFact', 'StableConstraint']
    status: str
    version: int
    supersedes_id: Optional[str] = None
    title: str
    summary: str
    tags_json: str
    entities_json: str
    claims_json: str
    applicability_json: str
    confidence: float
    evidence_count: int
    confirmation_count: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    last_confirmed_at: datetime.datetime

class Relation(BaseModel):
    tenant_id: str
    workspace_id: str
    from_id: str
    rel: str # e.g., MENTIONS, DEPENDS_ON, REPLACES
    to_id: str
    weight: float
    created_at: datetime.datetime

# --- Input Payloads ---

class EventPayload(BaseModel):
    # This matches the payload for `event upsert`
    # The actual payload content is flexible, but we might want some structure
    content: str
    metadata: Optional[Dict[str, Any]] = None

class EpisodePayload(BaseModel):
    # Matches `episode commit`
    title: str
    summary: str
    tags: List[str] = []
    entities: List[str] = []
    claims: List[str] = []
    applicability: Dict[str, Any] = {}

class L2DraftPayload(BaseModel):
    # Matches `promote` draft
    type: Literal['Decision', 'Contract', 'VerifiedFact', 'StableConstraint']
    title: str
    summary: str
    tags: List[str] = []
    entities: List[str] = []
    claims: List[str] = []
    applicability: Dict[str, Any] = {}
