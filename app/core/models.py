from __future__ import annotations
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid


class IngestResponseItem(BaseModel):
    tenant_id: str
    doc_id: Optional[str] = None
    sha256: str
    state: str
    size_bytes: int
    mime: str
    uri: str
    duplicate: bool = False
    minio_key: Optional[str] = None
    events: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class StoredResult(BaseModel):
    # internal service result (maps 1:1 to response)
    response: IngestResponseItem
    event_payloads: List[Dict[str, Any]] = Field(default_factory=list)


def new_uuid() -> str:
    return str(uuid.uuid4())


def utc_now_iso() -> str:
    return datetime.utcnow().isoformat()
