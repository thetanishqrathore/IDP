from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Dict

from infra.db import DBClient
from infra.minio_store import MinioStore
from infra.qdrant import QdrantIndex


class _ResetRequest(BaseModel):
    confirm: bool = False


def create_admin_router(
    db: DBClient,
    tenant_id: str,
    raw_store: MinioStore,
    canonical_store: MinioStore,
    qdr: QdrantIndex | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/admin", tags=["admin"])

    @router.post("/reset")
    async def reset_workspace(payload: _ResetRequest) -> Dict[str, Any]:
        if not payload.confirm:
            raise HTTPException(status_code=400, detail="confirmation_required")

        summary = db.wipe_tenant_data(tenant_id)
        removed_raw = 0
        removed_canonical = 0

        for sha in summary.get("sha256", []):
            if not sha:
                continue
            key = MinioStore.build_key_for_sha256(str(sha))
            raw_store.delete_object(key)
            removed_raw += 1

        for prefix in summary.get("canonical_prefixes", []):
            prefix_str = str(prefix).strip().strip("/")
            if not prefix_str:
                continue
            canonical_store.remove_prefix(f"{prefix_str}/")
            removed_canonical += 1

        if qdr is not None:
            try:
                qdr.delete_tenant(tenant_id)
            except Exception:
                pass

        return {
            "ok": True,
            "deleted": summary.get("deleted", {}),
            "doc_count": len(summary.get("doc_records", [])),
            "raw_objects_removed": removed_raw,
            "canonical_prefixes_removed": removed_canonical,
        }

    return router
