from __future__ import annotations
from uuid import UUID
from typing import Optional
from fastapi import APIRouter, HTTPException, Query

from infra.db import DBClient
from services.structured import StructuredIndexerService


def create_structured_router(db: DBClient, svc: StructuredIndexerService) -> APIRouter:
    router = APIRouter(prefix="/structured", tags=["structured"])

    @router.post("/index/{doc_id}")
    async def index_one(doc_id: UUID):
        did = str(doc_id)
        with db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM documents WHERE doc_id=%s::uuid LIMIT 1;", (did,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="document not found")
        try:
            return svc.index_doc(did)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"structured_index_failed: {e}")

    @router.get("/metrics/total_spend")
    async def total_spend(start: str = Query(..., description="YYYY-MM-DD"),
                          end: str = Query(..., description="YYYY-MM-DD")):
        try:
            v = db.total_spend(start=start, end=end)
            return {"start": start, "end": end, "total_spend": v}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"metrics_failed: {e}")

    return router

