from __future__ import annotations
from typing import List, Optional
from fastapi import APIRouter, HTTPException
from infra.db import DBClient
from services.normalization import NormalizationService
from uuid import UUID
from fastapi.concurrency import run_in_threadpool
def create_normalize_router(db: DBClient, service: NormalizationService) -> APIRouter:
    router = APIRouter(prefix="", tags=["normalization"])

    @router.post("/normalize/{doc_id}")
    async def normalize_one(doc_id: UUID):
        did = str(doc_id)
        with db.conn.cursor() as cur:
            cur.execute("SELECT doc_id, sha256, mime FROM documents WHERE doc_id=%s::uuid LIMIT 1;", (did,))
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="document not found")
        try:
            return await run_in_threadpool(service.run_one, doc_id=row["doc_id"], sha256=row["sha256"], mime=row["mime"])  # type: ignore
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"normalize_failed: {e}")

    @router.post("/normalize")
    async def normalize_many(doc_ids: list[UUID]):
        ids = [str(x) for x in doc_ids]
        results = []
        with db.conn.cursor() as cur:
            cur.execute("SELECT doc_id, sha256, mime FROM documents WHERE doc_id = ANY(%s::uuid[]);", (ids,))
            rows = cur.fetchall()
        found = {r["doc_id"]: r for r in rows}
        for did in ids:
            row = found.get(did)
            if not row:
                results.append({"doc_id": did, "status": "NOT_FOUND"})
                continue
            try:
                results.append(await run_in_threadpool(service.run_one, doc_id=row["doc_id"], sha256=row["sha256"], mime=row["mime"]))  # type: ignore
            except Exception as e:
                results.append({"doc_id": did, "status": "FAIL", "error": str(e)})
        return {"results": results}

    return router
