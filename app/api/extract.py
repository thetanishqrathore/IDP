from __future__ import annotations
from typing import List
from fastapi import APIRouter, HTTPException
from infra.db import DBClient
from services.extraction import ExtractionService
from uuid import UUID
from fastapi.concurrency import run_in_threadpool
def create_extract_router(db: DBClient, service: ExtractionService) -> APIRouter:
    router = APIRouter(prefix="", tags=["extraction"])

    @router.post("/extract/{doc_id}")
    async def extract_one(doc_id: UUID):          # <-- validate path param
        did = str(doc_id)
        with db.conn.cursor() as cur:
            cur.execute("SELECT doc_id FROM documents WHERE doc_id=%s::uuid LIMIT 1;", (did,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="document not found")
        try:
            return await run_in_threadpool(service.run_one, did)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"extract_failed: {e}")

    @router.post("/extract")
    async def extract_many(doc_ids: list[UUID]):  # <-- validate body items
        ids = [str(x) for x in doc_ids]
        results = []
        for did in ids:
            try:
                results.append(await run_in_threadpool(service.run_one, did))
            except Exception as e:
                results.append({"doc_id": did, "status": "FAIL", "error": str(e)})
        return {"results": results}

    return router
