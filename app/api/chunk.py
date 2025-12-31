from __future__ import annotations
from uuid import UUID
from typing import List
from fastapi import APIRouter, HTTPException
from infra.db import DBClient
from services.chunking import ChunkingService
from fastapi.concurrency import run_in_threadpool

def create_chunk_router(db: DBClient, svc: ChunkingService) -> APIRouter:
    router = APIRouter(prefix="", tags=["chunking"])

    @router.post("/chunk/{doc_id}")
    async def chunk_one(doc_id: UUID):
        did = str(doc_id)
        with db.conn.cursor() as cur:
            cur.execute("SELECT doc_id FROM documents WHERE doc_id=%s::uuid LIMIT 1;", (did,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="document not found")
        try:
            return await run_in_threadpool(svc.run_one, did)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"chunk_failed: {e}")

    @router.post("/chunk")
    async def chunk_many(doc_ids: List[UUID]):
        out = []
        for d in doc_ids:
            did = str(d)
            try:
                out.append(await run_in_threadpool(svc.run_one, did))
            except Exception as e:
                out.append({"doc_id": did, "status":"FAIL", "error": str(e)})
        return {"results": out}

    return router
