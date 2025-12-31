from __future__ import annotations
from uuid import UUID
from typing import Optional
from fastapi import APIRouter, HTTPException, Body
from infra.db import DBClient
from services.chunking import ChunkingService
from services.embedder import EmbeddingService
from fastapi.concurrency import run_in_threadpool

def create_embed_router(db: DBClient, chunk_svc: ChunkingService, emb_svc: EmbeddingService) -> APIRouter:
    router = APIRouter(prefix="", tags=["indexing"])

    @router.post("/embed/{doc_id}")
    async def embed_one(doc_id: UUID, plan_id: Optional[str] = Body(default=None)):
        did = str(doc_id)
        with db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM documents WHERE doc_id=%s::uuid LIMIT 1;", (did,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="document not found")
        try:
            return await run_in_threadpool(emb_svc.run_one, did, plan_id=plan_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"embed_failed: {e}")

    @router.post("/index/{doc_id}")
    async def index_one(doc_id: UUID):
        did = str(doc_id)
        with db.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM documents WHERE doc_id=%s::uuid LIMIT 1;", (did,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="document not found")
        try:
            chunk_res = await run_in_threadpool(chunk_svc.run_one, did)
            embed_res = await run_in_threadpool(emb_svc.run_one, did, plan_id=chunk_res.get("plan_id"))
            return {"doc_id": did, "chunked": chunk_res, "embedded": embed_res}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"index_failed: {e}")

    return router
