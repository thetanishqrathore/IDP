from __future__ import annotations
from typing import Any, Dict, Optional
from fastapi import APIRouter, HTTPException, Body
from services.retrieval import RetrievalService
from fastapi.concurrency import run_in_threadpool

def create_search_router(retrieval: RetrievalService) -> APIRouter:
    router = APIRouter(prefix="", tags=["search"])

    @router.post("/search/vector")
    async def search_vector(
        q: str = Body(..., embed=True),
        k: int = Body(8),
        filters: Optional[Dict[str, Any]] = Body(default=None)
    ):
        if not q or not q.strip():
            raise HTTPException(status_code=400, detail="empty_query")
        f = filters or {}
        return await run_in_threadpool(
            retrieval.vector_search,
            q=q,
            k=k,
            doc_ids=f.get("doc_ids") or [],
            types_any=f.get("types") or [],
        )

    @router.post("/search/keyword")
    async def search_keyword(
        q: str = Body(..., embed=True),
        k: int = Body(8),
        filters: Optional[Dict[str, Any]] = Body(default=None)
    ):
        if not q or not q.strip():
            raise HTTPException(status_code=400, detail="empty_query")
        f = filters or {}
        return await run_in_threadpool(
            retrieval.keyword_search,
            q=q,
            k=k,
            doc_ids=f.get("doc_ids") or [],
            types_any=f.get("types") or [],
        )

    @router.post("/search")
    async def search_hybrid(
        q: str = Body(..., embed=True),
        k: int = Body(8),
        hybrid: bool = Body(True),
        filters: Optional[Dict[str, Any]] = Body(default=None)
    ):
        if not q or not q.strip():
            raise HTTPException(status_code=400, detail="empty_query")
        try:
            return await run_in_threadpool(retrieval.search, q=q, k=k, hybrid=hybrid, filters=(filters or {}))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"search_failed: {e}")

    return router
