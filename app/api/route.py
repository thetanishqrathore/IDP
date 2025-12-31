# app/api/route.py
from __future__ import annotations
from typing import Any, Dict, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from services.router import QueryRouter

class RouteReq(BaseModel):
    q: str = Field(..., description="User question")
    k: int = Field(8, ge=1, le=100, description="Top-k to plan for")
    filters: Optional[Dict[str, Any]] = Field(None, description="Optional router filters (doc_ids, types, etc.)")

def create_route_router(router: QueryRouter) -> APIRouter:
    r = APIRouter(prefix="", tags=["route"])

    @r.post("/route")
    async def route(req: RouteReq):
        q = (req.q or "").strip()
        if not q:
            raise HTTPException(status_code=400, detail="empty_query")
        try:
            return router.route(q, want_k=req.k, filters=req.filters or {})
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"route_failed: {e}")

    return r