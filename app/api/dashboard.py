from fastapi import APIRouter, HTTPException, Depends, Body
from typing import List, Any, Dict
from core.config import settings
from infra.db import DBClient
from fastapi.concurrency import run_in_threadpool

def create_dashboard_router(db: DBClient, tenant_id: str) -> APIRouter:
    router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

    @router.get("/activity")
    async def get_activity(limit: int = 50, filter: str = "ALL"):
        try:
            return await run_in_threadpool(db.fetch_recent_activity, tenant_id=tenant_id, limit=limit, filter_mode=filter)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to fetch activity: {str(e)}")

    @router.get("/stats")
    async def get_stats():
        try:
            stats = await run_in_threadpool(db.get_dashboard_stats, tenant_id=tenant_id)
            history = await run_in_threadpool(db.get_ingestion_history, tenant_id=tenant_id)
            return {
                "documents": stats.get("documents", 0),
                "queries_today": stats.get("queries_24h", 0),
                "chunks": stats.get("chunks", 0),
                "system_health": "operational",
                "ingestion_history": history
            }
        except Exception as e:
            # Fallback on error
            return {
                "documents": 0,
                "queries_today": 0,
                "chunks": 0,
                "system_health": "degraded",
                "ingestion_history": []
            }

    return router
