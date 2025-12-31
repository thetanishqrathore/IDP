from __future__ import annotations
from typing import List, Optional
import os, time
from fastapi import APIRouter, UploadFile, File, Form, Request, HTTPException, Body
from core.models import IngestResponseItem
from services.ingestion import IngestionService
from fastapi.concurrency import run_in_threadpool
from core.config import settings


def create_ingest_router(service: IngestionService) -> APIRouter:
    router = APIRouter(prefix="", tags=["ingestion"])

    # very simple in-memory rate limiter (best to enforce at proxy)
    _RATE: dict[str, dict] = {}
    _LIMIT = settings.ingest_rate_limit_per_min

    @router.post("/ingest")
    async def ingest(request: Request,
                     files: List[UploadFile] = File(...),
                     source_uri: Optional[str] = Form(None),
                     source: Optional[str] = Form(None)) -> dict:
        # rate limit by client IP (approximate)
        try:
            ip = request.client.host if request and request.client else "unknown"
            now = int(time.time() // 60)
            rec = _RATE.get(ip)
            if not rec or rec.get("bucket") != now:
                _RATE[ip] = {"bucket": now, "count": 0}
            _RATE[ip]["count"] += 1
            if _RATE[ip]["count"] > _LIMIT:
                raise HTTPException(status_code=429, detail="rate_limited")
        except HTTPException:
            raise
        except Exception:
            pass
        results: List[IngestResponseItem] = await run_in_threadpool(service.store_many, files, source_uri, source)
        return {"results": [r.model_dump() for r in results]}

    @router.post("/ingest/url")
    async def ingest_url(
        url: str = Body(..., embed=True),
        source: Optional[str] = Body(None, embed=True),
    ) -> dict:
        """
        Ingest a single document from a public URL.
        """
        # (Rate limiting logic could be reused here)
        result = await run_in_threadpool(service.ingest_from_url, url, source)
        return {"results": [result.model_dump()]}

    return router
