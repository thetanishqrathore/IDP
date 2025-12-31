from __future__ import annotations
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from infra.db import DBClient
from core.config import settings

class FeedbackRequest(BaseModel):
    query_id: Optional[str] = None # Corresponds to trace_id or a specific event_id
    score: int # 1 (like) or -1 (dislike)
    comment: Optional[str] = None

def create_feedback_router(db: DBClient, tenant_id: str) -> APIRouter:
    r = APIRouter(prefix="/feedback", tags=["feedback"])

    @r.post("/")
    def submit_feedback(req: FeedbackRequest):
        try:
            db.insert_event(
                tenant_id,
                stage="FEEDBACK",
                status="OK",
                details={
                    "event": "USER_FEEDBACK",
                    "query_id": req.query_id,
                    "score": req.score,
                    "comment": req.comment
                },
                doc_id=None
            )
            return {"ok": True}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    return r
