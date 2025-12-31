from __future__ import annotations
from fastapi import APIRouter, HTTPException
from infra.db import DBClient


def create_jobs_router(db: DBClient) -> APIRouter:
    r = APIRouter(prefix="/jobs", tags=["jobs"])

    @r.get("/{job_id}")
    async def job_status(job_id: str):
        row = db.get_job(job_id)
        if not row:
            raise HTTPException(status_code=404, detail="job_not_found")
        # normalize to simple dict
        if isinstance(row, dict):
            return {
                "job_id": row.get("job_id"),
                "job_type": row.get("job_type"),
                "status": row.get("status"),
                "payload": row.get("payload"),
                "progress": row.get("progress"),
                "result": row.get("result"),
                "error": row.get("error"),
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
            }
        # tolerate tuple style rows
        job_id, job_type, status, payload, progress, result, error, created_at, updated_at = row
        return {
            "job_id": str(job_id),
            "job_type": job_type,
            "status": status,
            "payload": payload,
            "progress": progress,
            "result": result,
            "error": error,
            "created_at": str(created_at),
            "updated_at": str(updated_at),
        }

    return r

