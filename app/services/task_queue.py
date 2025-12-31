from __future__ import annotations
import time, threading, traceback
from typing import Optional
from infra.db import DBClient
from core.config import settings

class TaskQueueWorker:
    """
    Background thread worker that polls 'jobs' table for PENDING tasks.
    Scalability: Poor-man's Celery.
    """
    def __init__(self, db: DBClient, logger, norm_service=None, extract_service=None, chunk_service=None, embed_service=None):
        self.db = db
        self.log = logger
        self.running = False
        self.thread: Optional[threading.Thread] = None
        
        # Service dependencies for job execution
        self.norm_service = norm_service
        self.extract_service = extract_service
        self.chunk_service = chunk_service
        self.embed_service = embed_service

    def start(self):
        if self.running: return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True, name="JobWorker")
        self.thread.start()
        self.log("info", "worker-started")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
        self.log("info", "worker-stopped")

    def _loop(self):
        while self.running:
            try:
                # Poll for job
                # We need a method in DB to fetch-and-lock a job to avoid race conditions
                # For MVP, we just fetch one pending job.
                job = self._fetch_next_job()
                if not job:
                    time.sleep(2.0) # Backoff
                    continue
                
                job_id = job["job_id"]
                job_type = job["job_type"]
                payload = job.get("payload") or {}
                
                self.log("info", "worker-job-start", job_id=job_id, type=job_type)
                
                # Update to RUNNING
                self.db.update_job(job_id, status="RUNNING")
                
                # Execute
                try:
                    result = self._execute_job(job_type, payload)
                    self.db.update_job(job_id, status="DONE", result=result, progress=100.0)
                    self.log("info", "worker-job-done", job_id=job_id)
                except Exception as e:
                    stack = traceback.format_exc()
                    self.db.update_job(job_id, status="ERROR", error=str(e))
                    self.log("error", "worker-job-fail", job_id=job_id, error=str(e), stack=stack)
            
            except Exception as e:
                self.log("error", "worker-loop-crash", error=str(e))
                time.sleep(5.0)

    def _fetch_next_job(self) -> Optional[dict]:
        # Simple polling; ideally use SELECT ... FOR UPDATE SKIP LOCKED
        with self.db.conn.cursor() as cur:
            cur.execute("""
                SELECT job_id::text, job_type, payload
                FROM jobs
                WHERE status = 'PENDING'
                ORDER BY created_at ASC
                LIMIT 1
            """)
            row = cur.fetchone()
        if not row: return None
        if isinstance(row, dict): return row
        return {"job_id": str(row[0]), "job_type": row[1], "payload": row[2]}

    def _execute_job(self, jtype: str, payload: dict):
        # Dispatcher
        if jtype == "pipeline_process_doc":
            doc_id = payload.get("doc_id")
            if not doc_id: raise ValueError("doc_id_missing")
            
            # 1. Normalize
            if self.norm_service:
                self.norm_service.run_one(doc_id)
            
            # 2. Extract
            if self.extract_service:
                self.extract_service.run_one(doc_id)
            
            # 3. Chunk
            if self.chunk_service:
                self.chunk_service.run_one(doc_id)
            
            # 4. Embed
            if self.embed_service:
                self.embed_service.run_one(doc_id)
            
            return {"doc_id": doc_id, "status": "fully_indexed"}
        
        raise ValueError(f"unknown_job_type:{jtype}")
