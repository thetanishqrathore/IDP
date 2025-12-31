from __future__ import annotations
from typing import List, Optional, Any, Dict
import time
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Body, BackgroundTasks
from uuid import UUID

from infra.db import DBClient
from services.ingestion import IngestionService
from services.normalization import NormalizationService
from services.extraction import ExtractionService
from services.chunking import ChunkingService
from services.embedder import EmbeddingService
from services.generation import GenerationService
from services.graph import KnowledgeGraphService
from fastapi import status
from fastapi.concurrency import run_in_threadpool


def create_pipeline_router(
    db: DBClient,
    ingest: IngestionService,
    norm: NormalizationService,
    extract: ExtractionService,
    chunk: ChunkingService,
    embed: EmbeddingService,
    gen: GenerationService,
    graph: KnowledgeGraphService,
) -> APIRouter:
    r = APIRouter(prefix="/pipeline", tags=["pipeline"])

    @r.post("/ingest_index")
    async def ingest_index(
        files: List[UploadFile] = File(...),
        source_uri: Optional[str] = Form(None),
        source: Optional[str] = Form(None),
        background_tasks: BackgroundTasks = None,
        async_process: bool = Form(True),
    ):
        # store files first (offload blocking store to threadpool)
        res = await run_in_threadpool(ingest.store_many, files, source_uri=source_uri, source=source)
        out = []
        for item in res:
            if item.state != "STORED" or not item.doc_id:
                out.append({"doc_id": item.doc_id, "status": item.state, "warnings": item.warnings})
                continue
            did = item.doc_id
            try:
                # normalize -> extract -> chunk -> embed
                with db.conn.cursor() as cur:
                    cur.execute("SELECT sha256, mime FROM documents WHERE doc_id=%s::uuid LIMIT 1;", (did,))
                    row = cur.fetchone()
                if not row:
                    out.append({"doc_id": did, "status": "NOT_FOUND"}); continue

                def _process(doc_id: str, sha: str, mime: str):
                    timings: Dict[str, int] = {}
                    try:
                        t_stage = time.time()
                        norm.run_one(doc_id=doc_id, sha256=sha, mime=mime)  # type: ignore
                        timings["normalize_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        extract.run_one(doc_id)
                        timings["extract_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        chunk_res = chunk.run_one(doc_id)
                        timings["chunk_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        graph.build(doc_id)
                        timings["graph_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        embed.run_one(doc_id, plan_id=chunk_res.get("plan_id"))
                        timings["embed_ms"] = int((time.time() - t_stage) * 1000)

                        db.insert_event(
                            tenant_id,
                            stage="PIPELINE",
                            status="OK",
                            details={"event": "PIPELINE_ASYNC_OK", "timings": timings},
                            doc_id=doc_id,
                        )
                    except Exception as exc:
                        db.insert_event(
                            tenant_id,
                            stage="PIPELINE",
                            status="FAIL",
                            details={"event": "PIPELINE_ASYNC_FAIL", "timings": timings, "error": str(exc)},
                            doc_id=doc_id,
                        )
                        # swallow to avoid crashing background task; individual stages already logged

                if async_process and background_tasks is not None:
                    background_tasks.add_task(_process, did, row["sha256"], row["mime"])  # type: ignore
                    out.append({"doc_id": did, "status": "QUEUED"})
                else:
                    timings: Dict[str, int] = {}
                    try:
                        t_stage = time.time()
                        norm_res = await run_in_threadpool(norm.run_one, doc_id=did, sha256=row["sha256"], mime=row["mime"])  # type: ignore
                        timings["normalize_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        extr_res = await run_in_threadpool(extract.run_one, did)
                        timings["extract_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        chunk_res = await run_in_threadpool(chunk.run_one, did)
                        timings["chunk_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        graph_res = await run_in_threadpool(graph.build, did)
                        timings["graph_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        embed_res = await run_in_threadpool(embed.run_one, did, plan_id=chunk_res.get("plan_id"))
                        timings["embed_ms"] = int((time.time() - t_stage) * 1000)

                        db.insert_event(
                            tenant_id,
                            stage="PIPELINE",
                            status="OK",
                            details={"event": "PIPELINE_SYNC_OK", "timings": timings},
                            doc_id=did,
                        )
                        out.append({
                            "doc_id": did,
                            "status": "OK",
                            "normalize": norm_res,
                            "extract": extr_res,
                            "chunk": chunk_res,
                            "graph": graph_res,
                            "embed": embed_res,
                            "timings": timings,
                        })
                    except Exception as exc:
                        db.insert_event(
                            tenant_id,
                            stage="PIPELINE",
                            status="FAIL",
                            details={"event": "PIPELINE_SYNC_FAIL", "timings": timings, "error": str(exc)},
                            doc_id=did,
                        )
                        out.append({"doc_id": did, "status": "FAIL", "error": str(exc)})
            except Exception as e:
                out.append({"doc_id": did, "status": "FAIL", "error": str(e)})
        return {"results": out}

    class IngestAnswerReq:
        q: str
        k: int = 8
        filters: Optional[Dict[str, Any]] = None

    @r.post("/ingest_answer")
    async def ingest_answer(
        q: str = Form(...),
        k: int = Form(8),
        files: List[UploadFile] = File(...),
        source_uri: Optional[str] = Form(None),
        source: Optional[str] = Form(None),
    ):
        if not q or not q.strip():
            raise HTTPException(status_code=400, detail="empty_query")
        idx = await ingest_index(files=files, source_uri=source_uri, source=source)  # type: ignore
        # collect doc_ids that were successfully processed
        processed = [r.get("doc_id") for r in idx.get("results", []) if r.get("status") == "OK" and r.get("doc_id")]
        # answer over these docs only (filters)
        filters = {"doc_ids": processed} if processed else {}
        ans = await run_in_threadpool(gen.answer, q=q, k=k, filters=filters)
        return {"index": idx, "answer": ans}

    @r.post("/ingest_job", status_code=status.HTTP_202_ACCEPTED)
    async def ingest_job(
        files: List[UploadFile] = File(...),
        source_uri: Optional[str] = Form(None),
        source: Optional[str] = Form(None),
        background_tasks: BackgroundTasks = None,
    ):
        # Store files synchronously (threadpool) to get doc_ids fast
        res = await run_in_threadpool(ingest.store_many, files, source_uri=source_uri, source=source)
        doc_ids = [it.doc_id for it in res if getattr(it, 'doc_id', None)]
        # Create job
        payload = {"doc_ids": doc_ids, "source_uri": source_uri, "source": source}
        job_id = db.insert_job(job_type="INGEST_INDEX", payload=payload, status="PENDING")

        def _process_job(job_id: str, doc_ids: List[str]):
            try:
                db.update_job(job_id, status="RUNNING", progress=0.0)
                total = max(1, len(doc_ids))
                for i, did in enumerate(doc_ids, start=1):
                    try:
                        with db.conn.cursor() as cur:
                            cur.execute("SELECT sha256, mime FROM documents WHERE doc_id=%s::uuid LIMIT 1;", (did,))
                            row = cur.fetchone()
                        if not row:
                            continue
                        timings: Dict[str, int] = {}
                        t_stage = time.time()
                        norm.run_one(doc_id=did, sha256=row["sha256"], mime=row["mime"])  # type: ignore
                        timings["normalize_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        extract.run_one(did)
                        timings["extract_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        chunk_res = chunk.run_one(did)
                        timings["chunk_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        graph.build(did)
                        timings["graph_ms"] = int((time.time() - t_stage) * 1000)

                        t_stage = time.time()
                        embed.run_one(did, plan_id=chunk_res.get("plan_id"))
                        timings["embed_ms"] = int((time.time() - t_stage) * 1000)

                        db.insert_event(
                            tenant_id,
                            stage="PIPELINE",
                            status="OK",
                            details={"event": "PIPELINE_JOB_OK", "timings": timings, "job_id": job_id},
                            doc_id=did,
                        )
                    except Exception as e:
                        # continue with others; record error in payload
                        db.insert_event(
                            tenant_id,
                            stage="PIPELINE",
                            status="FAIL",
                            details={"event": "PIPELINE_JOB_FAIL", "error": str(e), "job_id": job_id},
                            doc_id=did,
                        )
                        db.update_job(job_id, payload={"error_doc": did, "error": str(e)})
                    db.update_job(job_id, progress=round((i/total)*100.0, 2))
                db.update_job(job_id, status="DONE", progress=100.0, result={"doc_ids": doc_ids})
            except Exception as e:
                db.update_job(job_id, status="ERROR", error=str(e))

        if background_tasks is not None:
            background_tasks.add_task(_process_job, job_id, doc_ids)
        else:
            # fire-and-forget in current threadpool as last resort
            _process_job(job_id, doc_ids)

        return {"job_id": job_id, "accepted": True, "doc_ids": doc_ids}

    return r

def create_smart_ingest_router(
    db: DBClient,
    ingest: IngestionService,
    norm: NormalizationService,
    extract: ExtractionService,
    chunk: ChunkingService,
    embed: EmbeddingService,
    gen: GenerationService,
    graph: KnowledgeGraphService,
) -> APIRouter:
    """
    Root-level router for 'smart' ingestion that triggers the full pipeline.
    Replaces the basic storage-only endpoints.
    """
    r = APIRouter(prefix="", tags=["ingestion-smart"])

    @r.post("/ingest")
    async def smart_ingest(
        files: List[UploadFile] = File(...),
        source_uri: Optional[str] = Form(None),
        source: Optional[str] = Form(None),
        background_tasks: BackgroundTasks = None,
    ):
        # reuse the logic from ingest_index but exposed at /ingest
        # We need to call the logic. Ideally we'd extract the logic to a service method, 
        # but for now we can just duplicate the orchestration or call the pipeline router's function if we had it.
        # Let's duplicate the orchestration logic for clarity and independence.
        
        # 1. Store
        res = await run_in_threadpool(ingest.store_many, files, source_uri=source_uri, source=source)
        out = []
        for item in res:
            if item.state != "STORED" or not item.doc_id:
                out.append({"doc_id": item.doc_id, "status": item.state, "warnings": item.warnings})
                continue
            did = item.doc_id
            
            # 2. Trigger Async Pipeline
            def _process(doc_id: str):
                try:
                    # fetch fresh SHA/Mime
                    with db.conn.cursor() as cur:
                        cur.execute("SELECT sha256, mime FROM documents WHERE doc_id=%s::uuid LIMIT 1;", (doc_id,))
                        row = cur.fetchone()
                    if not row: return
                    
                    # Normalize
                    norm.run_one(doc_id=doc_id, sha256=row["sha256"], mime=row["mime"])
                    # Extract
                    extract.run_one(doc_id)
                    # Chunk
                    chunk_res = chunk.run_one(doc_id)
                    # Graph
                    graph.build(doc_id)
                    # Embed
                    embed.run_one(doc_id, plan_id=chunk_res.get("plan_id"))
                    
                    db.insert_event(ingest.tenant_id, stage="PIPELINE", status="OK", details={"event": "SMART_INGEST_OK"}, doc_id=doc_id)
                except Exception as exc:
                    db.insert_event(ingest.tenant_id, stage="PIPELINE", status="FAIL", details={"event": "SMART_INGEST_FAIL", "error": str(exc)}, doc_id=doc_id)

            if background_tasks:
                background_tasks.add_task(_process, did)
            else:
                # synchronous fallback (rarely used if called correctly)
                _process(did)
            
            out.append({"doc_id": did, "status": "QUEUED", "details": "Pipeline triggered"})
            
        return {"results": out}

    @r.post("/ingest/url")
    async def smart_ingest_url(
        url: str = Body(..., embed=True),
        source: Optional[str] = Body(None, embed=True),
        background_tasks: BackgroundTasks = None,
    ):
        # 1. Ingest from URL
        item = await run_in_threadpool(ingest.ingest_from_url, url, source)
        if item.state != "STORED" or not item.doc_id:
             return {"results": [{"doc_id": item.doc_id, "status": item.state, "warnings": item.warnings}]}
        
        did = item.doc_id
        
        # 2. Trigger Async Pipeline
        def _process(doc_id: str):
            try:
                with db.conn.cursor() as cur:
                    cur.execute("SELECT sha256, mime FROM documents WHERE doc_id=%s::uuid LIMIT 1;", (doc_id,))
                    row = cur.fetchone()
                if not row: return
                norm.run_one(doc_id=doc_id, sha256=row["sha256"], mime=row["mime"])
                extract.run_one(doc_id)
                chunk_res = chunk.run_one(doc_id)
                graph.build(doc_id)
                embed.run_one(doc_id, plan_id=chunk_res.get("plan_id"))
                db.insert_event(ingest.tenant_id, stage="PIPELINE", status="OK", details={"event": "SMART_INGEST_URL_OK"}, doc_id=doc_id)
            except Exception as exc:
                db.insert_event(ingest.tenant_id, stage="PIPELINE", status="FAIL", details={"event": "SMART_INGEST_URL_FAIL", "error": str(exc)}, doc_id=doc_id)

        if background_tasks:
            background_tasks.add_task(_process, did)
        else:
            _process(did)

        return {"results": [{"doc_id": did, "status": "QUEUED", "details": "Pipeline triggered"}]}

    return r
