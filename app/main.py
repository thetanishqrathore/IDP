import os, json, time
from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import os as _os
from infra.db import DBClient
from infra.minio_store import MinioStore
from qdrant_client import QdrantClient  # used only in /healthz check

from services.ingestion import IngestionService
from api.ingest import create_ingest_router
from api.normalize import create_normalize_router
from services.normalization import NormalizationService
from api.extract import create_extract_router
from services.extraction import ExtractionService
from api.chunk import create_chunk_router
from services.chunking import ChunkingService
from services.graph import KnowledgeGraphService
from infra.qdrant import QdrantIndex
from services.embedder import EmbeddingService
from api.embed import create_embed_router
from services.retrieval import RetrievalService
from api.search import create_search_router
from services.generation import GenerationService
from api.answer import create_answer_router
from api.pipeline import create_pipeline_router, create_smart_ingest_router
from services.li_bridge import init_llamaindex
from services.router import QueryRouter
from api.route import create_route_router
from services.structured import StructuredIndexerService
from api.structured import create_structured_router
from api.ui import create_ui_router
from api.jobs import create_jobs_router
from api.metrics import create_metrics_router
from api.admin import create_admin_router
from api.v1_openai import create_openai_router
from api.dashboard import create_dashboard_router
from api.feedback import create_feedback_router
from core.config import settings

init_llamaindex()
# ---------- tiny JSON logger ----------
def jlog(level: str, message: str, **kw):
    payload = {
        "level": level,
        "ts": datetime.now(timezone.utc).isoformat(),
        "message": message,
        **kw,
    }
    print(json.dumps(payload, default=str), flush=True)  # <-- default=str


# ---------- config ----------
APP_ENV = settings.app_env
APP_VERSION = settings.app_version
REGION = settings.region
TENANT_ID = settings.tenant_id

DB_HOST = settings.db_host
DB_PORT = settings.db_port
DB_NAME = settings.db_name
DB_USER = settings.db_user
DB_PASSWORD = settings.db_password

S3_ENDPOINT = settings.s3_endpoint
S3_BUCKET = settings.s3_bucket
S3_CANONICAL_BUCKET = settings.s3_canonical_bucket
MINIO_ROOT_USER = settings.minio_root_user
MINIO_ROOT_PASSWORD = settings.minio_root_password

QDRANT_URL = settings.qdrant_url
QDRANT_COLLECTION = settings.qdrant_collection
EMBEDDING_DIM = settings.embedding_dim
QDRANT_DISTANCE = settings.qdrant_distance

MAX_FILES_PER_REQUEST = settings.max_files_per_request
MAX_FILE_MB = settings.max_file_mb
ALLOWED_MIME_PREFIXES = settings.allowed_mime_prefixes

CHUNK_TARGET_TOKENS = settings.chunk_target_tokens
CHUNK_OVERLAP_TOKENS = settings.chunk_overlap_tokens
MAX_CHUNKS_PER_DOC = settings.max_chunks_per_doc


# ---------- app startup ----------
app = FastAPI(title="RAG MVP", version=APP_VERSION)

# CORS configuration: default permissive for dev; override via CORS_ALLOW_ORIGINS
_cors_env = settings.cors_allow_origins
_allow_origins = ["*"] if not _cors_env else [o.strip() for o in _cors_env.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

"""
Frontend static build
- Expect React build output in app/ui/ with index.html and assets/ folder
- Serve / -> index.html
- Serve /assets/* -> static files from app/ui/assets
"""
_UI_DIR = _os.path.join(_os.path.dirname(__file__), "ui")
_UI_ASSETS = _os.path.join(_UI_DIR, "assets")
if _os.path.isdir(_UI_DIR):
    if _os.path.isdir(_UI_ASSETS):
        app.mount("/assets", StaticFiles(directory=_UI_ASSETS, html=False), name="assets")

    @app.get("/", include_in_schema=False)
    def ui_index():
        idx = _os.path.join(_UI_DIR, "index.html")
        if _os.path.isfile(idx):
            return FileResponse(idx)
        return {"ok": True, "message": "UI build not found", "path": _UI_DIR}

    @app.get("/favicon.png", include_in_schema=False)
    def ui_favicon():
        fav = _os.path.join(_UI_DIR, "favicon.png")
        if _os.path.isfile(fav):
            return FileResponse(fav)
        return FileResponse(_os.path.join(_UI_DIR, "assets", "favicon.png")) if _os.path.exists(_os.path.join(_UI_DIR, "assets", "favicon.png")) else None
else:
    jlog("warn", "ui-dir-missing", ui_dir=_UI_DIR)

# Single DB client and schema init (once)
dbc = DBClient(host=DB_HOST, port=DB_PORT, db=DB_NAME, user=DB_USER, password=DB_PASSWORD)

# Stores (create both and alias store_raw for health checks below)
store_raw = MinioStore(endpoint=S3_ENDPOINT, access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, bucket=S3_BUCKET)
store_canonical = MinioStore(endpoint=S3_ENDPOINT, access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, bucket=S3_CANONICAL_BUCKET)
store = store_raw

_QDRANT_HEALTH_TIMEOUT = settings.qdrant_health_timeout
qdrant = QdrantClient(url=QDRANT_URL, api_key=settings.qdrant_api_key, timeout=_QDRANT_HEALTH_TIMEOUT)  # healthz only

# Defer router wiring below to avoid duplicate registrations


@app.on_event("startup")
def on_startup():
    t0 = time.time()
    # Initialize infra on startup (avoid heavy work at import time)
    try:
        dbc.connect()
        dbc.init_schema_phase1_and_2()
        dbc.init_schema_phase3()
        dbc.init_schema_phase4()
        dbc.init_schema_phase5()
        dbc.init_schema_graph()
        dbc.init_schema_jobs()
        dbc.init_schema_hardening()
        dbc.init_schema_structured()
        dbc.ensure_chunks_fts_index()
        try:
            dbc.ensure_perf_indexes()
        except Exception as _e:
            jlog("warn", "ensure-perf-indexes-failed", error=str(_e))
    except Exception as e:
        jlog("error", "db-init-failed", error=str(e))

    try:
        store_raw.ensure_bucket()
        store_canonical.ensure_bucket()
    except Exception as e:
        jlog("error", "minio-init-failed", error=str(e))
    try:
        qdr.ensure_collection()
    except Exception as e:
        jlog("warn", "qdrant-ensure-collection-failed", error=str(e))
    # startup event
    dbc.insert_event(TENANT_ID, stage="SYSTEM", status="INFO", details={
        "event": "SYSTEM_STARTUP",
        "app_env": APP_ENV,
        "app_version": APP_VERSION,
        "region": REGION,
        "services": {"db": True, "minio": True, "qdrant": True}
    }, doc_id=None)
    jlog("info", "system-startup", app_env=APP_ENV, app_version=APP_VERSION, region=REGION,
         latency_ms=int((time.time()-t0)*1000))

@app.on_event("shutdown")
def on_shutdown():
    try:
        if dbc.conn is not None:
            dbc.conn.close()
    except Exception:
        pass

_HEALTHZ_TTL = settings.healthz_ttl_seconds
_last_health: dict[str, Any] | None = None


@app.get("/healthz")
def healthz():
    global _last_health
    now = time.time()
    if _last_health and (now - _last_health.get("ts", 0)) < _HEALTHZ_TTL:
        return _last_health.get("payload", {})
    ok_db = ok_minio = ok_qdrant = False
    try:
        ok_db = dbc.ping()  # use the connected client
    except Exception as e:
        jlog("error", "db-health-fail", error=str(e))
    try:
        # best-effort; MinIO client lacks quick timeout hooks here
        ok_minio = store.ping()
    except Exception as e:
        jlog("error", "minio-health-fail", error=str(e))
    


    try:
        # simple lightweight call; client configured with a short timeout
        qdrant.get_collections()
        ok_qdrant = True
    except Exception as e:
        jlog("error", "qdrant-health-fail", error=str(e))
    

    ok = bool(ok_db and ok_minio and ok_qdrant)

    payload = {
        "ok": ok,
        "services": {"db": ok_db, "minio": ok_minio, "qdrant": ok_qdrant},
        "version": APP_VERSION,
        "env": APP_ENV,
        "collection": QDRANT_COLLECTION,
        "embedding_dim": EMBEDDING_DIM
    }
    _last_health = {"ts": now, "payload": payload}
    return payload


# services
service = IngestionService(
    dbc, store_raw, tenant_id=TENANT_ID, logger=jlog,
    allowed_mime_prefixes=ALLOWED_MIME_PREFIXES,
    max_file_mb=MAX_FILE_MB,
    max_files_per_request=MAX_FILES_PER_REQUEST,
)
jlog("info", "ingest-config", max_files_per_request=MAX_FILES_PER_REQUEST,
     max_file_mb=MAX_FILE_MB, allowed_mime_prefixes=ALLOWED_MIME_PREFIXES)

norm_service = NormalizationService(dbc, store_raw, store_canonical, tenant_id=TENANT_ID, logger=jlog, canonical_bucket=S3_CANONICAL_BUCKET)
extract_service = ExtractionService(dbc, store_canonical, tenant_id=TENANT_ID, logger=jlog, canonical_bucket=S3_CANONICAL_BUCKET)
chunk_service = ChunkingService(
    dbc,
    tenant_id=TENANT_ID,
    logger=jlog,
    target_tokens=CHUNK_TARGET_TOKENS,
    overlap_tokens=CHUNK_OVERLAP_TOKENS,
    max_chunks_per_doc=MAX_CHUNKS_PER_DOC,
)
graph_service = KnowledgeGraphService(dbc, tenant_id=TENANT_ID, logger=jlog)

qdr = QdrantIndex(url=QDRANT_URL, collection=QDRANT_COLLECTION, dim=EMBEDDING_DIM, distance=QDRANT_DISTANCE)
embed_service = EmbeddingService(dbc, qdr, tenant_id=TENANT_ID, logger=jlog)

retrieval_service = RetrievalService(dbc, qdr, tenant_id=TENANT_ID, logger=jlog)
app.include_router(create_search_router(retrieval_service))
# Init router
router_service = QueryRouter(logger=jlog)
app.include_router(create_route_router(router_service))

# Generation service now receives router
gen_service = GenerationService(dbc, retrieval_service, tenant_id=TENANT_ID, logger=jlog, router=router_service)
app.include_router(create_answer_router(gen_service))

# Structured indexing service and router
structured_service = StructuredIndexerService(dbc, tenant_id=TENANT_ID, logger=jlog)
app.include_router(create_structured_router(dbc, structured_service))

# Lightweight UI helpers
app.include_router(create_ui_router(dbc, TENANT_ID, qdr))
app.include_router(create_jobs_router(dbc))
app.include_router(create_metrics_router(dbc))
app.include_router(create_admin_router(dbc, TENANT_ID, store_raw, store_canonical, qdr))
app.include_router(create_dashboard_router(dbc, TENANT_ID))
app.include_router(create_feedback_router(dbc, TENANT_ID))

# Compat endpoints for legacy index.html (Deleted)
# app.include_router(create_compat_router(dbc, service, norm_service, extract_service, chunk_service, embed_service, gen_service, graph_service, tenant_id=TENANT_ID))

# routers
# app.include_router(create_ingest_router(service)) # Replaced by smart ingest below
app.include_router(create_smart_ingest_router(dbc, service, norm_service, extract_service, chunk_service, embed_service, gen_service, graph_service))
app.include_router(create_normalize_router(dbc, norm_service))
app.include_router(create_extract_router(dbc, extract_service))
app.include_router(create_chunk_router(dbc, chunk_service))
app.include_router(create_embed_router(dbc, chunk_service, embed_service))
app.include_router(create_pipeline_router(dbc, service, norm_service, extract_service, chunk_service, embed_service, gen_service, graph_service))

# v1 OpenAI Compatibility
app.include_router(create_openai_router(gen_service))

# Init Background Worker
from services.task_queue import TaskQueueWorker
worker = TaskQueueWorker(
    dbc, 
    logger=jlog,
    norm_service=norm_service,
    extract_service=extract_service,
    chunk_service=chunk_service,
    embed_service=embed_service
)

@app.on_event("startup")
def on_worker_startup():
    try:
        worker.start()
    except Exception as e:
        jlog("error", "worker-start-failed", error=str(e))

@app.on_event("shutdown")
def on_worker_shutdown():
    try:
        worker.stop()
    except Exception:
        pass
