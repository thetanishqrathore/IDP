from __future__ import annotations
import os, time, math, uuid
from typing import Any, Dict, List, Optional

import backoff
try:
    from openai import OpenAI  # type: ignore
    _HAS_OPENAI = True
except Exception:
    OpenAI = None  # type: ignore
    _HAS_OPENAI = False
from infra.db import DBClient
from core.interfaces import VectorStore
from core.config import settings

try:
    from sentence_transformers import SentenceTransformer  # type: ignore
    _HAS_ST = True
except Exception:
    SentenceTransformer = None  # type: ignore
    _HAS_ST = False

BATCH = settings.embed_batch_size

class EmbeddingService:
    def __init__(self, db: DBClient, qd: VectorStore, *, tenant_id: str, logger):
        self.db = db
        self.qd = qd
        self.tenant_id = tenant_id
        self.log = logger
        self.client = None
        self.local_encoder = None
        self.local_dim: Optional[int] = None
        
        self.model = settings.embed_model
        self.dim = settings.embedding_dim

        if _HAS_OPENAI and settings.openai_api_key:
            try:
                self.client = OpenAI(api_key=settings.openai_api_key)
            except Exception as e:
                self.client = None
                self.log("warn", "openai-init-fail", error=str(e))
        if (not self.client) and _HAS_ST:
            # Try local encoder as fallback (defaults to bge-m3)
            local_model = settings.embed_local_model
            try:
                self.local_encoder = SentenceTransformer(local_model)
                vec = self.local_encoder.encode(["test"], normalize_embeddings=True)[0]
                self.local_dim = len(vec)
                self.log("info", "embed-local-ready", model=local_model, dim=self.local_dim)
            except Exception as e:
                self.local_encoder = None
                self.local_dim = None
                self.log("warn", "embed-local-init-fail", reason=str(e))

    def _embed_batch_fallback(self, texts: List[str]) -> List[List[float]]:
        """Deterministic hashing-based embedding fallback with fixed dim.
        Preserves pipeline when OpenAI is unavailable. Not semantically strong.
        """
        dim = self.dim
        out: List[List[float]] = []
        for t in texts:
            vec = [0.0] * dim
            s = (t or "")
            # simple feature hashing over tokens
            for tok in s.split():
                h = abs(hash(tok)) % dim
                vec[h] += 1.0
            # L2 normalize
            norm = math.sqrt(sum(v*v for v in vec)) or 1.0
            out.append([v / norm for v in vec])
        return out

    def _embed_batch_local(self, texts: List[str]) -> List[List[float]]:
        if not self.local_encoder:
            raise RuntimeError("local_encoder_unavailable")
        vecs = self.local_encoder.encode(texts, normalize_embeddings=True)
        return [list(map(float, v)) for v in vecs]

    def _payload_for_chunk(self, chunk: Dict[str, Any], *, doc_uri: Optional[str]) -> Dict[str, Any]:
        meta = chunk.get("meta") or {}
        return {
            "tenant_id": self.tenant_id,
            "doc_id": str(chunk["doc_id"]),
            "chunk_id": str(chunk["chunk_id"]),
            "plan_id": str(chunk["plan_id"]),
            "page_start": int(chunk["page_start"]),
            "page_end": int(chunk["page_end"]),
            "span_start": int(chunk["span_start"]),
            "span_end": int(chunk["span_end"]),
            "types": meta.get("types", []),
            "source_block_ids": meta.get("source_block_ids", []),
            "context_headers": meta.get("context_headers", []),
            "uri": doc_uri,
            "checksum": str(chunk.get("checksum") or ""),
            "model": self.model,
        }

    @backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
    def _embed_batch_openai(self, texts: List[str]) -> List[List[float]]:
        if not self.client:
            raise RuntimeError("openai_client_unavailable")
        resp = self.client.embeddings.create(model=self.model, input=texts)
        return [d.embedding for d in resp.data]

    def run_one(self, doc_id: str, *, plan_id: Optional[str] = None) -> Dict[str, Any]:
        t0 = time.time()

        # ensure collection
        self.qd.ensure_collection()

        # Choose engine and expected dim
        engine = "openai" if self.client else ("local" if self.local_encoder else "hash")
        
        # Quality/Performance: Boost batch size for OpenAI specifically
        current_batch = BATCH
        if engine == "openai":
            current_batch = 500 # OpenAI handles up to 2048; 500 is very safe and fast
        
        expected_dim = self.dim
        if engine == "local" and self.local_dim:
            expected_dim = int(self.local_dim)

        # Sanity: embedding dim must match index dim
        if expected_dim != self.qd.dim:
            # If local engine chosen but dim mismatches index, fall back to hash-based with EMBED_DIM
            if engine == "local":
                self.log("warn", "embed-dim-mismatch-local-fallback", local_dim=expected_dim, index_dim=self.qd.dim)
                engine = "hash"
                expected_dim = self.dim
            if expected_dim != self.qd.dim:
                try:
                    self.db.insert_event(self.tenant_id, stage="EMBEDDED", status="FAIL",
                                         details={"event":"EMBED_DIM_MISMATCH",
                                                  "engine": engine,
                                                  "embed_dim": expected_dim, "index_dim": self.qd.dim}, doc_id=doc_id)
                except Exception:
                    pass
                raise RuntimeError(f"embedding_dim_mismatch: embed={expected_dim} index={self.qd.dim}")

        # gather chunks
        chunks = self.db.fetch_chunks_for_doc(doc_id)
        if not chunks:
            self.db.insert_event(self.tenant_id, stage="EMBEDDED", status="FAIL",
                                 details={"event":"EMBEDDED_FAIL","reason":"no_chunks"}, doc_id=doc_id)
            raise RuntimeError("no_chunks")

        # opt: use latest plan
        if plan_id is None:
            plan_row = self.db.fetch_latest_plan_for_doc(doc_id)
            plan_id = plan_row["plan_id"] if plan_row else None

        # get doc URI (for payload)
        dmeta = self.db.fetch_document_meta(doc_id)
        doc_uri = dmeta["uri"] if isinstance(dmeta, dict) else (dmeta[0] if dmeta else None)

        # Incremental upsert: compute deltas via checksum
        # Fetch existing points for this doc (chunk_id -> checksum)
        try:
            existing = self.qd.get_existing_checksums(doc_id)
        except Exception as e:
            self.log("warn", "qdrant-scroll-fail", reason=str(e))
            existing = {}

        # batch embed + upsert
        def build_embed_text(c: Dict[str, Any]) -> str:
            meta = c.get("meta") or {}
            types = meta.get("types", [])
            prefix_parts = []
            if "table" in types:
                rows = meta.get("rows")
                cols = meta.get("cols")
                prefix_parts.append(f"[table rows={rows} cols={cols}]")
            elif "list" in types:
                prefix_parts.append("[list]")
            headers = meta.get("context_headers") or []
            if headers:
                prefix_parts.append(" / ".join(headers))
            prefix = ""
            if prefix_parts:
                prefix = " ".join([p for p in prefix_parts if p]).strip()
                if prefix:
                    prefix = prefix + "\n\n"
            return (prefix + (c.get("text") or "")).strip()

        # Plan changes
        by_id = {str(c["chunk_id"]): c for c in chunks}
        need_ids = [cid for cid, c in ((str(c["chunk_id"]), c) for c in chunks) if existing.get(cid) != str(c.get("checksum"))]
        stale_ids = [cid for cid in existing.keys() if cid not in by_id]
        need_chunks = [by_id[cid] for cid in need_ids]
        texts = [build_embed_text(c) for c in need_chunks]
        total = len(texts)
        upserted = 0

        for i in range(0, total, current_batch):
            batch_texts = texts[i:i+current_batch]
            try:
                if engine == "openai" and self.client:
                    vectors = self._embed_batch_openai(batch_texts)
                elif engine == "local" and self.local_encoder:
                    vectors = self._embed_batch_local(batch_texts)
                else:
                    # Only fallback to hash in strict dev/test environments to avoid polluting prod with garbage
                    if settings.app_env in ("dev", "test", "local") and not settings.openai_api_key:
                         vectors = self._embed_batch_fallback(batch_texts)
                         if i == 0:
                            self.db.insert_event(self.tenant_id, stage="EMBEDDED", status="WARN",
                                                 details={"event":"EMBED_FALLBACK_HASH", "reason":f"engine={engine}"}, doc_id=doc_id)
                    else:
                        raise RuntimeError(f"No valid embedding engine available (mode={engine})")

            except Exception as e:
                # Critical failure: Do not silently fallback to hash in production.
                # Propagate error so doc status becomes ERROR.
                self.db.insert_event(self.tenant_id, stage="EMBEDDED", status="FAIL",
                                     details={"event":"EMBED_BATCH_FAIL", "error": str(e)}, doc_id=doc_id)
                raise

            pts = []
            for j, vec in enumerate(vectors):
                c = need_chunks[i+j]
                # safety: trim unexpected dims
                if len(vec) != expected_dim:
                    # rare; skip to avoid corrupt collection
                    continue
                pts.append({
                    "id": str(c["chunk_id"]),
                    "vector": vec,
                    "payload": self._payload_for_chunk(c, doc_uri=doc_uri),
                })

            if pts:
                self.qd.upsert_points(pts)
                upserted += len(pts)

        # Delete stale points
        if stale_ids:
            try:
                self.qd.delete_points(stale_ids)
            except Exception as e:
                self.log("warn", "qdrant-delete-stale-fail", reason=str(e))

        dt = int((time.time() - t0) * 1000)
        status = "OK"
        self.db.insert_event(self.tenant_id, stage="EMBEDDED", status=status, details={
            "event": "EMBEDDED_OK", "doc_id": doc_id, "plan_id": plan_id,
            "count": upserted, "engine": engine, "model": (self.model if engine=="openai" else (settings.embed_local_model if engine=="local" else "hash")),
            "collection": self.qd.collection, "latency_ms": dt
        }, doc_id=doc_id)
        self.log("info", "embedded", doc_id=doc_id, plan_id=plan_id, count=upserted,
                 model=self.model, collection=self.qd.collection, latency_ms=dt)
        return {"doc_id": doc_id, "embedded": upserted, "model": self.model, "collection": self.qd.collection}
