from __future__ import annotations
from typing import Optional
from uuid import UUID
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse, RedirectResponse
import os
import boto3
from botocore.client import Config as BotoConfig

from infra.db import DBClient
from infra.storage import presign
from infra.qdrant import QdrantIndex
from core.config import settings


def create_ui_router(db: DBClient, tenant_id: str, qdr: QdrantIndex | None = None) -> APIRouter:
    r = APIRouter(prefix="/ui", tags=["ui"])

    @r.get("/docs")
    async def list_docs(limit: int = Query(100, ge=1, le=1000)):
        db.connect()
        with db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT doc_id::text, uri, mime, size_bytes, state, collected_at
                FROM documents
                WHERE tenant_id = %s AND state != 'DELETED'
                ORDER BY collected_at DESC
                LIMIT %s
                """,
                (tenant_id, limit),
            )
            rows = cur.fetchall() or []
        out = []
        for r in rows:
            if isinstance(r, dict):
                out.append(r)
            else:
                doc_id, uri, mime, size_bytes, state, collected_at = r
                out.append({
                    "doc_id": str(doc_id),
                    "uri": uri,
                    "mime": mime,
                    "size_bytes": int(size_bytes or 0),
                    "state": state,
                    "collected_at": str(collected_at),
                })
        return {"docs": out}

    @r.delete("/docs/{doc_id}")
    async def soft_delete_doc(doc_id: UUID):
        did = str(doc_id)
        db.connect()
        with db.conn.cursor() as cur:
            cur.execute("SELECT state FROM documents WHERE doc_id=%s::uuid AND tenant_id=%s LIMIT 1;", (did, tenant_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="document not found")
            # enforce tenant filter on update
            cur.execute("UPDATE documents SET state='DELETED' WHERE doc_id=%s::uuid AND tenant_id=%s;", (did, tenant_id))
        # Best-effort: remove any vectors for this doc from Qdrant so it no longer appears in results
        try:
            if qdr is not None:
                qdr.delete_doc(did)
        except Exception:
            # non-fatal
            pass
        return {"ok": True, "doc_id": did, "state": "DELETED"}

    @r.get("/link/{doc_id}")
    async def link_for_doc(doc_id: UUID, variant: Optional[str] = Query(None)):
        """
        Return a browser-accessible URL for the document (presigned if needed).
        Prefers canonical_uri if present; falls back to raw minio_key.
        """
        did = str(doc_id)
        try:
            # Verify tenant ownership first
            db.connect()
            with db.conn.cursor() as cur:
                cur.execute("SELECT state FROM documents WHERE doc_id=%s::uuid AND tenant_id=%s LIMIT 1;", (did, tenant_id))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="document not found")
                state = row.get("state") if isinstance(row, dict) else (row[0] if row else None)
                if str(state).upper() == 'DELETED':
                    raise HTTPException(status_code=404, detail="document deleted")

            keys = db.get_doc_storage_keys(did) or {}
            # Choose asset based on variant
            if (variant or "").lower() == "original":
                key = keys.get("minio_key") or keys.get("canonical_uri")
                bucket_hint = settings.s3_bucket
            elif (variant or "").lower() == "canonical":
                key = keys.get("canonical_uri") or keys.get("minio_key")
                bucket_hint = settings.s3_canonical_bucket or settings.s3_bucket
            else:
                # default behavior: prefer canonical, fallback original
                key = keys.get("canonical_uri") or keys.get("minio_key")
                bucket_hint = settings.s3_canonical_bucket or settings.s3_bucket
            if not key:
                raise HTTPException(status_code=404, detail="no storage key for document")
            # choose bucket: canonical keys (not under sha256/) live in S3_CANONICAL_BUCKET
            if str(key).startswith("sha256/"):
                url = presign(key, bucket=settings.s3_bucket)
            else:
                url = presign(key, bucket=bucket_hint)
            if not url:
                raise HTTPException(status_code=500, detail="unable to generate link")
            return {"url": url}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"link generation failed: {e}")

    @r.get("/open/{doc_id}")
    async def open_doc(doc_id: UUID, variant: Optional[str] = Query(None)):
        did = str(doc_id)
        db.connect()
        with db.conn.cursor() as cur:
            cur.execute("SELECT mime, state FROM documents WHERE doc_id=%s::uuid AND tenant_id=%s LIMIT 1;", (did, tenant_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="document not found")
            state = row.get("state") if isinstance(row, dict) else (row[1] if row else None)
            if str(state).upper() == 'DELETED':
                raise HTTPException(status_code=404, detail="document deleted")
            # Original document MIME (may differ from canonical rendition)
            original_mime = row.get("mime") if isinstance(row, dict) else (row[0] if row else None)

        keys = db.get_doc_storage_keys(did) or {}
        # Choose asset based on variant
        if (variant or "").lower() == "original":
            key = keys.get("minio_key") or keys.get("canonical_uri")
        elif (variant or "").lower() == "canonical":
            key = keys.get("canonical_uri") or keys.get("minio_key")
        else:
            key = keys.get("canonical_uri") or keys.get("minio_key")
        if not key:
            raise HTTPException(status_code=404, detail="no storage key for document")

        try:
            endpoint = settings.s3_endpoint
            region = settings.region
            access = settings.minio_root_user
            secret = settings.minio_root_password
            bucket = settings.s3_bucket if str(key).startswith("sha256/") else (settings.s3_canonical_bucket or settings.s3_bucket)
            if not bucket:
                raise HTTPException(status_code=500, detail="bucket not configured")

            cli = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access,
                aws_secret_access_key=secret,
                region_name=region,
                config=BotoConfig(signature_version="s3v4"),
            )
            obj = cli.get_object(Bucket=bucket, Key=key)
            # Prefer the object's ContentType (canonical HTML uploads set this correctly)
            content_type = obj.get("ContentType") or original_mime or "application/octet-stream"
            resp = StreamingResponse(obj["Body"], media_type=content_type)
            # Allow embedding in same-origin iframes (for right-side drawer)
            resp.headers["X-Frame-Options"] = "SAMEORIGIN"
            resp.headers["Content-Security-Policy"] = "frame-ancestors 'self'"
            return resp
        except Exception as e:
            print(f"Streaming failed: {e}")
            raise HTTPException(status_code=500, detail=f"stream failed: {e}")

    @r.get("/status/{doc_id}")
    async def pipeline_status(doc_id: UUID):
        """
        Return coarse-grained pipeline status for a document.
        - normalized: documents.normalized_at is set or normalization row exists
        - extracted:  documents.extracted_at is set or any blocks exist
        - chunked:    any chunks exist
        - embedded:   any qdrant points exist for this doc (best-effort; falls back to chunked)
        """
        did = str(doc_id)
        db.connect()
        normalized = extracted = chunked = False
        state = "UNKNOWN"
        with db.conn.cursor() as cur:
            cur.execute("SELECT normalized_at, extracted_at, state FROM documents WHERE doc_id=%s::uuid AND tenant_id=%s LIMIT 1;", (did, tenant_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="document not found")
            na = row.get("normalized_at") if isinstance(row, dict) else (row[0] if row else None)
            ea = row.get("extracted_at") if isinstance(row, dict) else (row[1] if row else None)
            state = row.get("state") if isinstance(row, dict) else (row[2] if row else "UNKNOWN")
            
            normalized = bool(na)
            extracted = bool(ea)
            # blocks/chunks existence checks
            cur.execute("SELECT 1 FROM blocks WHERE doc_id=%s::uuid LIMIT 1;", (did,))
            if cur.fetchone():
                extracted = True
            cur.execute("SELECT 1 FROM chunks WHERE doc_id=%s::uuid LIMIT 1;", (did,))
            chunked = bool(cur.fetchone())
        embedded = False
        if qdr is not None:
            try:
                from qdrant_client.http.models import Filter as _F, FieldCondition as _FC, MatchValue as _MV
                flt = _F(must=[_FC(key="doc_id", match=_MV(value=did))])
                res = qdr.client.scroll(collection_name=qdr.collection, scroll_filter=flt, with_payload=False, limit=1)
                pts = res[0] if isinstance(res, tuple) else res.points
                embedded = bool(pts)
            except Exception:
                embedded = chunked  # fallback approx
        else:
            embedded = chunked
        return {"doc_id": did, "normalized": normalized, "extracted": extracted, "chunked": chunked, "embedded": embedded, "state": state}

    return r