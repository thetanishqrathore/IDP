from __future__ import annotations
import hashlib, binascii, tempfile, mimetypes, time, os
from typing import List, Optional, Tuple
from fastapi import UploadFile

from core.models import IngestResponseItem, StoredResult, new_uuid
from infra.db import DBClient
from infra.minio_store import MinioStore
from core.config import settings


def compute_hashes_to_tmp(upload: UploadFile, chunk_size: int = 1_048_576) -> Tuple[str, str, int, str]:
    """
    Stream the UploadFile to a temp file, computing sha256 and crc32.
    Returns: (tmp_path, sha256_hex, crc32_hex, size_bytes)
    """
    h = hashlib.sha256()
    crc = 0
    size = 0
    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        while True:
            chunk = upload.file.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
            crc = binascii.crc32(chunk, crc)
            size += len(chunk)
            tmp.write(chunk)
    finally:
        tmp.flush()
        tmp.close()
        upload.file.seek(0)  # rewind for safety (not needed further, but good hygiene)
    sha256_hex = h.hexdigest()
    crc32_hex = format(crc & 0xFFFFFFFF, "08x")
    return tmp.name, sha256_hex, crc32_hex, size


def guess_mime(upload: UploadFile) -> str:
    # prefer provided content type; else guess from filename; fallback to octet-stream
    if upload.content_type and upload.content_type != "application/octet-stream":
        return upload.content_type
    mt, _ = mimetypes.guess_type(upload.filename or "")
    return mt or "application/octet-stream"


def detect_mime_from_file(tmp_path: str, fallback: str) -> str:
    """Try to detect MIME using python-magic from actual bytes; fallback to provided value."""
    try:
        import magic  # type: ignore
        try:
            m = magic.Magic(mime=True)  # type: ignore
            mt = m.from_file(tmp_path)
        except Exception:
            mt = magic.from_file(tmp_path, mime=True)  # type: ignore
        if mt and isinstance(mt, str):
            return mt
    except Exception:
        pass
    return fallback


class IngestionService:
    def __init__(
        self,
        db: DBClient,
        store: MinioStore,
        *,
        tenant_id: str,
        logger,
        allowed_mime_prefixes: list[str] | None = None,
        max_file_mb: int = 50,
        max_files_per_request: int = 10,
    ):
        self.db = db
        self.store = store
        self.tenant_id = tenant_id
        self.log = logger
        self.allowed_mime_prefixes = allowed_mime_prefixes or []
        self.max_file_mb = max_file_mb
        self.max_files_per_request = max_files_per_request
        # additional guards
        self.max_filename_len = settings.ingest_max_filename_len
        self.disallowed_exts = set(
            (settings.ingest_disallowed_exts or "")
            .lower().split(",")
        )

        # Strict mode: when true, reject files on extension/mime/size/filename policy violations.
        # When false (default), log WARN and proceed with ingestion.
        self.strict_mode = settings.ingest_strict_mode


    def store_many(self, uploads: List[UploadFile], source_uri: Optional[str] = None, source: Optional[str] = None) -> List[IngestResponseItem]:
        # ---- request-level guard: too many files ----
        if len(uploads) > self.max_files_per_request:
            self.log(
                "warn", "ingest-reject-too-many-files",
                stage="STORED", count=len(uploads), max=self.max_files_per_request
            )
            # Uniform per-file rejections (do not touch DB/MinIO)
            return [
                IngestResponseItem(
                    tenant_id=self.tenant_id,
                    doc_id=None,
                    sha256="",
                    state="REJECTED",
                    size_bytes=0,
                    mime=guess_mime(u),
                    uri=(u.filename or "unnamed"),
                    duplicate=False,
                    minio_key=None,
                    events=["DOC_REJECTED_TOO_MANY_FILES"],
                    warnings=[f"max_files_per_request={self.max_files_per_request}"],
                )
                for u in uploads
            ]

        from concurrent.futures import ThreadPoolExecutor
        
        def _process_one(upload: UploadFile) -> IngestResponseItem:
            t0 = time.time()
            fname = upload.filename or "unnamed"
            try:
                # Note: compute_hashes_to_tmp is I/O bound on disk write
                tmp_path, sha256_hex, crc32_hex, size = compute_hashes_to_tmp(upload)
                mime = detect_mime_from_file(tmp_path, guess_mime(upload))
                file_warnings: list[str] = []
                # ---- filename sanity ----
                if len(fname) > self.max_filename_len:
                    self.db.insert_event(self.tenant_id, stage="STORED", status="WARN", details={
                        "event": "DOC_REJECTED_FILENAME_LEN", "filename": fname, "limit": self.max_filename_len
                    })
                    if self.strict_mode:
                        try: os.remove(tmp_path)
                        except Exception: pass
                        return IngestResponseItem(
                            tenant_id=self.tenant_id, doc_id=None, sha256="", state="REJECTED",
                            size_bytes=size, mime=mime, uri=source_uri or fname, duplicate=False,
                            minio_key=None, events=["DOC_REJECTED_FILENAME_LEN"],
                            warnings=[f"filename too long (> {self.max_filename_len})"]
                        )
                    else:
                        file_warnings.append(f"filename too long (> {self.max_filename_len})")
                
                # ---- suspicious extensions ----
                ext = os.path.splitext(fname)[1].lower()
                if ext and ext in self.disallowed_exts:
                    self.db.insert_event(self.tenant_id, stage="STORED", status="WARN", details={
                        "event": "DOC_REJECTED_EXTENSION", "filename": fname, "ext": ext
                    })
                    if self.strict_mode:
                        try: os.remove(tmp_path)
                        except Exception: pass
                        return IngestResponseItem(
                            tenant_id=self.tenant_id, doc_id=None, sha256="", state="REJECTED",
                            size_bytes=size, mime=mime, uri=source_uri or fname, duplicate=False,
                            minio_key=None, events=["DOC_REJECTED_EXTENSION"],
                            warnings=[f"disallowed extension: {ext}"]
                        )
                    else:
                        file_warnings.append(f"disallowed extension: {ext}")
                
                # ---- per-file size cap ----
                max_bytes = self.max_file_mb * 1024 * 1024
                # reject empty files
                if size == 0:
                    self.db.insert_event(self.tenant_id, stage="STORED", status="WARN", details={
                        "event": "DOC_REJECTED_EMPTY", "filename": fname, "mime": mime
                    })
                    try: os.remove(tmp_path)
                    except Exception: pass
                    self.log("warn", "ingest-reject-empty", stage="STORED", filename=fname, mime=mime)
                    return IngestResponseItem(
                        tenant_id=self.tenant_id, doc_id=None, sha256="", state="REJECTED",
                        size_bytes=size, mime=mime, uri=source_uri or fname, duplicate=False,
                        minio_key=None, events=["DOC_REJECTED_EMPTY"], warnings=["empty file"]
                    )

                if size > max_bytes:
                    self.db.insert_event(self.tenant_id, stage="STORED", status="WARN", details={
                        "event": "DOC_REJECTED_OVERSIZE",
                        "size_bytes": size,
                        "limit_bytes": max_bytes,
                        "filename": fname,
                        "mime": mime,
                    })
                    if self.strict_mode:
                        try: os.remove(tmp_path)
                        except Exception: pass
                        self.log("warn", "ingest-reject-oversize", stage="STORED", filename=fname, size_bytes=size, limit=max_bytes)
                        return IngestResponseItem(
                            tenant_id=self.tenant_id,
                            doc_id=None,
                            sha256="",
                            state="REJECTED",
                            size_bytes=size,
                            mime=mime,
                            uri=source_uri or fname,
                            duplicate=False,
                            minio_key=None,
                            events=["DOC_REJECTED_OVERSIZE"],
                            warnings=[f"file too large: {size} > {max_bytes}"],
                        )
                    else:
                        self.log("warn", "ingest-oversize-allowed", stage="STORED", filename=fname, size_bytes=size, limit=max_bytes)
                        file_warnings.append(f"file too large: {size} > {max_bytes}")

                # ---- per-file MIME allowlist ----
                if self.allowed_mime_prefixes and not any(mime.startswith(pfx) for pfx in self.allowed_mime_prefixes):
                    self.db.insert_event(self.tenant_id, stage="STORED", status="WARN", details={
                        "event": "DOC_REJECTED_MIME",
                        "mime": mime,
                        "allowed": self.allowed_mime_prefixes,
                        "filename": fname,
                    })
                    if self.strict_mode:
                        try: os.remove(tmp_path)
                        except Exception: pass
                        self.log("warn", "ingest-reject-mime", stage="STORED", filename=fname, mime=mime, allowed=self.allowed_mime_prefixes)
                        return IngestResponseItem(
                            tenant_id=self.tenant_id,
                            doc_id=None,
                            sha256="",
                            state="REJECTED",
                            size_bytes=size,
                            mime=mime,
                            uri=source_uri or fname,
                            duplicate=False,
                            minio_key=None,
                            events=["DOC_REJECTED_MIME"],
                            warnings=[f"disallowed mime: {mime}"]
                        )
                    else:
                        self.log("warn", "ingest-mime-allowed", stage="STORED", filename=fname, mime=mime, allowed=self.allowed_mime_prefixes)
                        file_warnings.append(f"disallowed mime: {mime}")

                # dedupe
                existing_doc = self.db.find_doc_by_hash(self.tenant_id, sha256_hex)
                if existing_doc:
                    # event: DOC_DUPLICATE
                    self.db.insert_event(self.tenant_id, stage="STORED", status="INFO", details={
                        "event": "DOC_DUPLICATE",
                        "sha256": sha256_hex,
                        "uri": source_uri or fname,
                        "mime": mime,
                        "size_bytes": size
                    }, doc_id=existing_doc)

                    try: os.remove(tmp_path)
                    except Exception: pass
                    
                    self.log("info", "ingest-duplicate", stage="STORED", sha256=sha256_hex, filename=fname,
                             mime=mime, size_bytes=size, latency_ms=int((time.time()-t0)*1000))
                    
                    return IngestResponseItem(
                        tenant_id=self.tenant_id,
                        doc_id=existing_doc,
                        sha256=sha256_hex,
                        state="STORED",
                        size_bytes=size,
                        mime=mime,
                        uri=source_uri or fname,
                        duplicate=True,
                        minio_key=MinioStore.build_key_for_sha256(sha256_hex),
                        events=["DOC_DUPLICATE"],
                        warnings=[]
                    )

                # upload to MinIO
                key = self.store.put_file(sha256_hex, tmp_path, size, mime)
                self.db.upsert_blob(sha256=sha256_hex, location=key, crc32=crc32_hex)

                # insert document
                doc_id = new_uuid()
                pipeline_versions = {"ingestor": "0.1.0"}
                meta = {"filename": fname, "source": source}
                self.db.insert_document(
                    doc_id=doc_id, tenant_id=self.tenant_id, sha256=sha256_hex, uri=(source_uri or fname),
                    mime=mime, size_bytes=size, state="STORED", pipeline_versions=pipeline_versions, meta=meta
                )

                # event: DOC_STORED
                self.db.insert_event(self.tenant_id, stage="STORED", status="OK", details={
                    "event": "DOC_STORED",
                    "sha256": sha256_hex,
                    "uri": source_uri or fname,
                    "filename": fname,
                    "mime": mime,
                    "size_bytes": size,
                    "node": "api",
                    "pipeline_versions": pipeline_versions
                }, doc_id=doc_id)

                # quick checkers
                warnings = []
                # MIME vs extension
                mt_guess, _ = mimetypes.guess_type(fname)
                if mt_guess and mt_guess != mime:
                    warn_msg = f"mime_extension_mismatch: ext_guess={mt_guess} provided={mime}"
                    self.db.insert_event(self.tenant_id, stage="CHECKER", status="WARN", details={
                        "checker": "mime_extension_mismatch",
                        "message": warn_msg,
                        "context": {"filename": fname, "ext_guess": mt_guess, "provided": mime}
                    }, doc_id=doc_id)
                    warnings.append(warn_msg)

                # size parity with MinIO
                try:
                    st = self.store.stat(key)
                    if st.size != size:
                        warn_msg = f"blob_size_mismatch: minio={st.size} computed={size}"
                        self.db.insert_event(self.tenant_id, stage="CHECKER", status="WARN", details={
                            "checker": "blob_size_mismatch",
                            "message": warn_msg,
                            "context": {"minio_size": st.size, "computed_size": size, "key": key}
                        }, doc_id=doc_id)
                        warnings.append(warn_msg)
                except Exception as e:
                    warn_msg = f"blob_stat_failed: {e}"
                    self.db.insert_event(self.tenant_id, stage="CHECKER", status="WARN", details={
                        "checker": "blob_stat_failed",
                        "message": str(e),
                        "context": {"key": key}
                    }, doc_id=doc_id)
                    warnings.append(warn_msg)

                self.log("info", "ingest-stored", stage="STORED", sha256=sha256_hex, filename=fname,
                         mime=mime, size_bytes=size, latency_ms=int((time.time()-t0)*1000), key=key)

                return IngestResponseItem(
                    tenant_id=self.tenant_id,
                    doc_id=doc_id,
                    sha256=sha256_hex,
                    state="STORED",
                    size_bytes=size,
                    mime=mime,
                    uri=source_uri or fname,
                    duplicate=False,
                    minio_key=key,
                    events=["DOC_STORED"] + (["CHECKER_WARN"] if warnings else []),
                    warnings=(file_warnings + warnings)
                )

            except Exception as e:
                self.db.insert_event(self.tenant_id, stage="STORED", status="FAIL", details={
                    "event": "DOC_STORE_FAIL", "error": str(e), "filename": fname
                }, doc_id=None)
                self.log("error", "ingest-fail", stage="STORED", filename=fname, error=str(e))
                return IngestResponseItem(
                    tenant_id=self.tenant_id, doc_id=None, sha256="", state="ERROR",
                    size_bytes=0, mime=upload.content_type or "application/octet-stream",
                    uri=source_uri or fname, duplicate=False, minio_key=None,
                    events=["DOC_STORE_FAIL"], warnings=[str(e)]
                )

            finally:
                # clean temp file if exists
                try:
                    if 'tmp_path' in locals() and os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception as cleanup_err:
                     self.log("warn", "cleanup-failed", path=tmp_path, error=str(cleanup_err))

        # Execute in parallel
        # We use min(len(uploads), 8) threads to prevent overwhelming disk I/O
        with ThreadPoolExecutor(max_workers=min(len(uploads), 8)) as executor:
            results = list(executor.map(_process_one, uploads))
        
        return results

    def _is_safe_url(self, url: str) -> bool:
        import socket
        import ipaddress
        from urllib.parse import urlparse

        try:
            parsed = urlparse(url)
            hostname = parsed.hostname
            if not hostname:
                return False
            # resolve
            ip = socket.gethostbyname(hostname)
            ip_obj = ipaddress.ip_address(ip)
            
            # block private, loopback, link-local, multicast
            if (ip_obj.is_private or ip_obj.is_loopback or 
                ip_obj.is_link_local or ip_obj.is_multicast or ip_obj.is_reserved):
                return False
            return True
        except Exception:
            return False

    def ingest_from_url(self, url: str, source: Optional[str] = None) -> IngestResponseItem:
        """
        Download a file from a URL to a temp file and ingest it.
        Useful for n8n/Zapier integrations.
        """
        import requests
        import shutil
        from urllib.parse import urlparse

        # SSRF Guard
        if not self._is_safe_url(url):
             self.log("warn", "ingest-url-ssrf-blocked", url=url)
             return IngestResponseItem(
                tenant_id=self.tenant_id, doc_id=None, sha256="", state="REJECTED",
                size_bytes=0, mime="unknown", uri=url, duplicate=False, minio_key=None,
                events=["DOC_REJECTED_SSRF"], warnings=["unsafe_url_target"]
            )

        t0 = time.time()
        # Basic filename inference
        path = urlparse(url).path
        filename = os.path.basename(path) or "downloaded_file"
        
        # Download to temp
        tmp = tempfile.NamedTemporaryFile(delete=False)
        try:
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    tmp.write(chunk)
            tmp.close() # finish write
            
            # Re-open as a file-like object to reuse existing logic logic or just compute hashes directly
            # For DRY, let's just reuse the logic parts manually since we don't have an UploadFile
            
            # Compute hashes from the temp file on disk
            h = hashlib.sha256()
            crc = 0
            size = 0
            with open(tmp.name, "rb") as f:
                while True:
                    chunk = f.read(1_048_576)
                    if not chunk: break
                    h.update(chunk)
                    crc = binascii.crc32(chunk, crc)
                    size += len(chunk)
            
            sha256_hex = h.hexdigest()
            crc32_hex = format(crc & 0xFFFFFFFF, "08x")
            
            # MIME
            mime = detect_mime_from_file(tmp.name, "application/octet-stream")
            
            # Check dupes
            existing_doc = self.db.find_doc_by_hash(self.tenant_id, sha256_hex)
            if existing_doc:
                os.remove(tmp.name)
                return IngestResponseItem(
                    tenant_id=self.tenant_id, doc_id=existing_doc, sha256=sha256_hex,
                    state="STORED", size_bytes=size, mime=mime, uri=url, duplicate=True,
                    minio_key=MinioStore.build_key_for_sha256(sha256_hex),
                    events=["DOC_DUPLICATE"], warnings=[]
                )

            # Store to MinIO
            key = self.store.put_file(sha256_hex, tmp.name, size, mime)
            self.db.upsert_blob(sha256=sha256_hex, location=key, crc32=crc32_hex)
            
            # DB Record
            doc_id = new_uuid()
            pipeline_versions = {"ingestor": "0.1.0"}
            meta = {"filename": filename, "source": source or "url_ingest"}
            self.db.insert_document(
                doc_id=doc_id, tenant_id=self.tenant_id, sha256=sha256_hex, uri=url,
                mime=mime, size_bytes=size, state="STORED", pipeline_versions=pipeline_versions, meta=meta
            )
            
            # Event
            self.db.insert_event(self.tenant_id, stage="STORED", status="OK", details={
                "event": "DOC_STORED_URL", "url": url, "filename": filename
            }, doc_id=doc_id)
            
            return IngestResponseItem(
                tenant_id=self.tenant_id, doc_id=doc_id, sha256=sha256_hex,
                state="STORED", size_bytes=size, mime=mime, uri=url, duplicate=False,
                minio_key=key, events=["DOC_STORED"], warnings=[]
            )

        except Exception as e:
            try: os.remove(tmp.name)
            except: pass
            self.log("error", "ingest-url-fail", url=url, error=str(e))
            return IngestResponseItem(
                tenant_id=self.tenant_id, doc_id=None, sha256="", state="ERROR",
                size_bytes=0, mime="unknown", uri=url, duplicate=False, minio_key=None,
                events=["DOC_STORE_FAIL"], warnings=[str(e)]
            )

        finally:
            try:
                if 'tmp' in locals() and os.path.exists(tmp.name):
                    os.remove(tmp.name)
            except Exception as cleanup_err:
                 self.log("warn", "cleanup-failed", path=tmp.name, error=str(cleanup_err))