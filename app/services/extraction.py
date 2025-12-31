from __future__ import annotations
import uuid, time, os, json
from typing import Any, List, Tuple, Optional
from bs4 import BeautifulSoup

from infra.db import DBClient
from infra.minio_store import MinioStore
from core.config import settings
from .manifests import CanonicalManifest, CanonicalArtifact

def _new_id() -> str:
    return str(uuid.uuid4())
def _looks_like_table(text: str) -> tuple[bool, int]:
    """Heuristic: treat OCR/plaintext as a table if it looks pipe/column-like."""
    if not text:
        return (False, 0)
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return (False, 0)
    pipe_rows = [ln for ln in lines if ln.count("|") >= 2]
    if len(pipe_rows) >= 2:
        cols = max(2, pipe_rows[0].count("|"))
        return (True, cols)
    wide_rows = [ln for ln in lines if "  " in ln]
    if len(wide_rows) >= 2:
        return (True, 0)
    return (False, 0)

class ExtractionService:
    def __init__(self, db: DBClient, canonical_store: MinioStore, *, tenant_id: str, logger, canonical_bucket: str | None = None):
        self.db = db
        self.store = canonical_store
        self.tenant_id = tenant_id
        self.log = logger
        self.canonical_bucket = canonical_bucket or settings.s3_canonical_bucket

    def _load_canonical_html(self, canonical_key: str) -> str:
        tmp = self.store.fget_to_tmp(canonical_key)
        try:
            with open(tmp, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        finally:
            try:
                os.remove(tmp)
            except Exception:
                pass

    @staticmethod
    def _serialize_table(tbl: Any) -> Tuple[str, int, int]:
        rows_text: List[str] = []
        max_cols = 0
        for tr in tbl.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            max_cols = max(max_cols, len(cells))
            rows_text.append(" | ".join(cells))
        return "\n".join(rows_text).strip(), len(rows_text), max_cols

    @staticmethod
    def _table_from_body(table_body: Any) -> str:
        try:
            rows = []
            for row in table_body or []:
                cells = [str(cell or "").strip() for cell in row]
                rows.append(" | ".join(cells))
            return "\n".join(rows).strip()
        except Exception:
            return ""

    def _load_manifest(self, doc_id: str, manifest_key: Optional[str]) -> Optional[CanonicalManifest]:
        if not manifest_key:
            return None
        tmp = self.store.fget_to_tmp(manifest_key)
        try:
            with open(tmp, "r", encoding="utf-8") as fp:
                payload = json.load(fp)
            return CanonicalManifest.from_dict(payload)
        except Exception as exc:
            self.log("warn", "manifest-load-failed", doc_id=doc_id, error=str(exc))
            return None
        finally:
            try:
                os.remove(tmp)
            except Exception:
                pass

    def _ensure_anchor_script(self, html: str) -> str:
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            return html
        if soup.find(id="rag-anchor-script"):
            return html
        js = (
            "(function(){try{var h=decodeURIComponent(window.location.hash||'').slice(1);"
            "if(!h)return;var parts=h.split('&');var p={};for(var i=0;i<parts.length;i++){var kv=parts[i].split('=');p[kv[0]]=kv[1];}"
            "if(p.a){var el=document.getElementById('a-'+p.a)||document.getElementById(p.a)||document.querySelector('[data-block-id="'+p.a+'"]');"
            "if(el){el.scrollIntoView({behavior:'smooth',block:'center'});el.style.outline='2px solid #f59e0b';setTimeout(function(){el.style.outline='';},2500);}}}catch(e){}})();"
        )
        script = soup.new_tag("script", id="rag-anchor-script")
        script.string = js
        if soup.body:
            soup.body.append(script)
        else:
            soup.append(script)
        return str(soup)

    def _build_blocks_from_manifest(self, doc_id: str, manifest: CanonicalManifest) -> Tuple[List[dict], List[str], float]:
        blocks_rows: List[dict] = []
        warnings: List[str] = []
        cursor = 0
        coverage_chars = 0
        total_chars = sum(len((a.text or "")) for a in manifest.artifacts if (a.type != "image"))
        if manifest.stats.get("text_chars"):
            total_chars = int(manifest.stats.get("text_chars", total_chars))

        strip_headers = settings.extract_strip_headers
        header_footer: set[str] = set()
        if strip_headers:
            from collections import Counter

            firsts, lasts = [], []
            page_groups: dict[int, List[str]] = {}
            for artifact in manifest.artifacts:
                if artifact.type not in {"paragraph", "header", "list", "text"}:
                    continue
                txt = (artifact.text or "").strip()
                if not txt:
                    continue
                page = artifact.page_idx or 0
                page_groups.setdefault(page, []).append(txt)
            for texts in page_groups.values():
                firsts.append(texts[0])
                lasts.append(texts[-1])
            for cand, cnt in Counter(firsts).most_common(2):
                if cnt >= 2:
                    header_footer.add(cand)
            for cand, cnt in Counter(lasts).most_common(2):
                if cnt >= 2:
                    header_footer.add(cand)

        type_map = {
            "header": "header",
            "paragraph": "paragraph",
            "text": "paragraph",
            "list": "list",
            "table": "table",
            "code": "code",
        }

        for artifact in manifest.artifacts:
            block_type = type_map.get(artifact.type, artifact.type)
            text = (artifact.text or "").strip()
            meta = dict(artifact.metadata or {})

            # PREFER MARKDOWN FOR TABLES if available (MinerU/Docling often provide this)
            if block_type == "table" and meta.get("table_markdown"):
                text = meta.get("table_markdown")

            if block_type == "table" and not text:
                table_body = meta.get("table_body")
                if table_body:
                    text = self._table_from_body(table_body)
            if block_type == "table" and meta.get("table_html") and not meta.get("html"):
                meta["html"] = meta.get("table_html")

            if block_type == "image" and not text and meta.get("caption"):
                text = f"[Image: {meta['caption']}]"

            if block_type not in {"image"} and not text:
                continue
            if strip_headers and text and text in header_footer and block_type in {"paragraph", "header"}:
                continue
            if artifact.headers:
                meta["headers"] = artifact.headers
            if artifact.caption:
                meta["caption"] = artifact.caption
            meta["artifact_id"] = artifact.artifact_id
            meta["origin_type"] = artifact.type
            meta["source"] = "manifest"

            start = cursor
            end = start + len(text)
            if block_type != "image":
                cursor = end + 2
                coverage_chars += len(text)
            else:
                cursor = end

            blocks_rows.append(
                {
                    "block_id": _new_id(),
                    "doc_id": doc_id,
                    "page": artifact.page_idx or 0,
                    "span_start": start,
                    "span_end": end,
                    "type": block_type,
                    "text": text,
                    "meta": meta,
                }
            )

        coverage_ratio = (coverage_chars / max(1, total_chars)) if total_chars else 0.0
        if coverage_ratio < 0.6:
            warnings.append(f"low_coverage:{coverage_ratio:.2f}")
        return blocks_rows, warnings, coverage_ratio

    def _run_with_html(self, doc_id: str, canonical_key: str) -> dict:
        t0 = time.time()
        html = self._load_canonical_html(canonical_key)
        if not html or not html.strip():
            self.db.insert_event(self.tenant_id, stage="EXTRACTED", status="FAIL", details={
                "event":"DOC_EXTRACTED_FAIL","error":"canonical_empty"
            }, doc_id=doc_id)
            raise RuntimeError("canonical_empty")

        soup = BeautifulSoup(html, "lxml")
        total_chars_canonical = len(soup.get_text() or "")

        # pages
        sections = soup.find_all("section", attrs={"data-page": True})
        if not sections:
    # treat entire document as a single “section” = the <body> (or whole soup)
            sections = [soup.body or soup]   # <-- CHANGED


        blocks_rows: List[dict] = []
        # Optional: strip repeating header/footer lines across pages
        strip_headers = settings.extract_strip_headers
        header_footer: set[str] = set()
        if strip_headers and sections:
            firsts, lasts = [], []
            for sec in sections:
                txt = (sec.get_text("\n") or "").splitlines()
                # first and last non-empty lines
                first = next((l.strip() for l in txt if l.strip()), None)
                last = next((l.strip() for l in reversed(txt) if l.strip()), None)
                if first: firsts.append(first)
                if last: lasts.append(last)
            from collections import Counter
            # Quality Refinement: Only strip if it appears on at least 3 pages AND >20% of doc
            # This prevents aggressive stripping on short docs
            total_secs = len(sections)
            threshold = max(3, int(total_secs * 0.2))
            
            for cand, cnt in Counter(firsts).most_common(5):
                if cnt >= threshold: header_footer.add(cand)
            for cand, cnt in Counter(lasts).most_common(5):
                if cnt >= threshold: header_footer.add(cand)
        cursor = 0  # span cursor in conceptual flattened text
        block_count = 0

        for sec in sections:
            try:
                page = int(sec.get("data-page", "1")) if hasattr(sec, "get") else 1  # <-- CHANGED
            except Exception:
                page = 1


            # extraction order: headers, paragraphs, lists, tables — as they appear
            # We'll iterate DOM children to preserve order, but collect per tag
            for node in sec.descendants:
                if not getattr(node, "name", None):
                    continue
                name = node.name.lower()

                # Skip any node that is inside a TABLE or a LIST; we handle containers only
                if any(getattr(anc, "name", "").lower() in ("table", "ul", "ol") for anc in getattr(node, "parents", [])):
                    # Allow the container itself through
                    if name not in ("table", "ul", "ol"):
                        continue

                # TABLE (one block per table)
                if name == "table":
                    text, rows, cols = self._serialize_table(node)
                    if not text:
                        continue
                    bid = _new_id()
                    try:
                        node["id"] = f"a-{bid}"
                        node["data-block-id"] = bid
                    except Exception:
                        pass
                    start = cursor
                    end = start + len(text)
                    cursor = end + 2
                    blocks_rows.append({
                        "block_id": bid,
                        "doc_id": doc_id,
                        "page": page,
                        "span_start": start,
                        "span_end": end,
                        "type": "table",
                        "text": text,
                        "meta": {"rows": rows, "cols": cols}
                    })
                    block_count += 1

                # LIST (whole UL/OL as one block)
                elif name in ("ul", "ol"):
                    items = [li.get_text(" ", strip=True) for li in node.find_all("li", recursive=False)]
                    if not items:
                        continue
                    text = "\n".join(items).strip()
                    if not text:
                        continue
                    bid = _new_id()
                    try:
                        node["id"] = f"a-{bid}"
                        node["data-block-id"] = bid
                    except Exception:
                        pass
                    start = cursor
                    end = start + len(text)
                    cursor = end + 2
                    blocks_rows.append({
                        "block_id": bid,
                        "doc_id": doc_id,
                        "page": page,
                        "span_start": start,
                        "span_end": end,
                        "type": "list",
                        "text": text,
                        "meta": {"items": len(items)}
                    })
                    block_count += 1

                # PARAGRAPH
                elif name == "p":
                    text = node.get_text(" ", strip=True)
                    if strip_headers and text in header_footer:
                        continue
                    if not text:
                        continue
                    bid = _new_id()
                    try:
                        node["id"] = f"a-{bid}"
                        node["data-block-id"] = bid
                    except Exception:
                        pass
                    start = cursor
                    end = start + len(text)
                    cursor = end + 2
                    blocks_rows.append({
                        "block_id": bid,
                        "doc_id": doc_id,
                        "page": page,
                        "span_start": start,
                        "span_end": end,
                        "type": "paragraph",
                        "text": text,
                        "meta": {}
                    })
                    block_count += 1

                # HEADERS
                elif name in ("h1","h2","h3","h4","h5","h6"):
                    text = node.get_text(" ", strip=True)
                    if strip_headers and text in header_footer:
                        continue
                    if not text:
                        continue
                    level = int(name[1])
                    bid = _new_id()
                    try:
                        node["id"] = f"a-{bid}"
                        node["data-block-id"] = bid
                    except Exception:
                        pass
                    start = cursor
                    end = start + len(text)
                    cursor = end + 2
                    blocks_rows.append({
                        "block_id": bid,
                        "doc_id": doc_id,
                        "page": page,
                        "span_start": start,
                        "span_end": end,
                        "type": "header",
                        "text": text,
                        "meta": {"level": level}
                    })
                    block_count += 1
                elif name == "pre":
                    text = node.get_text("\n", strip=True)
                    if not text:
                        continue
                    is_tabular, cols = _looks_like_table(text)
                    block_type = "table" if is_tabular else "paragraph"
                    meta = {"source": "pre"}
                    if is_tabular:
                        rows_count = len([ln for ln in text.splitlines() if ln.strip()])
                        if cols:
                            meta["cols"] = cols
                        meta["rows"] = rows_count
                    bid = _new_id()
                    try:
                        node["id"] = f"a-{bid}"
                        node["data-block-id"] = bid
                    except Exception:
                        pass
                    start = cursor
                    end = start + len(text)
                    cursor = end + 2
                    blocks_rows.append({
                        "block_id": bid,
                        "doc_id": doc_id,
                        "page": page,
                        "span_start": start,
                        "span_end": end,
                        "type": block_type,
                        "text": text,
                        "meta": meta
                    })
                    block_count += 1


                # ----- CHECKER: tables not split -----
        dom_table_count = sum(len(sec.find_all("table")) for sec in sections)
        blk_table_count = sum(1 for r in blocks_rows if r["type"] == "table")
        checker_warnings = []

        if dom_table_count != blk_table_count:
            checker_warnings.append(f"table_block_count_mismatch: dom={dom_table_count} blocks={blk_table_count}")
            self.db.insert_event(self.tenant_id, stage="CHECKER", status="WARN", details={
                "checker": "table_block_count_mismatch",
                "message": "number of <table> elements != number of table blocks",
                "context": {"dom_tables": dom_table_count, "block_tables": blk_table_count}
            }, doc_id=doc_id)


        # Also ensure no non-table block overlaps a table span
        def _overlap(a, b):
            return not (a["span_end"] <= b["span_start"] or b["span_end"] <= a["span_start"])

        table_blocks = [r for r in blocks_rows if r["type"] == "table"]
        other_blocks = [r for r in blocks_rows if r["type"] != "table"]
        for tb in table_blocks:
            for ob in other_blocks:
                if _overlap(tb, ob):
                    checker_warnings.append("table_span_overlap_with_non_table")
                    self.db.insert_event(self.tenant_id, stage="CHECKER", status="WARN", details={
                        "checker": "table_span_overlap_with_non_table",
                        "message": "a non-table block's span overlaps a table block",
                        "context": {"table_span": [tb["span_start"], tb["span_end"]], "other_type": ob["type"],
                                    "other_span": [ob["span_start"], ob["span_end"]]}
                    }, doc_id=doc_id)
                    break

        printable_chars = sum(len(r["text"]) for r in blocks_rows)
        printable_ratio = (printable_chars / max(1, total_chars_canonical)) if total_chars_canonical else 0.0

                # Persist
        # span integrity check
        try:
            prev_end = -1
            for r in blocks_rows:
                if r["span_start"] < prev_end:
                    checker_warnings.append("span_regression_detected")
                    self.db.insert_event(self.tenant_id, stage="CHECKER", status="WARN", details={
                        "checker": "span_regression_detected",
                        "message": "block span_start regressed",
                        "context": {"prev_end": prev_end, "curr_start": r["span_start"], "curr_end": r["span_end"]}
                    }, doc_id=doc_id)
                    break
                prev_end = r["span_end"]
        except Exception:
            pass

        # Cap total blocks per doc - Quality: Allow effectively infinite blocks
        MAX_BLOCKS = 1000000 # settings.extract_max_blocks
        if len(blocks_rows) > MAX_BLOCKS:
            checker_warnings.append("blocks_capped")
            self.db.insert_event(self.tenant_id, stage="CHECKER", status="WARN", details={
                "checker": "blocks_capped", "message": "excess blocks trimmed",
                "context": {"count": len(blocks_rows), "cap": MAX_BLOCKS}
            }, doc_id=doc_id)
            blocks_rows = blocks_rows[:MAX_BLOCKS]

        # Persist (idempotent): delete-old-then-insert
        removed = self.db.delete_blocks_for_doc(doc_id)
        self.log("info", "extract-idempotent-delete", stage="EXTRACTED", doc_id=doc_id, removed_blocks=removed)
        self.db.insert_blocks_bulk(blocks_rows)

        # Persist updated canonical HTML with block anchors and small scroll-to-anchor script
        try:
            html_updated = self._ensure_anchor_script(str(soup))
            self.store.put_canonical_html(bucket=self.canonical_bucket, doc_id=doc_id, html=html_updated, version="v1")
        except Exception as e:
            self.log("warn", "canonical-anchor-update-failed", stage="EXTRACTED", doc_id=doc_id, error=str(e))


        # Event + logging
        status = "OK"
        warnings: list[str] = []
        if block_count == 0:
            status = "WARN"
            warnings.append("no_blocks")
        if printable_ratio < 0.05:
            status = "WARN"
            warnings.append("low_printable_ratio")
        warnings.extend(checker_warnings)  # <-- include checker warnings

        self.db.insert_event(self.tenant_id, stage="EXTRACTED", status=status, details={
            "event": f"DOC_EXTRACTED_{status}",
            "blocks_count": block_count,
            "printable_ratio": round(printable_ratio, 3),
            "warnings": warnings
        }, doc_id=doc_id)
        self.db.update_document_state(doc_id, "EXTRACTED", ts_column="extracted_at")

        self.log("info", "extraction-summary", stage="EXTRACTED",
         doc_id=doc_id, dom_tables=sum(len(sec.find_all('table')) for sec in sections),
         blocks=len(blocks_rows))

        return {"doc_id": doc_id, "status": status, "blocks": block_count, "warnings": warnings}

    def _run_with_manifest(self, doc_id: str, canonical_key: str, manifest: CanonicalManifest) -> dict:
        t0 = time.time()
        blocks_rows, builder_warnings, coverage_ratio = self._build_blocks_from_manifest(doc_id, manifest)

        # Ensure anchor script present
        html_original = self._load_canonical_html(canonical_key)
        html_updated = self._ensure_anchor_script(html_original)
        if html_updated != html_original:
            try:
                self.store.put_canonical_html(bucket=self.canonical_bucket, doc_id=doc_id, html=html_updated, version="v1")
            except Exception as exc:
                self.log("warn", "manifest-canonical-update-failed", doc_id=doc_id, error=str(exc))

        dom_table_count = len([a for a in manifest.artifacts if a.type == "table"])
        blk_table_count = sum(1 for r in blocks_rows if r["type"] == "table")
        checker_warnings: List[str] = []
        if dom_table_count != blk_table_count:
            warning = f"table_block_count_mismatch: dom={dom_table_count} blocks={blk_table_count}"
            checker_warnings.append(warning)
            self.db.insert_event(
                self.tenant_id,
                stage="CHECKER",
                status="WARN",
                details={
                    "checker": "table_block_count_mismatch",
                    "message": "number of manifest tables != blocks",
                    "context": {"dom_tables": dom_table_count, "block_tables": blk_table_count},
                },
                doc_id=doc_id,
            )

        MAX_BLOCKS = 1000000 # settings.extract_max_blocks
        if len(blocks_rows) > MAX_BLOCKS:
            checker_warnings.append("blocks_capped")
            self.db.insert_event(
                self.tenant_id,
                stage="CHECKER",
                status="WARN",
                details={
                    "checker": "blocks_capped",
                    "message": "excess blocks trimmed",
                    "context": {"count": len(blocks_rows), "cap": MAX_BLOCKS},
                },
                doc_id=doc_id,
            )
            blocks_rows = blocks_rows[:MAX_BLOCKS]

        removed = self.db.delete_blocks_for_doc(doc_id)
        self.log("info", "extract-idempotent-delete", stage="EXTRACTED", doc_id=doc_id, removed_blocks=removed)
        self.db.insert_blocks_bulk(blocks_rows)

        printable_chars = sum(len(r["text"]) for r in blocks_rows if r["type"] != "image")
        total_chars = sum(len((a.text or "")) for a in manifest.artifacts if a.type != "image")
        printable_ratio = (printable_chars / max(1, total_chars)) if total_chars else coverage_ratio

        status = "OK"
        warnings = builder_warnings + checker_warnings
        if not blocks_rows:
            status = "WARN"
            warnings.append("no_blocks")
        if printable_ratio < 0.05:
            status = "WARN"
            warnings.append("low_printable_ratio")

        self.db.insert_event(
            self.tenant_id,
            stage="EXTRACTED",
            status=status,
            details={
                "event": f"DOC_EXTRACTED_{status}",
                "blocks_count": len(blocks_rows),
                "printable_ratio": round(printable_ratio, 3),
                "coverage_ratio": round(coverage_ratio, 3),
                "warnings": warnings,
                "source": "manifest",
            },
            doc_id=doc_id,
        )
        self.db.update_document_state(doc_id, "EXTRACTED", ts_column="extracted_at")

        self.log(
            "info",
            "extraction-summary",
            stage="EXTRACTED",
            doc_id=doc_id,
            dom_tables=dom_table_count,
            blocks=len(blocks_rows),
            source="manifest",
            latency_ms=int((time.time() - t0) * 1000),
        )

        return {
            "doc_id": doc_id,
            "status": status,
            "blocks": len(blocks_rows),
            "warnings": warnings,
            "source": "manifest",
        }

    def run_one(self, doc_id: str) -> dict:
        with self.db.conn.cursor() as cur:
            cur.execute(
                "SELECT canonical_uri, manifest_uri FROM normalizations WHERE doc_id=%s LIMIT 1;",
                (doc_id,),
            )
            row = cur.fetchone()

        if not row:
            self.db.insert_event(
                self.tenant_id,
                stage="EXTRACTED",
                status="FAIL",
                details={"event": "DOC_EXTRACTED_FAIL", "error": "canonical_missing"},
                doc_id=doc_id,
            )
            raise RuntimeError("canonical_missing")

        canonical_key = row["canonical_uri"]
        manifest = self._load_manifest(doc_id, row.get("manifest_uri"))

        if manifest:
            return self._run_with_manifest(doc_id, canonical_key, manifest)
        return self._run_with_html(doc_id, canonical_key)