from __future__ import annotations
import hashlib, math, time, uuid
from typing import Any, Dict, List, Tuple, Optional

try:
    import tiktoken
    _ENC = tiktoken.get_encoding("cl100k_base")
except Exception:
    _ENC = None

from infra.db import DBClient
from core.config import settings
from services.llm.providers import OpenAIProvider

def _id() -> str:
    return str(uuid.uuid4())

def _tok_count(text: str) -> int:
    if not text:
        return 0
    if _ENC:
        try: return len(_ENC.encode(text))
        except Exception: pass
    return math.ceil(len(text) / 4)

def _checksum(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

def _norm_text(s: str) -> str:
    # collapse whitespace; keep single newlines; bullets for lists if already line-broken
    s = s.replace("\xa0", " ")
    lines = [ln.strip() for ln in s.splitlines()]
    out = []
    for ln in lines:
        if not ln: continue
        out.append(ln)
    return "\n".join(out).strip()

class ChunkingService:
    def __init__(self, db: DBClient, *, tenant_id: str, logger,
                 target_tokens: int = 800, overlap_tokens: int = 120, max_chunks_per_doc: int = 5000):
        self.db = db
        self.tenant_id = tenant_id
        self.log = logger
        self.target_tokens = target_tokens
        self.overlap_tokens = overlap_tokens
        self.max_chunks_per_doc = max_chunks_per_doc
        
        self.ctx_provider = None
        if settings.contextual_chunking_enabled:
            key = settings.gemini_api_key or settings.openai_api_key
            if key:
                try:
                    self.ctx_provider = OpenAIProvider(
                        api_key=key,
                        base_url=settings.gen_base_url,
                        model=settings.gen_model # use same model for now
                    )
                except Exception:
                    pass

    def _enrich_chunk(self, text: str, context_headers: List[str]) -> str:
        # Optimized: Deferred to _enrich_rows_parallel in run_one to allow concurrency
        return text

    def _enrich_rows_parallel(self, rows: List[Dict[str, Any]]) -> None:
        if not self.ctx_provider or not rows:
            return
        
        # Filter rows that actually have context headers
        candidates = [r for r in rows if r["meta"].get("context_headers")]
        if not candidates:
            return

        from concurrent.futures import ThreadPoolExecutor

        def _process(row):
            text = row["text"]
            headers = row["meta"].get("context_headers") or []
            
            ctx_str = " | ".join(headers[:3]) # Limit context length
            prompt = (
                f"Document Context: {ctx_str}\n"
                f"Text: {text[:800]}...\n\n"
                "Write a single, short sentence explaining what this text is about in the context of the document."
            )
            try:
                c = self.ctx_provider.generate([{"role":"user", "content":prompt}], max_tokens=50)
                if c:
                    row["text"] = f"[{c.strip()}]\n{text}"
                    # Update checksum and token count
                    row["checksum"] = _checksum(row["text"])
                    row["meta"]["tokens"] = _tok_count(row["text"])
            except Exception:
                pass

        # Use parallel threads for IO-bound LLM calls
        with ThreadPoolExecutor(max_workers=min(10, len(candidates))) as executor:
            list(executor.map(_process, candidates))

    # ---------- main entry ----------
    def run_one(self, doc_id: str) -> Dict[str, Any]:
        t0 = time.time()
        blocks = self.db.fetch_blocks_for_doc(doc_id)
        if not blocks:
            self.db.insert_event(self.tenant_id, stage="CHUNKED", status="FAIL",
                                 details={"event":"CHUNKED_FAIL","reason":"no_blocks"}, doc_id=doc_id)
            raise RuntimeError("no_blocks")

        # stats
        total_chars = sum(len(b["text"] or "") for b in blocks)
        table_blocks = [b for b in blocks if b["type"] == "table"]
        table_chars = sum(len(b["text"] or "") for b in table_blocks)
        table_density = (table_chars / total_chars) if total_chars else 0.0
        has_big_table = any((b.get("meta") or {}).get("rows", 0) >= 3 for b in table_blocks)

        # strategy
        tiny_doc = total_chars < 600
        has_table = len(table_blocks) > 0
        has_big_table = any((b.get("meta") or {}).get("rows", 0) >= 3 for b in table_blocks)
        table_density = (table_chars / total_chars) if total_chars else 0.0

        # Always define layout_mode
        if has_table:
            layout_mode = True
            strategy = "layout"           # prefer layout whenever a table exists
        else:
            layout_mode = (table_density >= 0.25 or has_big_table) and not tiny_doc
            strategy = "tiny" if tiny_doc else ("layout" if layout_mode else "section")


        params = {
            "target_tokens": self.target_tokens,
            "overlap_tokens": self.overlap_tokens,
            "layout_mode": layout_mode,
            "table_density": round(table_density, 3),
            "total_chars": total_chars,
        }
        pages = [b["page"] for b in blocks if b["page"] is not None]
        page_span = [min(pages), max(pages)] if pages else None
        plan_id = self.db.insert_chunk_plan(doc_id=doc_id, strategy=strategy,
                                            params=params, page_span=page_span, block_count=len(blocks))
        self.db.insert_event(self.tenant_id, stage="CHUNK_PLAN", status="OK",
                             details={"event":"CHUNK_PLAN_OK","plan_id":plan_id,"strategy":strategy,"params":params}, doc_id=doc_id)

        # idempotent: clear previous
        removed = self.db.delete_chunks_for_doc(doc_id)

        # Context Injection: Fetch doc meta to get filename
        dmeta = self.db.fetch_document_meta(doc_id)
        doc_filename = (dmeta.get("meta") or {}).get("filename")
        root_context = f"[Document: {doc_filename}]" if doc_filename else None

        # materialize
        if strategy == "tiny":
            rows, cov = self._make_tiny(doc_id, plan_id, blocks, root_context)
        elif strategy == "layout":
            rows, cov = self._make_layout(doc_id, plan_id, blocks, root_context)
        else:
            rows, cov = self._make_section(doc_id, plan_id, blocks, root_context)

        # Optimize: Parallelize contextual enrichment
        self._enrich_rows_parallel(rows)

        if len(rows) > self.max_chunks_per_doc:
            rows = rows[:self.max_chunks_per_doc]

        inserted = self.db.insert_chunks_bulk(rows)

        # checkers
        warnings: List[str] = []
        coverage_ratio = cov
        if coverage_ratio < 0.85:
            warnings.append(f"low_coverage:{coverage_ratio:.2f}")
        max_tokens = max([r["meta"]["tokens"] for r in rows], default=0)
        if max_tokens > 1400:
            warnings.append(f"chunk_too_large:{max_tokens}")
        tiny_chunks = [r for r in rows if r["meta"]["tokens"] < 60 and "table" not in r["meta"].get("types", [])]
        if rows and (len(tiny_chunks) / len(rows)) > 0.30:
            warnings.append("too_many_tiny_chunks")

        status = "OK" if not warnings else "WARN"
        self.db.insert_event(self.tenant_id, stage="CHUNKED", status=status, details={
            "event": f"CHUNKED_{status}", "plan_id": plan_id, "chunks": inserted,
            "coverage_ratio": round(coverage_ratio, 3), "max_tokens": max_tokens, "warnings": warnings
        }, doc_id=doc_id)

        self.log("info", "chunked",
                 doc_id=doc_id, plan_id=plan_id, strategy=strategy,
                 removed_prev=removed, inserted=inserted,
                 coverage_ratio=round(coverage_ratio,3),
                 latency_ms=int((time.time()-t0)*1000))
        return {"doc_id": doc_id, "plan_id": plan_id, "chunks": inserted, "warnings": warnings}

    # ---------- strategies ----------
    def _make_tiny(self, doc_id: str, plan_id: str, blocks: List[Dict[str, Any]], root_context: Optional[str] = None) -> Tuple[List[Dict[str, Any]], float]:
        nonempty = [b for b in blocks if (b["text"] or "").strip()]
        
        if not nonempty:
            return [], 0.0
        
        header_paths: List[str] = []
        for b in nonempty:
            meta = b.get("meta") or {}
            header_paths.extend(meta.get("headers") or [])
        
        # Deduplicate while preserving order
        context_headers = [h for i, h in enumerate(header_paths) if h and h not in header_paths[:i]]
        
        # Inject root context (filename) if provided
        if root_context:
            context_headers.insert(0, root_context)

        txt = _norm_text("\n\n".join(b["text"] for b in nonempty))
        if context_headers:
            prefix = " / ".join(context_headers)
            txt = _norm_text(f"{prefix}\n\n{txt}")
        
        # Contextual Chunking
        txt = self._enrich_chunk(txt, context_headers)

        span_start = min(b["span_start"] for b in nonempty)
        span_end   = max(b["span_end"] for b in nonempty)
        page_start = min(b["page"] for b in nonempty if b["page"] is not None) or 1
        page_end   = max(b["page"] for b in nonempty if b["page"] is not None) or page_start
        toks = _tok_count(txt)
        row = {
            "chunk_id": _id(), "plan_id": plan_id, "doc_id": doc_id,
            "span_start": span_start, "span_end": span_end,
            "page_start": page_start, "page_end": page_end,
            "text": txt,
            "meta": {"types": list(sorted(set(b["type"] for b in nonempty))),
                     "source_block_ids": [str(b["block_id"]) for b in nonempty],
                     "tokens": toks, "strategy":"tiny", "context_headers": context_headers},
            "checksum": _checksum(txt)
        }
        covered = sum(len(b["text"] or "") for b in nonempty)
        total = sum(len(b["text"] or "") for b in blocks)
        return [row], (covered / max(1,total))

    def _make_layout(self, doc_id: str, plan_id: str, blocks: List[Dict[str, Any]], root_context: Optional[str] = None) -> Tuple[List[Dict[str, Any]], float]:
        rows: List[Dict[str, Any]] = []
        total = sum(len(b["text"] or "") for b in blocks)
        covered = 0
        # one chunk per table; keep surrounding small paras out (simple MVP)
        for b in blocks:
            if b["type"] != "table": continue
            txt = _norm_text(b["text"] or "")
            toks = _tok_count(txt)
            
            headers_ctx = (b.get("meta") or {}).get("headers") or []
            if root_context:
                headers_ctx.insert(0, root_context)

            # Contextual Chunking
            txt = self._enrich_chunk(txt, headers_ctx)
            toks = _tok_count(txt) # Re-count

            meta = {
                "types":["table"], "source_block_ids": [str(b["block_id"])],
                "rows": (b.get("meta") or {}).get("rows"), "cols": (b.get("meta") or {}).get("cols"),
                "html": (b.get("meta") or {}).get("html"), # Preserving HTML for generation
                "tokens": toks, "strategy":"layout"
            }
            if headers_ctx:
                meta["context_headers"] = headers_ctx
                
            row = {
                "chunk_id": _id(), "plan_id": plan_id, "doc_id": doc_id,
                "span_start": b["span_start"], "span_end": b["span_end"],
                "page_start": b["page"] or 1, "page_end": b["page"] or 1,
                "text": txt,
                "meta": meta,
                "checksum": _checksum(txt)
            }
            covered += len(b["text"] or "")
            rows.append(row)
        # pack non-table narrative into chunks around target size
        narr = [b for b in blocks if b["type"] != "table" and (b["text"] or "").strip()]
        rows2, cov2 = self._pack_narrative(doc_id, plan_id, narr, include_headers=True, root_context=root_context)
        rows.extend(rows2)
        covered += int(cov2 * sum(len(b["text"] or "") for b in narr))
        return rows, (covered / max(1,total))

    def _make_section(self, doc_id: str, plan_id: str, blocks: List[Dict[str, Any]], root_context: Optional[str] = None) -> Tuple[List[Dict[str, Any]], float]:
        total = sum(len(b["text"] or "") for b in blocks)
        # tables as their own chunks even in section mode
        table_rows = [b for b in blocks if b["type"] == "table"]
        rows: List[Dict[str, Any]] = []
        covered_tables = 0
        for b in table_rows:
            txt = _norm_text(b["text"] or "")
            toks = _tok_count(txt)
            
            headers_ctx = (b.get("meta") or {}).get("headers") or []
            if root_context:
                headers_ctx.insert(0, root_context)
                
            # Contextual Chunking
            txt = self._enrich_chunk(txt, headers_ctx)
            toks = _tok_count(txt)

            meta = {
                "types":["table"], "source_block_ids": [str(b["block_id"])],
                "rows": (b.get("meta") or {}).get("rows"), "cols": (b.get("meta") or {}).get("cols"),
                "html": (b.get("meta") or {}).get("html"), # Preserving HTML for generation
                "tokens": toks, "strategy":"section"
            }
            if headers_ctx:
                meta["context_headers"] = headers_ctx
                
            rows.append({
                "chunk_id": _id(), "plan_id": plan_id, "doc_id": doc_id,
                "span_start": b["span_start"], "span_end": b["span_end"],
                "page_start": b["page"] or 1, "page_end": b["page"] or 1,
                "text": txt,
                "meta": meta,
                "checksum": _checksum(txt)
            })
            covered_tables += len(b["text"] or "")
        narr = [b for b in blocks if b["type"] != "table" and (b["text"] or "").strip()]
        rows2, cov2 = self._pack_narrative(doc_id, plan_id, narr, include_headers=True, root_context=root_context)
        rows.extend(rows2)
        covered_narr = int(cov2 * sum(len(b["text"] or "") for b in narr))
        covered_total = covered_tables + covered_narr
        return rows, (covered_total / max(1,total))

    # ---------- narrative packer ----------
    def _pack_narrative(self, doc_id: str, plan_id: str, blocks: List[Dict[str, Any]], include_headers: bool, root_context: Optional[str] = None) -> Tuple[List[Dict[str, Any]], float]:
        rows: List[Dict[str, Any]] = []
        if not blocks: return rows, 0.0

        # Adaptive target based on structure density
        target = self.target_tokens
        kinds = [b["type"] for b in blocks]
        if any(t in ("pre",) for t in kinds):
            target = max(300, int(target * 0.8))
        if kinds.count("list") >= max(2, int(0.2 * len(kinds))):
            target = max(350, int(target * 0.85))
        base_overlap = self.overlap_tokens

        # maintain header chain
        headers: Dict[int, str] = {}  # level -> text

        # Build segments (text, span, page, type, id)
        segs: List[Dict[str, Any]] = []
        for b in blocks:
            meta = (b.get("meta") or {})
            if b["type"] in ("h1","h2","h3","h4","h5","h6","header"):
                # normalize to header & level
                level = int(meta.get("level") or (int(b["type"][1]) if b["type"].startswith("h") else 2))
                headers[level] = _norm_text(b["text"] or "")
                # trim deeper levels when higher one resets
                for k in list(headers.keys()):
                    if k > level: headers.pop(k, None)
                continue
            # lists become bullet lines
            txt = b["text"] or ""
            if b["type"] == "list":
                lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
                txt = "\n".join(f"• {ln}" for ln in lines)
            txt = _norm_text(txt)
            if not txt: continue
            prefix = ""
            chain_from_meta = list(meta.get("headers") or [])
            header_chain = chain_from_meta
            if include_headers:
                if not header_chain and headers:
                    header_chain = [headers[k] for k in sorted(headers.keys())]
                
                # Inject root context if provided
                if root_context:
                    header_chain.insert(0, root_context)

                if header_chain:
                    prefix = (" / ".join(header_chain)).strip()
            full_txt = f"{prefix}\n\n{txt}" if prefix else txt
            segs.append({
                "text": full_txt,
                "span_start": b["span_start"],
                "span_end": b["span_end"],
                "page": b["page"] or 1,
                "type": b["type"],
                "block_id": b["block_id"],
                "tokens": _tok_count(full_txt),
                "headers": header_chain,
            })

        if not segs:
            return rows, 0.0

        # Greedy pack with overlap. If an individual segment is very long,
        # split it recursively along sentence/line separators instead of truncating.
        i = 0
        covered = 0
        while i < len(segs):
            if segs[i]["tokens"] > target:
                t = segs[i]
                long_rows, long_cov = self._split_long_segment(doc_id, plan_id, t, target, base_overlap)
                rows.extend(long_rows)
                covered += int(long_cov * len(t["text"]))
                i += 1
                continue

            toks_sum = 0
            text_parts: List[str] = []
            span_s = segs[i]["span_start"]
            span_e = segs[i]["span_end"]
            page_s = segs[i]["page"]
            page_e = segs[i]["page"]
            block_ids: List[str] = []
            types: List[str] = []
            header_paths: List[List[str]] = []

            j = i
            while j < len(segs) and toks_sum + segs[j]["tokens"] <= target:
                s = segs[j]
                text_parts.append(s["text"])
                toks_sum += s["tokens"]
                span_e = max(span_e, s["span_end"])
                page_e = max(page_e, s["page"])
                block_ids.append(str(s["block_id"]))
                types.append(s["type"])
                header_paths.append(s.get("headers") or [])
                j += 1

            chunk_text = _norm_text("\n\n".join(text_parts))
            
            context_headers: List[str] = []
            for path in header_paths:
                for item in path:
                    if item and item not in context_headers:
                        context_headers.append(item)
            
            # Contextual Chunking
            chunk_text = self._enrich_chunk(chunk_text, context_headers)
            toks = _tok_count(chunk_text)

            rows.append({
                "chunk_id": _id(), "plan_id": plan_id, "doc_id": doc_id,
                "span_start": span_s, "span_end": span_e,
                "page_start": page_s, "page_end": page_e,
                "text": chunk_text,
                "meta": {"types": list(sorted(set(types))), "source_block_ids": block_ids,
                         "tokens": toks, "strategy":"pack", "context_headers": context_headers},
                "checksum": _checksum(chunk_text)
            })
            covered += sum(len(segs[k]["text"]) for k in range(i, j))

            if j >= len(segs):
                break
            # backtrack for overlap
            # Adaptive overlap: smaller for lists/headers, larger for dense paragraphs
            overlap = base_overlap
            if segs[i]["type"] in ("list", "header"):
                overlap = max(0, int(base_overlap * 0.5))
            elif segs[i]["tokens"] > int(target * 0.7):
                overlap = int(base_overlap * 1.5)
            back = overlap
            k = j - 1
            while k > i and back > 0:
                back -= segs[k]["tokens"]
                k -= 1
            i = max(i + 1, k + 1)

        # Optimization: Merge tiny orphan chunks (< 50 tokens) into previous chunk
        # This prevents "Page 2" or "Introduction" from becoming low-value independent vectors.
        if len(rows) > 1:
            merged_rows = []
            prev = rows[0]
            for curr in rows[1:]:
                # If current is tiny and compatible with previous (same section/headers roughly?)
                # For now, just simplistic merge if tiny
                if curr["meta"]["tokens"] < 50 and prev["meta"]["tokens"] < (target * 1.2):
                    # Merge curr into prev
                    prev["text"] += "\n\n" + curr["text"]
                    prev["span_end"] = curr["span_end"]
                    prev["page_end"] = max(prev["page_end"], curr["page_end"])
                    prev["meta"]["tokens"] += curr["meta"]["tokens"]
                    prev["meta"]["source_block_ids"].extend(curr["meta"]["source_block_ids"])
                    prev["checksum"] = _checksum(prev["text"])
                else:
                    merged_rows.append(prev)
                    prev = curr
            merged_rows.append(prev)
            rows = merged_rows

        total_chars = sum(len(s["text"]) for s in segs)
        coverage = (covered / max(1, total_chars))
        return rows, coverage

    # ---------- helpers ----------
    def _split_long_segment(self, doc_id: str, plan_id: str, seg: Dict[str, Any], target: int, overlap: int) -> Tuple[List[Dict[str, Any]], float]:
        """Split a single oversized segment using a prioritized list of separators,
        then pack greedily to target with overlap, preserving block context.
        """
        text = seg["text"]
        seps = ["\n\n", "\n", ". ", "? ", "! ", "; ", ", ", " "]

        # break into atomic units
        parts = [text]
        for sep in seps:
            if len(parts) > 1 or _tok_count(text) <= target:
                break
            parts = self._split_keep_sep(text, sep)
        # if still one giant part, just hard-slice into ~target token windows
        if len(parts) == 1 and _tok_count(parts[0]) > target:
            parts = self._hard_slice(parts[0], target)

        # pack parts into chunks
        rows: List[Dict[str, Any]] = []
        tokens = [_tok_count(p) for p in parts]
        i = 0
        covered = 0
        while i < len(parts):
            toks_sum = 0
            buf: List[str] = []
            j = i
            while j < len(parts) and toks_sum + tokens[j] <= target:
                buf.append(parts[j])
                toks_sum += tokens[j]
                j += 1
            chunk_text = _norm_text("".join(buf))
            
            # Contextual Chunking
            context_headers = list(seg.get("headers") or [])
            chunk_text = self._enrich_chunk(chunk_text, context_headers)
            toks = _tok_count(chunk_text)

            rows.append({
                "chunk_id": _id(), "plan_id": plan_id, "doc_id": doc_id,
                "span_start": seg["span_start"], "span_end": seg["span_end"],
                "page_start": seg["page"], "page_end": seg["page"],
                "text": chunk_text,
                "meta": {"types":[seg["type"]], "source_block_ids":[seg["block_id"]],
                         "tokens": toks, "strategy":"pack:split", "context_headers": context_headers},
                "checksum": _checksum(chunk_text)
            })
            covered += sum(len(parts[k]) for k in range(i, j))
            if j >= len(parts):
                break
            # overlap by tokens across parts
            back = 0 if (str(seg.get("type")) == "table") else overlap
            k = j - 1
            while k > i and back > 0:
                back -= tokens[k]
                k -= 1
            i = max(i + 1, k + 1)

        coverage = covered / max(1, len(text))
        return rows, coverage

    @staticmethod
    def _split_keep_sep(text: str, sep: str) -> List[str]:
        parts: List[str] = []
        if not text:
            return parts
        if sep.strip() == "":
            # space separator: default split keeps spaces in join later
            return text.split(sep)
        chunks = text.split(sep)
        for idx, c in enumerate(chunks):
            if idx == 0:
                parts.append(c)
            else:
                parts.append(sep + c)
        return [p for p in parts if p]

    @staticmethod
    def _hard_slice(text: str, target_tokens: int) -> List[str]:
        # approximate: 4 chars per token when tokenizer is absent
        approx = target_tokens * 4
        out: List[str] = []
        i = 0
        n = len(text)
        while i < n:
            out.append(text[i:i+approx])
            i += approx
        return out
