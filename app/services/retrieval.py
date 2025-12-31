# app/services/retrieval.py
from __future__ import annotations
import os, time, math, re
from typing import Any, Dict, List, Optional
from services.llm.providers import OpenAIProvider

try:
    from openai import OpenAI  # type: ignore
    _HAS_OPENAI = True
except Exception:
    OpenAI = None  # type: ignore
    _HAS_OPENAI = False
try:
    from sentence_transformers import SentenceTransformer  # type: ignore
    _HAS_ST = True
except Exception:
    SentenceTransformer = None  # type: ignore
    _HAS_ST = False
from qdrant_client.http.models import Filter, FieldCondition, MatchAny, MatchValue
try:
    from sentence_transformers import CrossEncoder  # type: ignore
    _HAS_CE = True
except Exception:
    CrossEncoder = None  # type: ignore
    _HAS_CE = False

from infra.db import DBClient
from datetime import datetime, timedelta
from core.config import settings
from core.interfaces import VectorStore, SearchFilter
from circuitbreaker import circuit

_NUMERIC_HINT = re.compile(r"\b(total|amount|due|sum|balance|qty|quantity|price|fee|fees|tax|subtotal|grand\s*total)\b", re.I)

def _safe_int(v, default: int = 1) -> int:
    try:
        if isinstance(v, int):
            return v
        if isinstance(v, float) and v.is_integer():
            return int(v)
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())
    except Exception:
        pass
    return default

def _norm(v: float) -> float:
    try:
        if v is None: return 0.0
        return 1.0 / (1.0 + math.exp(-4.0 * (float(v) - 0.5)))
    except Exception:
        return 0.0

def _enrich_kw(q: str) -> str:
    s = (q or "").strip()
    if not s: return s
    s_l = s.lower()
    extra: List[str] = []
    if any(k in s_l for k in ["invoice", "line item", "line items"]):
        extra += ["line items", "total", "amount", "due date"]
    if any(k in s_l for k in ["expense", "fees", "receipt"]):
        extra += ["category", "table", "amount", "total"]
    if any(k in s_l for k in ["contract", "clause", "term", "obligation"]):
        extra += ["section", "clause", "obligation", "termination"]
    if extra:
        # de-dup, order-preserving
        s = s + " " + " ".join(dict.fromkeys(extra))
    return s

class RetrievalService:
    def __init__(self, db: DBClient, qdr: VectorStore, *, tenant_id: str, logger):
        self.db = db
        self.qdr = qdr
        self.tenant_id = tenant_id
        self.log = logger
        self.client = None
        self.local_encoder = None
        self.local_dim: Optional[int] = None
        if _HAS_OPENAI and settings.openai_api_key:
            try:
                self.client = OpenAI(api_key=settings.openai_api_key)
            except Exception as e:
                self.client = None
                self.log("warn", "openai-init-fail", reason=str(e))
        if (not self.client) and _HAS_ST:
            try:
                local_model = settings.embed_local_model
                self.local_encoder = SentenceTransformer(local_model)
                vec = self.local_encoder.encode(["test"], normalize_embeddings=True)[0]
                self.local_dim = len(vec)
                self.log("info", "retr-local-ready", model=local_model, dim=self.local_dim)
            except Exception as e:
                self.local_encoder = None
                self.local_dim = None
                self.log("warn", "retr-local-init-fail", reason=str(e))
        # Optional reranker
        self.reranker = None
        if settings.rerank_enabled and _HAS_CE:
            try:
                self.reranker = CrossEncoder(settings.rerank_model)
                self.log("info", "reranker-ready", model=settings.rerank_model)
            except Exception as e:
                self.reranker = None
                self.log("warn", "reranker-init-fail", reason=str(e))
        
        # HyDE Provider
        self.hyde_provider = None
        if settings.hyde_enabled:
            key = settings.gemini_api_key or settings.openai_api_key
            if key:
                try:
                    self.hyde_provider = OpenAIProvider(
                        api_key=key,
                        base_url=settings.gen_base_url,
                        model=settings.gen_model
                    )
                    self.log("info", "hyde-ready", model=settings.gen_model)
                except Exception as e:
                    self.log("warn", "hyde-init-fail", reason=str(e))
        
        self._chunk_block_cache: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _token_set(text: str) -> set:
        import re as _re
        return set([t for t in _re.split(r"[^A-Za-z0-9]+", (text or "").lower()) if t])

    def _mmr(self, candidates: List[Dict[str, Any]], k: int, lam: float = 0.65) -> List[Dict[str, Any]]:
        # lam defaults to 0.65 locally if not passed, but caller usually passes settings.mmr_lambda
        if not candidates:
            return []
        toks = [self._token_set(c.get("text") or "") for c in candidates]
        scores = [float(c.get("score", 0.0) or 0.0) for c in candidates]
        selected: List[int] = []
        used = set()
        if not scores:
            return candidates[:k]
        best = max(range(len(candidates)), key=lambda i: scores[i])
        selected.append(best); used.add(best)
        while len(selected) < min(k, len(candidates)):
            best_i = None
            best_val = -1e9
            for i in range(len(candidates)):
                if i in used:
                    continue
                rel = scores[i]
                div = 0.0
                for j in selected:
                    a, b = toks[i], toks[j]
                    if not a or not b:
                        sim = 0.0
                    else:
                        inter = len(a.intersection(b)); uni = len(a.union(b)) or 1
                        sim = inter / uni
                    if sim > div:
                        div = sim
                val = lam * rel - (1.0 - lam) * div
                if val > best_val:
                    best_val = val; best_i = i
            if best_i is None:
                break
            selected.append(best_i); used.add(best_i)
        return [candidates[i] for i in selected]

    # ---------- vector ----------
    def _embed_query(self, text: str) -> List[float]:
        # prefer OpenAI; fallback to local encoder if available and dims match; else raise
        if self.client:
            r = self.client.embeddings.create(model=settings.embed_model, input=text or "")
            return r.data[0].embedding
        if self.local_encoder and self.local_dim == self.qdr.dim:
            v = self.local_encoder.encode([text or ""], normalize_embeddings=True)[0]
            return list(map(float, v))
        raise RuntimeError("embed_query_unavailable")

    def _get_chunk_cache(self, doc_id: str) -> Dict[str, Any]:
        cache = self._chunk_block_cache.get(doc_id)
        if cache:
            return cache
        try:
            chunks = self.db.fetch_chunks_for_doc(doc_id)
        except Exception:
            chunks = []
        chunk_map: Dict[str, Dict[str, Any]] = {}
        block_map: Dict[str, set[str]] = {}
        for ch in chunks:
            cid = str(ch.get("chunk_id"))
            chunk_map[cid] = ch
            meta = ch.get("meta") or {}
            for bid in meta.get("source_block_ids", []) or []:
                block_map.setdefault(str(bid), set()).add(cid)
        cache = {"chunks": chunk_map, "block_to_chunk": block_map}
        self._chunk_block_cache[doc_id] = cache
        return cache

    def _graph_context_boost(self, hit: Dict[str, Any], query_terms: set[str], numeric_query: bool) -> float:
        boost = 0.0
        headers = hit.get("context_headers") or []
        if headers:
            header_terms: set[str] = set()
            for h in headers:
                header_terms.update(self._token_set(h))
            overlap = header_terms.intersection(query_terms)
            if overlap:
                boost += 0.05 * len(overlap)
        types = hit.get("types") or []
        if numeric_query and "table" in types:
            boost += 0.03
        return boost

    def _graph_expand_hits(
        self,
        base_hits: List[Dict[str, Any]],
        query_terms: set[str],
        numeric_query: bool,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        expanded: List[Dict[str, Any]] = []
        seen = {(h.get("doc_id"), h.get("chunk_id")) for h in base_hits}

        for hit in base_hits[:limit]:
            block_ids = hit.get("source_block_ids") or []
            doc_id = hit.get("doc_id")
            if not block_ids or not doc_id:
                continue
            try:
                neighbors = self.db.fetch_graph_neighbors(doc_id, block_ids)
            except Exception as exc:
                self.log("warn", "graph-neighbor-fail", doc_id=doc_id, error=str(exc))
                continue
            if not neighbors:
                continue
            cache = self._get_chunk_cache(doc_id)
            block_map = cache["block_to_chunk"]
            chunk_map = cache["chunks"]

            for row in neighbors:
                child_block = row.get("child_block_id")
                if not child_block:
                    continue
                candidate_chunks = block_map.get(str(child_block)) or []
                for cid in candidate_chunks:
                    key = (doc_id, cid)
                    if key in seen:
                        continue
                    chunk = chunk_map.get(cid)
                    if not chunk:
                        continue
                    meta = chunk.get("meta") or {}
                    text = chunk.get("text") or ""
                    context_headers = meta.get("context_headers") or meta.get("headers") or []
                    base_score = float(hit.get("score_vec", 0.0) or 0.0) * 0.6
                    boost = self._graph_context_boost({"context_headers": context_headers, "types": meta.get("types", [])}, query_terms, numeric_query)
                    expanded.append(
                        {
                            "chunk_id": cid,
                            "doc_id": doc_id,
                            "plan_id": str(chunk.get("plan_id")),
                            "page_start": chunk.get("page_start") or meta.get("page_start") or hit.get("page_start"),
                            "page_end": chunk.get("page_end") or meta.get("page_end") or hit.get("page_end"),
                            "span_start": chunk.get("span_start") or meta.get("span_start"),
                            "span_end": chunk.get("span_end") or meta.get("span_end"),
                            "text": text,
                            "types": meta.get("types", []),
                            "source_block_ids": meta.get("source_block_ids", []),
                            "context_headers": context_headers,
                            "uri": None,
                            "mime": None,
                            "score_vec": base_score,
                            "score_graph": boost,
                            "score": round(base_score + boost, 6),
                        }
                    )
                    seen.add(key)

        return expanded

    def _generate_hyde_doc(self, q: str) -> str:
        if not self.hyde_provider:
            return q
        prompt = (
            "You are an expert document writer. Write a detailed, hypothetical passage "
            "that answers the following question directly. Include plausible details, "
            "entities, or specific clauses that would appear in a real business document.\n"
            "Do NOT explain; just write the document text.\n\n"
            f"Question: {q}\n\n"
            "Hypothetical Document Passage:"
        )
        try:
            # fast generation
            return self.hyde_provider.generate([{"role": "user", "content": prompt}], max_tokens=256, temperature=0.7)
        except Exception as e:
            self.log("warn", "hyde-gen-fail", reason=str(e))
            return q

    @circuit(failure_threshold=5, recovery_timeout=30)
    def _execute_vector_search(self, query_vector, limit, filter):
        return self.qdr.search(query_vector=query_vector, limit=limit, filter=filter)

    def vector_search(self, *, q: str, k: int = 8,
                      doc_ids: Optional[List[str]] = None,
                      types_any: Optional[List[str]] = None,
                      mime_any: Optional[List[str]] = None) -> Dict[str, Any]:
        t0 = time.time()
        
        # HyDE transformation
        search_text = q
        if self.hyde_provider and settings.hyde_enabled:
            hypo = self._generate_hyde_doc(q)
            if hypo and len(hypo) > len(q):
                search_text = hypo
                # self.log("info", "hyde-generated", q=q, hypo=hypo[:50]+"...")

        try:
            qv = self._embed_query(search_text)
        except Exception as e:
            self.db.insert_event(self.tenant_id, stage="RETRIEVE", status="WARN",
                                 details={"event":"VECTOR_QUERY_SKIPPED","reason":str(e)})
            return {"results": [], "mode": "vector", "timing_ms": {"vector": 0, "total": 0}}
        
        flt = SearchFilter(tenant_id=self.tenant_id, doc_ids=doc_ids, mime_any=mime_any)
        
        try:
            # Use Circuit Breaker wrapper
            scored = self._execute_vector_search(query_vector=qv, limit=max(k, settings.vector_topn), filter=flt)
        except Exception as e:
            # Catch CircuitBreakerError or connection errors
            self.db.insert_event(self.tenant_id, stage="RETRIEVE", status="WARN",
                                 details={"event":"VECTOR_QUERY_FAIL","reason":str(e)})
            self.log("error", "vector-search-fail", error=str(e))
            return {"results": [], "mode": "vector", "timing_ms": {"vector": 0, "total": 0}}

        vec_hits: List[Dict[str, Any]] = []
        for sp in scored:
            p = sp.payload or {}
            # NOTE: Qdrant .score for cosine is already a similarity (higher is better) → do NOT invert
            hit = {
                "chunk_id": str(p.get("chunk_id")),
                "doc_id":   str(p.get("doc_id")),
                "plan_id":  str(p.get("plan_id")),
                "page_start": _safe_int(p.get("page_start"), 1),
                "page_end":   _safe_int(p.get("page_end"), 1),
                "span_start": _safe_int(p.get("span_start"), 0),
                "span_end":   _safe_int(p.get("span_end"), 0),
                "types": p.get("types", []),
                "source_block_ids": p.get("source_block_ids", []),
                "context_headers": p.get("context_headers", []),
                "uri": p.get("uri"),
                "mime": p.get("mime"),
                "score_vec": float(getattr(sp, "score", 0.0) or 0.0),
                "text": p.get("text"),  # may be missing; we backfill below if needed
            }
            vec_hits.append(hit)

        # post-filter types (safer than Qdrant array matches)
        if types_any:
            vec_hits = [h for h in vec_hits if any(t in (h.get("types") or []) for t in types_any)]

        # Fetch dates for recency boosting & backfill text if needed
        all_ids = list(set(h["chunk_id"] for h in vec_hits))
        chunk_meta_map = {}
        if all_ids:
            self.db.connect()
            with self.db.conn.cursor() as cur:
                cur.execute("""
                SELECT
                    c.chunk_id::text AS chunk_id,
                    c.text           AS text,
                    d.uri            AS uri,
                    d.mime           AS mime,
                    n.canonical_uri  AS canonical_uri,
                    d.collected_at   AS collected_at
                FROM chunks c
                JOIN documents d ON d.doc_id = c.doc_id
                LEFT JOIN normalizations n ON n.doc_id = c.doc_id
                WHERE c.chunk_id::text = ANY(%s) AND d.state != 'DELETED'
                """, (all_ids,))
                rows = cur.fetchall()
            for r in rows:
                if isinstance(r, dict):
                    cid = str(r.get("chunk_id"))
                    if cid: chunk_meta_map[cid] = r
                else:
                    cid, text, uri, mime, canonical_uri, collected_at = r
                    chunk_meta_map[str(cid)] = {
                        "text": text, "uri": uri, "mime": mime, 
                        "canonical_uri": canonical_uri, "collected_at": collected_at
                    }

        # Apply backfill and Recency Boost
        now = datetime.utcnow()
        for h in vec_hits:
            cid = h["chunk_id"]
            if cid in chunk_meta_map:
                m = chunk_meta_map[cid]
                if not h.get("text"):
                    h["text"] = m.get("text")
                if not h.get("uri"):
                    h["uri"] = m.get("uri")
                if not h.get("mime"):
                    h["mime"] = m.get("mime")
                
                # Recency Boost
                # < 24h: +0.05, < 7d: +0.02
                cat = m.get("collected_at")
                if cat:
                    if isinstance(cat, str):
                        try: cat = datetime.fromisoformat(cat)
                        except: cat = None
                    if cat:
                        # naive UTC handling
                        if cat.tzinfo: cat = cat.replace(tzinfo=None)
                        delta = (now - cat).total_seconds()
                        if delta < 86400:
                            h["score_vec"] += 0.05
                        elif delta < 7 * 86400:
                            h["score_vec"] += 0.02

        query_terms = self._token_set(q)
        numeric_query = bool(_NUMERIC_HINT.search(q or ""))

        for h in vec_hits:
            base = float(h.get("score_vec", 0.0) or 0.0)
            graph_boost = self._graph_context_boost(h, query_terms, numeric_query)
            h["score_graph"] = round(graph_boost, 6)
            h["score"] = round(base + graph_boost, 6)

        neighbor_hits = self._graph_expand_hits(vec_hits[: min(len(vec_hits), k)], query_terms, numeric_query)
        graph_added = len(neighbor_hits)
        if neighbor_hits:
            vec_hits.extend(neighbor_hits)

        vec_hits.sort(key=lambda x: x.get("score", 0.0), reverse=True)
        # We don't slice to k here yet; let the parent search() handle diversity then slice
        
        took = int((time.time() - t0) * 1000)
        self.db.insert_event(
            self.tenant_id,
            stage="RETRIEVE",
            status="OK",
            details={
                "event": "RETRIEVE_OK",
                "mode": "vector",
                "vector_hits": len(vec_hits),
                "keyword_hits": 0,
                "graph_hits": graph_added,
                "merged_hits": len(vec_hits),
                "took_ms": {"vector": took, "total": took},
            },
        )
        self.log("info", "retrieve-vector", hits=len(vec_hits), graph_hits=graph_added, took_ms=took, q=q)
        return {"results": vec_hits, "mode": "vector", "timing_ms": {"vector": took, "total": took}}

    # ---------- keyword ----------
    def keyword_search(self, *, q: str, k: int = 8,
                       doc_ids: Optional[List[str]] = None,
                       types_any: Optional[List[str]] = None) -> Dict[str, Any]:
        t0 = time.time()
        enriched = _enrich_kw(q)
        rows = self.db.keyword_search_chunks(q=enriched, limit=max(k, settings.keyword_topn),
                                             doc_ids=doc_ids or None,
                                             types_any=types_any or None,
                                             tenant_id=self.tenant_id)
        if types_any:
            rows = [r for r in rows if any(t in (r.get("meta", {}).get("types") or []) for t in types_any)]

        max_rank = max([r["rank"] for r in rows], default=1.0)
        out: List[Dict[str, Any]] = []
        for r in rows[:k]:
            out.append({
                "chunk_id": r["chunk_id"],
                "doc_id": r["doc_id"],
                "plan_id": r["plan_id"],
                "page_start": r["page_start"],
                "page_end": r["page_end"],
                "span_start": r["span_start"],
                "span_end": r["span_end"],
                "text": r["text"],
                "types": (r["meta"] or {}).get("types", []),
                "source_block_ids": (r["meta"] or {}).get("source_block_ids", []),
                "uri": r["uri"],
                "mime": r["mime"],
                "score": round((r["rank"] / max_rank) if max_rank else 0.0, 6),
            })
        took = int((time.time() - t0) * 1000)
        self.db.insert_event(self.tenant_id, stage="RETRIEVE", status="OK",
                             details={"event":"RETRIEVE_OK","mode":"keyword","vector_hits":0,
                                      "keyword_hits":len(out),"merged_hits":len(out),"took_ms":{"total":took}})
        self.log("info", "retrieve-keyword", hits=len(out), took_ms=took, q=q)
        return {"results": out, "mode": "keyword", "timing_ms": {"keyword": took, "total": took}}

    # ---------- hybrid + safety ----------
    def _parse_date_range(self, q: str) -> Optional[tuple[str, str]]:
        """Parse simple natural language ranges like 'last quarter', 'Q4 2024', 'last month', 'last year'.
        Returns (start_iso, end_iso) inclusive, or None if not detected.
        """
        s = (q or "").lower()
        today = datetime.utcnow().date()
        # Last X days/weeks/months
        import re
        m = re.search(r"last\s+(\d{1,3})\s*(day|days|week|weeks|month|months)", s)
        if m:
            n = int(m.group(1)); unit = m.group(2)
            if 'day' in unit:
                start = today - timedelta(days=n)
            elif 'week' in unit:
                start = today - timedelta(weeks=n)
            else:
                # months approx 30 days
                start = today - timedelta(days=30*n)
            return start.isoformat(), today.isoformat()
        # Last month
        if "last month" in s:
            first_this = today.replace(day=1)
            last_month_end = first_this - timedelta(days=1)
            last_month_start = last_month_end.replace(day=1)
            return last_month_start.isoformat(), last_month_end.isoformat()
        # Last year
        if "last year" in s:
            y = today.year - 1
            return f"{y}-01-01", f"{y}-12-31"
        # This quarter / last quarter
        def quarter_bounds(year: int, qn: int):
            if qn == 1: return f"{year}-01-01", f"{year}-03-31"
            if qn == 2: return f"{year}-04-01", f"{year}-06-30"
            if qn == 3: return f"{year}-07-01", f"{year}-09-30"
            return f"{year}-10-01", f"{year}-12-31"
        qn = ((today.month - 1) // 3) + 1
        if "this quarter" in s:
            return quarter_bounds(today.year, qn)
        if "last quarter" in s:
            y, ql = (today.year, qn - 1)
            if ql == 0:
                y -= 1; ql = 4
            return quarter_bounds(y, ql)
        # QN YYYY
        m = re.search(r"\bq([1-4])\s*(20\d{2})\b", s)
        if m:
            qnum = int(m.group(1)); year = int(m.group(2))
            return quarter_bounds(year, qnum)
        # FYQ like Q1 FY2025 or Q1 FY25 (assume FY starts April)
        m = re.search(r"\bq([1-4])\s*fy\s*(20?\d{2})\b", s)
        if m:
            qnum = int(m.group(1)); y = m.group(2)
            year = int(y if len(y)==4 else ("20"+y))
            # Financial year starting April: Q1=Apr-Jun
            fyq = {1:(4,6), 2:(7,9), 3:(10,12), 4:(1,3)}
            m1,m2 = fyq[qnum]
            yy1 = year if qnum!=4 else (year+1)
            yy2 = year if qnum!=4 else (year+1)
            start = datetime(yy1, m1, 1).date()
            if m2==12:
                end = datetime(yy2, 12, 31).date()
            else:
                end = (datetime(yy2, m2+1, 1).date() - timedelta(days=1))
            return start.isoformat(), end.isoformat()
        # Month YYYY like 'January 2024'
        months = {m.lower(): i for i, m in enumerate(["January","February","March","April","May","June","July","August","September","October","November","December"], start=1)}
        for name, mnum in months.items():
            if name in s:
                m2 = re.search(name + r"\s+(20\d{2})", s)
                if m2:
                    y = int(m2.group(1))
                    start = datetime(y, mnum, 1).date()
                    if mnum == 12:
                        end = datetime(y, 12, 31).date()
                    else:
                        end = (datetime(y, mnum+1, 1).date() - timedelta(days=1))
                    return start.isoformat(), end.isoformat()
        return None

    def _safety_net(self, *, k: int, doc_ids: Optional[List[str]], types_any: Optional[List[str]], prefer_tables: bool) -> List[Dict[str, Any]]:
        self.db.connect()
        params: List[Any] = []
        where = " WHERE TRUE AND d.state != 'DELETED' "
        if doc_ids:
            params.append(doc_ids)
            where += " AND c.doc_id::text = ANY(%s) "
        if types_any:
            params.append(types_any)
            where += " AND EXISTS (SELECT 1 FROM jsonb_array_elements_text(c.meta->'types') t WHERE t = ANY(%s)) "

        # try table chunks first if numeric
        if prefer_tables:
            sql = f"""
            SELECT c.chunk_id::text, c.doc_id::text, d.uri, d.mime, c.text, c.meta,
                   COALESCE((c.meta->>'page_start')::int,(c.meta->>'page')::int,1) AS page_start,
                   COALESCE((c.meta->>'page_end')::int,(c.meta->>'page')::int,(c.meta->>'page_start')::int,1) AS page_end
            FROM chunks c
            JOIN documents d ON d.doc_id = c.doc_id
            {where}
              AND EXISTS (SELECT 1 FROM jsonb_array_elements_text(c.meta->'types') t WHERE t='table')
            ORDER BY char_length(coalesce(c.text,'')) DESC NULLS LAST
            LIMIT %s
            """
            with self.db.conn.cursor() as cur:
                cur.execute(sql, params + [k])
                rows = cur.fetchall()
            if rows:
                out = []
                for r in rows:
                    out.append({
                        "chunk_id": r[0], "doc_id": r[1], "uri": r[2], "mime": r[3],
                        "text": r[4], "meta": r[5], "page_start": r[6], "page_end": r[7],
                        "score": 0.01
                    })
                return out

        # else longest chunks
        sql = f"""
        SELECT c.chunk_id::text, c.doc_id::text, d.uri, d.mime, c.text, c.meta,
               COALESCE((c.meta->>'page_start')::int,(c.meta->>'page')::int,1) AS page_start,
               COALESCE((c.meta->>'page_end')::int,(c.meta->>'page')::int,(c.meta->>'page_start')::int,1) AS page_end
        FROM chunks c
        JOIN documents d ON d.doc_id = c.doc_id
        {where}
        ORDER BY char_length(coalesce(c.text,'')) DESC NULLS LAST
        LIMIT %s
        """
        with self.db.conn.cursor() as cur:
            cur.execute(sql, params + [k])
            rows = cur.fetchall()
        out = []
        for r in rows:
            out.append({
                "chunk_id": r[0], "doc_id": r[1], "uri": r[2], "mime": r[3],
                "text": r[4], "meta": r[5], "page_start": r[6], "page_end": r[7],
                "score": 0.005
            })
        return out

    def _expand_context_window(self, hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not hits:
            return hits
        
        # Optimization: Don't expand if we have too many hits (latency trade-off)
        # Limit to top 5 for windowing to save DB calls
        targets = hits[:5]
        rest = hits[5:]
        
        t0 = time.time()
        expanded_count = 0
        
        for h in targets:
            did = h.get("doc_id")
            span_s = h.get("span_start")
            if not did or span_s is None:
                continue
            
            # Fetch Prev
            prev = self.db.fetch_neighbor_chunks(did, span_s, direction="prev")
            # Fetch Next
            next_c = self.db.fetch_neighbor_chunks(did, span_s, direction="next")
            
            # Stitch
            parts = []
            if prev:
                parts.append(prev["text"])
                h["span_start"] = prev["span_start"] # Expand span
                
            parts.append(h["text"])
            
            if next_c:
                parts.append(next_c["text"])
                h["span_end"] = next_c["span_end"] # Expand span
            
            if prev or next_c:
                h["text"] = "\n\n".join(parts)
                h["window_expanded"] = True
                expanded_count += 1
                
        self.log("info", "window-expansion", count=expanded_count, took_ms=int((time.time()-t0)*1000))
        return targets + rest

    def search(self, *, q: str, k: int = 8, hybrid: bool = True,
               filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        filters = filters or {}
        doc_ids = list(filters.get("doc_ids") or [])
        types_any = list(filters.get("types") or [])
        numeric = bool(_NUMERIC_HINT.search(q or ""))

        # query improvisation: infer date range → constrain to invoice docs in that range
        try:
            dr = self._parse_date_range(q)
        except Exception:
            dr = None
        if dr:
            start, end = dr
            try:
                inv_doc_ids = self.db.find_invoice_doc_ids_between(start=start, end=end)
            except Exception:
                inv_doc_ids = []
            if inv_doc_ids:
                if doc_ids:
                    # intersect existing doc filters
                    s = set(doc_ids).intersection(set(inv_doc_ids))
                    doc_ids = list(s)
                else:
                    doc_ids = inv_doc_ids
            # when numeric, bias toward table chunks
            if numeric and "table" not in types_any:
                types_any.append("table")

        # Invoice ID pattern (e.g., INV-2024-001) to constrain docs
        try:
            m = re.search(r"\b([A-Z]{2,6}[-_/]?[0-9]{2,4}[-_/]?[0-9]{1,6})\b", (q or ""))
            if m:
                tok = m.group(1)
                try:
                    ids = self.db.find_invoice_doc_ids_by_number_like(tok)
                except Exception:
                    ids = []
                if ids:
                    doc_ids = list(set(doc_ids).intersection(set(ids)) if doc_ids else ids)
        except Exception:
            pass

        # Optional explicit date filter from UI (e.g., last 90d)
        try:
            days = int(filters.get("date_last_days")) if isinstance(filters.get("date_last_days"), int) else None
        except Exception:
            days = None
        if days and days > 0:
            try:
                today = datetime.utcnow().date()
                start = (today - timedelta(days=days)).isoformat(); end = today.isoformat()
                inv_doc_ids = self.db.find_invoice_doc_ids_between(start=start, end=end)
                if inv_doc_ids:
                    doc_ids = list(set(doc_ids).intersection(set(inv_doc_ids)) if doc_ids else inv_doc_ids)
            except Exception:
                pass

        if not hybrid:
            res = self.vector_search(q=q, k=k, doc_ids=doc_ids, types_any=types_any,
                                      mime_any=(filters.get("mime_any") if isinstance(filters.get("mime_any"), list) else None))
            # Expand Window
            res["results"] = self._expand_context_window(res["results"])
            return res

        # Gather legs
        v = self.vector_search(q=q, k=max(k, 32),  doc_ids=doc_ids, types_any=types_any,
                               mime_any=(filters.get("mime_any") if isinstance(filters.get("mime_any"), list) else None))["results"]

        kw_rows = self.db.keyword_search_chunks(
            q=_enrich_kw(q), limit=max(k, settings.keyword_topn), doc_ids=doc_ids or None,
            types_any=types_any or None, tenant_id=self.tenant_id,
            mime_any=(filters.get("mime_any") if isinstance(filters.get("mime_any"), list) else None),
            uri_like=(filters.get("uri_like") if isinstance(filters.get("uri_like"), str) else None),
            filename_like=(filters.get("filename_like") if isinstance(filters.get("filename_like"), str) else None),
            vendor_like=(filters.get("vendor_like") if isinstance(filters.get("vendor_like"), str) else None),
        )
        kw: List[Dict[str, Any]] = []
        max_rank = max([r.get("rank", 0.0) for r in kw_rows], default=1.0)
        for r in kw_rows:
            kw.append({
                "chunk_id": r["chunk_id"], "doc_id": r["doc_id"], "plan_id": r["plan_id"],
                "page_start": r["page_start"], "page_end": r["page_end"], "span_start": r["span_start"], "span_end": r["span_end"],
                "text": r["text"], "types": (r.get("meta") or {}).get("types", []),
                "source_block_ids": (r.get("meta") or {}).get("source_block_ids", []),
                "uri": r.get("uri"), "mime": r.get("mime"), "canonical_uri": r.get("canonical_uri"),
                "score_kw": (r.get("rank", 0.0) / max_rank) if max_rank else 0.0
            })

        # Fusion
        by_id: dict[str, dict] = {}
        for h in v:
            by_id[h["chunk_id"]] = dict(h)
        for h in kw:
            by_id.setdefault(h["chunk_id"], {}).update(h)

        merged: List[tuple[str, float]] = []
        if settings.hybrid_mode == "norm":
            vec_scores = [float(h.get("score", 0.0)) for h in v]
            kw_scores = [float(h.get("score_kw", 0.0)) for h in kw]
            # normalize separately
            def _norm_map(arr, vals):
                if not vals: return {}
                lo, hi = min(vals), max(vals)
                if hi <= lo:
                    nm = [0.0 for _ in vals]
                else:
                    nm = [ (x - lo) / (hi - lo) for x in vals ]
                return {a["chunk_id"]: s for a,s in zip(arr, nm)}
            vec_map = _norm_map(v, vec_scores)
            kw_map  = _norm_map(kw, kw_scores)
            for cid in by_id.keys():
                merged.append((cid, settings.hybrid_alpha * vec_map.get(cid, 0.0) + (1.0 - settings.hybrid_alpha) * kw_map.get(cid, 0.0)))
        else:
            id2rank_v = {h["chunk_id"]: i+1 for i, h in enumerate(v)}
            id2rank_k = {h["chunk_id"]: i+1 for i, h in enumerate(kw)}
            for cid in dict.fromkeys([*id2rank_v.keys(), *id2rank_k.keys()]):
                rv = id2rank_v.get(cid, 10**9); rk = id2rank_k.get(cid, 10**9)
                rrf_v = 1.0 / (60.0 + rv); rrf_k = 1.0 / (60.0 + rk)
                merged.append((cid, settings.hybrid_alpha * rrf_v + (1.0 - settings.hybrid_alpha) * rrf_k))
        merged.sort(key=lambda x: x[1], reverse=True)

        # Build candidates
        candidates: List[Dict[str, Any]] = []
        for cid, score in merged[:max(settings.rerank_topn, k*4)]:
            h = by_id[cid]
            h2 = dict(h)
            h2["score"] = float(score)
            candidates.append(h2)

        # Optional rerank with cross-encoder
        if self.reranker and candidates:
            try:
                pairs = [(q, (c.get("text") or "")) for c in candidates]
                logits = self.reranker.predict(pairs)
                rescored = list(zip(candidates, [float(x) for x in logits]))
                rescored.sort(key=lambda x: x[1], reverse=True)
                candidates = [c for c,_ in rescored]
            except Exception as e:
                self.log("warn", "rerank-fail", reason=str(e))

        # Doc-type boosting
        boost_types = set([t for t in (filters.get("boost_types") or []) if isinstance(t, str)])
        if boost_types:
            for c in candidates:
                types = set(c.get("types") or [])
                if types.intersection(boost_types):
                    c["score"] = float(c.get("score", 0.0)) + 0.05

        # MMR for diversity
        results = self._mmr(candidates, k * 2, lam=settings.mmr_lambda)  # Fetch more candidates first

        # Doc Diversity: Force cap per document
        final_results = []
        doc_counts = {}
        cap = settings.doc_cap_per_doc  # e.g. 3
        for r in results:
            did = r["doc_id"]
            if doc_counts.get(did, 0) >= cap:
                continue
            final_results.append(r)
            doc_counts[did] = doc_counts.get(did, 0) + 1
            if len(final_results) >= k:
                break
        results = final_results

        # Safety net
        if settings.retr_safety_net and len(results) == 0:
            results = self._safety_net(k=k, doc_ids=doc_ids, types_any=types_any, prefer_tables=numeric)
            
        # SOTA: Window Expansion
        results = self._expand_context_window(results)

        return {"results": results, "mode": "hybrid", "timing_ms": {"total": 0}, "date_range": (dr if dr else None), "filters_applied": {"doc_ids": doc_ids, "types": types_any}}
