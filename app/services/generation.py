# app/services/generation.py
from __future__ import annotations
import os, json, time, math, re
from typing import Any, Dict, List, Optional, Tuple, Iterator
from services.fact_lookup import FactLookupService
# soft-deps: we degrade gracefully if they’re not present
try:
    from jsonschema import validate as json_validate, ValidationError
except Exception:
    json_validate = None
    class ValidationError(Exception): ...

from infra.db import DBClient
import os as _os
from infra.storage import presign
from services.retrieval import RetrievalService
from datetime import datetime
import backoff
from core.config import settings
from services.llm import OpenAIProvider, pack_context, build_messages, build_messages_no_context, OUT_SCHEMA

# Router is optional; don’t crash if module/file isn’t there
try:
    from services.router import QueryRouter  # type: ignore
except Exception:
    QueryRouter = None  # type: ignore

def _estimated_tokens(text: str) -> int:
    return max(1, math.ceil(len(text or "") / 4))

def _extract_cite_nums(s: str) -> List[int]:
    try:
        nums = [int(m.group(1)) for m in re.finditer(r"[\^(\\d+)]", s or "")]
        # preserve order but unique
        out = []
        for n in nums:
            if n not in out:
                out.append(n)
        return out
    except Exception:
        return []

def _token_set(s: str) -> set:
    toks = re.split(r"[^A-Za-z0-9]+", (s or "").lower())
    return set([t for t in toks if len(t) >= 3])

def _numbers_in(s: str) -> List[str]:
    return re.findall(r"\b\d+(?:[\.,]\d+)?\b", s or "")

def _groundedness(answer: str, context: str) -> float:
    try:
        ats = _token_set(answer); cts = _token_set(context)
        if not ats:
            return 0.0
        inter = len(ats.intersection(cts)); base = inter / max(1, len(ats))
        anum = set(_numbers_in(answer)); cnum = set(_numbers_in(context))
        num_score = (len(anum.intersection(cnum)) / max(1, len(anum))) if anum else base
        # weight numbers slightly higher for numeric queries
        return round(0.4 * base + 0.6 * num_score, 3)
    except Exception:
        return 0.0

def _intent(q: str) -> str:
    s = (q or "").lower()
    # Use regex for precise word matching
    if re.search(r"\b(total|amount|sum|grand total|balance due)\b", s):
        return "NUMERIC_TOTAL"
    if re.search(r"\b(list|show|summarize|summarise|items|line items)\b", s):
        return "LIST"
    if re.search(r"\b(payment terms|termination|limitation of liability|governing law|confidentiality|clause)\b", s):
        return "CLAUSE"
    return "DEFAULT"

_money_rx = re.compile(
    r"(?<!\w)(?:₹|rs\.?\s*|usd\s*\$|\$)?\s*([0-9]{1,3}(?:[,\s][0-9]{2,3})*(?:\.[0-9]{1,2})|[0-9]+(?:\.[0-9]{1,2})?)",
    re.I
)

def _try_sum_from_context(context: str) -> Optional[float]:
    # rough heuristic: sum all positive currency-like numbers; used only as a hint
    nums = []
    for line in (context or "").splitlines():
        for m in _money_rx.finditer(line):
            try:
                raw = m.group(1).replace(",", "").replace(" ", "")
                val = float(raw)
                if val > 0:
                    nums.append(val)
            except Exception:
                continue
    if len(nums) >= 2:
        return round(sum(nums), 2)
    return None

# ---------- main service ----------
class GenerationService:
    def __init__(self, db: DBClient, retrieval: RetrievalService, *, tenant_id: str, logger, router: Optional[Any]=None):
        self.db = db
        self.retrieval = retrieval
        self.tenant_id = tenant_id
        self.log = logger
        self.router = router  # optional
        
        self.model = settings.gen_model
        # Initialize Provider
        self.llm_provider = OpenAIProvider(
            api_key=settings.gemini_api_key or "",
            base_url=settings.gen_base_url,
            model=self.model
        )

    def _is_greeting(self, q: str) -> bool:
        s = (q or "").strip().lower()
        return bool(s) and any(
            s.startswith(w) or s == w for w in [
                "hi", "hello", "hey", "howdy", "yo", "good morning", "good afternoon", "good evening"
            ]
        )

    # ---------- streaming prep & tokens (optional) ----------
    def prepare_for_stream(self, q: str, k: int = 8, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        warnings: List[str] = []
        # route (optional) -> plan
        if self.router and hasattr(self.router, "route"):
            try:
                plan = self.router.route(q, want_k=k, filters=filters or {}) or {}
            except Exception as e:
                self.log("warn", "router-fail", reason=str(e))
                plan = {}
        else:
            plan = {}
        plan.setdefault("semantic_query", q)
        plan.setdefault("k", k)
        plan.setdefault("hybrid", True)
        plan.setdefault("filters", filters or {})

        # retrieval
        ret = self.retrieval.search(q=plan["semantic_query"], k=plan["k"], hybrid=plan["hybrid"], filters=plan["filters"])  # type: ignore
        hits = ret.get("results", [])
        context_str, footnotes, used_chunks = pack_context(q, hits, token_budget=settings.gen_token_budget)
        mode = _intent(q)
        msgs = build_messages(q, context_str, mode)

        # Special Case: Telegram Plain Text Override
        if "asking from telegram" in q.lower():
            msgs[-1]["content"] += "\n\nCRITICAL OVERRIDE: Return the answer in PLAIN TEXT ONLY. Do NOT use Markdown, HTML, bolding (**), italics (*), or any special formatting. Just simple text. Dont put souces name too. just simple text answer."

        cites_expanded = self._add_presigned_links(footnotes)
        return {"messages": msgs, "citations": cites_expanded, "used_chunks": used_chunks, "warnings": warnings, "mode": mode}

    def iter_llm_tokens(self, messages: List[Dict[str, str]]) -> Iterator[str]:
        return self.llm_provider.stream(messages)

    def _parse_and_validate(self, raw: str) -> Optional[Dict[str, Any]]:
        if not raw:
            return None
        raw = raw.strip()
        # Strip code fences if present
        if raw.startswith("```"):
            raw = raw.strip('`')
            # remove optional json/lang tag
            if raw.lower().startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        # Fast path with jsonschema if available
        if json_validate:
            try:
                obj = json.loads(raw)
                json_validate(obj, OUT_SCHEMA)
                return obj
            except Exception:
                pass
        # Best-effort extraction
        s, e = raw.find("{"), raw.rfind("}")
        if s != -1 and e != -1 and e > s:
            try:
                obj = json.loads(raw[s:e+1])
                if json_validate:
                    json_validate(obj, OUT_SCHEMA)
                return obj
            except Exception:
                return None
        return None

    def _add_presigned_links(self, citations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Attach reliable, clickable URLs to citations.
        - Always set `url` to the internal proxy `/ui/open/{doc_id}` to avoid signature/host issues.
        - If presign works, also attach `direct_url` for optional external access.
        """
        out = []
        for c in citations:
            doc_id = c.get("doc_id")
            if not doc_id:
                out.append(c)
                continue

            # default, reliable proxy route (same-origin)
            c2 = dict(c)
            # Build deep-link to page anchor (canonical HTML ensures id="p-<page>")
            frag = ""
            try:
                p = int(c.get("page_start") or 0) or None
            except Exception:
                p = None
            if p:
                frag = f"#p-{p}"
            c2["url"] = f"/ui/open/{doc_id}{frag}"

            # best-effort: also provide a direct presigned link as `direct_url`
            try:
                key = None
                if hasattr(self.db, "get_doc_storage_keys"):
                    keys = self.db.get_doc_storage_keys(doc_id)
                    key = (keys or {}).get("canonical_uri") or (keys or {}).get("minio_key")
                else:
                    self.db.connect()
                    with self.db.conn.cursor() as cur:
                        cur.execute(
                            "SELECT canonical_uri, minio_key FROM documents WHERE doc_id=%s::uuid LIMIT 1;",
                            (doc_id,),
                        )
                        row = cur.fetchone()
                    if row:
                        key = row.get("canonical_uri") if isinstance(row, dict) else (row[0] or None)
                        if not key:
                            key = row.get("minio_key") if isinstance(row, dict) else row[1]

                if key:
                    can_bucket = settings.s3_canonical_bucket
                    if can_bucket and (not str(key).startswith("sha256/")):
                        direct = presign(key, bucket=can_bucket)
                    else:
                        direct = presign(key)
                    if direct:
                        # attach fragment to direct URL too
                        c2["direct_url"] = f"{direct}{frag}"
            except Exception:
                pass

            out.append(c2)
        return out

    def process_citations(self, answer_text: str, all_footnotes: List[Dict[str, Any]], parsed_citations: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Filter and format citations based on the generated answer.
        1. If parsed_citations is provided (from JSON mode), use those.
        2. Otherwise, extract [^n] markers from text.
        3. Filter all_footnotes to only include used ones.
        4. Add presigned links.
        """
        # If no explicit parsed citations, extract from text
        if not parsed_citations:
            nums = _extract_cite_nums(answer_text)
            parsed_citations = [{"n": n} for n in nums if any(f["n"] == n for f in all_footnotes)]
            
            # Fallback: if still empty and we have footnotes, maybe cite the first one if it looks like a RAG answer?
            # Actually, for strict 'only used' requirements, we should return empty if none cited.
            # But existing logic was: if not parsed.get("citations"): parsed["citations"] = footnotes[:2]
            # We will adhere to strict extraction here for accuracy, but the caller can decide fallback.
        
        # map model citations -> full footnote payload
        fn_map = {f["n"]: f for f in all_footnotes}
        cleaned: List[Dict[str, int]] = []
        for c in parsed_citations or []:
            try:
                n = int(c.get("n"))
                if n in fn_map:
                    cleaned.append({"n": n})
            except Exception:
                continue

        expanded: List[Dict[str, Any]] = []
        for c in cleaned:
            f = fn_map.get(c["n"])
            if not f:
                continue
            expanded.append({
                "n": f["n"], "doc_id": f["doc_id"], "chunk_id": f.get("chunk_id"),
                "page_start": f.get("page_start"), "page_end": f.get("page_end"),
                "uri": f.get("uri"), "block_ids": f.get("block_ids") or [], "score": f.get("score")
            })
        
        return self._add_presigned_links(expanded)

    # ---------- public API ----------
    def answer(self, q: str, *, k: int = 8, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        t0 = time.time()
        warnings: List[str] = []
        filters = filters or {}

        # Friendly small-talk greeting without requiring context/LLM
        if self._is_greeting(q):
            msg = (
                "Hello! I can help answer questions about your documents. "
                "You can upload a file to index it, or ask a question directly — "
                "I'll do my best even with limited context."
            )
            return {
                "answer": msg,
                "citations": [],
                "used_chunks": [],
                "mode": "greeting",
                "confidence": 0.5,
                "warnings": warnings,
            }

        # route (optional) -> plan
        if self.router and hasattr(self.router, "route"):
            try:
                plan = self.router.route(q, want_k=k, filters=filters) or {}
            except Exception as e:
                self.log("warn", "router-fail", reason=str(e))
                plan = {}
        else:
            plan = {}
        plan.setdefault("semantic_query", q)
        plan.setdefault("k", k)
        plan.setdefault("hybrid", True)
        plan.setdefault("filters", filters)

        # ---------- FACT LOOKUP FAST PATH ----------
        if plan.get("intent") == "FACT_LOOKUP":
            try:
                fl = FactLookupService(self.db, logger=self.log)
                fact = fl.run(plan)
            except Exception as e:
                self.log("warn", "fact-lookup-fail", reason=str(e))
                fact = None

            conf_min = settings.fact_conf_min
            if fact and float(fact.get("confidence", 0.0)) >= conf_min:
                # record success
                try:
                    self.db.insert_event(self.tenant_id, stage="GENERATE", status="OK",
                                        details={"event": "FACT_OK", "confidence": float(fact.get("confidence", 0.0))})
                except Exception:
                    pass

                # add presigned links to citations if available
                cites = self._add_presigned_links(fact.get("citations", []))

                return {
                    "answer": fact.get("answer", "").strip(),
                    "citations": cites,
                    "used_chunks": fact.get("used_chunks", []),
                    "mode": "fact",
                    "confidence": float(fact.get("confidence", 0.0)),
                    "warnings": warnings
                }
            else:
                warnings.append("fact_fallback_rag")

        # ---------- RAG PATH (HYBRID BY DEFAULT) ----------
        ret = self.retrieval.search(
            q=plan["semantic_query"],
            k=plan["k"],
            hybrid=plan["hybrid"],
            filters=plan["filters"]
        )
        hits = ret.get("results", [])

        # Structured enrichment: if query looks like spend/amount and has a date range parsed by retrieval,
        # compute total_spend and add as a synthetic hit to ground the LLM.
        try:
            s = (q or "").lower()
            wants_total = any(k in s for k in ["total spend", "spend", "amount due", "total amount", "sum of invoices"]) \
                           or ("invoice" in s and any(k in s for k in ["total", "amount"]))
            # reuse the same parser via retrieval
            if hasattr(self.retrieval, "_parse_date_range"):
                dr = self.retrieval._parse_date_range(q)  # type: ignore
            else:
                dr = None
            if wants_total and dr:
                start, end = dr
                try:
                    tot = self.db.total_spend(start=start, end=end)
                    synth = {
                        "chunk_id": f"structured:invoices:{start}:{end}",
                        "doc_id": "structured:invoices",
                        "uri": f"db://invoices?start={start}&end={end}",
                        "text": f"Structured metric: total_spend from {start} to {end} = {tot:.2f}",
                        "meta": {"types": ["metric", "table"]},
                        "page_start": 1,
                        "page_end": 1,
                        "score": 0.99,
                    }
                    hits = [synth] + hits
                except Exception:
                    pass
        except Exception:
            pass

        if not hits:
            # Try best-effort LLM answer with a polite limited-context disclaimer
            self.db.insert_event(self.tenant_id, stage="GENERATE", status="WARN",
                                 details={"event": "GENERATE_NO_CONTEXT", "q": q})
            try:
                raw = self.llm_provider.generate_json(build_messages_no_context(q))
                llm_warn = None
            except Exception as e:
                raw = None
                llm_warn = str(e)

            if llm_warn:
                warnings.append(llm_warn)
            parsed = self._parse_and_validate(raw or "")
            if parsed:
                return {
                    "answer": (parsed.get("answer", "") or "").strip(),
                    "citations": [],
                    "used_chunks": [],
                    "mode": "llm_no_context",
                    "confidence": float(parsed.get("confidence", 0.0) or 0.0),
                    "warnings": ["no_context"] + warnings,
                }
            # No LLM available or parse failed → friendly fallback
            return {
                "answer": (
                    "I don’t have enough document context yet, but here’s a suggestion: "
                    "try uploading a file or ask a more specific question, and I’ll do my best."
                ),
                "citations": [],
                "used_chunks": [],
                "mode": "no_context",
                "confidence": 0.3,
                "warnings": ["no_context"] + warnings,
            }

        context_str, footnotes, used_chunks = pack_context(
            q, hits, token_budget=settings.gen_token_budget
        )
        mode = _intent(q)

        # numeric guardrail (best-effort hint; do NOT override a confident model answer)
        computed_total = _try_sum_from_context(context_str) if mode == "NUMERIC_TOTAL" else None

        msgs = build_messages(q, context_str, mode)

        # Special Case: Telegram Plain Text Override
        if "asking from telegram" in q.lower():
            msgs[-1]["content"] += "\n\nCRITICAL OVERRIDE: Return the answer in PLAIN TEXT ONLY. Do NOT use Markdown, HTML, bolding (**), italics (*), or any special formatting. Just simple text."

        try:
            raw = self.llm_provider.generate_json(msgs)
            llm_warn = None
        except Exception as e:
            raw = None
            llm_warn = str(e)

        if llm_warn:
            warnings.append(llm_warn)

        parsed = self._parse_and_validate(raw or "")
        if not parsed:
            # retry once with stricter instruction
            msgs2 = list(msgs)
            msgs2[-1] = {
                "role": "user",
                "content": msgs[-1]["content"] + "\nCRITICAL: Return ONLY a valid JSON object matching the schema. No extra text."
            }
            try:
                raw2 = self.llm_provider.generate_json(msgs2)
                llm_warn2 = None
            except Exception as e:
                raw2 = None
                llm_warn2 = str(e)

            if llm_warn2:
                warnings.append(llm_warn2)
            parsed = self._parse_and_validate(raw2 or "")

        if not parsed:
            parsed = {
                "answer": (
                    "I’m not fully confident due to limited context, but here’s my best attempt "
                    "based on the provided snippets."
                ),
                "citations": [{"n": f["n"]} for f in footnotes[:2]],
                "confidence": 0.3,
            }
            warnings.append("schema_parse_failed")

        # Schema-lite repair: if citations missing, infer from footnote markers in answer
        if not parsed.get("citations"):
            nums = _extract_cite_nums(parsed.get("answer", ""))
            if nums:
                parsed["citations"] = [{"n": n} for n in nums if any(f["n"] == n for f in footnotes)]
            if not parsed.get("citations"):
                parsed["citations"] = [{"n": f["n"]} for f in footnotes[:2]]

        # If numeric and model didn't include any number, prepend computed hint
        if (mode == "NUMERIC_TOTAL") and (computed_total is not None):
            if not re.search(r"[0-9][0-9,]*\.[0-9]{1,2}|[0-9][0-9,]*", parsed.get("answer", "")):
                parsed["answer"] = f"Total: {computed_total:.2f} [^1]\n" + parsed.get("answer", "")
                # ensure we at least cite the first block
                if not parsed.get("citations"):
                    parsed["citations"] = [{"n": 1}]

        # Use unified citation processing
        expanded = self.process_citations(
            answer_text=parsed.get("answer", "") or "",
            all_footnotes=footnotes,
            parsed_citations=parsed.get("citations")
        )

        # Grounding and hallucination guard
        ans_text = (parsed.get("answer", "") or "").strip()
        gscore = _groundedness(ans_text, context_str)
        if gscore < settings.gen_grounded_min or not expanded:
            # prepend gentle guidance once
            note = "Note: Based on limited matching context, this may be incomplete.\n\n"
            if not ans_text.startswith("## Answer"):
                parsed["answer"] = note + ans_text
            else:
                parsed["answer"] = re.sub(r"(##\s*Answer\s*\n)", r"\\1" + note, ans_text, count=1)
            warnings.append("low_groundedness")

        dt = int((time.time() - t0) * 1000)
        tokens_in = _estimated_tokens(context_str)
        tokens_out = _estimated_tokens(parsed.get("answer", ""))
        try:
            self.db.insert_event(self.tenant_id, stage="GENERATE", status="OK",
                                details={"event": "GENERATE_OK", "model": self.model, "latency_ms": dt,
                                        "tokens_in": tokens_in, "tokens_out": tokens_out,
                                        "used_chunks": used_chunks, "mode": mode, "q": q})
        except Exception:
            pass

        self.log("info", "generate", model=self.model, used=len(used_chunks),
                latency_ms=dt, tokens_in=tokens_in, tokens_out=tokens_out, mode=mode)

        # combine confidence with grounding score (cap to [0,1])
        try:
            conf0 = float(parsed.get("confidence", 0.0) or 0.0)
        except Exception:
            conf0 = 0.0
        confidence = max(0.0, min(1.0, 0.5 * conf0 + 0.5 * gscore))

        return {
            "answer": (parsed.get("answer", "") or "").strip(),
            "citations": expanded,
            "used_chunks": used_chunks,
            "mode": "rag",
            "confidence": confidence,
            "groundedness": gscore,
            "warnings": warnings
        }