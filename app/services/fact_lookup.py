# app/services/fact_lookup.py
from __future__ import annotations
import re
from typing import Any, Dict, List, Optional, Tuple

from infra.db import DBClient

# Patterns for table-ish summaries
_TOTAL_LABEL_RX = re.compile(r"\b(grand\s*total|total\s*amount|amount\s*due|total)\b[:\s]*([\₹$]?\s?[0-9][0-9,]*(?:\.[0-9]{1,2})?)", re.I)
_STUDENT_NAME_LINE_RX = re.compile(r"\bstudent\s*name\b[:\s-]*([A-Za-z][A-Za-z\s\.\'-]{1,80})", re.I)
_CURRENCY_RX = re.compile(r"([\₹$]?\s?[0-9][0-9,]*(?:\.[0-9]{1,2})?)")

def _as_float(s: str) -> Optional[float]:
    try:
        if not s: return None
        v = s.replace(",", "").replace("₹", "").replace("$", "").strip()
        return float(v)
    except Exception:
        return None

def _page_from_meta(meta: Dict[str, Any] | None) -> int:
    m = meta or {}
    for k in ("page_start", "page", "p", "pg"):
        v = m.get(k)
        try:
            if v is None: continue
            if isinstance(v, int): return v
            if isinstance(v, str) and v.strip().isdigit(): return int(v.strip())
        except Exception:
            continue
    return 1

class FactLookupService:
    """
    Uses SQL first if structured tables exist (e.g., invoice_headers/invoice_lines),
    otherwise falls back to robust regex scanning of chunks scoped to doc_ids.
    Returns a standard dict: {answer, citations[], used_chunks[], confidence}.
    """

    def __init__(self, db: DBClient, logger=None):
        self.db = db
        self.log = logger or (lambda *a, **k: None)

    # --------- Public ---------
    def run(self, plan: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        kind = (plan.get("fact") or {}).get("kind")
        filters = plan.get("filters") or {}
        doc_ids: List[str] = filters.get("doc_ids") or []

        if kind == "invoice_total":
            return self._invoice_total(invoice_no=(plan["fact"].get("invoice_no") or "").strip(),
                                       doc_ids=doc_ids)
        if kind == "student_fees":
            fields = (plan["fact"] or {}).get("fields") or {}
            return self._student_fees(doc_ids=doc_ids,
                                      want_name=bool(fields.get("student_name")),
                                      want_total=bool(fields.get("total_fees")))
        return None

    # --------- Specific resolvers ---------
    def _invoice_total(self, invoice_no: str, doc_ids: List[str]) -> Optional[Dict[str, Any]]:
        if not invoice_no:
            return None

        # 1) Try structured table if present
        try:
            self.db.connect()
            with self.db.conn.cursor() as cur:
                cur.execute("""
                    SELECT invoice_id::text, invoice_number, total
                    FROM invoices
                    WHERE invoice_number = %s
                    LIMIT 1
                """, (invoice_no,))
                row = cur.fetchone()
            if row:
                if isinstance(row, dict):
                    doc_id, inv, total = row.get("invoice_id"), row.get("invoice_number"), row.get("total")
                else:
                    doc_id, inv, total = row
                ans = f"Invoice {inv} total: {total}."
                cite = [{"n": 1, "doc_id": doc_id, "page_start": 1, "page_end": 1, "uri": None}]
                return {"answer": ans, "citations": cite, "used_chunks": [], "confidence": 0.9}
        except Exception:
            pass

        # 2) Fallback: scan chunks within doc_ids (or across recent docs if none given)
        hits = self._scan_chunks(
            doc_ids=doc_ids,
            like_terms=["invoice", invoice_no],
            limit=200
        )

        best_total: Optional[Tuple[str, float, Dict[str, Any]]] = None  # (chunk_id, value, chunk)
        for ch in hits:
            text = (ch.get("text") or "")
            # Prefer direct "Total: X" lines
            for m in _TOTAL_LABEL_RX.finditer(text):
                val = _as_float(m.group(2))
                if val is None: continue
                score = 2.0  # direct label match
                if (best_total is None) or (score > best_total[1]):
                    best_total = (ch["chunk_id"], val, ch)
            # fallback: any currency numbers on lines with invoice hint
            if "invoice" in text.lower():
                for m in _CURRENCY_RX.finditer(text):
                    val = _as_float(m.group(1))
                    if val and val > 0:
                        score = 1.0
                        if (best_total is None) or (score > best_total[1]):
                            best_total = (ch["chunk_id"], val, ch)

        if best_total:
            cid, val, ch = best_total
            p = _page_from_meta(ch.get("meta"))
            ans = f"Invoice {invoice_no} total: {val:.2f}."
            cite = [{"n": 1, "doc_id": ch["doc_id"], "chunk_id": cid, "page_start": p, "page_end": p, "uri": ch.get("uri")}]
            return {"answer": ans, "citations": cite, "used_chunks": [cid], "confidence": 0.7}

        return None

    def _student_fees(self, doc_ids: List[str], want_name: bool, want_total: bool) -> Optional[Dict[str, Any]]:
        # Try structured tables if you have them; else scan chunks
        hits = self._scan_chunks(
            doc_ids=doc_ids,
            like_terms=["student", "name", "fees", "total", "amount due"],
            limit=200
        )
        student_name = None
        best_total: Optional[Tuple[str, float, Dict[str, Any]]] = None

        for ch in hits:
            text = (ch.get("text") or "")
            if want_name and student_name is None:
                m = _STUDENT_NAME_LINE_RX.search(text)
                if m:
                    student_name = m.group(1).strip().strip(":").strip()
            if want_total:
                for m in _TOTAL_LABEL_RX.finditer(text):
                    val = _as_float(m.group(2))
                    if val is None: continue
                    score = 2.0
                    if (best_total is None) or (score > best_total[1]):
                        best_total = (ch["chunk_id"], val, ch)

        # Build answer
        parts = []
        cites = []
        used = []
        conf = 0.0
        idx = 1

        if student_name:
            parts.append(f"Student name: {student_name} [^{idx}].")
            ch = hits[0]  # not strictly accurate; we’ll try to find a cite with name
            for ch in hits:
                if _STUDENT_NAME_LINE_RX.search(ch.get("text") or ""):
                    p = _page_from_meta(ch.get("meta")); used.append(ch["chunk_id"])
                    cites.append({"n": idx, "doc_id": ch["doc_id"], "chunk_id": ch["chunk_id"], "page_start": p, "page_end": p, "uri": ch.get("uri")})
                    break
            else:
                cites.append({"n": idx})
            idx += 1
            conf += 0.35

        if best_total:
            cid, val, ch = best_total
            p = _page_from_meta(ch.get("meta"))
            parts.append(f"Total fees: {val:.2f} [^{idx}].")
            cites.append({"n": idx, "doc_id": ch["doc_id"], "chunk_id": cid, "page_start": p, "page_end": p, "uri": ch.get("uri")})
            used.append(cid)
            conf += 0.45
            idx += 1

        if not parts:
            return None

        return {
            "answer": " ".join(parts),
            "citations": cites,
            "used_chunks": used,
            "confidence": min(conf, 0.95)
        }

    # --------- helpers ---------
    def _scan_chunks(self, *, doc_ids: List[str], like_terms: List[str], limit: int = 200) -> List[Dict[str, Any]]:
        """
        Pull chunks whose text ILIKE any of the like_terms. If no doc_ids, scan recent extracted docs.
        """
        self.db.connect()
        rows: List[Dict[str, Any]] = []
        with self.db.conn.cursor() as cur:
            if doc_ids:
                cur.execute("""
                    SELECT c.chunk_id::text, c.doc_id::text, c.text, c.meta, d.uri, d.mime
                    FROM chunks c
                    JOIN documents d ON d.doc_id = c.doc_id
                    WHERE c.doc_id::text = ANY(%s)
                      AND (""" + " OR ".join(["c.text ILIKE %s" for _ in like_terms]) + """)
                    LIMIT %s
                """, (doc_ids, *[f"%{t}%" for t in like_terms], limit))
            else:
                cur.execute("""
                    SELECT c.chunk_id::text, c.doc_id::text, c.text, c.meta, d.uri, d.mime
                    FROM chunks c
                    JOIN documents d ON d.doc_id = c.doc_id
                    WHERE (""" + " OR ".join(["c.text ILIKE %s" for _ in like_terms]) + """)
                    ORDER BY d.extracted_at DESC NULLS LAST
                    LIMIT %s
                """, (*[f"%{t}%" for t in like_terms], limit))
            rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            if isinstance(r, dict):
                out.append(r)
            else:
                cid, did, text, meta, uri, mime = r
                out.append({"chunk_id": cid, "doc_id": did, "text": text, "meta": meta, "uri": uri, "mime": mime})
        return out
