from typing import List, Dict, Any, Tuple, Optional
import math
import re
from core.config import settings

def _to_int_or_none(v):
    try:
        if v is None: return None
        if isinstance(v, int): return v
        if isinstance(v, float) and v.is_integer(): return int(v)
        if isinstance(v, str) and v.strip().isdigit(): return int(v.strip())
    except Exception: pass
    return None

def _page_from_hit(hit: dict) -> int:
    m = (hit.get("meta") or {})
    for key in ("page_start", "page", "p", "pg"):
        p = _to_int_or_none(hit.get(key)) or _to_int_or_none(m.get(key))
        if p is not None: return p
    return 1

def _estimated_tokens(text: str) -> int:
    return max(1, math.ceil(len(text or "") / 4))

def stitch_hits(hits: List[Dict[str, Any]], max_chars: int = 2000) -> List[Dict[str, Any]]:
    if not hits: return []
    out: List[Dict[str, Any]] = []
    cur = None
    for h in hits:
        txt = (h.get("text") or "").strip()
        if not txt: continue
        doc = h.get("doc_id")
        p1 = _page_from_hit(h)
        p2 = _to_int_or_none(h.get("page_end")) or p1
        if cur and cur["doc_id"] == doc:
            prev_end = _to_int_or_none(cur.get("page_end")) or cur["page_start"]
            if (p1 - prev_end) in (0, 1):
                if len(cur["text"]) + 1 + len(txt) <= max_chars:
                    cur["text"] += "\n" + txt
                    cur["page_end"] = max(cur["page_end"], p2)
                    cur["chunk_ids"].append(h.get("chunk_id"))
                    continue
        cur = {
            "doc_id": doc,
            "chunk_ids": [h.get("chunk_id")],
            "text": txt,
            "uri": h.get("uri") or h.get("canonical_uri") or "",
            "page_start": p1,
            "page_end": p2,
            "meta": h.get("meta") or {},
            "score": h.get("score")
        }
        out.append(cur)
    return out

def pack_context(q: str, hits: List[Dict[str, Any]], token_budget: int = 3500) -> Tuple[str, List[Dict[str, Any]], List[str]]:
    # Prefer tables/lists for numeric queries
    def _types(h): return ((h.get("meta") or {}).get("types") or [])
    if any(w in (q or "").lower() for w in ["total", "amount", "sum", "balance", "fee", "fees", "tax", "subtotal"]):
        hits = sorted(hits, key=lambda h: ("table" in _types(h) or "list" in _types(h), h.get("score", 0.0)), reverse=True)

    by_doc: Dict[str, List[Dict[str, Any]]] = {}
    for h in hits:
        by_doc.setdefault(h["doc_id"], []).append(h)
    for v in by_doc.values():
        v.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)

    ordered: List[Dict[str, Any]] = []
    while True:
        moved = False
        for arr in by_doc.values():
            if arr:
                ordered.append(arr.pop(0))
                moved = True
        if not moved:
            break

    stitched = stitch_hits(ordered, max_chars=2000)

    header = _estimated_tokens(q) + 150
    budget = max(600, token_budget - header)

    parts: List[str] = []
    footnotes: List[Dict[str, Any]] = []
    used: List[str] = []

    per_doc_cap = settings.gen_max_stitch_per_doc
    used_per_doc: Dict[str, int] = {}
    t, n = 0, 1
    for h in stitched:
        txt = (h.get("text") or "").strip()
        if not txt: continue
        did = str(h.get("doc_id"))
        if per_doc_cap > 0 and did:
            if used_per_doc.get(did, 0) >= per_doc_cap:
                continue
        trimmed = txt[:8000]
        tok = _estimated_tokens(trimmed) + 20
        if t + tok > budget:
            break
        p1 = _to_int_or_none(h.get("page_start")) or 1
        p2 = _to_int_or_none(h.get("page_end")) or p1
        uri = h.get("uri") or ""

        parts.append(f"Source ID: [^{n}]\nDocument: {uri}\nPage: {p1}" + (f"-{p2}" if p2 != p1 else "") + f"\nContent:\n{trimmed}\n---")
        footnotes.append({
            "n": n,
            "doc_id": h.get("doc_id"),
            "chunk_id": (h.get("chunk_ids") or [None])[0],
            "page_start": p1,
            "page_end": p2,
            "uri": uri,
            "block_ids": ((h.get("meta") or {}).get("source_block_ids") or []),
            "score": float(h.get("score", 0.0) or 0.0),
        })
        used.extend([cid for cid in (h.get("chunk_ids") or []) if cid])
        t += tok
        n += 1
        if did:
            used_per_doc[did] = used_per_doc.get(did, 0) + 1

    return "\n".join(parts), footnotes, used
