from __future__ import annotations
import re, time, uuid
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from infra.db import DBClient


_AMOUNT = re.compile(r"(?i)\b(total(?:\s*amount)?|amount\s*due|subtotal|grand\s*total)\b[:\s]*([\$₹€£]?\s*[-+]?[0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})?|[0-9]+(?:\.[0-9]{1,2})?)")
_CURR = re.compile(r"(?i)\b(USD|INR|EUR|GBP|JPY|AUD|CAD)\b")
_INV_NO = re.compile(r"(?i)\b(invoice\s*(?:no|number)\b[:#\s]*([A-Za-z0-9\-_/]+))")
_DATE = re.compile(r"(?i)\b([0-3]?[0-9][\-/](?:[0-1]?[0-9]|[A-Za-z]{3,9})[\-/][0-9]{2,4}|[0-9]{4}-[0-1][0-9]-[0-3][0-9])\b")


def _parse_date(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    fmts = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%d-%b-%Y", "%d %b %Y", "%b %d, %Y"]
    for f in fmts:
        try:
            return datetime.strptime(s, f).date().isoformat()
        except Exception:
            continue
    return None


class StructuredIndexerService:
    def __init__(self, db: DBClient, *, tenant_id: str, logger):
        self.db = db
        self.tenant_id = tenant_id
        self.log = logger

    def _guess_type(self, blocks: List[Dict[str, Any]]) -> str:
        t = "generic"
        text = "\n".join((b.get("text") or "")[:400] for b in blocks[:30]).lower()
        if ("invoice" in text) or any("table" == b.get("type") for b in blocks):
            t = "invoice"
        if any(k in text for k in ["agreement", "party", "effective date", "term", "termination"]):
            # prefer contract if explicit legal cues exist
            t = "contract"
        return t

    def _extract_invoice(self, doc_id: str, blocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        header_text = "\n".join(b.get("text") or "" for b in blocks if b.get("type") in ("header", "paragraph", "list"))

        vendor = None
        # naive vendor: first non-empty line that is not a label; fallback None
        for ln in header_text.splitlines():
            ls = ln.strip()
            if not ls:
                continue
            if ls.lower().startswith("invoice"):
                continue
            vendor = ls
            break

        inv_no = None
        m = _INV_NO.search(header_text)
        if m:
            inv_no = m.group(2) if m.lastindex and m.lastindex >= 2 else m.group(1)

        # dates
        invoice_date = None
        due_date = None
        for lbl in ["invoice date", "date", "issued"]:
            m = re.search(lbl + r"[:\s]*([A-Za-z0-9,\-/ ]{6,20})", header_text, re.I)
            if m:
                invoice_date = _parse_date(m.group(1)) or invoice_date
        for lbl in ["due date", "pay by", "payment due"]:
            m = re.search(lbl + r"[:\s]*([A-Za-z0-9,\-/ ]{6,20})", header_text, re.I)
            if m:
                due_date = _parse_date(m.group(1)) or due_date

        # currency + total
        currency = None
        m = _CURR.search(header_text)
        if m:
            currency = m.group(1).upper()
        total = None
        for mm in _AMOUNT.finditer(header_text):
            try:
                amt = mm.group(2)
                amt = amt.replace(",", "").replace("$", "").replace("₹", "").replace("€", "").replace("£", "").strip()
                total = float(amt)
                break
            except Exception:
                continue

        # line items from first table block
        items: List[dict] = []
        for b in blocks:
            if b.get("type") != "table":
                continue
            rows = [ln.strip() for ln in (b.get("text") or "").splitlines() if ln.strip()]
            if not rows:
                continue
            headers = [h.strip().lower() for h in rows[0].split("|")]
            idx = {h: i for i, h in enumerate(headers)}
            # heuristic column mapping
            def find_col(*cands):
                for c in cands:
                    for h, i in idx.items():
                        if c in h:
                            return i
                return None
            i_desc = find_col("description", "item")
            i_qty = find_col("qty", "quantity")
            i_unit = find_col("unit price", "price")
            i_amt = find_col("amount", "total")
            for r in rows[1:]:
                cols = [c.strip() for c in r.split("|")]
                if len([c for c in cols if c]) < 2:
                    continue
                def getn(i):
                    try:
                        return float(cols[i].replace(",","")) if i is not None and i < len(cols) else None
                    except Exception:
                        return None
                item = {
                    "description": cols[i_desc] if i_desc is not None and i_desc < len(cols) else None,
                    "qty": getn(i_qty),
                    "unit_price": getn(i_unit),
                    "amount": getn(i_amt),
                    "meta": {"source_block_id": str(b.get("block_id"))}
                }
                if any(v is not None for v in [item["description"], item["qty"], item["unit_price"], item["amount"]]):
                    items.append(item)
            break  # single table for MVP

        # persist
        self.db.upsert_invoice(
            invoice_id=doc_id,
            vendor=vendor,
            invoice_number=inv_no,
            invoice_date=invoice_date,
            due_date=due_date,
            total=total,
            currency=currency,
            meta={"extracted_at": time.time()}
        )
        self.db.replace_invoice_items(invoice_id=doc_id, items=items)
        return {
            "type": "invoice",
            "invoice_id": doc_id,
            "vendor": vendor,
            "invoice_number": inv_no,
            "invoice_date": invoice_date,
            "due_date": due_date,
            "total": total,
            "currency": currency,
            "line_items": len(items)
        }

    def _extract_contract(self, doc_id: str, blocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        text = "\n".join((b.get("text") or "") for b in blocks)
        party_a = None
        party_b = None
        m = re.search(r"(?i)between\s+(.+?)\s+and\s+(.+?)\s", text)
        if m:
            party_a, party_b = m.group(1).strip(), m.group(2).strip()
        eff = None
        for lbl in ["effective date", "dated", "date of commencement"]:
            m = re.search(lbl + r"[:\s]*([A-Za-z0-9,\-/ ]{6,20})", text, re.I)
            if m:
                eff = _parse_date(m.group(1)) or eff
        # persist
        self.db.upsert_contract(
            contract_id=doc_id,
            party_a=party_a,
            party_b=party_b,
            effective_date=eff,
            end_date=None,
            renewal_date=None,
            governing_law=None,
            meta={"extracted_at": time.time()}
        )
        return {
            "type": "contract",
            "contract_id": doc_id,
            "party_a": party_a,
            "party_b": party_b,
            "effective_date": eff,
        }

    def index_doc(self, doc_id: str) -> Dict[str, Any]:
        t0 = time.time()
        blocks = self.db.fetch_blocks_for_doc(doc_id)
        if not blocks:
            raise RuntimeError("no_blocks")
        doc_type = self._guess_type(blocks)
        if doc_type == "invoice":
            res = self._extract_invoice(doc_id, blocks)
        elif doc_type == "contract":
            res = self._extract_contract(doc_id, blocks)
        else:
            res = {"type": "generic", "doc_id": doc_id}
        self.log("info", "structured-index", doc_id=doc_id, doc_type=doc_type,
                 latency_ms=int((time.time()-t0)*1000))
        return res

