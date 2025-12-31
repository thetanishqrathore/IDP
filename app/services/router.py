# app/services/router.py
from __future__ import annotations
import re
from typing import Any, Dict, Optional
from services.planner import AgenticPlanner

_NUMERIC_HINTS = ("total", "amount", "sum", "balance", "grand total", "amount due", "fees", "fee", "tax", "subtotal")
_LIST_HINTS = ("list", "show", "summarize", "items", "line items")
_CLAUSE_HINTS = ("payment terms", "termination", "limitation of liability", "governing law", "confidentiality", "clause")

_INV_RX = re.compile(r"\b(?:inv(?:oice)?)[\s:#-]*([A-Za-z0-9-_/]+)\b", re.I)
_STUDENT_NAME_RX = re.compile(r"\bstudent\s*name\b", re.I)
_FEES_RX = re.compile(r"\bfees?\b|\bfee\b|\bamount due\b|\btotal\b", re.I)

class QueryRouter:
    """
    Agentic-first router. Uses LLM to plan, falls back to regex rules.
    """

    def __init__(self, logger=None):
        self.log = logger or (lambda *a, **k: None)
        self.planner = AgenticPlanner(logger=self.log)

    def route(self, q: str, *, want_k: int = 8, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        s = (q or "").lower()
        
        # Default Plan
        plan: Dict[str, Any] = {
            "intent": "HYBRID",
            "semantic_query": q,
            "filters": filters or {},
            "hybrid": True,
            "k": want_k,
            "fact": {},
            "flavor": "DEFAULT"
        }

        # 1. Try Agentic Planner
        try:
            agent_plan = self.planner.plan_query(q)
            if agent_plan and "error" not in agent_plan:
                # Map Agentic Intent
                p_intent = agent_plan.get("intent", "RETRIEVAL")
                p_queries = agent_plan.get("queries", [])
                
                # Optimize Search Query
                if p_queries:
                    # For now, we join multiple queries to ensure broad context for the single-step retriever
                    # In Phase 2, we can dispatch parallel searches
                    plan["semantic_query"] = " ".join(p_queries)
                
                if p_intent == "FACT_LOOKUP":
                    plan["intent"] = "FACT_LOOKUP"
                    # We might need to extract specific fact fields here if the planner provided them
                    # For now, relies on the downstream fact_lookup service which expects specific keys
                    # This is a soft integration; if planner says FACT_LOOKUP, we trust it, 
                    # but we might still need the regex details for specific fields (like invoice #)
                    pass 
                elif p_intent == "COMPARISON":
                    plan["flavor"] = "COMPARISON"
                elif p_intent == "SUMMARIZATION":
                    plan["flavor"] = "LIST"
                
                # Merge Filters
                p_filters = agent_plan.get("filters") or {}
                if p_filters.get("types"):
                    # user filters override agent suggestions if present? or merge?
                    # let's merge: append agent types to user types
                    existing_types = plan["filters"].get("types") or []
                    new_types = list(set(existing_types + p_filters["types"]))
                    plan["filters"]["types"] = new_types
                
                self.log("info", "agentic-route-success", plan=agent_plan)
                return plan

        except Exception as e:
            self.log("warn", "agentic-route-fail", error=str(e))
            # Fallthrough to regex

        # 2. Regex Fallback (Original Logic)
        
        # Detect structured targets first
        inv_m = _INV_RX.search(q or "")
        student_name_ask = bool(_STUDENT_NAME_RX.search(q or ""))
        fees_ask = bool(_FEES_RX.search(q or ""))

        # Numeric/list/clause flavor
        if any(h in s for h in _NUMERIC_HINTS):
            plan["flavor"] = "NUMERIC"
        elif any(h in s for h in _LIST_HINTS):
            plan["flavor"] = "LIST"
        elif any(h in s for h in _CLAUSE_HINTS):
            plan["flavor"] = "CLAUSE"

        # Fact-lookup intents we support immediately:
        # - Invoice totals by invoice number
        if inv_m:
            plan["intent"] = "FACT_LOOKUP"
            plan["fact"] = {
                "kind": "invoice_total",
                "invoice_no": inv_m.group(1),
            }
            return plan

        # - Student name + total fees style questions
        if student_name_ask or (fees_ask and "student" in s):
            plan["intent"] = "FACT_LOOKUP"
            plan["fact"] = {
                "kind": "student_fees",
                "fields": {
                    "student_name": student_name_ask,
                    "total_fees": fees_ask,
                },
            }
            return plan

        # If not matched, default HYBRID RAG
        return plan