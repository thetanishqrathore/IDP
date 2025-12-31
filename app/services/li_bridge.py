# app/services/li_bridge.py
from __future__ import annotations
import os
from typing import Any, Dict, List

# Guarded imports: degrade gracefully if llama_index is not installed
try:
    from llama_index.core import Settings
    from llama_index.core.retrievers import BaseRetriever
    from llama_index.core.schema import TextNode, NodeWithScore
    from llama_index.llms.openai import OpenAI as LIOpenAI
    _LI_AVAILABLE = True
except Exception:
    Settings = None  # type: ignore
    BaseRetriever = object  # type: ignore
    TextNode = None  # type: ignore
    NodeWithScore = None  # type: ignore
    LIOpenAI = None  # type: ignore
    _LI_AVAILABLE = False

from services.retrieval import RetrievalService
from core.config import settings

GEN_MODEL   = settings.gen_model
GEN_BASEURL = settings.gen_base_url
GEMINI_KEY  = settings.gemini_api_key or ""

def init_llamaindex():
    """Initialize LlamaIndex if available; no-op otherwise."""
    if _LI_AVAILABLE and LIOpenAI and Settings and GEMINI_KEY:
        try:
            Settings.llm = LIOpenAI(model=GEN_MODEL, api_key=GEMINI_KEY, base_url=GEN_BASEURL)
        except Exception:
            # best-effort; do not fail app startup
            pass

class HybridRetrieverLI(BaseRetriever):  # type: ignore
    """LlamaIndex retriever that delegates to our RetrievalService (hybrid).
    Safe to import even if LlamaIndex is missing (acts as a stub until used).
    """
    def __init__(self, retr: RetrievalService, *, k: int = 8, filters: Dict[str, Any] | None = None):
        if not _LI_AVAILABLE:
            raise RuntimeError("HybridRetrieverLI requires llama_index; not installed")
        super().__init__()  # type: ignore
        self._retr = retr
        self._k = k
        self._filters = filters or {}

    def _retrieve(self, query: str):  # -> List[NodeWithScore]
        result = self._retr.search(q=query, k=self._k, hybrid=True, filters=self._filters)
        nodes = []
        for h in result.get("results", []):
            text = (h.get("text") or "").strip()
            meta = {
                "doc_id": h.get("doc_id"),
                "chunk_id": h.get("chunk_id"),
                "plan_id": h.get("plan_id"),
                "uri": h.get("uri"),
                "page_start": h.get("page_start"),
                "page_end": h.get("page_end"),
                "types": h.get("types", []),
                "source_block_ids": h.get("source_block_ids", []),
            }
            if TextNode and NodeWithScore:
                node = TextNode(text=text, metadata=meta, id_=h.get("chunk_id"))
                nodes.append(NodeWithScore(node=node, score=float(h.get("score", 0.0))))
        return nodes
