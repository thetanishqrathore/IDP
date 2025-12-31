from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from infra.db import DBClient


def _new_id() -> str:
    return str(uuid.uuid4())


class KnowledgeGraphService:
    """Lightweight structural knowledge graph over extracted blocks."""

    def __init__(self, db: DBClient, *, tenant_id: str, logger):
        self.db = db
        self.tenant_id = tenant_id
        self.log = logger

    def build(self, doc_id: str) -> Dict[str, Any]:
        t0 = time.time()
        blocks = self.db.fetch_blocks_for_doc(doc_id)
        if not blocks:
            self.db.insert_event(
                self.tenant_id,
                stage="GRAPH",
                status="FAIL",
                details={"event": "GRAPH_BUILD_FAIL", "reason": "no_blocks"},
                doc_id=doc_id,
            )
            raise RuntimeError("no_blocks")

        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []

        root_id = _new_id()
        nodes.append(
            {
                "node_id": root_id,
                "doc_id": doc_id,
                "type": "document",
                "label": doc_id,
                "meta": {},
            }
        )

        header_stack: List[Tuple[int, str]] = []
        previous_node: Optional[str] = None

        for block in blocks:
            btype = block.get("type") or "paragraph"
            meta = block.get("meta") or {}
            label = (block.get("text") or "").strip()[:160]
            if not label:
                label = f"{btype.title()}@{block.get('page') or 0}"

            node_id = _new_id()
            node_meta = {
                "page": block.get("page"),
                "span": [block.get("span_start"), block.get("span_end")],
                "source_block_id": block.get("block_id"),
                "headers": meta.get("headers"),
                "origin_type": btype,
            }

            nodes.append(
                {
                    "node_id": node_id,
                    "doc_id": doc_id,
                    "type": btype,
                    "label": label,
                    "meta": node_meta,
                }
            )

            parent_id = root_id
            if btype == "header":
                level = int(meta.get("level") or 1)
                header_stack = [item for item in header_stack if item[0] < level]
                if header_stack:
                    parent_id = header_stack[-1][1]
                header_stack.append((level, node_id))
            else:
                if header_stack:
                    parent_id = header_stack[-1][1]

            edges.append(
                {
                    "edge_id": _new_id(),
                    "doc_id": doc_id,
                    "src_node_id": parent_id,
                    "dst_node_id": node_id,
                    "rel_type": "contains",
                    "weight": None,
                    "meta": {"source": "structure"},
                }
            )

            if previous_node:
                edges.append(
                    {
                        "edge_id": _new_id(),
                        "doc_id": doc_id,
                        "src_node_id": previous_node,
                        "dst_node_id": node_id,
                        "rel_type": "follows",
                        "weight": None,
                        "meta": {"source": "sequence"},
                    }
                )

            previous_node = node_id

        self.db.replace_graph(doc_id, nodes, edges)

        latency_ms = int((time.time() - t0) * 1000)
        self.db.insert_event(
            self.tenant_id,
            stage="GRAPH",
            status="OK",
            details={
                "event": "GRAPH_BUILT_OK",
                "nodes": len(nodes),
                "edges": len(edges),
                "latency_ms": latency_ms,
            },
            doc_id=doc_id,
        )
        self.log(
            "info",
            "graph-built",
            doc_id=doc_id,
            nodes=len(nodes),
            edges=len(edges),
            latency_ms=latency_ms,
        )

        return {"doc_id": doc_id, "nodes": len(nodes), "edges": len(edges)}
