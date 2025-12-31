from __future__ import annotations
import os
from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, Filter, FieldCondition, MatchValue, PointStruct,  MatchAny, PointIdsList
from core.interfaces import SearchFilter


class QdrantIndex:
    def __init__(self, *, url: str, collection: str, dim: int, distance: str = "cosine"):
        self.url = url
        self.collection = collection
        self.dim = int(dim)
        self.client = QdrantClient(url=url)
        self._distance = Distance.COSINE if distance.lower() == "cosine" else Distance.DOT

    def ensure_collection(self):
        exists = self.client.collection_exists(self.collection)
        if not exists:
            self.client.recreate_collection(
                collection_name=self.collection,
                vectors_config=VectorParams(size=self.dim, distance=self._distance),
            )
        return True

    def delete_doc(self, doc_id: str):
        flt = Filter(must=[FieldCondition(key="doc_id", match=MatchValue(value=doc_id))])
        self.client.delete(collection_name=self.collection, points_selector=flt, wait=True)

    def delete_tenant(self, tenant_id: str):
        flt = Filter(must=[FieldCondition(key="tenant_id", match=MatchValue(value=tenant_id))])
        self.client.delete(collection_name=self.collection, points_selector=flt, wait=True)

    def delete_points(self, ids: List[str]):
        if not ids:
            return
        self.client.delete(collection_name=self.collection, points_selector=PointIdsList(points=ids), wait=True)

    def get_existing_checksums(self, doc_id: str) -> Dict[str, str]:
        existing: Dict[str, str] = {}
        flt = Filter(must=[FieldCondition(key="doc_id", match=MatchValue(value=doc_id))])
        next_off = None
        while True:
            res = self.client.scroll(collection_name=self.collection, scroll_filter=flt, with_payload=True, limit=256, offset=next_off)
            pts = res[0] if isinstance(res, tuple) else res.points
            next_off = (res[1] if isinstance(res, tuple) else res.next_page_offset)
            for p in pts or []:
                pid = str(p.id)
                pl = p.payload or {}
                # prefer chunk_id in payload; else use point id
                cid = str(pl.get("chunk_id") or pid)
                csum = str(pl.get("checksum") or "")
                existing[cid] = csum
            if not next_off:
                break
        return existing

    def upsert_points(self, points: List[Dict[str, Any]]):
        # points: [{"id": <str>, "vector": [..], "payload": {...}}, ...]
        qdrant_points = [PointStruct(id=p["id"], vector=p["vector"], payload=p["payload"]) for p in points]
        self.client.upsert(collection_name=self.collection, points=qdrant_points, wait=True)

    def search(self, *, query_vector, limit: int, filter: Optional[SearchFilter] = None):
        flt = None
        if filter:
            must = []
            if filter.tenant_id:
                must.append(FieldCondition(key="tenant_id", match=MatchValue(value=filter.tenant_id)))
            if filter.doc_ids:
                must.append(FieldCondition(key="doc_id", match=MatchAny(any=filter.doc_ids)))
            if filter.mime_any:
                must.append(FieldCondition(key="mime", match=MatchAny(any=filter.mime_any)))
            if must:
                flt = Filter(must=must)

        return self.client.search(
            collection_name=self.collection,
            query_vector=query_vector,
            with_payload=True,
            limit=limit,
            query_filter=flt
        )
