from typing import List, Dict, Any, Protocol, Optional, Iterator
from dataclasses import dataclass

@dataclass
class SearchFilter:
    tenant_id: str
    doc_ids: Optional[List[str]] = None
    mime_any: Optional[List[str]] = None

class VectorStore(Protocol):
    @property
    def dim(self) -> int:
        ...

    @property
    def collection(self) -> str:
        ...

    def ensure_collection(self) -> bool:
        ...

    def delete_doc(self, doc_id: str) -> None:
        ...

    def delete_tenant(self, tenant_id: str) -> None:
        ...

    def delete_points(self, ids: List[str]) -> None:
        ...

    def upsert_points(self, points: List[Dict[str, Any]]) -> None:
        ...

    def search(self, *, query_vector: List[float], limit: int, filter: Optional[SearchFilter] = None) -> Any:
        ...

    def get_existing_checksums(self, doc_id: str) -> Dict[str, str]:
        ...

class LLMProvider(Protocol):
    def generate(self, messages: List[Dict[str, str]], **kwargs) -> str:
        ...

    def generate_json(self, messages: List[Dict[str, str]], **kwargs) -> str:
        ...

    def stream(self, messages: List[Dict[str, str]], **kwargs) -> Iterator[str]:
        ...
