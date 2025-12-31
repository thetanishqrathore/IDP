from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def new_artifact_id(prefix: str = "art") -> str:
    return f"{prefix}-{uuid.uuid4()}"


@dataclass
class CanonicalArtifact:
    """Structured representation of a parsed document fragment."""

    artifact_id: str
    type: str
    text: str
    page_idx: Optional[int] = None
    headers: List[str] = field(default_factory=list)
    caption: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    raw_path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "type": self.type,
            "text": self.text,
            "page_idx": self.page_idx,
            "headers": self.headers,
            "caption": self.caption,
            "metadata": self.metadata,
            "raw_path": self.raw_path,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CanonicalArtifact":
        return CanonicalArtifact(
            artifact_id=data.get("artifact_id") or new_artifact_id(),
            type=data.get("type") or "text",
            text=data.get("text") or "",
            page_idx=data.get("page_idx"),
            headers=list(data.get("headers") or []),
            caption=data.get("caption"),
            metadata=data.get("metadata") or {},
            raw_path=data.get("raw_path"),
        )


@dataclass
class CanonicalManifest:
    """Container bundling canonical HTML and structured artefacts."""

    html: str
    tool_name: str
    tool_version: str
    page_count: int
    ocr_pages: int
    artifacts: List[CanonicalArtifact] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)

    def iter_text_blocks(self) -> List[CanonicalArtifact]:
        return [a for a in self.artifacts if a.type in {"paragraph", "header", "list", "code", "text"}]

    def iter_tables(self) -> List[CanonicalArtifact]:
        return [a for a in self.artifacts if a.type == "table"]

    def iter_images(self) -> List[CanonicalArtifact]:
        return [a for a in self.artifacts if a.type == "image"]

    def to_dict(self, include_html: bool = True) -> Dict[str, Any]:
        data = {
            "tool_name": self.tool_name,
            "tool_version": self.tool_version,
            "page_count": self.page_count,
            "ocr_pages": self.ocr_pages,
            "warnings": self.warnings,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "stats": self.stats,
        }
        if include_html:
            data["html"] = self.html
        return data

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CanonicalManifest":
        return CanonicalManifest(
            html=data.get("html") or "",
            tool_name=data.get("tool_name") or "unknown",
            tool_version=str(data.get("tool_version") or ""),
            page_count=int(data.get("page_count") or 0),
            ocr_pages=int(data.get("ocr_pages") or 0),
            warnings=list(data.get("warnings") or []),
            artifacts=[CanonicalArtifact.from_dict(a) for a in data.get("artifacts") or []],
            stats=data.get("stats") or {},
        )
