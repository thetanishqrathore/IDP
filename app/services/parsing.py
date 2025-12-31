from __future__ import annotations

import importlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Protocol, Dict, Any

try:
    import markdown as markdown_lib
except ImportError:
    markdown_lib = None

from .manifests import CanonicalArtifact, CanonicalManifest, new_artifact_id
from .parsers.external import MineruParser, DoclingParser


class ParserAdapter(Protocol):
    def parse(
        self,
        file_path: str,
        mime: str,
        *,
        parse_method: str = "auto",
        prefer: Optional[str] = None,
    ) -> Optional[CanonicalManifest]:
        ...


@dataclass
class ParserOptions:
    prefer: Optional[str] = None  # "mineru" | "docling" | None
    parse_method: str = "auto"
    auto_ocr_fallback: bool = True
    sparse_text_threshold: int = 400


class AdvancedParserAdapter:
    """
    Wrapper around local advanced parsers (MinerU, Docling) to obtain structured content.
    """

    def __init__(self) -> None:
        self._mineru = None
        self._docling = None

        try:
            self._mineru = MineruParser()
        except Exception:
            self._mineru = None
        try:
            self._docling = DoclingParser()
        except Exception:
            self._docling = None
            
        self._enabled = (self._mineru is not None or self._docling is not None)

    def _is_scanned_pdf(self, file_path: str) -> bool:
        """
        Quickly check if a PDF is likely scanned (image-only) using PyMuPDF.
        Returns True if text density is low.
        """
        try:
            import fitz  # type: ignore
            doc = fitz.open(file_path)
            total_text_chars = 0
            # Check up to first 5 pages to save time on large docs
            pages_to_check = min(5, doc.page_count)
            for i in range(pages_to_check):
                try:
                    text = doc[i].get_text("text") or ""
                    total_text_chars += len(text.strip())
                except Exception:
                    pass
            doc.close()
            # Threshold: average < 50 chars per page is suspicious for a text doc
            return total_text_chars < (pages_to_check * 50)
        except Exception:
            return False

    def parse(
        self,
        file_path: str,
        mime: str,
        *,
        parse_method: str = "auto",
        prefer: Optional[str] = None,
    ) -> Optional[CanonicalManifest]:
        if not self._enabled:
            return None
        
        # Optimization: If PDF is scanned, skip 'auto' layout analysis and go straight to OCR.
        # This keeps Mineru as the parser but avoids the "double-parse" penalty.
        if parse_method == "auto" and "pdf" in (mime or "").lower():
            if self._is_scanned_pdf(file_path):
                parse_method = "ocr"

        parser = self._select_parser(mime, prefer=prefer)
        if not parser:
            return None

        try:
            parse_fn = getattr(parser, "parse_document", None) or getattr(parser, "parse", None)
            if not parse_fn:
                return None
            result = parse_fn(file_path, method=parse_method)
        except Exception:
            return None

        # --- Smart Fallback for Tables & Content ---
        # If we used one parser but found 0 tables OR very little text, and we have the other parser, try it.
        # This is "self-deciding" logic for robustness.
        primary_table_count = 0
        total_text_chars = 0
        
        content_for_stats = []
        if isinstance(result, dict):
             content_for_stats = result.get("content_list") or []
        elif isinstance(result, list):
             content_for_stats = result
             
        for x in content_for_stats:
            if not isinstance(x, dict): continue
            if x.get("type") == "table":
                primary_table_count += 1
            text_len = len((x.get("text") or "").strip())
            total_text_chars += text_len

        # Fallback trigger: No tables found OR text is suspiciously sparse (< 500 chars for a whole PDF)
        # This handles cases where one parser fails to OCR a "Ghost Table" invoice.
        needs_fallback = (primary_table_count == 0) or (total_text_chars < 500)

        if needs_fallback and "pdf" in (mime or "").lower() and not prefer:
            secondary_parser = None
            if parser == self._mineru and self._docling:
                secondary_parser = self._docling
            elif parser == self._docling and self._mineru:
                secondary_parser = self._mineru
            
            if secondary_parser:
                try:
                    s_fn = getattr(secondary_parser, "parse_document", None) or getattr(secondary_parser, "parse", None)
                    if s_fn:
                        s_result = s_fn(file_path, method=parse_method)
                        
                        s_table_count = 0
                        s_text_chars = 0
                        s_cl = []
                        if isinstance(s_result, dict):
                            s_cl = s_result.get("content_list") or []
                        elif isinstance(s_result, list):
                            s_cl = s_result
                        
                        for x in s_cl:
                            if not isinstance(x, dict): continue
                            if x.get("type") == "table":
                                s_table_count += 1
                            s_text_chars += len((x.get("text") or "").strip())
                        
                        # If secondary found tables OR significantly more text, use it!
                        if s_table_count > primary_table_count or s_text_chars > (total_text_chars * 1.5):
                            result = s_result
                            # Update parser ref just for correctness if needed later, though mainly we need 'result'
                            parser = secondary_parser
                except Exception:
                    pass
        # ---------------------------------

        content_list: Optional[List[dict]] = None
        html: Optional[str] = None
        md_text: Optional[str] = None
        warnings: List[str] = []
        page_count = 0
        ocr_pages = 0

        if isinstance(result, dict):
            content_list = result.get("content_list")
            html = result.get("html")
            md_text = result.get("markdown")
            warnings = result.get("warnings") or []
            page_count = int(result.get("page_count") or 0)
            ocr_pages = int(result.get("ocr_pages") or 0)
        elif isinstance(result, list):
            content_list = result

        if not content_list:
            return None

        artifacts: List[CanonicalArtifact] = []
        stats: Dict[str, Any] = {
            "artifact_counts": {},
            "text_chars": 0,
            "non_text_chars": 0,
            "tables": 0,
            "images": 0,
        }

        for item in content_list:
            if not isinstance(item, dict):
                continue
            atype = (item.get("type") or "text").lower()
            text = (item.get("text") or "").strip()

            stats["artifact_counts"][atype] = stats["artifact_counts"].get(atype, 0) + 1
            if text:
                stats["text_chars"] += len(text)
            elif atype != "image" and item.get("raw_text"):
                stats["non_text_chars"] += len(item.get("raw_text") or "")

            if atype == "table":
                stats["tables"] += 1
            if atype == "image":
                stats["images"] += 1

            metadata = item.get("meta") or {}
            for key in ("bbox", "layout_type", "confidence", "rotation"):
                if item.get(key) is not None:
                    metadata[key] = item.get(key)
            if item.get("table_html"):
                metadata["table_html"] = item.get("table_html")
            if item.get("table_markdown"):
                metadata["table_markdown"] = item.get("table_markdown")
            if item.get("table_body"):
                metadata["table_body"] = item.get("table_body")
            if item.get("section"):
                metadata["section"] = item.get("section")

            artifact = CanonicalArtifact(
                artifact_id=new_artifact_id(),
                type=atype,
                text=text,
                page_idx=item.get("page_idx"),
                headers=item.get("headers") or [],
                caption=item.get("caption"),
                metadata=metadata,
                raw_path=item.get("img_path") or item.get("file_path") or item.get("image_path"),
            )
            artifacts.append(artifact)

        if not html:
            # 1. Try to generate HTML from Markdown if available (Best Quality)
            if md_text and markdown_lib:
                try:
                    # 'extra' enables tables, fenced_code, etc.
                    html = markdown_lib.markdown(md_text, extensions=['extra', 'nl2br'])
                    html = f"<!doctype html><html><body>{html}</body></html>"
                except Exception:
                    pass
            
            # 2. Fallback: Synthesize item-by-item
            if not html:
                parts = []
                for a in artifacts:
                    if a.type == "table":
                        # If table HTML exists in metadata (e.g. from Docling), use it
                        if a.metadata.get("table_html"):
                            parts.append(a.metadata["table_html"])
                        # Otherwise wrap the text (likely markdown) in pre so it preserves alignment
                        else:
                            import html as _html
                            safe_text = _html.escape(a.text or "")
                            parts.append(f"<pre class='markdown-table'>{safe_text}</pre>")
                    elif a.type == "image":
                        src = a.raw_path or ""
                        alt = a.metadata.get("alt") or "Image"
                        parts.append(f"<figure><img src='{src}' alt='{alt}'/><figcaption>{a.caption or ''}</figcaption></figure>")
                    else:
                        # Paragraphs
                        import html as _html
                        safe_text = _html.escape(a.text or "")
                        parts.append(f"<p>{safe_text}</p>")
                        
                body = "\n".join(parts)
                html = f"<!doctype html><html><body>{body}</body></html>"

        stats["page_count_detected"] = page_count or (max((a.page_idx or 0) for a in artifacts) + 1 if artifacts else 0)
        stats["ocr_pages"] = ocr_pages
        stats.setdefault("warnings", warnings)
        stats.setdefault("artifact_total", len(artifacts))

        return CanonicalManifest(
            html=html,
            tool_name="advanced-local",
            tool_version="v1",
            page_count=page_count or max((a.page_idx or 0) for a in artifacts) + 1,
            ocr_pages=ocr_pages,
            artifacts=artifacts,
            warnings=warnings,
            stats=stats,
        )

    def _select_parser(self, mime: str, prefer: Optional[str] = None):
        prefer = (prefer or "").lower()
        if prefer == "docling" and self._docling:
            return self._docling
        if prefer == "mineru" and self._mineru:
            return self._mineru
        if "pdf" in mime and self._mineru:
            return self._mineru
        if mime.endswith("html") or "text" in mime:
            return self._docling or self._mineru
        if "excel" in mime or mime.endswith("spreadsheet"):
            return self._docling or self._mineru
        return self._mineru or self._docling


class SimpleFallbackAdapter:
    """Very lightweight parser used when advanced tooling is missing."""

    def parse(
        self,
        file_path: str,
        mime: str,
        *,
        parse_method: str = "auto",
        prefer: Optional[str] = None,
    ) -> Optional[CanonicalManifest]:
        path = Path(file_path)
        text = path.read_text(encoding="utf-8", errors="ignore")
        artifact = CanonicalArtifact(
            artifact_id=new_artifact_id(),
            type="text",
            text=text,
            page_idx=0,
        )
        html = "<!doctype html><html><body><pre>{}</pre></body></html>".format(
            text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        )
        return CanonicalManifest(
            html=html,
            tool_name="simple-fallback",
            tool_version="1.0",
            page_count=1,
            ocr_pages=0,
            artifacts=[artifact],
            warnings=["advanced_parser_unavailable"],
        )


class DocumentParserManager:
    def __init__(self, adapters: Optional[Iterable[ParserAdapter]] = None):
        self.adapters: List[ParserAdapter] = list(adapters) if adapters else [AdvancedParserAdapter(), SimpleFallbackAdapter()]

    def parse(self, file_path: str, mime: str, *, options: Optional[ParserOptions] = None) -> CanonicalManifest:
        opts = options or ParserOptions()
        parse_method = opts.parse_method
        for adapter in self.adapters:
            manifest = adapter.parse(
                file_path,
                mime,
                parse_method=parse_method,
                prefer=opts.prefer,
            )
            if not manifest:
                continue

            # Optional OCR fallback when content looks sparse
            if (
                isinstance(adapter, AdvancedParserAdapter)
                and opts.auto_ocr_fallback
                and parse_method == "auto"
                and "pdf" in (mime or "").lower()
            ):
                text_chars = manifest.stats.get("text_chars", 0)
                if text_chars < opts.sparse_text_threshold:
                    ocr_manifest = adapter.parse(file_path, mime, parse_method="ocr")
                    if ocr_manifest and ocr_manifest.stats.get("text_chars", 0) > text_chars:
                        return ocr_manifest

            return manifest

        raise RuntimeError("no_parser_available")