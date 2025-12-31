from __future__ import annotations
from pydantic import Field, field_validator, computed_field
from pydantic_settings import BaseSettings
from typing import List, Any
import json


class Settings(BaseSettings):
    # App
    app_env: str = Field(default="dev", alias="APP_ENV")
    app_version: str = Field(default="0.1.0", alias="APP_VERSION")
    region: str = Field(default="local", alias="REGION")
    tenant_id: str = Field(default="00000000-0000-0000-0000-000000000001", alias="TENANT_ID")

    # Postgres
    db_host: str = Field(default="db", alias="DB_HOST")
    db_port: int = Field(default=5432, alias="DB_PORT")
    db_name: str = Field(default="ragdb", alias="DB_NAME")
    db_user: str = Field(default="rag", alias="DB_USER")
    db_password: str = Field(default="ragpassword", alias="DB_PASSWORD")

    # Object storage (MinIO/S3)
    s3_endpoint: str = Field(default="http://minio:9000", alias="S3_ENDPOINT")
    s3_public_endpoint: str | None = Field(default=None, alias="S3_PUBLIC_ENDPOINT")
    s3_bucket: str = Field(default="rag-blobs", alias="S3_BUCKET")
    s3_canonical_bucket: str = Field(default="rag-canonical", alias="S3_CANONICAL_BUCKET")
    minio_root_user: str = Field(default="ragminio", alias="MINIO_ROOT_USER")
    minio_root_password: str = Field(default="ragminiopass", alias="MINIO_ROOT_PASSWORD")

    # Qdrant
    qdrant_url: str = Field(default="http://qdrant:6333", alias="QDRANT_URL")
    qdrant_api_key: str | None = Field(default=None, alias="QDRANT_API_KEY")
    qdrant_collection: str = Field(default="chunks_te3large_v1", alias="QDRANT_COLLECTION")
    qdrant_distance: str = Field(default="cosine", alias="QDRANT_DISTANCE")
    embedding_dim: int = Field(default=3072, alias="EMBEDDING_DIM")

    # Ingest limits
    max_files_per_request: int = Field(default=10, alias="MAX_FILES_PER_REQUEST")
    max_file_mb: int = Field(default=50, alias="MAX_FILE_MB")
    ingest_max_filename_len: int = Field(default=200, alias="INGEST_MAX_FILENAME_LEN")
    ingest_disallowed_exts: str = Field(default=".js,.exe,.sh,.bat,.dll,.msi,.apk,.bin", alias="INGEST_DISALLOWED_EXTS")
    ingest_strict_mode: bool = Field(default=False, alias="INGEST_STRICT_MODE")
    ingest_rate_limit_per_min: int = Field(default=120, alias="INGEST_RATE_LIMIT_PER_MIN")

    # Security
    idp_api_key: str | None = Field(default=None, alias="IDP_API_KEY")

    # Extraction & Normalization
    extract_strip_headers: bool = Field(default=True, alias="EXTRACT_STRIP_HEADERS")
    extract_max_blocks: int = Field(default=10000, alias="EXTRACT_MAX_BLOCKS")
    ocr_langs: str = Field(default="en", alias="OCR_LANGS")
    ocr_max_pages: int = Field(default=25, alias="OCR_MAX_PAGES")
    pdf_native_only_if_pages_gt: int = Field(default=300, alias="PDF_NATIVE_ONLY_IF_PAGES_GT")
    parse_method: str = Field(default="auto", alias="PARSE_METHOD")
    parse_auto_ocr_fallback: bool = Field(default=True, alias="PARSE_AUTO_OCR_FALLBACK")
    parse_sparse_text_threshold: int = Field(default=400, alias="PARSE_SPARSE_TEXT_THRESHOLD")

    # Raw env mapping (accepts CSV or JSON array or list). Use computed property for normalized list
    allowed_mime_prefixes_raw: List[str] | str | None = Field(
        default_factory=lambda: [
            "application/pdf",
            "text/plain",
            "text/html",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "image/png",
            "image/jpeg",
            "image/webp",
        ],
        alias="ALLOWED_MIME_PREFIXES",
    )
    @field_validator("allowed_mime_prefixes_raw", mode="before")
    @classmethod
    def _split_csv(cls, v: Any) -> Any:
        if v is None:
            return v
        if isinstance(v, (list, tuple)):
            return [str(s).strip() for s in v if str(s).strip()]
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return []
            # Support JSON array in env or plain CSV
            if s.startswith("[") and s.endswith("]"):
                try:
                    arr = json.loads(s)
                    return [str(x).strip() for x in (arr or []) if str(x).strip()]
                except Exception:
                    # fall through to CSV
                    pass
            return [p.strip() for p in s.split(",") if p.strip()]
        return v

    @computed_field(return_type=List[str])
    def allowed_mime_prefixes(self) -> List[str]:
        v = self.allowed_mime_prefixes_raw
        if v is None:
            return []
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return []
            if s.startswith("[") and s.endswith("]"):
                try:
                    arr = json.loads(s)
                    return [str(x).strip() for x in (arr or []) if str(x).strip()]
                except Exception:
                    pass
            return [p.strip() for p in s.split(",") if p.strip()]
        return [str(x).strip() for x in (v or []) if str(x).strip()]

    # Chunking
    chunk_target_tokens: int = Field(default=800, alias="CHUNK_TARGET_TOKENS")
    chunk_overlap_tokens: int = Field(default=120, alias="CHUNK_OVERLAP_TOKENS")
    max_chunks_per_doc: int = Field(default=5000, alias="MAX_CHUNKS_PER_DOC")

    # CORS
    cors_allow_origins: str | None = Field(default=None, alias="CORS_ALLOW_ORIGINS")

    # Providers
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    gemini_api_key: str | None = Field(default=None, alias="GEMINI_API_KEY")
    
    # Generation
    gen_base_url: str = Field(default="https://generativelanguage.googleapis.com/v1beta/openai/", alias="GEN_BASE_URL")
    gen_model: str = Field(default="gemini-2.0-flash", alias="GEN_MODEL")
    gen_stream_tokens: bool = Field(default=False, alias="GEN_STREAM_TOKENS")
    gen_token_budget: int = Field(default=3500, alias="GEN_TOKEN_BUDGET")
    gen_max_stitch_per_doc: int = Field(default=2, alias="GEN_MAX_STITCH_PER_DOC")
    gen_grounded_min: float = Field(default=0.18, alias="GEN_GROUNDED_MIN")
    fact_conf_min: float = Field(default=0.6, alias="FACT_CONF_MIN")
    stream_chunk_delay_ms: int = Field(default=70, alias="STREAM_CHUNK_DELAY_MS")
    stream_chunk_chars: int = Field(default=64, alias="STREAM_CHUNK_CHARS")

    # Retrieval & Embedding
    embed_model: str = Field(default="text-embedding-3-large", alias="EMBED_MODEL")
    embed_local_model: str = Field(default="BAAI/bge-m3", alias="EMBED_LOCAL_MODEL")
    embed_batch_size: int = Field(default=64, alias="EMBED_BATCH_SIZE")
    vector_topn: int = Field(default=60, alias="VECTOR_TOPN")
    keyword_topn: int = Field(default=100, alias="KEYWORD_TOPN")
    hybrid_alpha: float = Field(default=0.7, alias="HYBRID_ALPHA")
    doc_cap_per_doc: int = Field(default=3, alias="DOC_CAP_PER_DOC")
    retr_safety_net: bool = Field(default=True, alias="RETR_SAFETY_NET")
    hybrid_mode: str = Field(default="rrf", alias="HYBRID_MODE")
    rerank_enabled: bool = Field(default=False, alias="RERANK_ENABLED")
    rerank_topn: int = Field(default=50, alias="RERANK_TOPN")
    rerank_model: str = Field(default="cross-encoder/ms-marco-MiniLM-L-6-v2", alias="RERANK_MODEL")
    mmr_lambda: float = Field(default=0.65, alias="MMR_LAMBDA")
    hyde_enabled: bool = Field(default=False, alias="HYDE_ENABLED")
    contextual_chunking_enabled: bool = Field(default=False, alias="CONTEXTUAL_CHUNKING_ENABLED")

    # Health & Misc
    qdrant_health_timeout: float = Field(default=2.0, alias="QDRANT_HEALTH_TIMEOUT")
    healthz_ttl_seconds: float = Field(default=2.0, alias="HEALTHZ_TTL_SECONDS")

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"


settings = Settings()
