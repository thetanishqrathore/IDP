# app/infra/storage.py
from __future__ import annotations
import os, datetime
from typing import Optional
import boto3
from botocore.client import Config
from core.config import settings

S3_ENDPOINT = settings.s3_endpoint
S3_PUBLIC_ENDPOINT = settings.s3_public_endpoint
S3_REGION   = settings.region
S3_ACCESS   = settings.minio_root_user
S3_SECRET   = settings.minio_root_password
S3_BUCKET   = settings.s3_bucket
S3_CANONICAL_BUCKET = settings.s3_canonical_bucket
APP_ENV = settings.app_env.lower()

def _client(endpoint: str = None):
    url = endpoint or S3_ENDPOINT
    return boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET,
        region_name=S3_REGION,
        config=Config(signature_version="s3v4"),
    )

def presign(key: str, *, bucket: Optional[str] = None, expires: int = 3600) -> Optional[str]:
    if not key:
        return None
    # Use public endpoint for browser-accessible links.
    # Fallback to internal endpoint is not useful for this purpose.
    endpoint = S3_PUBLIC_ENDPOINT
    if not endpoint:
        if APP_ENV == "prod":
            raise RuntimeError("S3_PUBLIC_ENDPOINT must be set in prod for presigned URLs")
        # In dev, we can make a reasonable guess for local docker-compose setups.
        endpoint = "http://localhost:9000"

    cli = _client(endpoint)
    bkt = bucket or S3_BUCKET
    return cli.generate_presigned_url(
        "get_object",
        Params={"Bucket": bkt, "Key": key},
        ExpiresIn=expires,
    )
