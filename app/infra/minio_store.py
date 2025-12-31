from __future__ import annotations
from minio import Minio
from urllib.parse import urlparse
from typing import BinaryIO, Optional
import os


class MinioStore:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, *, secure: Optional[bool] = None, bucket: str = "rag-blobs"):
        url = urlparse(endpoint)
        self.secure = secure if secure is not None else (url.scheme == "https")
        self.bucket = bucket
        self.client = Minio(
            f"{url.hostname}:{url.port or (443 if self.secure else 80)}",
            access_key=access_key,
            secret_key=secret_key,
            secure=self.secure,
        )

    def ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    @staticmethod
    def build_key_for_sha256(sha256_hex: str) -> str:
        # sha256/aa/bb/<fullhash>
        return f"sha256/{sha256_hex[0:2]}/{sha256_hex[2:4]}/{sha256_hex}"

    def put_file(self, sha256_hex: str, file_path: str, size: int, content_type: str) -> str:
        key = self.build_key_for_sha256(sha256_hex)
        with open(file_path, "rb") as fp:
            self.client.put_object(self.bucket, key, data=fp, length=size, content_type=content_type)
        return key

    def stat(self, key: str):
        return self.client.stat_object(self.bucket, key)

    def ping(self) -> bool:
        self.client.list_buckets()
        return True
    
    def delete_object(self, key: str, *, bucket: Optional[str] = None):
        target_bucket = bucket or self.bucket
        try:
            self.client.remove_object(target_bucket, key)
        except Exception:
            pass

    def remove_prefix(self, prefix: str, *, bucket: Optional[str] = None):
        target_bucket = bucket or self.bucket
        try:
            objects = self.client.list_objects(target_bucket, prefix=prefix, recursive=True)
            for obj in objects:
                try:
                    self.client.remove_object(target_bucket, obj.object_name)
                except Exception:
                    continue
        except Exception:
            pass

    def fget_to_tmp(self, key: str) -> str:
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()
        self.client.fget_object(self.bucket, key, tmp.name)
        return tmp.name

    def put_canonical_html(self, *, bucket: str, doc_id: str, html: str, version: str = "v1") -> str:
    # key: <doc_id>/<version>/index.html
        key = f"{doc_id}/{version}/index.html"
        import io
        data = io.BytesIO(html.encode("utf-8"))
        length = data.getbuffer().nbytes
        data.seek(0)  # <-- critical: rewind before upload
        self.client.put_object(
            bucket,
            key,
            data=data,
            length=length,
            content_type="text/html; charset=utf-8"
        )
        return key

    def put_canonical_json(self, *, bucket: str, doc_id: str, name: str, payload: dict, version: str = "v1") -> str:
        key = f"{doc_id}/{version}/{name}"
        import io
        import json

        data = io.BytesIO(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        length = data.getbuffer().nbytes
        data.seek(0)
        self.client.put_object(
            bucket,
            key,
            data=data,
            length=length,
            content_type="application/json; charset=utf-8",
        )
        return key
