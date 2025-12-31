from __future__ import annotations
import os, json, uuid
from typing import Any, Dict, List, Optional, Tuple
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json  # add at top

def _json_dumps(obj):  # always JSON-safe
    return json.dumps(obj, default=str)


class DBClient:
    def __init__(self, dsn: Optional[str] = None, *, host=None, port=None, db=None, user=None, password=None):
        # Prefer keyword args over DSN string to avoid quoting issues with special chars
        self._dsn = dsn
        self._conn_kwargs: Optional[dict] = None
        if dsn is None:
            self._conn_kwargs = {
                "host": host,
                "port": port,
                "dbname": db,
                "user": user,
                "password": password,
            }
        self.conn = None

    def connect(self):
        if self.conn is None:
            if self._conn_kwargs is not None:
                self.conn = psycopg.connect(autocommit=True, row_factory=dict_row, **self._conn_kwargs)
            else:
                self.conn = psycopg.connect(self._dsn, autocommit=True, row_factory=dict_row)
        return self.conn

    # ---- schema ----
    def init_schema_hardening(self):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS normalized_at TIMESTAMPTZ NULL;")
            cur.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS extracted_at  TIMESTAMPTZ NULL;")



    def init_schema_phase1_and_2(self):
        self.connect()
        with self.conn.cursor() as cur:
            # events
            cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id UUID PRIMARY KEY,
                tenant_id UUID NOT NULL,
                doc_id UUID NULL,
                stage TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt INT NOT NULL DEFAULT 1,
                ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                latency_ms INT NULL,
                cost_cents NUMERIC(10,2) NULL,
                details_json JSONB NULL,
                trace_id TEXT NULL,
                job_id TEXT NULL
            );
            """)
            # documents
            cur.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                doc_id UUID PRIMARY KEY,
                tenant_id UUID NOT NULL,
                sha256 TEXT NOT NULL,
                uri TEXT NOT NULL,
                mime TEXT NOT NULL,
                size_bytes BIGINT NOT NULL,
                state TEXT NOT NULL,
                collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                pipeline_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
                meta JSONB NULL
            );
            """)
            cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_tenant_sha ON documents(tenant_id, sha256);""")
            cur.execute("""CREATE INDEX IF NOT EXISTS ix_documents_tenant_state ON documents(tenant_id, state);""")
            # blobs
            cur.execute("""
            CREATE TABLE IF NOT EXISTS blobs (
                sha256 TEXT PRIMARY KEY,
                location TEXT NOT NULL,
                crc32 TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
    def init_schema_phase3(self):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS normalizations (
                doc_id UUID PRIMARY KEY REFERENCES documents(doc_id),
                canonical_uri TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                tool_version TEXT NOT NULL,
                manifest_uri TEXT NULL,
                page_count INT NOT NULL DEFAULT 0,
                ocr_pages INT NULL,
                warnings JSONB NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            cur.execute("ALTER TABLE normalizations ADD COLUMN IF NOT EXISTS manifest_uri TEXT NULL;")

    def insert_normalization(self, *, doc_id: str, canonical_uri: str, tool_name: str, tool_version: str,
                              page_count: int, ocr_pages: int | None, warnings: list[str] | None,
                              manifest_uri: str | None):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO normalizations (doc_id, canonical_uri, tool_name, tool_version, manifest_uri,
                                        page_count, ocr_pages, warnings, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (doc_id) DO UPDATE SET canonical_uri=EXCLUDED.canonical_uri,
                                              tool_name=EXCLUDED.tool_name,
                                              tool_version=EXCLUDED.tool_version,
                                              manifest_uri=EXCLUDED.manifest_uri,
                                              page_count=EXCLUDED.page_count,
                                              ocr_pages=EXCLUDED.ocr_pages,
                                              warnings=EXCLUDED.warnings;
            """, (doc_id, canonical_uri, tool_name, tool_version, manifest_uri, page_count, ocr_pages, json.dumps(warnings or [])))
    def init_schema_phase4(self):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS blocks (
                block_id UUID PRIMARY KEY,
                doc_id UUID NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
                page INT NULL,
                span_start INT NOT NULL,
                span_end INT NOT NULL,
                type TEXT NOT NULL,
                text TEXT NOT NULL,
                meta JSONB NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS ix_blocks_doc ON blocks(doc_id);")


    def insert_blocks_bulk(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        adapted = []
        for r in rows:
            item = dict(r)
            if "meta" in item and item["meta"] is not None and not isinstance(item["meta"], str):
                item["meta"] = Json(item["meta"])   # <-- adapt dict -> JSONB
            adapted.append(item)
        with self.conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO blocks (block_id, doc_id, page, span_start, span_end, type, text, meta)
            VALUES (%(block_id)s, %(doc_id)s, %(page)s, %(span_start)s, %(span_end)s, %(type)s, %(text)s, %(meta)s)
            """, adapted)
        return len(adapted)

    def replace_graph(self, doc_id: str, nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> None:
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM kg_edges WHERE doc_id=%s;", (doc_id,))
            cur.execute("DELETE FROM kg_nodes WHERE doc_id=%s;", (doc_id,))

            if nodes:
                node_records = []
                for n in nodes:
                    record = dict(n)
                    meta = record.get("meta")
                    if meta is not None and not isinstance(meta, str):
                        record["meta"] = Json(meta, dumps=_json_dumps)
                    else:
                        record["meta"] = meta
                    node_records.append(record)
                cur.executemany(
                    """
                    INSERT INTO kg_nodes (node_id, doc_id, type, label, meta)
                    VALUES (%(node_id)s, %(doc_id)s, %(type)s, %(label)s, %(meta)s)
                    """,
                    node_records,
                )

            if edges:
                edge_records = []
                for e in edges:
                    record = dict(e)
                    meta = record.get("meta")
                    if meta is not None and not isinstance(meta, str):
                        record["meta"] = Json(meta, dumps=_json_dumps)
                    else:
                        record["meta"] = meta
                    edge_records.append(record)
                cur.executemany(
                    """
                    INSERT INTO kg_edges (edge_id, doc_id, src_node_id, dst_node_id, rel_type, weight, meta)
                    VALUES (%(edge_id)s, %(doc_id)s, %(src_node_id)s, %(dst_node_id)s, %(rel_type)s, %(weight)s, %(meta)s)
                    """,
                    edge_records,
                )

    def fetch_graph_neighbors(self, doc_id: str, block_ids: List[str], limit: int = 32) -> List[Dict[str, Any]]:
        if not block_ids:
            return []
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH target_nodes AS (
                    SELECT node_id, meta->>'source_block_id' AS block_id
                    FROM kg_nodes
                    WHERE doc_id = %s
                      AND meta ? 'source_block_id'
                      AND meta->>'source_block_id' = ANY(%s)
                ),
                outbound AS (
                    SELECT DISTINCT
                        t.block_id AS base_block_id,
                        child.meta->>'source_block_id' AS child_block_id,
                        child.type AS child_type,
                        child.label AS child_label,
                        e.rel_type AS rel_type
                    FROM target_nodes t
                    JOIN kg_edges e ON e.src_node_id = t.node_id
                    JOIN kg_nodes child ON child.node_id = e.dst_node_id
                    WHERE child.doc_id = %s
                      AND child.meta ? 'source_block_id'
                ),
                inbound AS (
                    SELECT DISTINCT
                        t.block_id AS base_block_id,
                        parent.meta->>'source_block_id' AS child_block_id,
                        parent.type AS child_type,
                        parent.label AS child_label,
                        'incoming_' || e.rel_type AS rel_type
                    FROM target_nodes t
                    JOIN kg_edges e ON e.dst_node_id = t.node_id
                    JOIN kg_nodes parent ON parent.node_id = e.src_node_id
                    WHERE parent.doc_id = %s
                      AND parent.meta ? 'source_block_id'
                ),
                siblings AS (
                    SELECT DISTINCT
                        t.block_id AS base_block_id,
                        sibling.meta->>'source_block_id' AS child_block_id,
                        sibling.type AS child_type,
                        sibling.label AS child_label,
                        'shared_parent' AS rel_type
                    FROM target_nodes t
                    JOIN kg_edges e_parent ON e_parent.dst_node_id = t.node_id
                    JOIN kg_nodes parent ON parent.node_id = e_parent.src_node_id
                    JOIN kg_edges e_sib ON e_sib.src_node_id = parent.node_id
                    JOIN kg_nodes sibling ON sibling.node_id = e_sib.dst_node_id
                    WHERE parent.doc_id = %s
                      AND sibling.doc_id = %s
                      AND sibling.meta ? 'source_block_id'
                      AND sibling.node_id <> t.node_id
                )
                SELECT base_block_id,
                       child_block_id,
                       child_type,
                       child_label,
                       rel_type
                FROM (
                    SELECT * FROM outbound
                    UNION ALL
                    SELECT * FROM inbound
                    UNION ALL
                    SELECT * FROM siblings
                ) AS combined
                WHERE child_block_id IS NOT NULL
                LIMIT %s;
                """,
                (doc_id, block_ids, doc_id, doc_id, doc_id, doc_id, limit),
            )
            rows = cur.fetchall() or []
        return rows

    def fetch_chunks_by_block_ids(self, doc_id: str, block_ids: List[str]) -> List[Dict[str, Any]]:
        if not block_ids:
            return []
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.chunk_id::text,
                       c.doc_id::text,
                       c.text,
                       c.meta,
                       c.span_start,
                       c.span_end,
                       c.page_start,
                       c.page_end
                FROM chunks c
                WHERE c.doc_id = %s
                  AND EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements_text(c.meta->'source_block_ids') AS bid(value)
                      WHERE bid.value = ANY(%s)
                  );
                """,
                (doc_id, block_ids),
            )
            rows = cur.fetchall() or []
        return rows
    def init_schema_phase5(self):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS chunk_plans (
                plan_id UUID PRIMARY KEY,
                doc_id UUID NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
                strategy TEXT NOT NULL,
                params JSONB NOT NULL,
                page_span INT[] NULL,
                block_count INT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS ix_chunk_plans_doc ON chunk_plans(doc_id);")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id UUID PRIMARY KEY,
                plan_id UUID NOT NULL REFERENCES chunk_plans(plan_id) ON DELETE CASCADE,
                doc_id UUID NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
                span_start INT NOT NULL,
                span_end INT NOT NULL,
                page_start INT NOT NULL,
                page_end INT NOT NULL,
                text TEXT NOT NULL,
                meta JSONB NULL,
                checksum TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS ix_chunks_doc ON chunks(doc_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_chunks_plan ON chunks(plan_id);")
    
    def init_schema_graph(self):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS kg_nodes (
                node_id UUID PRIMARY KEY,
                doc_id UUID NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
                type TEXT NOT NULL,
                label TEXT NOT NULL,
                meta JSONB NULL
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS kg_edges (
                edge_id UUID PRIMARY KEY,
                doc_id UUID NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
                src_node_id UUID NOT NULL REFERENCES kg_nodes(node_id) ON DELETE CASCADE,
                dst_node_id UUID NOT NULL REFERENCES kg_nodes(node_id) ON DELETE CASCADE,
                rel_type TEXT NOT NULL,
                weight NUMERIC NULL,
                meta JSONB NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS ix_kg_nodes_doc ON kg_nodes(doc_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_kg_edges_doc ON kg_edges(doc_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_kg_edges_src ON kg_edges(src_node_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_kg_edges_dst ON kg_edges(dst_node_id);")
    
    # ---- jobs ----
    def init_schema_jobs(self):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id UUID PRIMARY KEY,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,            -- PENDING | RUNNING | DONE | ERROR
                payload JSONB NULL,
                progress NUMERIC(5,2) NULL,     -- 0..100
                result JSONB NULL,
                error TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS ix_jobs_type_status ON jobs(job_type, status);")

    def insert_job(self, *, job_type: str, payload: Dict[str, Any] | None = None, status: str = "PENDING") -> str:
        import uuid as _uuid
        jid = str(_uuid.uuid4())
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO jobs (job_id, job_type, status, payload, progress, result, error, created_at, updated_at)
                VALUES (%s,%s,%s,%s,NULL,NULL,NULL,NOW(),NOW())
                """,
                (jid, job_type, status, Json(payload or {}, dumps=_json_dumps)),
            )
        return jid

    def update_job(self, job_id: str, *, status: Optional[str] = None, progress: Optional[float] = None,
                   result: Optional[Dict[str, Any]] = None, error: Optional[str] = None, payload: Optional[Dict[str, Any]] = None):
        self.connect()
        sets = ["updated_at=NOW()"]
        params: list[Any] = []
        if status is not None:
            sets.append("status=%s"); params.append(status)
        if progress is not None:
            sets.append("progress=%s"); params.append(progress)
        if result is not None:
            sets.append("result=%s"); params.append(Json(result, dumps=_json_dumps))
        if error is not None:
            sets.append("error=%s"); params.append(error)
        if payload is not None:
            sets.append("payload=%s"); params.append(Json(payload, dumps=_json_dumps))
        if not sets:
            return
        sql = f"UPDATE jobs SET {', '.join(sets)} WHERE job_id=%s;"
        params.append(job_id)
        with self.conn.cursor() as cur:
            cur.execute(sql, params)

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("SELECT job_id::text, job_type, status, payload, progress, result, error, created_at, updated_at FROM jobs WHERE job_id=%s LIMIT 1;", (job_id,))
            row = cur.fetchone()
        return row or None

    # ---- structured entities (invoices, contracts) ----
    def init_schema_structured(self):
        self.connect()
        with self.conn.cursor() as cur:
            # invoices header table (1:1 with documents when applicable)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS invoices (
                invoice_id UUID PRIMARY KEY REFERENCES documents(doc_id) ON DELETE CASCADE,
                vendor TEXT NULL,
                invoice_number TEXT NULL,
                invoice_date DATE NULL,
                due_date DATE NULL,
                total NUMERIC(14,2) NULL,
                currency TEXT NULL,
                meta JSONB NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            # line items (n:1 invoices)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS invoice_line_items (
                id UUID PRIMARY KEY,
                invoice_id UUID NOT NULL REFERENCES invoices(invoice_id) ON DELETE CASCADE,
                description TEXT NULL,
                qty NUMERIC(14,4) NULL,
                unit_price NUMERIC(14,4) NULL,
                amount NUMERIC(14,2) NULL,
                meta JSONB NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS ix_invoice_items_invoice ON invoice_line_items(invoice_id);")

            # contracts header table (1:1 with documents when applicable)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS contracts (
                contract_id UUID PRIMARY KEY REFERENCES documents(doc_id) ON DELETE CASCADE,
                party_a TEXT NULL,
                party_b TEXT NULL,
                effective_date DATE NULL,
                end_date DATE NULL,
                renewal_date DATE NULL,
                governing_law TEXT NULL,
                meta JSONB NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)

    def upsert_invoice(self, *, invoice_id: str, vendor: str | None, invoice_number: str | None,
                       invoice_date: str | None, due_date: str | None, total: float | None,
                       currency: str | None, meta: dict | None):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO invoices (invoice_id, vendor, invoice_number, invoice_date, due_date, total, currency, meta, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                ON CONFLICT (invoice_id) DO UPDATE SET
                    vendor=EXCLUDED.vendor,
                    invoice_number=EXCLUDED.invoice_number,
                    invoice_date=EXCLUDED.invoice_date,
                    due_date=EXCLUDED.due_date,
                    total=EXCLUDED.total,
                    currency=EXCLUDED.currency,
                    meta=EXCLUDED.meta,
                    updated_at=NOW()
                """,
                (invoice_id, vendor, invoice_number, invoice_date, due_date, total, currency, Json(meta or {}, dumps=_json_dumps))
            )

    def replace_invoice_items(self, *, invoice_id: str, items: list[dict]) -> int:
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM invoice_line_items WHERE invoice_id=%s;", (invoice_id,))
            if not items:
                return 0
            adapted = []
            for it in items:
                adapted.append({
                    "id": str(uuid.uuid4()),
                    "invoice_id": invoice_id,
                    "description": it.get("description"),
                    "qty": it.get("qty"),
                    "unit_price": it.get("unit_price"),
                    "amount": it.get("amount"),
                    "meta": Json(it.get("meta") or {}, dumps=_json_dumps),
                })
            cur.executemany("""
                INSERT INTO invoice_line_items (id, invoice_id, description, qty, unit_price, amount, meta)
                VALUES (%(id)s, %(invoice_id)s, %(description)s, %(qty)s, %(unit_price)s, %(amount)s, %(meta)s)
            """, adapted)
            return len(adapted)

    def upsert_contract(self, *, contract_id: str, party_a: str | None, party_b: str | None,
                        effective_date: str | None, end_date: str | None, renewal_date: str | None,
                        governing_law: str | None, meta: dict | None):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO contracts (contract_id, party_a, party_b, effective_date, end_date, renewal_date, governing_law, meta, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                ON CONFLICT (contract_id) DO UPDATE SET
                    party_a=EXCLUDED.party_a,
                    party_b=EXCLUDED.party_b,
                    effective_date=EXCLUDED.effective_date,
                    end_date=EXCLUDED.end_date,
                    renewal_date=EXCLUDED.renewal_date,
                    governing_law=EXCLUDED.governing_law,
                    meta=EXCLUDED.meta,
                    updated_at=NOW()
                """,
                (contract_id, party_a, party_b, effective_date, end_date, renewal_date, governing_law, Json(meta or {}, dumps=_json_dumps))
            )

    def total_spend(self, *, start: str, end: str) -> float:
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(total),0) FROM invoices WHERE invoice_date BETWEEN %s AND %s;", (start, end))
            row = cur.fetchone()
        v = 0.0
        if isinstance(row, dict):
            v = float(list(row.values())[0] or 0.0)
        else:
            v = float((row[0] if row else 0.0) or 0.0)
        return v

    def fetch_blocks_for_doc(self, doc_id: str) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT block_id, page, span_start, span_end, type, text, meta
                FROM blocks
                WHERE doc_id=%s
                ORDER BY span_start ASC
            """, (doc_id,))
            rows = cur.fetchall()
        return rows or []

    def delete_chunks_for_doc(self, doc_id: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM chunks WHERE doc_id=%s;", (doc_id,))
            return cur.rowcount

    def insert_chunk_plan(self, *, doc_id: str, strategy: str, params: Dict[str, Any],
                        page_span: Optional[List[int]], block_count: int) -> str:
        plan_id = str(uuid.uuid4())
        with self.conn.cursor() as cur:
            cur.execute(
        """
        INSERT INTO chunk_plans (plan_id, doc_id, strategy, params, page_span, block_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (plan_id, doc_id, strategy, Json(params, dumps=_json_dumps), page_span, block_count),
    )
        return plan_id

    def insert_chunks_bulk(self, rows: List[Dict[str, Any]]) -> int:
        if not rows: return 0
        adapted = []
        for r in rows:
            x = dict(r)
            if "meta" in x and x["meta"] is not None and not isinstance(x["meta"], str):
                x["meta"] = Json(x["meta"], dumps=_json_dumps)   # <- use safe dumper
            adapted.append(x)
        with self.conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO chunks (chunk_id, plan_id, doc_id, span_start, span_end,
                                    page_start, page_end, text, meta, checksum)
                VALUES (%(chunk_id)s, %(plan_id)s, %(doc_id)s, %(span_start)s, %(span_end)s,
                        %(page_start)s, %(page_end)s, %(text)s, %(meta)s, %(checksum)s)
            """, adapted)
        return len(adapted)
    def fetch_chunks_for_doc(self, doc_id: str):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT chunk_id, plan_id, doc_id, span_start, span_end, page_start, page_end, text, meta
                FROM chunks WHERE doc_id=%s ORDER BY span_start
            """, (doc_id,))
            rows = cur.fetchall()
        return rows or []

    def fetch_neighbor_chunks(self, doc_id: str, span_start: int, direction: str = "next") -> Optional[Dict[str, Any]]:
        self.connect()
        if direction == "next":
            sql = """
            SELECT chunk_id, text, span_start, span_end
            FROM chunks
            WHERE doc_id = %s AND span_start > %s
            ORDER BY span_start ASC
            LIMIT 1
            """
        else:
            # prev
            sql = """
            SELECT chunk_id, text, span_start, span_end
            FROM chunks
            WHERE doc_id = %s AND span_start < %s
            ORDER BY span_start DESC
            LIMIT 1
            """
        with self.conn.cursor() as cur:
            cur.execute(sql, (doc_id, span_start))
            row = cur.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            return row
        # tuple fallback
        return {"chunk_id": str(row[0]), "text": row[1], "span_start": row[2], "span_end": row[3]}

    def fetch_latest_plan_for_doc(self, doc_id: str):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT plan_id, strategy, params
                FROM chunk_plans
                WHERE doc_id=%s
                ORDER BY created_at DESC
                LIMIT 1
            """, (doc_id,))
            row = cur.fetchone()
        return row

    def fetch_document_meta(self, doc_id: str):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT uri, mime, meta FROM documents WHERE doc_id=%s LIMIT 1
            """, (doc_id,))
            row = cur.fetchone()
        return row or {"uri": None, "mime": None, "meta": {}}
    def ensure_chunks_fts_index(self):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE INDEX IF NOT EXISTS ix_chunks_tsv
            ON chunks USING GIN (to_tsvector('english', text));
            """)
        return True
    def get_doc_storage_keys(self, doc_id: str):
        """Return {uri, minio_key, canonical_uri}.
        - uri: the original URI provided on ingest (may be a filename)
        - minio_key: derived from documents.sha256 (sha256/aa/bb/hash) if available
        - canonical_uri: path in canonical bucket from normalizations (preferred for browser viewing)
        """
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.uri, d.sha256, n.canonical_uri
                FROM documents d
                LEFT JOIN normalizations n ON n.doc_id = d.doc_id
                WHERE d.doc_id=%s
                LIMIT 1
                """,
                (doc_id,),
            )
            row = cur.fetchone()
        if not row:
            return {"uri": None, "minio_key": None, "canonical_uri": None}
        if isinstance(row, dict):
            uri = row.get("uri")
            sha = row.get("sha256")
            can = row.get("canonical_uri")
        else:
            uri, sha, can = row

        minio_key = None
        if sha:
            s = str(sha)
            if len(s) >= 4:
                minio_key = f"sha256/{s[0:2]}/{s[2:4]}/{s}"
        return {"uri": uri, "minio_key": minio_key, "canonical_uri": can}

    def wipe_tenant_data(self, tenant_id: str) -> Dict[str, Any]:
        """
        Delete all persisted artifacts for a tenant and return a summary of what needs cleanup
        outside the database (e.g., MinIO objects, Qdrant vectors).
        """
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    d.doc_id::text AS doc_id,
                    d.sha256::text AS sha256,
                    n.canonical_uri AS canonical_uri
                FROM documents d
                LEFT JOIN normalizations n ON n.doc_id = d.doc_id
                WHERE d.tenant_id = %s;
                """,
                (tenant_id,),
            )
            rows = cur.fetchall() or []

        doc_records: List[Dict[str, Any]] = []
        sha_set: set[str] = set()
        canon_prefixes: set[str] = set()

        for row in rows:
            if isinstance(row, dict):
                doc_id = row.get("doc_id")
                sha = row.get("sha256")
                canonical_uri = row.get("canonical_uri")
            else:
                doc_id, sha, canonical_uri = row
            if not doc_id:
                continue
            doc_records.append({"doc_id": str(doc_id), "sha256": sha, "canonical_uri": canonical_uri})
            if sha:
                sha_set.add(str(sha))
            if canonical_uri:
                prefix = str(canonical_uri).split("/", 1)[0]
                if prefix:
                    canon_prefixes.add(prefix)
            else:
                canon_prefixes.add(str(doc_id))

        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM events WHERE tenant_id=%s;", (tenant_id,))
            events_deleted = cur.rowcount

            cur.execute("DELETE FROM documents WHERE tenant_id=%s;", (tenant_id,))
            docs_deleted = cur.rowcount

            if sha_set:
                cur.execute("DELETE FROM blobs WHERE sha256 = ANY(%s);", (list(sha_set),))
                blobs_deleted = cur.rowcount
            else:
                blobs_deleted = 0

            cur.execute("DELETE FROM jobs;")
            jobs_deleted = cur.rowcount

        return {
            "doc_records": doc_records,
            "sha256": list(sha_set),
            "canonical_prefixes": list(canon_prefixes),
            "deleted": {
                "documents": docs_deleted,
                "events": events_deleted,
                "blobs": blobs_deleted,
                "jobs": jobs_deleted,
            },
        }
    def find_doc_ids_by_terms(self, terms: list[str], limit: int = 50) -> list[str]:
        """
        Return distinct doc_ids whose chunks.text contain ALL terms (websearch AND semantics).
        Uses the existing GIN index on to_tsvector(text) for speed.
        """
        terms = [t.strip() for t in terms if t and t.strip()]
        if not terms:
            return []
        q = " ".join(terms)  # spaces act like AND for websearch_to_tsquery
        self.connect()
        sql = """
        WITH wq AS (SELECT websearch_to_tsquery('english', %s) AS q)
        SELECT DISTINCT c.doc_id::text
        FROM chunks c
        CROSS JOIN wq
        WHERE to_tsvector('english', c.text) @@ wq.q
        LIMIT %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (q, limit))
            rows = cur.fetchall()
        # tolerate both tuple/dict cursor types
        out: list[str] = []
        for r in rows:
            if isinstance(r, dict):
                out.append(str(r.get("doc_id")))
            else:
                out.append(str(r[0]))
        return out

    def keyword_search_chunks(self, *, q: str, limit: int = 100,
                            doc_ids: Optional[list[str]] = None,
                            types_any: Optional[list[str]] = None,
                            tenant_id: Optional[str] = None,
                            mime_any: Optional[list[str]] = None,
                            uri_like: Optional[str] = None,
                            filename_like: Optional[str] = None,
                            vendor_like: Optional[str] = None) -> list[dict]:
        """
        Full-text search over chunks.text with optional doc_id and types filters.
        Returns dicts with chunk + document info and rank (higher is better).
        """
        self.connect()

        where = ["to_tsvector('english', c.text) @@ wq.q", "d.state != 'DELETED'"]
        params: list = [q]  # first param binds the CTE websearch_to_tsquery

        if tenant_id:
            where.append("d.tenant_id = %s")
            params.append(tenant_id)

        if doc_ids:
            where.append("c.doc_id = ANY(%s)")
            params.append(doc_ids)

        if types_any:
            where.append("""
            EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(c.meta->'types') AS t(val)
            WHERE t.val = ANY(%s)
            )
            """)
            params.append(types_any)

        if mime_any:
            where.append("d.mime = ANY(%s)")
            params.append(mime_any)
        if uri_like:
            where.append("d.uri ILIKE %s")
            params.append(f"%{uri_like}%")
        if filename_like:
            where.append("(d.meta->>'filename') ILIKE %s")
            params.append(f"%{filename_like}%")
        # Optional vendor via invoices join (left join below)
        if vendor_like:
            where.append("(inv.vendor ILIKE %s)")
            params.append(f"%{vendor_like}%")

        sql = f"""
        WITH wq AS (
        SELECT websearch_to_tsquery('english', %s) AS q
        )
        SELECT
        c.chunk_id::text,
        c.doc_id::text,
        c.plan_id::text,
        c.page_start, c.page_end,
        c.span_start, c.span_end,
        c.text,
        c.meta,
        d.uri,
        d.mime,
        n.canonical_uri,
        ts_rank_cd(to_tsvector('english', c.text), wq.q) AS rank
        FROM chunks c
        JOIN documents d ON d.doc_id = c.doc_id
        LEFT JOIN normalizations n ON n.doc_id = c.doc_id
        LEFT JOIN invoices inv ON inv.invoice_id = c.doc_id
        CROSS JOIN wq
        WHERE {" AND ".join(where)}
        ORDER BY rank DESC
        LIMIT %s
        """
        params2 = params + [limit]  # placeholders now match exactly

        with self.conn.cursor() as cur:
            cur.execute(sql, tuple(params2))
            rows = cur.fetchall()

        out: list[dict] = []
        for r in rows or []:
            if isinstance(r, dict):
                chunk_id = r.get("chunk_id")
                doc_id = r.get("doc_id")
                plan_id = r.get("plan_id")
                page_start = r.get("page_start")
                page_end = r.get("page_end")
                span_start = r.get("span_start")
                span_end = r.get("span_end")
                text = r.get("text")
                meta = r.get("meta")
                uri = r.get("uri")
                mime = r.get("mime")
                canonical_uri = r.get("canonical_uri")
                rank = r.get("rank")
            else:
                (chunk_id, doc_id, plan_id, page_start, page_end, span_start, span_end, text, meta, uri, mime, canonical_uri, rank) = r

            out.append({
                "chunk_id": str(chunk_id),
                "doc_id": str(doc_id),
                "plan_id": str(plan_id),
                "page_start": int(page_start or 1),
                "page_end": int(page_end or (page_start or 1)),
                "span_start": int(span_start or 0),
                "span_end": int(span_end or 0),
                "text": text,
                "meta": meta or {},
                "uri": uri,
                "mime": mime,
                "canonical_uri": canonical_uri,
                "rank": float(rank or 0.0),
            })
        return out

    def get_dashboard_stats(self, tenant_id: str) -> Dict[str, Any]:
        self.connect()
        with self.conn.cursor() as cur:
            # Docs count
            cur.execute("SELECT COUNT(*) FROM documents WHERE tenant_id=%s AND state != 'DELETED';", (tenant_id,))
            row = cur.fetchone()
            doc_count = 0
            if row:
                doc_count = row[0] if not isinstance(row, dict) else list(row.values())[0]
            
            # Queries last 24h
            cur.execute("SELECT COUNT(*) FROM events WHERE tenant_id=%s AND stage='GENERATE' AND ts > NOW() - INTERVAL '24 HOURS';", (tenant_id,))
            row = cur.fetchone()
            query_count = 0
            if row:
                query_count = row[0] if not isinstance(row, dict) else list(row.values())[0]
            
            # Total chunks (knowledge density)
            # We need to join chunks->documents to check tenant_id
            cur.execute("SELECT COUNT(*) FROM chunks c JOIN documents d ON c.doc_id=d.doc_id WHERE d.tenant_id=%s AND d.state != 'DELETED';", (tenant_id,))
            row = cur.fetchone()
            chunk_count = 0
            if row:
                chunk_count = row[0] if not isinstance(row, dict) else list(row.values())[0]

        return {
            "documents": doc_count,
            "queries_24h": query_count,
            "chunks": chunk_count
        }

    def get_ingestion_history(self, tenant_id: str) -> List[Dict[str, Any]]:
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    date_trunc('hour', collected_at) as bucket, 
                    COUNT(*) as count 
                FROM documents 
                WHERE tenant_id=%s 
                  AND collected_at > NOW() - INTERVAL '24 HOURS'
                GROUP BY 1 
                ORDER BY 1 ASC;
            """, (tenant_id,))
            rows = cur.fetchall() or []
        
        # Fill gaps logic could be here, but frontend can handle sparse data or we just return what we have
        out = []
        for r in rows:
            if isinstance(r, dict):
                out.append({"ts": r["bucket"], "count": r["count"]})
            else:
                out.append({"ts": r[0], "count": r[1]})
        return out

    def fetch_recent_activity(self, tenant_id: str, limit: int = 50, filter_mode: str = "ALL") -> List[Dict[str, Any]]:
        self.connect()
        # Build SQL based on filter
        where_clauses = ["e.tenant_id = %s"]
        params = [tenant_id]
        
        if filter_mode == "INGEST":
            where_clauses.append("e.stage IN ('STORED', 'NORMALIZED', 'EXTRACTED', 'CHUNKED', 'EMBEDDED')")
        elif filter_mode == "QUERY":
            where_clauses.append("e.stage = 'GENERATE'")
        elif filter_mode == "ERROR":
            where_clauses.append("e.status IN ('ERROR', 'FAIL', 'WARN')")
            
        where_str = " AND ".join(where_clauses)
        
        sql = f"""
        SELECT 
            e.event_id::text AS id,
            e.stage,
            e.status,
            e.details_json AS details,
            e.ts AS created_at,
            d.uri AS doc_uri,
            e.doc_id::text AS doc_id
        FROM events e
        LEFT JOIN documents d ON e.doc_id = d.doc_id
        WHERE {where_str}
        ORDER BY e.ts DESC
        LIMIT %s;
        """
        params.append(limit)
        
        with self.conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        
        out = []
        for r in rows:
            if isinstance(r, dict):
                # map to frontend expected format
                stage = r["stage"]
                details = r["details"] or {}
                
                # Determine "Title" based on stage
                title = stage
                if stage == 'GENERATE':
                    title = details.get('q') or "User Query"
                elif stage == 'STORED':
                    title = details.get('filename') or r.get('doc_uri') or "File Upload"
                elif stage == 'NORMALIZED':
                    title = "Normalized Document"
                elif stage == 'CHUNKED':
                    title = f"Chunked ({details.get('chunks')} parts)"
                elif stage == 'EMBEDDED':
                    title = f"Embedded ({details.get('count')} vectors)"
                else:
                    title = f"System: {stage}"

                # "Type" for icon selection
                etype = 'system'
                if stage == 'GENERATE': etype = 'query'
                elif stage == 'STORED': etype = 'ingest'
                elif stage in ('NORMALIZED', 'EXTRACTED', 'CHUNKED', 'EMBEDDED'): etype = 'process'

                # Explicitly resolve document name for UI grouping
                doc_name = details.get('filename') or r.get('doc_uri')
                if doc_name:
                    doc_name = str(doc_name).split('/')[-1]

                out.append({
                    "id": str(r["event_id"] if r.get("event_id") else r.get("id")), 
                    "doc_id": r.get("doc_id"),
                    "type": etype,
                    "title": title,
                    "document_name": doc_name,
                    "status": r["status"],
                    "created_at": r["created_at"],
                    "details": details,
                    "stage": stage # pass raw stage for log rendering
                })
            else:
                # tuple fallback (id, stage, status, details, ts, doc_uri, doc_id)
                # Note: verify index if SELECT columns changed. 
                # SELECT id, stage, status, details, ts, uri, doc_id
                # 0, 1, 2, 3, 4, 5, 6
                stage = r[1]
                details = r[3] or {}
                title = stage
                if stage == 'GENERATE': title = details.get('q') or "User Query"
                elif stage == 'STORED': title = details.get('filename') or r[5] or "File Upload"
                
                etype = 'system'
                if stage == 'GENERATE': etype = 'query'
                elif stage == 'STORED': etype = 'ingest'
                elif stage in ('NORMALIZED', 'EXTRACTED', 'CHUNKED', 'EMBEDDED'): etype = 'process'

                doc_name = details.get('filename') or r[5]
                if doc_name:
                    doc_name = str(doc_name).split('/')[-1]

                out.append({
                    "id": str(r[0]), "doc_id": str(r[6]) if r[6] else None, 
                    "type": etype, "title": title, "document_name": doc_name,
                    "status": r[2],
                    "created_at": r[4], "details": details, "stage": stage
                })
        return out

    def ensure_perf_indexes(self) -> bool:
        """Create helpful indexes for multi-tenant and retrieval workloads."""
        self.connect()
        with self.conn.cursor() as cur:
            # events by tenant and time
            cur.execute("""
            CREATE INDEX IF NOT EXISTS ix_events_tenant_ts ON events(tenant_id, ts);
            """)
            # documents by tenant and collected time
            cur.execute("""
            CREATE INDEX IF NOT EXISTS ix_documents_tenant_collected ON documents(tenant_id, collected_at);
            """)
            # chunks composite for span scans
            cur.execute("""
            CREATE INDEX IF NOT EXISTS ix_chunks_doc_span ON chunks(doc_id, span_start);
            """)
            # chunk_id PK implies index, but add explicit for completeness (no-op if already exists)
            cur.execute("""
            CREATE INDEX IF NOT EXISTS ix_chunks_id ON chunks(chunk_id);
            """)
        return True

    def find_invoice_doc_ids_by_number_like(self, token: str, limit: int = 50) -> list[str]:
        self.connect()
        token = (token or '').strip()
        if not token:
            return []
        sql = "SELECT invoice_id::text FROM invoices WHERE invoice_number ILIKE %s LIMIT %s;"
        with self.conn.cursor() as cur:
            cur.execute(sql, (f"%{token}%", limit))
            rows = cur.fetchall() or []
        out: list[str] = []
        for r in rows:
            if isinstance(r, dict):
                out.append(str(list(r.values())[0]))
            else:
                out.append(str(r[0]))
        return out

    # ---- structured helpers for retrieval ----
    def find_invoice_doc_ids_between(self, *, start: str, end: str, limit: int = 1000) -> list[str]:
        """Return doc_ids for invoices whose invoice_date is within [start, end].
        Dates are ISO strings YYYY-MM-DD.
        """
        self.connect()
        sql = """
        SELECT invoice_id::text
        FROM invoices
        WHERE invoice_date IS NOT NULL
          AND invoice_date BETWEEN %s AND %s
        LIMIT %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (start, end, limit))
            rows = cur.fetchall()
        out: list[str] = []
        for r in rows:
            if isinstance(r, dict):
                out.append(str(list(r.values())[0]))
            else:
                out.append(str(r[0]))
        return out


    # ---- events ----
    def insert_event(
        self,
        tenant_id: str,
        *,
        stage: str,
        status: str,
        details: Dict[str, Any],
        doc_id: Optional[str] = None,
    ):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events (
                    event_id, tenant_id, doc_id, stage, status,
                    attempt, ts, details_json, trace_id, job_id
                )
                VALUES (%s, %s, %s, %s, %s, 1, NOW(), %s, %s, %s)
                """,
                (
                    str(uuid.uuid4()),
                    tenant_id,
                    doc_id,
                    stage,
                    status,
                    json.dumps(details, default=str),  # JSON-safe
                    str(uuid.uuid4()),
                    None,
                ),
            )


    # ---- documents ----
    def find_doc_by_hash(self, tenant_id: str, sha256: str) -> Optional[str]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT doc_id FROM documents WHERE tenant_id=%s AND sha256=%s LIMIT 1;", (tenant_id, sha256))
            row = cur.fetchone()
            if not row:
                return None
            # psycopg may return UUID objects; normalize to str for callers
            did = row["doc_id"] if isinstance(row, dict) else row[0]
            return str(did) if did is not None else None

    def insert_document(self, *, doc_id: str, tenant_id: str, sha256: str, uri: str, mime: str,
                        size_bytes: int, state: str, pipeline_versions: Dict[str, Any], meta: Dict[str, Any]):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO documents (doc_id, tenant_id, sha256, uri, mime, size_bytes, state, collected_at, pipeline_versions, meta)
                VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s)
            """, (doc_id, tenant_id, sha256, uri, mime, size_bytes, state, json.dumps(pipeline_versions), json.dumps(meta)))

    # ---- blobs ----
    def upsert_blob(self, *, sha256: str, location: str, crc32: Optional[str]):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO blobs (sha256, location, crc32, created_at)
            VALUES (%s,%s,%s,NOW())
            ON CONFLICT (sha256) DO UPDATE SET location=EXCLUDED.location, crc32=EXCLUDED.crc32;
            """, (sha256, location, crc32))

    def ping(self) -> bool:
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok;")
            row = cur.fetchone()
            return row and row["ok"] == 1
        
    def delete_blocks_for_doc(self, doc_id: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM blocks WHERE doc_id=%s;", (doc_id,))
            return cur.rowcount

    def update_document_state(self, doc_id: str, state: str, ts_column: str | None = None):
        with self.conn.cursor() as cur:
            if ts_column:
                cur.execute(f"UPDATE documents SET state=%s, {ts_column}=NOW() WHERE doc_id=%s;", (state, doc_id))
            else:
                cur.execute("UPDATE documents SET state=%s WHERE doc_id=%s;", (state, doc_id))
