from __future__ import annotations

from typing import Dict, Any

from fastapi import APIRouter

from infra.db import DBClient


def create_metrics_router(db: DBClient) -> APIRouter:
    r = APIRouter(prefix="/metrics", tags=["metrics"])

    @r.get("/pipeline_summary")
    def pipeline_summary(limit: int = 20) -> Dict[str, Any]:
        db.connect()
        with db.conn.cursor() as cur:
            cur.execute(
                """
                SELECT details_json
                FROM events
                WHERE stage = 'PIPELINE'
                ORDER BY ts DESC
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cur.fetchall() or []

        totals: Dict[str, list[float]] = {}
        count = 0
        for row in rows:
            details = row["details_json"] if isinstance(row, dict) else row[0]
            timings = (details or {}).get("timings") or {}
            if not timings:
                continue
            count += 1
            for key, value in timings.items():
                try:
                    val = float(value)
                except Exception:
                    continue
                totals.setdefault(key, []).append(val)

        averages = {k: round(sum(v) / len(v), 2) for k, v in totals.items() if v}
        return {"records": count, "avg_ms": averages}

    return r
