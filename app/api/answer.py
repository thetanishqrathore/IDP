# app/api/answer.py
from __future__ import annotations
from typing import Any, Dict, Optional
from fastapi import APIRouter, HTTPException, Body
from fastapi.responses import StreamingResponse
import os
import json as _json
import asyncio
import threading

from services.generation import GenerationService
from fastapi.concurrency import run_in_threadpool
from core.config import settings

def create_answer_router(gen: GenerationService) -> APIRouter:
    router = APIRouter(prefix="", tags=["answer"])

    @router.post("/answer")
    async def answer(
        q: str = Body(..., embed=True),
        k: int = Body(8),
        filters: Optional[Dict[str, Any]] = Body(default=None)
    ):
        if not q or not q.strip():
            raise HTTPException(status_code=400, detail="empty_query")
        try:
            return await run_in_threadpool(gen.answer, q=q, k=k, filters=(filters or {}))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"answer_failed: {e}")

    @router.post("/answer_stream")
    async def answer_stream(
        q: str = Body(..., embed=True),
        k: int = Body(8),
        filters: Optional[Dict[str, Any]] = Body(default=None)
    ):
        if not q or not q.strip():
            raise HTTPException(status_code=400, detail="empty_query")

        async def streamer():
            try:
                # token-level streaming flag
                flag = settings.gen_stream_tokens
                # Check provider client availability
                has_client = False
                try:
                    has_client = bool(gen.llm_provider.client)
                except Exception:
                    pass

                if flag and has_client:
                    prep = await run_in_threadpool(gen.prepare_for_stream, q, k, (filters or {}))
                    # Send meta first (optimistic empty citations to start)
                    # We will send the ACTUAL used citations at the end, once we see what the LLM used.
                    payload = {"type": "meta", "citations": [], "warnings": prep.get("warnings", []), "confidence": 0.0, "groundedness": None}
                    yield f"data: {_json.dumps(payload)}\n\n"
                    
                    # Bridge threaded token stream -> async generator via queue
                    queue: asyncio.Queue = asyncio.Queue()
                    loop = asyncio.get_running_loop()
                    full_text = []

                    def _run_stream():
                        try:
                            for tok in gen.iter_llm_tokens(prep.get("messages") or []):
                                if tok:
                                    # Accumulate text for citation processing
                                    loop.call_soon_threadsafe(full_text.append, tok)
                                loop.call_soon_threadsafe(queue.put_nowait, tok)
                        except Exception:
                            pass
                        finally:
                            loop.call_soon_threadsafe(queue.put_nowait, None)

                    threading.Thread(target=_run_stream, daemon=True).start()

                    delay_ms = settings.stream_chunk_delay_ms
                    while True:
                        part = await queue.get()
                        if part is None:
                            break
                        yield f"data: {_json.dumps({'type':'chunk','text': str(part)})}\n\n"
                        try:
                            await asyncio.sleep(max(0.0, float(delay_ms)/1000.0))
                        except Exception:
                            await asyncio.sleep(0)
                    
                    # Post-stream: Process citations based on full text
                    final_ans = "".join(full_text)

                    used_citations = await run_in_threadpool(
                        gen.process_citations, 
                        answer_text=final_ans, 
                        all_footnotes=prep.get("citations", []),
                        parsed_citations=None # Force extraction from text
                    )
                    
                    # Send final meta update with filtered citations
                    final_meta = {"type": "meta", "citations": used_citations}
                    yield f"data: {_json.dumps(final_meta)}\n\n"

                    yield f"data: {_json.dumps({'type':'done'})}\n\n"
                else:
                    # fallback: existing full-answer path
                    res = await run_in_threadpool(gen.answer, q=q, k=k, filters=(filters or {}))
                    payload = {"type": "meta", "citations": res.get("citations", []), "warnings": res.get("warnings", []), "confidence": res.get("confidence", 0.0), "groundedness": res.get("groundedness", None)}
                    yield f"data: {_json.dumps(payload)}\n\n"
                    text = res.get("answer", "")
                    chunk = settings.stream_chunk_chars
                    delay_ms = settings.stream_chunk_delay_ms
                    for i in range(0, len(text), chunk):
                        part = text[i:i+chunk]
                        yield f"data: {_json.dumps({'type':'chunk','text': part})}\n\n"
                        try:
                            await asyncio.sleep(max(0.0, float(delay_ms)/1000.0))
                        except Exception:
                            await asyncio.sleep(0)
                    yield f"data: {_json.dumps({'type':'done'})}\n\n"
            except asyncio.CancelledError:
                return
            except Exception as e:
                err = {"type":"error","detail": str(e)}
                yield f"data: {_json.dumps(err)}\n\n"

        return StreamingResponse(streamer(), media_type="text/event-stream")

    return router
