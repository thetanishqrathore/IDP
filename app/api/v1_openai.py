from __future__ import annotations
from typing import List, Optional, Any, Dict
from fastapi import APIRouter, Depends, HTTPException, Body, Request
from fastapi.concurrency import run_in_threadpool
from services.generation import GenerationService
from core.security import get_api_key
import time
import uuid

def create_openai_router(gen_service: GenerationService) -> APIRouter:
    # Mount at /v1 to mimic OpenAI
    router = APIRouter(prefix="/v1", tags=["openai-compat"], dependencies=[Depends(get_api_key)])

    @router.post("/chat/completions")
    async def chat_completions(
        request: Request,
        messages: List[Dict[str, str]] = Body(..., description="OpenAI-format messages list"),
        model: str = Body("idp-v2", description="Model name (ignored)"),
        temperature: float = Body(0.7, description="Temperature (ignored)"),
        max_tokens: int = Body(None, description="Max tokens (ignored)"),
        stream: bool = Body(False, description="Whether to stream response (not yet supported via this endpoint)")
    ):
        """
        OpenAI-compatible chat completion endpoint.
        Extracts the last user message as the query and runs the RAG pipeline.
        """
        if not messages:
            raise HTTPException(status_code=400, detail="No messages provided")
        
        # Extract last user message
        last_user_msg = None
        for m in reversed(messages):
            if m.get("role") == "user":
                last_user_msg = m.get("content")
                break
        
        if not last_user_msg:
            raise HTTPException(status_code=400, detail="No user message found")

        # Run RAG generation
        # Note: We use a default k=20 for better context diversity.
        try:
            rag_response = await run_in_threadpool(gen_service.answer, q=last_user_msg, k=20)
        except Exception as e:
            # Fallback or error
            raise HTTPException(status_code=500, detail=f"RAG pipeline failed: {str(e)}")

        answer_text = rag_response.get("answer", "")
        citations = rag_response.get("citations", [])
        
        # Format citations as a system footer or just append them?
        # Standard RAG usually appends them to the text.
        # Let's keep it simple and clean for now, maybe append if they exist.
        if citations:
            footer = "\n\n**Sources:**\n"
            for c in citations:
                uri = c.get('uri', 'doc')
                footer += f"- {uri}\n"
            answer_text += footer

        # Construct OpenAI response object
        res_id = f"chatcmpl-{uuid.uuid4()}"
        created = int(time.time())
        
        return {
            "id": res_id,
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": answer_text
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": 0, # To be calculated if needed
                "completion_tokens": 0,
                "total_tokens": 0
            }
        }

    return router
