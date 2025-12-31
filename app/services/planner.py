from __future__ import annotations
import json
from typing import Any, Dict, List, Optional
from core.config import settings
from services.llm.providers import OpenAIProvider

class AgenticPlanner:
    def __init__(self, logger=None):
        self.log = logger or (lambda *a, **k: None)
        self.provider = None
        
        # Initialize provider using settings
        key = settings.gemini_api_key or settings.openai_api_key
        if key:
            try:
                self.provider = OpenAIProvider(
                    api_key=key,
                    base_url=settings.gen_base_url,
                    model=settings.gen_model
                )
            except Exception as e:
                self.log("warn", "planner-llm-init-fail", reason=str(e))

    def plan_query(self, user_query: str) -> Dict[str, Any]:
        """
        Uses LLM to analyze the query and return a structured plan.
        Returns a dict with 'intent', 'queries', 'filters', 'reasoning'.
        """
        if not self.provider:
            return {"error": "no_provider"}

        system_prompt = (
            "You are an expert Query Planner for a RAG (Retrieval Augmented Generation) system.\n"
            "Your goal is to analyze the user's request and determine the best strategy to answer it.\n\n"
            "Available Intents:\n"
            "- RETRIEVAL: Standard search. Use when the user asks a question about documents.\n"
            "- COMPARISON: Use when the user wants to compare two or more entities, quarters, or items.\n"
            "- SUMMARIZATION: Use when the user asks for a summary or list of items.\n"
            "- FACT_LOOKUP: Use when the user asks for a specific value (e.g., 'What is the total of invoice #123?').\n\n"
            "Output Format (JSON Only):\n"
            "{\n"
            '  "intent": "RETRIEVAL" | "COMPARISON" | "SUMMARIZATION" | "FACT_LOOKUP",\n'
            '  "queries": ["list", "of", "search", "queries"],\n'
            '  "filters": { "doc_ids": [], "types": [], "date_range": null },\n'
            '  "reasoning": "Brief explanation of why this plan was chosen."\n'
            "}\n\n"
            "Guidelines:\n"
            "1. If the query is complex (e.g., 'Compare Q1 and Q2 revenue'), generate multiple atomic search queries (e.g., ['Q1 revenue', 'Q2 revenue']).\n"
            "2. Do NOT use 'types' filter for document classes (e.g. 'invoice', 'contract'). Only use 'types' for structural elements like 'table' or 'image'.\n"
            "3. Be precise with 'queries'. They should be optimized for vector search."
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ]

        try:
            # Use generate_json for structured output
            raw_json = self.provider.generate_json(messages, max_tokens=512, temperature=0.0)
            
            # Clean up potential markdown code blocks if the model adds them despite instructions
            if "```json" in raw_json:
                raw_json = raw_json.split("```json")[1].split("```")[0].strip()
            elif "```" in raw_json:
                raw_json = raw_json.split("```")[1].strip()
                
            plan = json.loads(raw_json)
            return plan
        except Exception as e:
            self.log("error", "planner-generation-fail", error=str(e))
            return {"error": str(e)}
