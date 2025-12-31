from typing import List, Dict, Optional, Iterator, Tuple
import backoff
try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

from core.interfaces import LLMProvider
from core.config import settings

class OpenAIProvider(LLMProvider):
    def __init__(self, api_key: str, base_url: str, model: str):
        self.model = model
        self.client = None
        if OpenAI and api_key:
            try:
                self.client = OpenAI(api_key=api_key, base_url=base_url)
            except Exception:
                self.client = None

    def generate(self, messages: List[Dict[str, str]], **kwargs) -> str:
        if not self.client:
            raise RuntimeError("llm_disabled")
        try:
            r = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                **kwargs
            )
            msg = r.choices[0].message
            return getattr(msg, "content", "") or ""
        except Exception as e:
            raise e

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def generate_json(self, messages: List[Dict[str, str]], **kwargs) -> str:
        if not self.client:
            raise RuntimeError("llm_disabled")
        # Try native JSON mode first
        try:
            r = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                response_format={"type": "json_object"},
                **kwargs
            )
            msg = r.choices[0].message
            return getattr(msg, "content", "") or ""
        except Exception:
            # Fallback to plain text if JSON mode fails or not supported by model
            return self.generate(messages, **kwargs)

    def stream(self, messages: List[Dict[str, str]], **kwargs) -> Iterator[str]:
        if not self.client:
            return iter(())
        try:
            stream = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                stream=True,
                **kwargs
            )
            for chunk in stream:
                try:
                    choices = getattr(chunk, "choices", []) or []
                    for ch in choices:
                        delta = getattr(ch, "delta", None)
                        content = getattr(delta, "content", None)
                        if content:
                            yield str(content)
                except Exception:
                    continue
        except Exception:
            return iter(())
