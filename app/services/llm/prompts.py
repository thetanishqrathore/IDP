from typing import List, Dict

OUT_SCHEMA = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "citations": {
            "type": "array",
            "items": {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
        },
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0}
    },
    "required": ["answer", "citations", "confidence"]
}

def build_messages(q: str, context_str: str, mode: str) -> List[Dict[str, str]]:
    system = (
        "You answer using the provided context. If not present, try to give your best coverup asking for more context or releted documents in a very helpful and hospitable manner.  "
        "Use footnote citations like [^1], [^2] that match the context blocks. "
        "Write the answer in clean, GitHub-flavored Markdown: use headings, bulleted lists, and tables when appropriate; avoid decorative bold for entire paragraphs.\n"
        "CRITICAL: Do NOT mention document filenames (e.g., 'report.pdf') in your answer text. Use ONLY the footnote markers [^n] to refer to sources."
    )
    style = "- Start with a second-level heading: '## Answer' followed by your response.\n"
    if mode == "NUMERIC_TOTAL":
        style += "- If the question asks for a total/amount, give a single concise answer first (e.g., \"Total: 12,345.00\"), then add one sentence and a citation.\n"
    elif mode == "LIST":
        style += "- Present a short bulleted list. Add a citation [^n] at the end of each bullet.\n"
    elif mode == "CLAUSE":
        style += "- Quote the relevant clause precisely, then summarize it in one sentence, with citations.\n"

    user = (
        f"Question:\n{q}\n\n"
        "Context:\n"
        f"{context_str}\n\n"
        "Instructions:\n"
        f"{style}"
        "- Output a strict JSON object with these keys exactly: {\"answer\": str_markdown, \"citations\": [{\"n\": int}, ...], \"confidence\": float(0..1)}\n"
        "- The answer must include footnote markers like [^1] that correspond to the citations you return.\n"
        "- Base your confidence on how directly the context answers the question and how many independent matching citations you used.\n"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]

def build_messages_no_context(q: str) -> List[Dict[str, str]]:
    system = (
        "You are a helpful assistant. If document context is unavailable, "
        "answer succinctly using general knowledge and clearly state that you're "
        "answering with limited context. Keep tone polite and practical."
    )
    user = (
        f"Question:\n{q}\n\n"
        "Instructions:\n"
        "- If you lack document context, do your best and add a short note like 'Based on limited context, here's my best effort.'\n"
        "- Output a strict JSON object with keys: {\"answer\": str_markdown, \"citations\": [], \"confidence\": float(0..1)}\n"
        "- Do not include citations when you don't have document context.\n"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]
