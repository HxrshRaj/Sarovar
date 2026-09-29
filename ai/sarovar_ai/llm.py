"""Ollama client (local open-weight model). PII enforcement lives INSIDE the client: every outgoing text passes
through the redactor's assert_clean(), so no code path can send a prompt or embedding input containing PII."""
import requests

from .config import EMBED_MODEL, LLM_MODEL, OLLAMA_URL
from .redact import Redactor


class OllamaClient:
    def __init__(self, redactor=None, model=LLM_MODEL, embed_model=EMBED_MODEL, url=OLLAMA_URL, record=False):
        self.redactor = redactor or Redactor()
        self.model, self.embed_model, self.url = model, embed_model, url
        self.sent = [] if record else None  # tests: every text that left the process

    def _guard(self, texts, where):
        for t in texts:
            self.redactor.assert_clean(t, where)
            if self.sent is not None:
                self.sent.append(t)

    def chat(self, messages, temperature=0.0, seed=7, num_ctx=6144, num_predict=400):
        self._guard([m["content"] for m in messages], "chat prompt")
        r = requests.post(f"{self.url}/api/chat", json={
            "model": self.model, "messages": messages, "stream": False,
            "options": {"temperature": temperature, "seed": seed, "num_ctx": num_ctx, "num_predict": num_predict}}, timeout=600)
        r.raise_for_status()
        j = r.json()
        return j["message"]["content"], {"eval_count": j.get("eval_count"), "prompt_eval_count": j.get("prompt_eval_count"),
                                         "total_ms": round(j.get("total_duration", 0) / 1e6)}

    def embed(self, texts, prefix="search_document: "):
        texts = [prefix + t for t in texts]
        self._guard(texts, "embedding input")
        r = requests.post(f"{self.url}/api/embed", json={"model": self.embed_model, "input": texts}, timeout=600)
        r.raise_for_status()
        return r.json()["embeddings"]
