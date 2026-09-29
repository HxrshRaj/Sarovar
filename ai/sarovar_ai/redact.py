"""Prompt-redaction layer. Nothing that looks like PII may reach an LLM prompt or a log line.

Two mechanisms:
  * regex patterns for structured identifiers (phone, email, UPI VPA, 12-digit UPI ref/Aadhaar-like, PAN)
  * optional exact-name matching against a known-names set (loaded in memory only; never logged)
Free-text names that are not in the known set cannot be detected reliably - see docs/ai-guardrails.md (limitations).
"""
import re

PATTERNS = [
    ("EMAIL", re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")),
    ("VPA", re.compile(r"(?<![\w.])[A-Za-z0-9._\-]{2,}@[A-Za-z][A-Za-z0-9]{1,}\b")),
    ("PAN", re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b")),
    ("AADHAAR", re.compile(r"(?<!\d)\d{4}[ -]\d{4}[ -]\d{4}(?!\d)")),
    ("PHONE", re.compile(r"(?<![\d])(?:\+?91[\s-]?)?[6-9]\d{4}[\s-]?\d{5}(?!\d)")),
    ("UPI_REF", re.compile(r"(?<!\d)\d{12}(?!\d)")),
]
_TOKEN = re.compile(r"[A-Za-z][A-Za-z.'\-]*")


class PromptPIIError(Exception):
    pass


class Redactor:
    def __init__(self, known_names=None):
        self.names = {n.strip().lower() for n in (known_names or []) if n and len(n.split()) >= 2}

    def _name_spans(self, text):
        if not self.names:
            return []
        toks = [(m.group(), m.start(), m.end()) for m in _TOKEN.finditer(text)]
        spans = []
        for n in (4, 3, 2):
            for i in range(len(toks) - n + 1):
                if " ".join(t[0].lower() for t in toks[i:i + n]) in self.names:
                    spans.append((toks[i][1], toks[i + n - 1][2]))
        return spans

    def redact(self, text):
        """Return (clean_text, findings) where findings is a list of PII kinds (never the values)."""
        findings = []
        spans = []
        for kind, rx in PATTERNS:
            for m in rx.finditer(text):
                spans.append((m.start(), m.end(), kind))
        spans += [(a, b, "NAME") for a, b in self._name_spans(text)]
        spans.sort(key=lambda s: (s[0], -s[1]))  # earliest start, then longest match wins
        out, pos = [], 0
        for a, b, kind in spans:
            if a < pos:
                continue  # overlapped by an earlier (longer-priority) match
            out.append(text[pos:a])
            out.append(f"[{kind}]")
            findings.append(kind)
            pos = b
        out.append(text[pos:])
        return "".join(out), findings

    def assert_clean(self, text, where="prompt"):
        _, findings = self.redact(text)
        if findings:
            raise PromptPIIError(f"{where} contains PII-like content ({sorted(set(findings))}); refusing to send")
