"""Embed catalog table/column descriptions (PII columns excluded) into pgvector. Run: python scripts/build_index.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ai"))
from sarovar_ai import discovery
from sarovar_ai.llm import OllamaClient
print("indexed docs:", discovery.build_index(OllamaClient()))
