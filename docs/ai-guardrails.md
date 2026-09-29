# AI-assisted data work and PII guardrails

All data is synthetic. The model is a **local open-weight model**; nothing is sent to a hosted LLM. Numbers below come from the runs in [`ai/eval/results/`](../ai/eval/results/) (commands at the end). Not production.

## Model choice
| | |
|---|---|
| Text-to-SQL model | **`llama3.2:3b`** (Q4_K_M, 3.2 B parameters) via Ollama 0.34.4, CPU only. Licence: Llama 3.2 Community License (check terms before any commercial use). Chosen because it was already on the machine and fits ≈ 2 GB RAM; temperature 0, seed 7, 6,144-token context. |
| Embedding model | `nomic-embed-text` (768-dim) via Ollama, stored in **pgvector** (`vectors` DB). |
| Latency | mean 28.5 s per question (first question loads the model; 8–61 s range) on a 12-logical-CPU laptop, while other containers ran. |

## Architecture of one question
```
question ─▶ redact (regex + known names) ─▶ PII-intent refusal ─▶ embed (question only)
   ─▶ pgvector: top-k tables ─▶ prompt = rules + metadata of those tables (PII columns removed) + 2 generic examples + question
   ─▶ llama3.2:3b ─▶ SQL ─▶ SQL guard (static rules) ─▶ EXPLAIN (TYPE IO) cost gate ─▶ [1 repair attempt on rejection] ─▶ execute
   audit log (text re-redacted before it is written)
```
Nothing else goes into a prompt: **no data values, no query results, no example rows.**

## PII rules, how each is enforced, how each is tested
| # | Rule | Enforcement | Test |
|---|---|---|---|
| 1 | PII **columns** never appear in LLM context or embeddings | `metadata.table_doc()` / `embedding_docs()` drop every column tagged `pii: true` in `docs/catalog.yml` (`full_name, phone, email, vpa, contact_phone, upi_ref`) | `test_metadata_never_contains_pii_columns`; live test `test_no_real_pii_value_in_any_prompt_or_embedding_input` |
| 2 | PII **values** never appear in a prompt | Only metadata + the redacted question are used; `OllamaClient` runs `assert_clean()` on *every* outgoing chat/embedding text and raises `PromptPIIError` (nothing is sent) | `test_client_refuses_pii_in_chat_and_embeddings`; the live test scans **every** phone, email, VPA, name and 12-digit UPI ref in the source DB (≈ 380,000 values: 20,000 users × 4 + 600 merchant phones + 300,000 refs) against all prompts for all table combinations and all embedding docs: **0 hits** |
| 3 | PII in the user's **question** is redacted | `Redactor`: regex for email, VPA, phone (with/without +91, spaced), 12-digit ref, Aadhaar-like, PAN; exact-match names from the source (in memory only) | `test_redactor_catches_each_identifier_kind`, `test_known_names_are_redacted`, `test_end_to_end_pipeline_sends_no_pii_and_logs_none` |
| 4 | PII never reaches **logs** | `AuditLog.write()` re-redacts the record before writing; structured pipeline logs contain counts and keys, not values | same end-to-end test asserts none of the injected values appear in the audit log (memory or file) |
| 5 | Generated SQL may not touch PII | `sql_guard`: any reference to a PII column (select **or** filter) rejected; `SELECT *` rejected when a referenced table has PII columns | `test_pii_columns_rejected`, `test_select_star_rejected_when_table_has_pii`, `test_generated_sql_touching_pii_is_blocked_before_execution` |
| 6 | Requests that *ask* for personal data are refused up front | keyword heuristic `PII_INTENT` before any model call (added after the first adversarial run, see below) | `test_pii_intent_is_refused_before_any_model_call` (asserts zero HTTP requests were made) |
| 7 | Same protections for tool access | the MCP server reuses the guard and strips PII from `describe_table` | `tests/integration/test_mcp_server.py` (DROP, PII column and missing-partition-filter queries are rejected; `describe_table('lake.raw.users')` contains none of the PII column names) |

Two real bugs were found by these tests during development and fixed: the email pattern lost to the shorter VPA pattern at the same offset (`priya.sharma1@example.org` left `.org` behind), and phone numbers with an internal space (`+91 98765 43210`) were not matched.

## SQL safety validator
`ai/sarovar_ai/sql_guard.py` (26 unit tests): one statement, SELECT only (no DDL/DML/EXPLAIN/SHOW/CALL); tables must be schema-qualified and allow-listed (`lake.raw|curated|analytics.*`, so `information_schema` and `system` are out); literal `LIMIT ≤ 1000` on the outermost query; a literal, conjunctive predicate on the partition column (`=`, range, `BETWEEN`, `IN`) in **every query block** that reads a table flagged `partition_filter_required` (`raw.transactions`, `raw.refunds`, `curated.daily_merchant_metrics`, `analytics.daily_gmv`, `analytics.failure_reasons`); an `OR`, a function on the column (`substr(dt, …)`) or a filter on a non-partition column (`created_at`) does **not** count. Dimensions and tiny tables (`users`, `merchants`, `user_cohorts`, `retention_weekly`, `merchant_leaderboard`) are exempt — the EXPLAIN gate still applies. Then `EXPLAIN (TYPE IO, FORMAT JSON)`: the query must plan, and the estimated rows scanned (needs `ANALYZE` statistics; missing stats fail closed) must be ≤ 200,000. Example estimates: one-day filter 5,603 rows; the unfiltered 300,000-row fact table is blocked.
Limits: the guard does not see *inside* views (`merchant_leaderboard` reads all of `curated.daily_merchant_metrics`); it is a static-analysis guard, not a sandbox.

## Evaluation
**Set:** 40 hand-written questions with gold SQL ([`ai/eval/questions.json`](../ai/eval/questions.json); 20 easy / 16 medium / 4 hard), all gold queries pass the guard and execute (`run_eval.py gold`). Written once before the first run; not edited afterwards. **Metric:** *execution accuracy* = generated SQL passes the guard, executes, and returns the same result set as the gold SQL (numbers rounded to 2 dp; column names/order ignored; row order compared only when the gold has `ORDER BY`). One run per configuration, n = 40 (one question = 2.5 points).

| configuration | execution accuracy | easy / medium / hard |
|---|---:|---|
| **main**: retrieve top-3 tables, 1 repair attempt | **19 / 40 = 47.5 %** | 14/20 · 5/16 · 0/4 |
| no repair attempt | 19 / 40 = 47.5 % | 14/20 · 5/16 · 0/4 |
| **oracle**: gold tables given to the model (isolates retrieval) | 22 / 40 = 55.0 % | 15/20 · 7/16 · 0/4 |
| main, ignoring row order (post-hoc audit, `lenient_check.py`) | 22 / 40 = 55.0 % | (3 "wrong" answers differ only in row order: #4, #12, #30) |

- The one-shot repair loop rescued **zero** additional correct answers (it turned some guard rejections into other wrong answers).
- 30 of 40 first attempts passed the static guard; the model is weakest on multi-step logic: **0 / 4 hard** questions in every configuration.
- Retrieval costs about 7.5 points (47.5 % vs 55.0 %); the model itself is the bigger limit.

### Failure modes (main run, 21 failures; categories are assigned automatically and are heuristic)
| outcome | count | what actually happened (examples from the run) |
|---|---:|---|
| wrong result, right tables, wrong logic | 6 | #27 compared `amount_paise > 10000.0` for "10,000 rupees" (unit error: should be 1,000,000 paise); #25 returned one `COUNT(*)` instead of a count per reason (missing `GROUP BY`); #34 selected one row's `refund_count` instead of `SUM`; #4/#12 missing/unstable ordering; #21 averaged retention over *all* weeks, ignoring "week-1" |
| EXPLAIN failed: hallucinated column/type/syntax | 5 | #38 used a non-existent `merchant_id` on `daily_gmv`; #18 `m.risk_tier` on the wrong view; #23 `EXTRACT(HOUR FROM dt)` on a varchar partition string instead of `hour(created_at)`; #20 PostgreSQL `::double` cast; #31 nested aggregation `AVG(COUNT(DISTINCT …))` |
| wrong table choice | 3 | #2 used the `failure_reasons` view but read the first row's `failed_count` instead of summing; #19, #22 picked plausible but wrong tables |
| retrieval missed a gold table | 3 | #8 (refunds attributed to payment date lives in `curated.daily_merchant_metrics`; retrieval returned `raw.transactions`, `raw.refunds`, `merchant_leaderboard`), #30, #32 (multi-table joins) |
| **missing partition filter (guard rejected)** | 3 | #16, #33, #36: the model filtered one table in a join but not the other; the guard caught it, the repair attempt did not fix it |
| unknown / non-qualified table | 1 | #17 invented `lake.analytics.daily_merchant_metrics` |

So the "wrong joins, missing partition filters" failure modes you would expect are all present: joins across two partitioned tables are where the model most often forgets the second filter. The guard converts those from expensive scans into rejections; it cannot make the answer right.

### Data discovery (embedding retrieval)
Labels: for each of the 40 questions, the gold set = tables in its gold SQL (mechanically extracted). Strict, because many questions can also be answered from an alternative table (e.g. raw vs curated), which this metric counts as a miss.
| | k=1 | k=3 | k=5 |
|---|---:|---:|---:|
| recall@k (fraction of gold tables retrieved, mean) | 0.588 | **0.912** | 0.988 |
| all gold tables in top-k | 0.575 | 0.875 | 0.975 |
| at least one gold table in top-k | 0.600 | 0.950 | 1.000 |
Top-3 missed a gold table on 5 questions (#8, #23, #30, #32, #33), mostly multi-table joins. Only the retrieved tables' metadata is fed to the model.

### PII red-team questions
| set | n | refused before any model call | any PII column / value returned | prompts sent containing PII-like text |
|---|---:|---:|---:|---:|
| **v1 (before the intent refusal existed)** — `pii_adversarial_v1_before_intent_refusal.json` | 6 | 0 | **0** | 0 |
| v1 re-run **after** adding the keyword refusal | 6 | 6 | 0 | 0 (no prompts at all) |
| **held-out** paraphrases written afterwards — `pii_heldout.json` | 6 | **0** | **0** | 0 |

Read this honestly: (a) in v1 the model was asked for phone numbers/emails/UPI refs/names and **returned no PII** — because PII columns are not in its metadata it either invented a non-existent column (blocked by EXPLAIN in 2/6) or answered a harmless substitute question (4/6, e.g. `count(distinct user_id)` for "list phone numbers"); that is safe but misleading, which is why I added the refusal. (b) The keyword refusal was **written after seeing those six questions, so 6/6 is not an independent result**. (c) On six new paraphrases ("contact info", "identity details", "who is the biggest spender", "handles people use to pay", …) the keyword list caught **0/6**; 4 were blocked only incidentally by hallucinated columns, 2 produced harmless SQL (`user_id`, `txn_id` filters). Protection therefore rests on the *structural* controls (rules 1, 2, 5), which returned no PII in 12/12 red-team prompts, not on the keyword refusal. n is tiny; this is a smoke test, not a security evaluation.

## Known gaps
- Free-text names not present in the source are not redacted (they would reach the local model). Regex redaction can miss unusual formats.
- PII protection depends on `catalog.yml` tags being correct; an untagged new PII column would flow into prompts. The catalog↔schema sync tests check columns exist, not that they are tagged.
- The keyword refusal is blunt and English-only.
- 47.5 % accuracy means most analytical use needs a human to read the SQL — the console shows the SQL, the retrieved tables and the cost estimate for exactly that reason.

## Reproduce
```bash
python ai/eval/run_eval.py gold                                   # gold SQL passes guard + executes
python scripts/build_index.py && python ai/eval/run_eval.py discovery
python ai/eval/run_eval.py text2sql --k 3 --repair 1 --tag main    # ~20-25 min on CPU
python ai/eval/run_eval.py text2sql --k 3 --repair 0 --tag norepair
python ai/eval/run_eval.py text2sql --k 3 --repair 1 --oracle 1 --tag oracle
python ai/eval/run_eval.py pii && python ai/eval/run_eval.py pii_heldout
python ai/eval/lenient_check.py main
python -m pytest tests/unit tests/integration/test_pii_guardrails_live.py -q
```
