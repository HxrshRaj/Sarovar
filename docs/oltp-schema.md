# OLTP source schema (synthetic UPI-style app)

All data is **synthetic** (seeded generator, `source-db/generate.py`, `--seed 42`). DDL: [`source-db/01_schema.sql`](../source-db/01_schema.sql).
Column-level metadata (descriptions, owners, PII tags) lives in [`catalog.yml`](catalog.yml); a test asserts it matches the live database.

## What the system represents

A payments app in the style of UPI. **Users** pay **merchants**. Each payment attempt is a row in **transactions**. Some successful payments are later **refunded**. The database is transactional (OLTP): rows are inserted and *updated in place*, which is what makes loading it into an analytical lake non-trivial.

```
users 1───* transactions *───1 merchants
                  1
                  │
                  *
               refunds
```

## Tables

### `users` — one row per registered payer
| column | type | key | PII | notes |
|---|---|---|---|---|
| user_id | bigint | PK | | surrogate key |
| full_name | text | | **PII** (direct) | |
| phone | text | | **PII** (direct) | +91 synthetic number |
| email | text | | **PII** (direct) | `@example.org` (reserved domain) |
| vpa | text | UNIQUE | **PII** (direct) | UPI address, `@sarovar` handle (fictional) |
| city | text | | | |
| kyc_status | text | | | MIN_KYC / FULL_KYC / REJECTED; **changes after signup** |
| created_at, updated_at | timestamp | | | `updated_at` is the incremental watermark column |

Lake partition: `dt` = date of `created_at`.

### `merchants` — one row per accepting merchant
| column | type | key | PII | notes |
|---|---|---|---|---|
| merchant_id | bigint | PK | | |
| merchant_name | text | | | synthetic business name |
| category | text | | | Grocery, Fuel, Food & Dining, … |
| city | text | | | |
| risk_tier | text | | | LOW / MEDIUM / HIGH; can be re-tiered later |
| contact_phone | text | | **PII** (direct) | |
| created_at, updated_at | timestamp | | | |

Lake partition: `dt` = date of `created_at`.

### `transactions` — one row per payment attempt (the fact table)
| column | type | key | PII | notes |
|---|---|---|---|---|
| txn_id | bigint | PK | | |
| upi_ref | text | UNIQUE | **PII** (transaction identifier) | 12-digit RRN; traceable to a person, so excluded from LLM context |
| payer_user_id | bigint | FK → users | | |
| merchant_id | bigint | FK → merchants | | |
| amount_paise | bigint | | | amounts are integer paise (no float rounding) |
| status | text | | | PENDING / SUCCESS / FAILED |
| failure_code | text | | | set only when FAILED (U16, U30, U69, BT, U19) |
| created_at | timestamp | | | when the payment was initiated |
| updated_at | timestamp | | | when the status last changed |

Lake partition: `dt` = date of `created_at`. **Late-arriving updates:** ~7% of payments start PENDING and resolve minutes to ~5 days later; the row's `updated_at` moves to a later day than `created_at`. The generator's actual count is printed at load time (`late-arriving txn updates`; 7,736 of 300,000 for seed 42). An incremental loader that only looks at "today's" `created_at` would silently miss these.

### `refunds` — one row per refund raised against a successful payment
| column | type | key | PII | notes |
|---|---|---|---|---|
| refund_id | bigint | PK | | |
| txn_id | bigint | FK → transactions | | |
| amount_paise | bigint | | | full or partial |
| status | text | | | PENDING / PROCESSED / REJECTED |
| reason | text | | | DUPLICATE_CHARGE, ITEM_RETURNED, … |
| created_at, updated_at | timestamp | | | refunds appear days after the payment |

Lake partition: `dt` = date of `created_at` **of the refund** (not of the original payment).

## Generator skew (so the data behaves like payments data)
- Merchant popularity is Zipf-like: the busiest merchant receives ~12% of all payments.
- Hourly peaks (morning and 18:00–21:00), a weekend bump, and volume growth across the window.
- Failure rate = base + high-risk merchants + night hours + one injected two-hour outage (2026-09-05 13:00–15:00, code `BT`).
- Window: 2026-08-01 … 2026-09-28 (59 days); users/merchants are created earlier.

## Why the lake partitions by `created_at` date
Analysts filter by transaction date, so `dt=YYYY-MM-DD` makes partition pruning effective (see [cost-aware-querying.md](cost-aware-querying.md)). The consequence is that a status update to an old payment must **rewrite an old partition** — see [pipeline-flow.md](pipeline-flow.md).
