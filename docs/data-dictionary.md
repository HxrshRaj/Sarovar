# Data dictionary

_Generated from [`catalog.yml`](catalog.yml) by `scripts/gen_docs.py` - do not edit by hand. All data is synthetic._

## Raw layer (mirrors OLTP tables; Parquet, partitioned by `dt`)

### `lake.raw.users`

One row per registered app user (payer). Attributes change late (KYC upgrades), so rows are updated after creation.

- **Owner:** payments-core | **Partition column:** `dt`

| Column | Type | PII | Description |
|---|---|---|---|
| `user_id` | bigint |  | Surrogate primary key of the user |
| `full_name` | varchar | **PII** (direct-identifier) | Full legal name |
| `phone` | varchar | **PII** (direct-identifier) | Mobile number in E.164 format |
| `email` | varchar | **PII** (direct-identifier) | Email address |
| `vpa` | varchar | **PII** (direct-identifier) | UPI virtual payment address of the user |
| `city` | varchar |  | Home city of the user |
| `kyc_status` | varchar |  | KYC level: MIN_KYC, FULL_KYC or REJECTED |
| `created_at` | timestamp(3) |  | When the user registered (UTC) |
| `updated_at` | timestamp(3) |  | Last modification time of the row; incremental-load watermark column |
| `dt` | varchar |  | Partition: creation date `YYYY-MM-DD` (directory name, derived from `created_at`) |

### `lake.raw.merchants`

One row per merchant that accepts UPI payments. Risk tier and attributes can change after onboarding.

- **Owner:** merchant-onboarding | **Partition column:** `dt`

| Column | Type | PII | Description |
|---|---|---|---|
| `merchant_id` | bigint |  | Surrogate primary key of the merchant |
| `merchant_name` | varchar |  | Trading name of the merchant (synthetic business name) |
| `category` | varchar |  | Merchant category, e.g. Grocery, Fuel, Food & Dining |
| `city` | varchar |  | City where the merchant operates |
| `risk_tier` | varchar |  | Risk classification: LOW, MEDIUM or HIGH |
| `contact_phone` | varchar | **PII** (direct-identifier) | Merchant contact phone number |
| `created_at` | timestamp(3) |  | When the merchant was onboarded (UTC) |
| `updated_at` | timestamp(3) |  | Last modification time of the row; incremental-load watermark column |
| `dt` | varchar |  | Partition: creation date `YYYY-MM-DD` (directory name, derived from `created_at`) |

### `lake.raw.transactions`

One row per UPI payment attempt from a user to a merchant. Status can change after creation (PENDING resolves to SUCCESS or FAILED, sometimes days later), producing late-arriving updates.

- **Owner:** payments-core | **Partition column:** `dt`

| Column | Type | PII | Description |
|---|---|---|---|
| `txn_id` | bigint |  | Surrogate primary key of the transaction |
| `upi_ref` | varchar | **PII** (transaction-identifier) | 12-digit UPI reference number (RRN); traceable to a person so treated as PII |
| `payer_user_id` | bigint |  | Foreign key to users.user_id |
| `merchant_id` | bigint |  | Foreign key to merchants.merchant_id |
| `amount_paise` | bigint |  | Payment amount in paise (1 rupee = 100 paise) |
| `status` | varchar |  | Payment status: PENDING, SUCCESS or FAILED |
| `failure_code` | varchar |  | UPI failure code when status is FAILED (e.g. U16, U30, U69, BT, U19); otherwise null |
| `created_at` | timestamp(3) |  | When the payment was initiated (UTC); defines the dt partition |
| `updated_at` | timestamp(3) |  | Last modification time (status resolution); incremental-load watermark column |
| `dt` | varchar |  | Partition: creation date `YYYY-MM-DD` (directory name, derived from `created_at`) |

### `lake.raw.refunds`

One row per refund raised against a successful transaction; created days after the original payment.

- **Owner:** payments-core | **Partition column:** `dt`

| Column | Type | PII | Description |
|---|---|---|---|
| `refund_id` | bigint |  | Surrogate primary key of the refund |
| `txn_id` | bigint |  | Foreign key to transactions.txn_id |
| `amount_paise` | bigint |  | Refund amount in paise (full or partial) |
| `status` | varchar |  | Refund status: PENDING, PROCESSED or REJECTED |
| `reason` | varchar |  | Refund reason: DUPLICATE_CHARGE, ITEM_RETURNED, SERVICE_NOT_DELIVERED or WRONG_AMOUNT |
| `created_at` | timestamp(3) |  | When the refund was raised (UTC) |
| `updated_at` | timestamp(3) |  | Last modification time (refund resolution); incremental-load watermark column |
| `dt` | varchar |  | Partition: creation date `YYYY-MM-DD` (directory name, derived from `created_at`) |

## Curated layer (Spark, Parquet)

### `lake.curated.daily_merchant_metrics`

Per merchant per transaction-creation day - payment counts by status, successful amount and refund totals. Refunds are attributed to the day the original payment was created.

- **Owner:** data-platform | **Partition column:** `dt`

| Column | Type | PII | Description |
|---|---|---|---|
| `merchant_id` | bigint |  | Merchant key (joins to raw.merchants) |
| `merchant_name` | varchar |  | Merchant trading name |
| `category` | varchar |  | Merchant category |
| `txn_count` | bigint |  | Number of payment attempts that day |
| `success_count` | bigint |  | Number of SUCCESS payments |
| `failed_count` | bigint |  | Number of FAILED payments |
| `pending_count` | bigint |  | Number of payments still PENDING |
| `success_amount_paise` | bigint |  | Sum of amount_paise over SUCCESS payments |
| `refund_count` | bigint |  | Number of refunds raised against that day's payments |
| `refund_amount_paise` | bigint |  | Sum of refund amount_paise (all statuses except REJECTED) |
| `dt` | varchar |  | Partition: transaction creation date, YYYY-MM-DD |

### `lake.curated.user_cohorts`

Signup-week retention cohorts - for each signup week and number of weeks since signup, how many cohort users made at least one successful payment.

- **Owner:** data-platform | **Partition column:** `cohort_week`

| Column | Type | PII | Description |
|---|---|---|---|
| `weeks_since_signup` | integer |  | Whole weeks between signup week and activity week (0 = signup week) |
| `cohort_size` | bigint |  | Number of users who signed up in the cohort week |
| `active_users` | bigint |  | Cohort users with at least one SUCCESS payment in that week offset |
| `cohort_week` | varchar |  | Partition: Monday of the signup week, YYYY-MM-DD |

## Analyst views (Trino views)

### `lake.analytics.daily_gmv`

Gross merchandise value per day - successful payment count and rupee amount, plus success rate. Reads curated.daily_merchant_metrics.

- **Owner:** data-platform

| Column | Type | PII | Description |
|---|---|---|---|
| `dt` | varchar |  | Transaction creation date |
| `txn_count` | bigint |  | All payment attempts |
| `success_count` | bigint |  | SUCCESS payments |
| `gmv_rupees` | double |  | Successful amount in rupees |
| `success_rate_pct` | double |  | 100 * success_count / txn_count |

### `lake.analytics.merchant_leaderboard`

Merchants ranked by successful GMV over all loaded days, with failure rate and refund rate.

- **Owner:** data-platform

| Column | Type | PII | Description |
|---|---|---|---|
| `merchant_id` | bigint |  | Merchant key |
| `merchant_name` | varchar |  | Merchant trading name |
| `category` | varchar |  | Merchant category |
| `txn_count` | bigint |  | All payment attempts |
| `gmv_rupees` | double |  | Successful amount in rupees |
| `failure_rate_pct` | double |  | 100 * failed / attempts |
| `refund_rate_pct` | double |  | 100 * refund_count / success_count |

### `lake.analytics.failure_reasons`

Failed payment counts per day and UPI failure code. Reads raw.transactions and is partition-filterable on dt.

- **Owner:** data-platform

| Column | Type | PII | Description |
|---|---|---|---|
| `dt` | varchar |  | Transaction creation date |
| `failure_code` | varchar |  | UPI failure code |
| `failed_count` | bigint |  | Number of failed payments |

### `lake.analytics.retention_weekly`

Weekly retention percentage per signup cohort. Reads curated.user_cohorts.

- **Owner:** data-platform

| Column | Type | PII | Description |
|---|---|---|---|
| `cohort_week` | varchar |  | Monday of the signup week |
| `weeks_since_signup` | integer |  | Weeks since signup |
| `cohort_size` | bigint |  | Users in the cohort |
| `retention_pct` | double |  | 100 * active_users / cohort_size |
