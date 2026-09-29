-- Reusable analyst views (lake.analytics). Documented in docs/data-dictionary.md (generated from docs/catalog.yml).
-- Views over curated tables keep analyst queries cheap; failure_reasons reads raw and stays partition-filterable on dt.
CREATE OR REPLACE VIEW lake.analytics.daily_gmv AS
SELECT dt,
       sum(txn_count) AS txn_count,
       sum(success_count) AS success_count,
       sum(success_amount_paise) / 100.0 AS gmv_rupees,
       round(100.0 * sum(success_count) / sum(txn_count), 2) AS success_rate_pct
FROM lake.curated.daily_merchant_metrics
GROUP BY dt;

CREATE OR REPLACE VIEW lake.analytics.merchant_leaderboard AS
SELECT merchant_id, merchant_name, category,
       sum(txn_count) AS txn_count,
       sum(success_amount_paise) / 100.0 AS gmv_rupees,
       round(100.0 * sum(failed_count) / sum(txn_count), 2) AS failure_rate_pct,
       round(100.0 * sum(refund_count) / nullif(sum(success_count), 0), 2) AS refund_rate_pct
FROM lake.curated.daily_merchant_metrics
GROUP BY merchant_id, merchant_name, category;

CREATE OR REPLACE VIEW lake.analytics.failure_reasons AS
SELECT dt, failure_code, count(*) AS failed_count
FROM lake.raw.transactions
WHERE status = 'FAILED'
GROUP BY dt, failure_code;

CREATE OR REPLACE VIEW lake.analytics.retention_weekly AS
SELECT cohort_week, weeks_since_signup, cohort_size,
       round(100.0 * active_users / cohort_size, 2) AS retention_pct
FROM lake.curated.user_cohorts;
