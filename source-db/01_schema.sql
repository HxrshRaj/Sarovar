-- Sarovar OLTP source: a UPI-style payments app. ALL DATA IS SYNTHETIC.
CREATE TABLE users (
  user_id      BIGINT PRIMARY KEY,
  full_name    TEXT        NOT NULL,
  phone        TEXT        NOT NULL,
  email        TEXT,
  vpa          TEXT        NOT NULL UNIQUE,
  city         TEXT        NOT NULL,
  kyc_status   TEXT        NOT NULL CHECK (kyc_status IN ('MIN_KYC','FULL_KYC','REJECTED')),
  created_at   TIMESTAMP   NOT NULL,
  updated_at   TIMESTAMP   NOT NULL
);
CREATE TABLE merchants (
  merchant_id    BIGINT PRIMARY KEY,
  merchant_name  TEXT      NOT NULL,
  category       TEXT      NOT NULL,
  city           TEXT      NOT NULL,
  risk_tier      TEXT      NOT NULL CHECK (risk_tier IN ('LOW','MEDIUM','HIGH')),
  contact_phone  TEXT      NOT NULL,
  created_at     TIMESTAMP NOT NULL,
  updated_at     TIMESTAMP NOT NULL
);
CREATE TABLE transactions (
  txn_id         BIGINT PRIMARY KEY,
  upi_ref        TEXT      NOT NULL UNIQUE,
  payer_user_id  BIGINT    NOT NULL REFERENCES users(user_id),
  merchant_id    BIGINT    NOT NULL REFERENCES merchants(merchant_id),
  amount_paise   BIGINT    NOT NULL CHECK (amount_paise > 0),
  status         TEXT      NOT NULL CHECK (status IN ('PENDING','SUCCESS','FAILED')),
  failure_code   TEXT,
  created_at     TIMESTAMP NOT NULL,
  updated_at     TIMESTAMP NOT NULL
);
CREATE TABLE refunds (
  refund_id      BIGINT PRIMARY KEY,
  txn_id         BIGINT    NOT NULL REFERENCES transactions(txn_id),
  amount_paise   BIGINT    NOT NULL CHECK (amount_paise > 0),
  status         TEXT      NOT NULL CHECK (status IN ('PENDING','PROCESSED','REJECTED')),
  reason         TEXT      NOT NULL,
  created_at     TIMESTAMP NOT NULL,
  updated_at     TIMESTAMP NOT NULL
);
CREATE INDEX ix_txn_updated   ON transactions(updated_at);
CREATE INDEX ix_txn_created   ON transactions(created_at);
CREATE INDEX ix_users_updated ON users(updated_at);
CREATE INDEX ix_merch_updated ON merchants(updated_at);
CREATE INDEX ix_ref_updated   ON refunds(updated_at);
