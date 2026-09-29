"""Seeded synthetic data generator for the Sarovar OLTP source. ALL DATA IS SYNTHETIC.

Realism knobs (all deterministic for a given --seed):
  * merchant popularity is Zipf-like (a few very busy merchants)
  * hourly peaks (morning + evening), weekend bump, volume growth over the window
  * failures: base rate + high-risk merchants + night hours + one simulated bank-outage window
  * late-arriving updates: ~7% of txns start PENDING and resolve minutes-to-days later, so
    updated_at lands in a *later* day than created_at (partition rewrites needed downstream)
  * refunds arrive days after their txn; users/merchants get late attribute updates
"""
import argparse
import io
import os
from datetime import datetime, timedelta

import numpy as np
import psycopg2
from faker import Faker

START = datetime(2026, 8, 1)
END = datetime(2026, 9, 29)  # exclusive
CATEGORIES = [("Grocery", 45000), ("Food & Dining", 30000), ("Fuel", 120000), ("Telecom", 29900),
              ("Utilities", 150000), ("Transport", 20000), ("Retail", 90000), ("Pharmacy", 45000),
              ("Education", 250000), ("Entertainment", 39900)]
CITIES = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Pune", "Kolkata", "Ahmedabad",
          "Jaipur", "Lucknow", "Indore", "Kochi"]
FAIL_CODES = ["U16", "U30", "U69", "BT", "U19"]
REFUND_REASONS = ["DUPLICATE_CHARGE", "ITEM_RETURNED", "SERVICE_NOT_DELIVERED", "WRONG_AMOUNT"]


def copy(cur, table, cols, rows):
    buf = io.StringIO()
    for r in rows:
        buf.write("\t".join("\\N" if v is None else str(v) for v in r) + "\n")
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({','.join(cols)}) FROM STDIN WITH (FORMAT text)", buf)


def ts(dt):
    return str(np.datetime64(dt, "s")).replace("T", " ")


def phone(rng):
    return "+91" + str(int(rng.integers(6, 10))) + "".join(map(str, rng.integers(0, 10, 9)))


def generate(dsn, n_users, n_merchants, n_txns, seed):
    rng = np.random.default_rng(seed)
    fake = Faker("en_IN")
    Faker.seed(seed)
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute("TRUNCATE refunds, transactions, merchants, users")

    # ---- users
    span = (END - datetime(2026, 6, 1)).days * 86400
    u_created = np.datetime64("2026-06-01T00:00:00") + (rng.beta(1.0, 2.2, n_users) * span).astype("timedelta64[s]")
    u_created.sort()
    kyc = rng.choice(["MIN_KYC", "FULL_KYC", "REJECTED"], n_users, p=[.55, .4, .05])
    u_updated = u_created.copy()
    upg = (kyc == "FULL_KYC") & (rng.random(n_users) < 0.35)
    u_updated[upg] = u_created[upg] + rng.integers(1, 30 * 86400, upg.sum()).astype("timedelta64[s]")
    u_updated = np.minimum(u_updated, np.datetime64(END - timedelta(seconds=1), "s"))
    users = []
    for i in range(n_users):
        name = fake.name()
        slug = "".join(c for c in name.lower() if c.isalpha())[:10]
        users.append((i + 1, name, phone(rng), f"{slug}{i+1}@example.org", f"{slug}{i+1}@sarovar",
                      CITIES[int(rng.integers(len(CITIES)))], kyc[i], ts(u_created[i]), ts(u_updated[i])))
    copy(cur, "users", "user_id full_name phone email vpa city kyc_status created_at updated_at".split(), users)

    # ---- merchants
    m_created = np.datetime64("2026-05-15T00:00:00") + rng.integers(0, 60 * 86400, n_merchants).astype("timedelta64[s]")
    m_updated = m_created.copy()
    risk = rng.choice(["LOW", "MEDIUM", "HIGH"], n_merchants, p=[.7, .22, .08])
    chg = rng.random(n_merchants) < 0.05
    m_updated[chg] = np.datetime64("2026-08-15T00:00:00") + rng.integers(0, 40 * 86400, chg.sum()).astype("timedelta64[s]")
    cat_idx = rng.integers(0, len(CATEGORIES), n_merchants)
    merchants = [(i + 1, f"{fake.company()} {CATEGORIES[cat_idx[i]][0]}"[:60], CATEGORIES[cat_idx[i]][0],
                  CITIES[int(rng.integers(len(CITIES)))], risk[i], phone(rng),
                  ts(m_created[i]), ts(m_updated[i])) for i in range(n_merchants)]
    copy(cur, "merchants", "merchant_id merchant_name category city risk_tier contact_phone created_at updated_at".split(), merchants)

    # ---- transactions
    days = (END - START).days
    day_w = np.array([(1 + 0.6 * d / days) * (1.25 if (START + timedelta(days=d)).weekday() >= 5 else 1.0) for d in range(days)])
    day_w /= day_w.sum()
    hour_w = np.array([1, .6, .4, .3, .3, .5, 1, 2, 3.5, 4, 3.5, 3, 3.2, 3, 2.6, 2.8, 3, 3.6, 4.5, 5, 4.5, 3.2, 2.2, 1.4])
    hour_w /= hour_w.sum()
    d = rng.choice(days, n_txns, p=day_w)
    h = rng.choice(24, n_txns, p=hour_w)
    sec = rng.integers(0, 3600, n_txns)
    t = np.datetime64(START, "s") + (d * 86400 + h * 3600 + sec).astype("timedelta64[s]")
    m_pop = 1.0 / np.arange(1, n_merchants + 1) ** 0.95
    rng.shuffle(m_pop)
    m_pop /= m_pop.sum()
    mid = rng.choice(n_merchants, n_txns, p=m_pop)
    u_pop = rng.pareto(1.6, n_users) + 1
    u_pop /= u_pop.sum()
    uid = rng.choice(n_users, n_txns, p=u_pop)
    t = np.maximum(t, u_created[uid] + np.timedelta64(300, "s"))
    t = np.minimum(t, np.datetime64(END - timedelta(seconds=10), "s"))
    order = np.argsort(t, kind="stable")
    t, mid, uid = t[order], mid[order], uid[order]
    base_amt = np.array([CATEGORIES[c][1] for c in cat_idx])[mid]
    amt = np.clip((base_amt * rng.lognormal(0, 0.9, n_txns)).astype(np.int64), 100, 10_000_000)
    amt = np.where(rng.random(n_txns) < 0.7, (amt // 100) * 100, amt)
    amt = np.maximum(amt, 100)
    hour = ((t - t.astype("datetime64[D]")) / np.timedelta64(1, "h")).astype(int)
    outage = (t >= np.datetime64("2026-09-05T13:00:00")) & (t < np.datetime64("2026-09-05T15:00:00"))
    p_fail = 0.035 + np.where(risk[mid] == "HIGH", 0.05, 0) + np.where(hour < 5, 0.03, 0) + np.where(outage, 0.30, 0)
    failed = rng.random(n_txns) < p_fail
    pending = (~failed) & (rng.random(n_txns) < 0.07)
    fcode = rng.choice(FAIL_CODES, n_txns, p=[.3, .3, .1, .2, .1])
    fcode = np.where(outage, "BT", fcode)
    refs = (400_000_000_000 + rng.permutation(n_txns * 3)[:n_txns]).astype(np.int64)
    status = np.where(failed, "FAILED", np.where(pending, "PENDING", "SUCCESS")).astype(object)
    upd = t + rng.integers(1, 6, n_txns).astype("timedelta64[s]")
    late = rng.random(n_txns) < 0.4
    delay = np.where(late, rng.integers(86400, 5 * 86400, n_txns), rng.integers(60, 3600, n_txns))
    resolved_at = t + delay.astype("timedelta64[s]")
    ok = pending & (resolved_at < np.datetime64(END, "s"))
    final_ok = rng.random(n_txns) < 0.85
    status[ok] = np.where(final_ok[ok], "SUCCESS", "FAILED")
    upd[ok] = resolved_at[ok]
    fcode_out = np.where(status == "FAILED", fcode, None)
    txn_rows = [(i + 1, refs[i], uid[i] + 1, mid[i] + 1, amt[i], status[i], fcode_out[i], ts(t[i]), ts(upd[i]))
                for i in range(n_txns)]
    copy(cur, "transactions", "txn_id upi_ref payer_user_id merchant_id amount_paise status failure_code created_at updated_at".split(), txn_rows)

    # ---- refunds
    succ = np.where(status == "SUCCESS")[0]
    ridx = succ[rng.random(len(succ)) < 0.025]
    r_created = upd[ridx] + rng.integers(3600, 10 * 86400, len(ridx)).astype("timedelta64[s]")
    keep = r_created < np.datetime64(END, "s")
    ridx, r_created = ridx[keep], r_created[keep]
    rstat = rng.choice(["PROCESSED", "REJECTED", "PENDING"], len(ridx), p=[.8, .05, .15])
    r_upd = np.where(rstat == "PENDING", r_created, r_created + rng.integers(3600, 3 * 86400, len(ridx)).astype("timedelta64[s]"))
    r_upd = np.minimum(r_upd, np.datetime64(END - timedelta(seconds=1), "s"))
    full = rng.random(len(ridx)) < 0.7
    ramt = np.where(full, amt[ridx], np.maximum(100, (amt[ridx] * rng.uniform(0.2, 0.9, len(ridx))).astype(np.int64)))
    refunds = [(i + 1, ridx[i] + 1, ramt[i], rstat[i], REFUND_REASONS[int(rng.integers(4))], ts(r_created[i]), ts(r_upd[i]))
               for i in range(len(ridx))]
    copy(cur, "refunds", "refund_id txn_id amount_paise status reason created_at updated_at".split(), refunds)
    conn.commit()
    for tb in ("users", "merchants", "transactions", "refunds"):
        cur.execute(f"SELECT count(*) FROM {tb}")
        print(tb, cur.fetchone()[0])
    cur.execute("SELECT count(*) FROM transactions WHERE updated_at::date > created_at::date")
    print("late-arriving txn updates (updated_at day > created_at day):", cur.fetchone()[0])
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.environ.get("OLTP_DSN", "postgresql://sarovar:sarovar_dev_pw@localhost:5433/sarovar_oltp"))
    ap.add_argument("--users", type=int, default=20000)
    ap.add_argument("--merchants", type=int, default=600)
    ap.add_argument("--txns", type=int, default=300000)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    generate(a.dsn, a.users, a.merchants, a.txns, a.seed)
