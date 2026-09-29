"""Spark job: raw Parquet (lake/raw) -> curated Parquet tables (lake/curated), partition-overwrite.

  daily_merchant_metrics  partitioned by dt (txn creation date)
  user_cohorts            partitioned by cohort_week (Monday of signup week)

Usage: python build_curated.py [--start YYYY-MM-DD --end YYYY-MM-DD]   (dt range for daily_merchant_metrics)
Re-running with the same inputs overwrites the same partitions with the same rows (idempotent).
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = os.environ.get("S3_BUCKET", "lake")


def spark_session():
    return (SparkSession.builder.appName("sarovar-build-curated").master("local[2]")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://localhost:9000"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "minio_dev"))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "minio_dev_pw"))
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate())


def raw(spark, table):
    return (spark.read.option("basePath", f"s3a://{BUCKET}/raw/{table}/")
            .parquet(f"s3a://{BUCKET}/raw/{table}/dt=*/")
            .withColumn("dt", F.col("dt").cast("string")))


def build_daily_merchant_metrics(spark, start, end):
    tx = raw(spark, "transactions")
    if start:
        tx = tx.filter(F.col("dt") >= start)
    if end:
        tx = tx.filter(F.col("dt") <= end)
    refunds = (raw(spark, "refunds").filter(F.col("status") != "REJECTED")
               .groupBy("txn_id").agg(F.count("*").alias("r_cnt"), F.sum("amount_paise").alias("r_amt")))
    merch = raw(spark, "merchants").select("merchant_id", "merchant_name", "category")
    ok = F.col("status") == "SUCCESS"
    out = (tx.join(refunds, "txn_id", "left")
           .groupBy("dt", "merchant_id")
           .agg(F.count("*").alias("txn_count"),
                F.sum(F.when(ok, 1).otherwise(0)).alias("success_count"),
                F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
                F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
                F.sum(F.when(ok, F.col("amount_paise")).otherwise(0)).alias("success_amount_paise"),
                F.sum(F.coalesce("r_cnt", F.lit(0))).alias("refund_count"),
                F.sum(F.coalesce("r_amt", F.lit(0))).alias("refund_amount_paise"))
           .join(merch, "merchant_id", "left")
           .select("merchant_id", "merchant_name", "category", F.col("txn_count").cast("long"),
                   F.col("success_count").cast("long"), F.col("failed_count").cast("long"),
                   F.col("pending_count").cast("long"), F.col("success_amount_paise").cast("long"),
                   F.col("refund_count").cast("long"), F.col("refund_amount_paise").cast("long"), "dt"))
    out.repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(f"s3a://{BUCKET}/curated/daily_merchant_metrics")
    return out.count()


def build_user_cohorts(spark):
    users = raw(spark, "users").select("user_id", F.date_trunc("week", "created_at").cast("date").alias("cw"))
    sizes = users.groupBy("cw").agg(F.count("*").alias("cohort_size"))
    tx = (raw(spark, "transactions").filter(F.col("status") == "SUCCESS")
          .select("payer_user_id", F.date_trunc("week", "created_at").cast("date").alias("aw"), "created_at"))
    act = (tx.join(users, tx.payer_user_id == users.user_id)
           .withColumn("weeks_since_signup", (F.datediff("aw", "cw") / 7).cast("int"))
           .filter(F.col("weeks_since_signup") >= 0)
           .groupBy("cw", "weeks_since_signup").agg(F.countDistinct("payer_user_id").alias("active_users")))
    out = (act.join(sizes, "cw")
           .select("weeks_since_signup", F.col("cohort_size").cast("long"), F.col("active_users").cast("long"),
                   F.date_format("cw", "yyyy-MM-dd").alias("cohort_week")))
    out.repartition("cohort_week").write.mode("overwrite").partitionBy("cohort_week").parquet(f"s3a://{BUCKET}/curated/user_cohorts")
    return out.count()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start")
    ap.add_argument("--end")
    a = ap.parse_args()
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    n1 = build_daily_merchant_metrics(spark, a.start, a.end)
    n2 = build_user_cohorts(spark)
    print(f"SPARK_DONE daily_merchant_metrics_rows={n1} user_cohorts_rows={n2} range=[{a.start},{a.end}]")
    spark.stop()
