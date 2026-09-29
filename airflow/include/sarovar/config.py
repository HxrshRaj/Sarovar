import os

import boto3
from botocore.config import Config

OLTP_DSN = os.environ.get("OLTP_DSN", "postgresql://sarovar:sarovar_dev_pw@localhost:5433/sarovar_oltp")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.environ.get("S3_BUCKET", "lake")
S3_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minio_dev")
S3_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio_dev_pw")
TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
CATALOG_PATH = os.environ.get("CATALOG_PATH") or next(
    (p for p in ("/opt/sarovar/catalog.yml",
                 os.path.join(os.path.dirname(__file__), "..", "..", "..", "docs", "catalog.yml"))
     if os.path.exists(p)),
    "docs/catalog.yml")
# first DAG interval: the extract window starts at the epoch (initial load)
PIPELINE_START = "2026-08-01"


def s3_client():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_KEY,
                        aws_secret_access_key=S3_SECRET, region_name="us-east-1",
                        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}))
