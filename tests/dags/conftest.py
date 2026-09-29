import os
import sys

for p in ("/opt/airflow/dags", "/opt/airflow/include"):
    if os.path.isdir(p) and p not in sys.path:
        sys.path.insert(0, p)
