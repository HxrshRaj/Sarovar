"""Block until an Airflow DAG has N successful runs (or any failure). Usage: wait_for_runs.py <dag_id> <n>"""
import subprocess
import sys
import time

dag, n = sys.argv[1], int(sys.argv[2])
while True:
    out = subprocess.run(["docker", "compose", "exec", "-T", "airflow", "airflow", "dags", "list-runs", dag],
                         capture_output=True, text=True).stdout
    ok = sum(1 for ln in out.splitlines() if "| success" in ln)
    bad = [ln for ln in out.splitlines() if "| failed" in ln]
    print(f"{dag}: {ok}/{n} success, {len(bad)} failed", flush=True)
    if bad:
        print("\n".join(bad))
        sys.exit(1)
    if ok >= n:
        break
    time.sleep(30)
