from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os, psycopg2
from urllib.parse import urlparse

def _connect():
    url = os.environ.get("AIRFLOW_CONN_LUXDB", "postgresql://lux:luxpwd@project-db:5432/luxdb")
    u = urlparse(url.replace("postgresql+psycopg2", "postgresql"))
    return psycopg2.connect(host=u.hostname, port=u.port or 5432,
                            dbname=u.path.lstrip("/"), user=u.username, password=u.password)

def dq_latest_has_rows():
    """Fail fast if the latest date in fact_mobility has zero rows."""
    conn = _connect()
    with conn, conn.cursor() as cur:
        cur.execute("select max(ts::date) from fact_mobility")
        latest = cur.fetchone()[0]
        if latest is None:
            raise ValueError("DQ FAIL: fact_mobility is empty")
        cur.execute("select count(*) from fact_mobility where ts::date=%s", (latest,))
        n = cur.fetchone()[0]
        if n == 0:
            raise ValueError(f"DQ FAIL: no rows for latest date {latest}")
        print(f"DQ OK: {n} rows for {latest}")
    conn.close()

def compute_kpi():
    """Compute vehicles_total and avg_speed_kmh for the latest available date (idempotent upsert)."""
    conn = _connect()
    with conn, conn.cursor() as cur:
        cur.execute("select max(ts::date) from fact_mobility")
        latest = cur.fetchone()[0]
        if latest is None:
            print("No mobility data; skip KPI.")
            return
        cur.execute("select coalesce(sum(vehicle_count),0) from fact_mobility where ts::date=%s", (latest,))
        vehicles_total = cur.fetchone()[0]
        cur.execute("select coalesce(avg(avg_speed_kmh),0) from fact_mobility where ts::date=%s", (latest,))
        avg_speed = float(cur.fetchone()[0])
        for metric, value in [("vehicles_total", vehicles_total), ("avg_speed_kmh", avg_speed)]:
            cur.execute("""
                insert into kpi_daily(dt, metric, value)
                values (%s,%s,%s)
                on conflict (dt, metric) do update set value=excluded.value
            """, (latest, metric, value))
        print(f"KPI computed for {latest}: vehicles_total={vehicles_total}, avg_speed_kmh={avg_speed}")
    conn.close()

with DAG(
    dag_id="kpi_mobility_daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 5 * * *",   # run every day at 05:30
    catchup=False,
    tags=["kpi","mobility"],
) as dag:
    dq = PythonOperator(task_id="dq_latest_has_rows", python_callable=dq_latest_has_rows)
    kpi = PythonOperator(task_id="compute_kpi", python_callable=compute_kpi)
    dq >> kpi
