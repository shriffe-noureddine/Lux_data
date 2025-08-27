from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import psycopg2
from urllib.parse import urlparse

def write_canary():
    # Read connection URL from env (provided via AIRFLOW_CONN_LUXDB)
    conn_url = os.environ.get("AIRFLOW_CONN_LUXDB")
    # Parse it to components
    u = urlparse(conn_url.replace("postgresql+psycopg2", "postgresql"))
    conn = psycopg2.connect(
        host=u.hostname, port=u.port or 5432,
        dbname=u.path.lstrip("/"), user=u.username, password=u.password
    )
    with conn, conn.cursor() as cur:
        cur.execute("insert into canary(note) values (%s)", ("hello from airflow",))
    conn.close()
    print("Canary insert done.")

with DAG(
    dag_id="db_canary",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=["test","db"],
) as dag:
    PythonOperator(task_id="insert_canary", python_callable=write_canary)
