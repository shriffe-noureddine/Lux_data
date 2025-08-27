# Minimal ingestion DAG
# What: reads a small CSV and inserts into locations and fact_mobility
# Why: end-to-end test of your ETL pattern without extra libs
# How: stdlib csv + psycopg2, using your existing Postgres connection

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import csv
import psycopg2
from pathlib import Path

from urllib.parse import urlparse

CSV_PATH = Path("/opt/airflow/dags/data/mobility_sample.csv")

def upsert_location(cur, name):
    """
    Insert the location if missing and return its id.
    """
    cur.execute("select id from locations where name = %s limit 1", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "insert into locations(name, type) values (%s, 'station') returning id",
        (name,),
    )
    return cur.fetchone()[0]

def load_csv_to_db():
    # Use the same env var pattern as before. It is already set in docker-compose.
    conn_url = os.environ.get("AIRFLOW_CONN_LUXDB", "postgresql://lux:luxpwd@project-db:5432/luxdb")

    # psycopg2.connect expects postgresql://... not ...+psycopg2
    u = urlparse(conn_url.replace("postgresql+psycopg2", "postgresql"))
    conn = psycopg2.connect(
        host=u.hostname, port=u.port or 5432,
        dbname=u.path.lstrip("/"), user=u.username, password=u.password
    )

    with conn:
        with conn.cursor() as cur, open(CSV_PATH, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row["name"].strip()
                ts = row["ts"].strip()
                veh = int(row["vehicle_count"])
                spd = float(row["avg_speed_kmh"])

                # ensure location exists, get id
                loc_id = upsert_location(cur, name)

                # insert the fact row
                cur.execute(
                    """
                    insert into fact_mobility(location_id, ts, vehicle_count, avg_speed_kmh)
                    values (%s, %s, %s, %s)
                    """,
                    (loc_id, ts, veh, spd),
                )
    conn.close()
    print("Inserted rows from CSV into fact_mobility.")

with DAG(
    dag_id="ingest_mobility_local",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # manual trigger only
    catchup=False,
    tags=["mobility","ingest"],
) as dag:
    PythonOperator(task_id="load_csv", python_callable=load_csv_to_db)
