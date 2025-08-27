from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from urllib.request import urlopen
import csv, io, os
import psycopg2
from urllib.parse import urlparse

DEFAULT_URL = "https://example.com/lux_mobility.csv"  # replace with real CSV endpoint

def upsert_location(cur, name):
    cur.execute("select id from locations where name=%s limit 1", (name,))
    row = cur.fetchone()
    if row: return row[0]
    cur.execute("insert into locations(name, type) values (%s,'station') returning id", (name,))
    return cur.fetchone()[0]

def ingest(url: str):
    # fetch CSV over HTTP(S)
    with urlopen(url) as resp:
        content = resp.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(content))

    # connect to project DB (from env)
    conn_url = os.environ.get("AIRFLOW_CONN_LUXDB", "postgresql://lux:luxpwd@project-db:5432/luxdb")
    u = urlparse(conn_url.replace("postgresql+psycopg2", "postgresql"))
    conn = psycopg2.connect(host=u.hostname, port=u.port or 5432,
                            dbname=u.path.lstrip("/"), user=u.username, password=u.password)

    inserted, upserted = 0, 0
    with conn:
        with conn.cursor() as cur:
            for row in reader:
                # normalize minimal fields (adapt to your source header names)
                name = row.get("station") or row.get("name") or "Unknown"
                ts   = row.get("timestamp") or row.get("ts")
                veh  = int(row.get("vehicle_count") or row.get("count") or 0)
                spd  = float(row.get("avg_speed_kmh") or row.get("speed") or 0.0)

                loc_id = upsert_location(cur, name)

                # idempotent upsert via unique(location_id, ts)
                cur.execute("""
                    insert into fact_mobility(location_id, ts, vehicle_count, avg_speed_kmh)
                    values (%s,%s,%s,%s)
                    on conflict (location_id, ts)
                    do update set vehicle_count=excluded.vehicle_count,
                                  avg_speed_kmh=excluded.avg_speed_kmh
                """, (loc_id, ts, veh, spd))
                if cur.rowcount == 1:
                    inserted += 1
                else:
                    upserted += 1
    conn.close()
    print(f"Ingestion done. inserted={inserted}, updated={upserted}")

def task_ingest(**context):
    url = context["params"].get("source_url") or DEFAULT_URL
    ingest(url)

with DAG(
    dag_id="ingest_mobility_remote",
    start_date=datetime(2025,1,1),
    schedule_interval=None,   # manual for now; we’ll schedule later
    catchup=False,
    params={"source_url": DEFAULT_URL},
    tags=["mobility","ingest","idempotent"],
) as dag:
    PythonOperator(task_id="ingest", python_callable=task_ingest, provide_context=True)
