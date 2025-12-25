from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from urllib.request import urlopen
import csv, io, os
import psycopg2
from urllib.parse import urlparse

# Default: no URL so you don’t accidentally 404; we’ll pass it at trigger time
DEFAULT_URL = ""

def _connect():
    url = os.environ.get("AIRFLOW_CONN_LUXDB", "postgresql://lux:luxpwd@project-db:5432/luxdb")
    u = urlparse(url.replace("postgresql+psycopg2", "postgresql"))
    return psycopg2.connect(host=u.hostname, port=u.port or 5432,
                            dbname=u.path.lstrip("/"), user=u.username, password=u.password)

def _upsert_location(cur, name):
    cur.execute("select id from locations where name=%s limit 1", (name,))
    row = cur.fetchone()
    if row: return row[0]
    cur.execute("insert into locations(name, type) values (%s,'station') returning id", (name,))
    return cur.fetchone()[0]

def _rows_from_url(url: str):
    with urlopen(url) as resp:
        content = resp.read().decode("utf-8", errors="replace")
    return list(csv.DictReader(io.StringIO(content)))

def task_ingest(**context):
    params = (context.get("params") or {})
    url = (params.get("source_url") or DEFAULT_URL).strip()
    if not url:
        raise ValueError("No source_url provided. Pass it in the Trigger config.")

    # Field map lets you adapt to any CSV header names
    # Map CSV columns -> logical names used below
    field_map = params.get("field_map") or {
        "name": "name",
        "ts": "ts",
        "vehicle_count": "vehicle_count",
        "avg_speed_kmh": "avg_speed_kmh"
    }

    rows = _rows_from_url(url)

    conn = _connect()
    inserted, updated = 0, 0
    with conn:
        with conn.cursor() as cur:
            for r in rows:
                name = (r.get(field_map["name"]) or "Unknown").strip()
                ts   = (r.get(field_map["ts"]) or "").strip()
                veh  = int(r.get(field_map["vehicle_count"]) or 0)
                spd  = float(r.get(field_map["avg_speed_kmh"]) or 0.0)

                loc_id = _upsert_location(cur, name)
                cur.execute("""
                    insert into fact_mobility(location_id, ts, vehicle_count, avg_speed_kmh)
                    values (%s,%s,%s,%s)
                    on conflict (location_id, ts)
                    do update set vehicle_count = excluded.vehicle_count,
                                  avg_speed_kmh = excluded.avg_speed_kmh
                """, (loc_id, ts, veh, spd))
                inserted += 1  # treat as success; PG counts can vary on UPSERT
    conn.close()
    print(f"Ingestion done. url={url} rows={len(rows)}")
    return True

with DAG(
    dag_id="ingest_mobility_remote",
    start_date=datetime(2025,1,1),
    schedule_interval=None,   # we’ll schedule after you validate a real URL
    catchup=False,
    params={"source_url": DEFAULT_URL},
    tags=["mobility","ingest","idempotent"],
) as dag:
    PythonOperator(task_id="ingest", python_callable=task_ingest, provide_context=True)
