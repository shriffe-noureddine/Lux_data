# Ingest ONE PCH mobility .xlsx into Postgres (idempotent)
# WHAT: Reads a local XLSX by default, or a remote XLSX if you pass source_url at trigger time.
# WHY: PCH files store 24 hourly columns (P00_01..P23_24). We "melt" them to 1 row/hour.
# HOW: pandas + openpyxl to read; UPSERT into fact_mobility on (location_id, ts).

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import urlparse
import io
import os
import re
import psycopg2
import pandas as pd

# ---------- CONFIG ----------
# Default local file path *inside the container*. Put your XLSX here on the host:
# lux-insights/etl/airflow/dags/data/donneestrafic-2023.xlsx
XLSX_PATH = Path("/opt/airflow/dags/data/donneestrafic-2023.xlsx")

# For columns that are NOT the hourly bins. Override via "field_map" at trigger if your headers differ.
# We only need these identifiers; everything else is derived.
DEFAULT_FIELD_MAP = {
    "date": "DATECOM",        # date of the record (one per day)
    "poste_id": "POSTE_ID",   # station id
    "localite": "LOCALITE",   # locality (nice to have for the station name)
    "route": "ROUTE"          # route code (nice to have for the station name)
}

# ---------- DB HELPERS ----------
def _connect():
    """Connect to project DB using AIRFLOW_CONN_LUXDB or default."""
    url = os.environ.get("AIRFLOW_CONN_LUXDB", "postgresql://lux:luxpwd@project-db:5432/luxdb")
    u = urlparse(url.replace("postgresql+psycopg2", "postgresql"))
    return psycopg2.connect(
        host=u.hostname, port=u.port or 5432,
        dbname=u.path.lstrip("/"), user=u.username, password=u.password
    )

def _upsert_location(cur, poste_id: str, localite: str, route: str) -> int:
    """
    Create/find a station row. We compose a readable name from POSTE_ID/LOCALITE/ROUTE.
    NOTE: Your 'locations' table has (id, name, type, lat, lon). We only fill name/type here.
    """
    name = f"POSTE_{poste_id} - {localite or ''} {route or ''}".strip()
    cur.execute("select id from locations where name=%s limit 1", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "insert into locations(name, type) values (%s,'station') returning id",
        (name,)
    )
    return cur.fetchone()[0]

# ---------- IO ----------
def _load_xlsx_local() -> pd.DataFrame:
    """Read the local XLSX (raises if missing)."""
    if not XLSX_PATH.exists():
        raise FileNotFoundError(f"Local XLSX not found at {XLSX_PATH}")
    return pd.read_excel(XLSX_PATH, engine="openpyxl")

def _load_xlsx_url(url: str) -> pd.DataFrame:
    """Fetch remote XLSX into memory and read with pandas."""
    with urlopen(url) as resp:
        raw = resp.read()
    return pd.read_excel(io.BytesIO(raw), engine="openpyxl")

# ---------- TRANSFORM ----------
def _normalize(df: pd.DataFrame, field_map: dict) -> pd.DataFrame:
    """
    From wide P00_01..P23_24 to long rows:
      columns needed: date, poste_id, localite (opt), route (opt), P??_??
    We output: [poste_id, localite, route, ts, vehicle_count]
    """
    # Clean up headers (trim whitespace)
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    print("XLSX columns:", df.columns.tolist())

    # Resolve mapping for identifier columns
    fm = {**DEFAULT_FIELD_MAP, **(field_map or {})}
    date_col = fm["date"]
    poste_col = fm["poste_id"]
    localite_col = fm.get("localite")
    route_col = fm.get("route")

    # Find the 24 hourly columns by regex like P00_01 .. P23_24
    hour_cols = [c for c in df.columns if re.fullmatch(r"P\d{2}_\d{2}", str(c))]
    if len(hour_cols) == 0:
        raise ValueError("Could not find any hourly columns like 'P00_01' .. 'P23_24'.")

    # Keep only id columns + hourly columns
    id_cols = [poste_col, date_col] + [c for c in [localite_col, route_col] if c and c in df.columns]
    df_keep = df[id_cols + hour_cols]

    # Melt to long form: one row per (date, poste, hour_bin)
    df_long = df_keep.melt(
        id_vars=id_cols,
        value_vars=hour_cols,
        var_name="hour_bin",
        value_name="vehicle_count"
    )

    # Parse hour from "P00_01" -> 0, "P15_16" -> 15, etc.
    df_long["hour"] = df_long["hour_bin"].str[1:3].astype(int)

    # Parse date. Files are LU-style; dayfirst=True is safer.
    # We set UTC for consistency—BI tools can handle TZ separately.
    dt = pd.to_datetime(df_long[date_col], errors="coerce", dayfirst=True)
    df_long["ts"] = pd.to_datetime(dt.dt.date) + pd.to_timedelta(df_long["hour"], unit="h")
    df_long["ts"] = pd.to_datetime(df_long["ts"], utc=True)

    # Coerce counts
    df_long["vehicle_count"] = pd.to_numeric(df_long["vehicle_count"], errors="coerce").fillna(0).astype(int)

    # Optional metadata (may be missing)
    if localite_col in df_long.columns:
        df_long["localite"] = df_long[localite_col].astype(str)
    else:
        df_long["localite"] = ""

    if route_col in df_long.columns:
        df_long["route"] = df_long[route_col].astype(str)
    else:
        df_long["route"] = ""

    # Drop rows with no station id or no timestamp (bad data)
    before = len(df_long)
    df_long = df_long.dropna(subset=[poste_col, "ts"])
    after = len(df_long)
    print(f"Normalized rows: {after} (dropped {before - after})")

    # Group by hour to sum across any per-vehicle/per-direction splits
    df_grp = (
        df_long
        .groupby([poste_col, "localite", "route", "ts"], as_index=False)["vehicle_count"]
        .sum()
    )

    # Rename for DB loader
    df_grp = df_grp.rename(columns={
        poste_col: "poste_id"
    })[["poste_id", "localite", "route", "ts", "vehicle_count"]]

    print("Sample rows to insert:\n", df_grp.head(10).to_string(index=False))
    return df_grp

# ---------- TASK ----------
def run_ingest(**context):
    """Choose local file or URL, normalize, then UPSERT rows into fact_mobility."""
    p = context.get("params") or {}
    source_url = (p.get("source_url") or "").strip()
    field_map = p.get("field_map") or {}

    # Load
    if source_url:
        print(f"Loading XLSX from URL: {source_url}")
        raw_df = _load_xlsx_url(source_url)
    else:
        print(f"Loading XLSX from local path: {XLSX_PATH}")
        raw_df = _load_xlsx_local()

    # Transform (wide -> long)
    df = _normalize(raw_df, field_map)

    # Write
    conn = _connect()
    inserted = 0
    with conn:
        with conn.cursor() as cur:
            for r in df.itertuples(index=False):
                # Ensure location exists
                loc_id = _upsert_location(cur, str(r.poste_id), r.localite, r.route)
                # Idempotent upsert on (location_id, ts)
                cur.execute(
                    """
                    insert into fact_mobility(location_id, ts, vehicle_count, avg_speed_kmh)
                    values (%s,%s,%s,%s)
                    on conflict (location_id, ts)
                    do update set vehicle_count = excluded.vehicle_count
                    """,
                    (loc_id, r.ts.to_pydatetime(), int(r.vehicle_count), 0.0)
                )
                inserted += 1
    conn.close()
    print(f"Ingest complete. rows_processed={inserted}, source={'url' if source_url else 'local'}")

# ---------- DAG ----------
with DAG(
    dag_id="ingest_mobility_pch_xlsx",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # keep manual until validated
    catchup=False,
    params={"source_url": "", "field_map": {}},  # you can override at Trigger time
    tags=["mobility", "ingest", "xlsx", "idempotent"],
) as dag:
    PythonOperator(
        task_id="ingest_one",
        python_callable=run_ingest,
        provide_context=True,
    )
