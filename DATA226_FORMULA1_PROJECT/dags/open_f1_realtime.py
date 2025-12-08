"""
Airflow DAG that pulls the latest completed F1 race or qualifying session from
the OpenF1 API into Snowflake RAW realtime tables, then triggers the dbt ELT DAG.
"""

import json
import math
from datetime import datetime, timezone

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.errors import ProgrammingError
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # NEW


SNOWFLAKE_CONN_ID = "my_snowflake_conn"
BASE = "https://api.openf1.org/v1"
YEAR = 2025

ENDPOINTS = ["laps", "intervals", "position", "race_control"]
SESSIONS_FILTER = ["Race", "Qualifying"]

BATCH_SIZE = 5000
SCHEMA_NAME = "RAW"
SESSIONS_TABLE_REALTIME = f"{SCHEMA_NAME}.OPENF1_SESSIONS_REALTIME"


def fetch_session_endpoint(endpoint, session_key):
    """
    Fetch full data for a given endpoint and session_key (no incremental filter).
    """
    params = {"session_key": session_key}
    url = f"{BASE}/{endpoint}"
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"fetch_session_endpoint {endpoint}: HTTP {r.status_code} - {r.text}")
    except Exception as e:
        print(f"fetch_session_endpoint error for {endpoint}: {e}")
    return []


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize values for Snowflake:
    - convert null-like strings to None
    - JSON-encode lists/dicts
    - NaN -> None
    - cast all non-None values to strings (avoid timestamp binding issues)
    """
    null_equivalents = {
        "",
        " ",
        "None",
        "none",
        "NULL",
        "null",
        "NaN",
        "nan",
        "NAN",
    }

    # Step 1: null-like strings -> None
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: None
            if isinstance(x, str) and x.strip() in null_equivalents
            else x
        )

    # Step 2: JSON encode lists/dicts
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
        )

    # Step 3: NaN/NaT -> None
    df = df.astype(object).where(pd.notnull(df), None)

    # Step 4: everything except None -> str
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    return df


def normalize_intervals_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean intervals data to avoid issues with values like '+1 LAP'.
    """

    def to_int_or_none(x):
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        s = str(x).strip()
        try:
            return str(int(float(s)))
        except Exception:
            return None

    def parse_lap_gap(x):
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        s = str(x).strip().upper()
        if "LAP" in s:
            parts = s.replace("+", "").split()
            for p in parts:
                try:
                    return str(int(float(p)))
                except Exception:
                    continue
            return None
        try:
            return str(float(s))
        except Exception:
            return None

    if "driver_number" in df.columns:
        df["driver_number"] = df["driver_number"].apply(to_int_or_none)
    if "gap_to_leader" in df.columns:
        df["gap_to_leader"] = df["gap_to_leader"].apply(parse_lap_gap)

    return df


def execute_append_logic(cursor, table_name, df):
    """
    Create table (if not exists) with all VARCHAR columns, then insert rows.
    """
    cols = ", ".join([f'"{c}" VARCHAR' for c in df.columns])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})")

    data = df.values.tolist()
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i : i + BATCH_SIZE]
        cursor.executemany(insert_sql, batch)
        print(f"Inserted batch of {len(batch)} rows into {table_name}")


def load_to_snowflake(df, table_name):
    """
    Load a dataframe into Snowflake with auto-heal on schema/type errors.
    """
    if df.empty:
        print(f"Empty dataframe, skipping load for {table_name}")
        return

    df = clean_df(df)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("BEGIN")
        execute_append_logic(cursor, table_name, df)
        conn.commit()
        print(f"Loaded {len(df)} rows into {table_name}")

    except ProgrammingError as e:
        conn.rollback()
        error_msg = str(e)
        print(f"ProgrammingError loading {table_name}: {error_msg}")

        if (
            "002020" in error_msg         # insert list vs column list mismatch
            or "match column list" in error_msg
            or "DML operation to table" in error_msg
            or "Numeric value" in error_msg
        ):
            print(f"Attempting schema/type recovery for {table_name} (drop and recreate).")
            try:
                cursor.execute("BEGIN")
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                execute_append_logic(cursor, table_name, df)
                conn.commit()
                print(f"Recovered {table_name} successfully after schema/type error.")
            except Exception as retry_e:
                conn.rollback()
                print(f"Recovery failed for {table_name}: {retry_e}")
        else:
            print(f"Non-recoverable ProgrammingError for {table_name}, giving up.")

    except Exception as e:
        conn.rollback()
        print(f"Unexpected error loading {table_name}: {e}")

    finally:
        cursor.close()
        conn.close()


def delete_session_rows(table_name: str, session_key):
    """
    Delete existing rows for a given session_key from a target table.
    If the table does not exist yet, this is a no-op.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN")
        sql = f'DELETE FROM {table_name} WHERE "session_key" = %s'
        cursor.execute(sql, (session_key,))
        conn.commit()
        print(f"Deleted existing rows from {table_name} for session_key={session_key}")
    except ProgrammingError as e:
        msg = str(e)
        if "does not exist" in msg or "not authorized" in msg:
            conn.rollback()
            print(f"Table {table_name} not found when deleting; ignoring.")
        else:
            conn.rollback()
            print(f"Error deleting rows from {table_name} for session_key={session_key}: {e}")
    except Exception as e:
        conn.rollback()
        print(f"Unexpected error deleting rows from {table_name}: {e}")
    finally:
        cursor.close()
        conn.close()


def run_realtime_sim(**context):
    """
    Select the latest completed race or qualifying session, refresh its
    realtime OpenF1 tables in Snowflake, and record the chosen session.
    """
    # 1) Fetch all sessions for the year
    try:
        sessions = requests.get(f"{BASE}/sessions?year={YEAR}", timeout=20).json()
    except Exception as e:
        print(f"Failed to fetch sessions: {e}")
        return

    df = pd.DataFrame(sessions)
    if df.empty:
        print("No sessions returned.")
        return

    # Choose time column for selecting latest completed session
    time_col = "date_end" if "date_end" in df.columns else "date_start"
    df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")

    if "meeting_name" not in df.columns:
        df["meeting_name"] = "Unknown GP"
    df["meeting_name"] = df["meeting_name"].fillna("Unknown GP")

    session_name_col = "session_name" if "session_name" in df.columns else "session_type"
    df[session_name_col] = df[session_name_col].fillna("Unknown")

    now_utc = datetime.now(timezone.utc)
    print(f"Current UTC time: {now_utc}")

    # Completed Race/Qualifying only
    mask_completed = df[time_col] <= now_utc
    mask_type = df[session_name_col].isin(SESSIONS_FILTER)
    filtered = df[mask_completed & mask_type].copy()
    if filtered.empty:
        print("No completed race/qualifying sessions found.")
        return

    # Prefer Race over Qualifying, newest first
    filtered["type_priority"] = filtered[session_name_col].apply(
        lambda x: 1 if x == "Race" else 0
    )

    target = filtered.sort_values(
        by=["type_priority", time_col],
        ascending=[False, False]
    ).iloc[0]

    sk = target["session_key"]
    meeting_name = target["meeting_name"]
    session_name = target[session_name_col]
    finished_at = target[time_col]

    print(f"Selected session: {meeting_name} - {session_name} (session_key={sk}, finished_at={finished_at})")

    # 2) Store this selected session into OPENF1_SESSIONS_REALTIME (no duplicates per session_key)
    try:
        df_session_rt = target.to_frame().T  # single row
        if "session_key" not in df_session_rt.columns:
            print("Warning: selected session has no session_key column; skipping session load.")
        else:
            delete_session_rows(SESSIONS_TABLE_REALTIME, sk)
            load_to_snowflake(df_session_rt, SESSIONS_TABLE_REALTIME)
    except Exception as e:
        print(f"Failed to load realtime session row: {e}")

    # 3) For each realtime endpoint, delete and reload full data for this session
    for ep in ENDPOINTS:
        tbl = f"{SCHEMA_NAME}.OPENF1_{ep.upper()}_REALTIME"
        print(f"Processing endpoint {ep} → table {tbl}")

        data = fetch_session_endpoint(ep, session_key=sk)
        if not data:
            print(f"No rows for {ep} for session_key={sk}")
            continue

        df_new = pd.DataFrame(data)

        if "session_key" not in df_new.columns:
            df_new["session_key"] = sk
        if "meeting_key" not in df_new.columns and "meeting_key" in target:
            df_new["meeting_key"] = target["meeting_key"]
        if "year" not in df_new.columns:
            df_new["year"] = YEAR

        if ep == "intervals":
            df_new = normalize_intervals_df(df_new)

        delete_session_rows(tbl, sk)
        load_to_snowflake(df_new, tbl)


with DAG(
    dag_id="openf1_realtime",
    default_args={"owner": "airflow"},
    schedule_interval="0 3 * * *",  # runs daily at 03:00
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    t1 = PythonOperator(
        task_id="run_sim",
        python_callable=run_realtime_sim,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="Formula1_ELT_dbt",
        wait_for_completion=False,
        conf={"source": "openf1_realtime"},
    )

    # Ensure dbt/ELT runs only after realtime load finishes
    t1 >> trigger_dbt
