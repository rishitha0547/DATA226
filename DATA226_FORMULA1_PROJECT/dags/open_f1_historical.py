"""
Airflow DAG that backfills recent F1 race and qualifying sessions from the
OpenF1 API into Snowflake RAW *_HISTORICAL tables on a rolling window.
"""

import json
import math
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.errors import ProgrammingError


SNOWFLAKE_CONN_ID = "my_snowflake_conn"
BASE = "https://api.openf1.org/v1"

# Current F1 season year 
YEAR = 2025

# Approximate window for "3–4 months" of history
WINDOW_DAYS = 120

ENDPOINTS = ["laps", "intervals", "position", "race_control"]
SESSIONS_FILTER = ["Race", "Qualifying"]

BATCH_SIZE = 5000
SCHEMA_NAME = "RAW"

SESSIONS_TABLE_HIST = f"{SCHEMA_NAME}.OPENF1_SESSIONS_HISTORICAL"
LAPS_TABLE_HIST = f"{SCHEMA_NAME}.OPENF1_LAPS_HISTORICAL"
INTERVALS_TABLE_HIST = f"{SCHEMA_NAME}.OPENF1_INTERVALS_HISTORICAL"
POSITION_TABLE_HIST = f"{SCHEMA_NAME}.OPENF1_POSITION_HISTORICAL"
RACE_CTRL_TABLE_HIST = f"{SCHEMA_NAME}.OPENF1_RACE_CONTROL_HISTORICAL"


def fetch_session_endpoint(endpoint, session_key):
    """
    Fetch full data for a given endpoint and session_key (no incremental filter).
    """
    params = {"session_key": session_key}
    url = f"{BASE}/{endpoint}"
    try:
        r = requests.get(url, params=params, timeout=30)
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


def get_loaded_session_keys() -> set:
    """
    Return the set of session_keys already present in the historical sessions table.
    If the table does not exist yet, returns empty set.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    try:
        df = hook.get_pandas_df(
            f'SELECT DISTINCT "session_key" AS session_key FROM {SESSIONS_TABLE_HIST}'
        )
        if df.empty:
            print(f"{SESSIONS_TABLE_HIST} exists but has no rows yet.")
            return set()
        col = df.columns[0]
        return set(df[col].tolist())
    except ProgrammingError as e:
        msg = str(e)
        if "does not exist" in msg or "Object '" in msg and " does not exist" in msg:
            print(f"{SESSIONS_TABLE_HIST} does not exist yet; no historical sessions loaded.")
            return set()
        else:
            print(f"Error querying existing sessions from {SESSIONS_TABLE_HIST}: {e}")
            return set()
    except Exception as e:
        print(f"Unexpected error querying existing sessions: {e}")
        return set()

def run_historical_recent(**context):
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=WINDOW_DAYS)
    print(f"Historical window: from {cutoff} to {now_utc} (last {WINDOW_DAYS} days)")

    # 1) Fetch all sessions for the given YEAR
    try:
        sessions = requests.get(f"{BASE}/sessions?year={YEAR}", timeout=30).json()
    except Exception as e:
        print(f"Failed to fetch sessions for year {YEAR}: {e}")
        return

    df = pd.DataFrame(sessions)
    if df.empty:
        print(f"No sessions returned for year {YEAR}.")
        return

    # Choose time column for filtering sessions and to find latest completed
    time_col = "date_end" if "date_end" in df.columns else "date_start"
    df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")

    if "meeting_name" not in df.columns:
        df["meeting_name"] = "Unknown GP"
    df["meeting_name"] = df["meeting_name"].fillna("Unknown GP")

    session_name_col = "session_name" if "session_name" in df.columns else "session_type"
    df[session_name_col] = df[session_name_col].fillna("Unknown")

    # 2) Identify latest completed Race/Quali session (used by realtime DAG)
    mask_completed_all = df[time_col] <= now_utc
    mask_type_all = df[session_name_col].isin(SESSIONS_FILTER)
    completed = df[mask_completed_all & mask_type_all].copy()

    latest_sk = None
    if not completed.empty:
        completed["type_priority"] = completed[session_name_col].apply(
            lambda x: 1 if x == "Race" else 0
        )
        latest = completed.sort_values(
            by=["type_priority", time_col],
            ascending=[False, False]
        ).iloc[0]
        latest_sk = latest["session_key"]
        print(
            f"Latest completed session (realtime): "
            f"session_key={latest_sk}, {latest['meeting_name']} - {latest[session_name_col]} at {latest[time_col]}"
        )
    else:
        print("No completed Race/Qualifying sessions found for latest realtime reference.")

    # 3) Historical candidate sessions in window (excluding latest session_key)
    mask_window = (df[time_col] >= cutoff) & (df[time_col] <= now_utc)
    mask_type = df[session_name_col].isin(SESSIONS_FILTER)

    if latest_sk is not None:
        mask_not_latest = df["session_key"] != latest_sk
    else:
        mask_not_latest = True

    filtered = df[mask_window & mask_type & mask_not_latest].copy()

    if filtered.empty:
        print("No Race/Qualifying sessions found in the historical window (excluding latest).")
        return

    print(
        f"Historical candidate sessions in window (excluding latest): {len(filtered)}"
    )

    # 4) Get already loaded session_keys (incremental)
    loaded_session_keys = get_loaded_session_keys()
    print(f"Existing historical sessions: {len(loaded_session_keys)} session_keys already loaded.")

    # 5) For each session in the window, load if not already present
    for _, row in filtered.sort_values(by=[time_col]).iterrows():
        sk = row.get("session_key")
        if sk is None:
            print("Skipping session row with no session_key.")
            continue

        if sk in loaded_session_keys:
            print(f"Session_key={sk} already in historical; skipping.")
            continue

        meeting_name = row["meeting_name"]
        session_name = row[session_name_col]
        session_time = row[time_col]

        print(f"Loading historical data for session_key={sk}: {meeting_name} - {session_name} at {session_time}")

        # 5a) Insert this session into sessions historical
        try:
            df_session = row.to_frame().T
            load_to_snowflake(df_session, SESSIONS_TABLE_HIST)
        except Exception as e:
            print(f"Failed to load session_key={sk} into {SESSIONS_TABLE_HIST}: {e}")
            continue

        # 5b) Load endpoints for this session into historical tables
        for ep in ENDPOINTS:
            if ep == "laps":
                target_table = LAPS_TABLE_HIST
            elif ep == "intervals":
                target_table = INTERVALS_TABLE_HIST
            elif ep == "position":
                target_table = POSITION_TABLE_HIST
            elif ep == "race_control":
                target_table = RACE_CTRL_TABLE_HIST
            else:
                continue

            print(f"  -> Loading endpoint={ep} into {target_table} for session_key={sk}")

            data = fetch_session_endpoint(ep, session_key=sk)
            if not data:
                print(f"  -> No rows for {ep} for session_key={sk}")
                continue

            df_new = pd.DataFrame(data)

            if "session_key" not in df_new.columns:
                df_new["session_key"] = sk
            if "meeting_key" not in df_new.columns and "meeting_key" in row:
                df_new["meeting_key"] = row["meeting_key"]
            if "year" not in df_new.columns:
                df_new["year"] = YEAR

            if ep == "intervals":
                df_new = normalize_intervals_df(df_new)

            try:
                load_to_snowflake(df_new, target_table)
            except Exception as e:
                print(f"  -> Failed to load {ep} for session_key={sk}: {e}")
                continue

        # Mark this session_key as loaded to avoid duplicate loads in same/future runs
        loaded_session_keys.add(sk)

with DAG(
    dag_id="openf1_historical_recent",
    default_args={"owner": "airflow"},
    schedule_interval="@weekly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    t_hist = PythonOperator(
        task_id="run_historical_recent",
        python_callable=run_historical_recent,
    )
