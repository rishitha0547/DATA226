from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import os
import ast
import pandas as pd
import yfinance as yf


def return_snowflake_conn():
    # Create and return a Snowflake cursor using Airflow connection."""
    hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
    return hook.get_conn().cursor()

def get_logical_date():
    # Return Airflow's logical date (execution date) as YYYY-MM-DD string 
    ctx = get_current_context()
    return str(ctx["logical_date"])[:10]

def get_next_day(date_str):
    # Return next day string (used for yfinance end-date parameter)
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return (d + timedelta(days=1)).strftime("%Y-%m-%d")

def save_stock_price_as_file(symbol, start_date, end_date, file_path):
    """Download 180-day OHLCV data for a symbol and save as CSV."""
    df = yf.download([symbol], start=start_date, end=end_date, auto_adjust=False, progress=False)

    # Flatten MultiIndex (created by yfinance when using list input)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    # Skip if no data
    if df is None or df.empty:
        pd.DataFrame(columns=["date","open","high","low","close","volume","symbol"]).to_csv(file_path, index=False)
        return

    # Reformat to match Snowflake table structure
    df = df[["Open","High","Low","Close","Volume"]].copy()
    df["Symbol"] = symbol
    df = df.reset_index()
    df.rename(columns={
        "Date":"date","Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume","Symbol":"symbol"
    }, inplace=True)
    df = df[["date","open","high","low","close","volume","symbol"]]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df.to_csv(file_path, index=False)

def populate_table_via_stage(cur, database, schema, table, file_path):
    # Upload CSV file to Snowflake using temporary stage and COPY INTO.
    stage_name = f"TEMP_STAGE_{table}"
    file_name = os.path.basename(file_path)

    cur.execute(f"USE DATABASE {database}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
    cur.execute(f"USE SCHEMA {database}.{schema}")
    cur.execute(f"CREATE TEMPORARY STAGE IF NOT EXISTS {stage_name}")
    cur.execute(f"PUT file://{file_path} @{stage_name} OVERWRITE = TRUE")

    # COPY INTO with explicit column mapping for safety
    copy_sql = f"""
        COPY INTO {database}.{schema}.{table} (date, open, high, low, close, volume, symbol)
        FROM @{stage_name}/{file_name}
        FILE_FORMAT = (TYPE='CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1)
        ON_ERROR='ABORT_STATEMENT'
    """
    cur.execute(copy_sql)


# Tasks

@task
def extract(symbol: str) -> str:
    """Extract last 180 days of data for a symbol and store locally as CSV."""
    logical = get_logical_date()
    start = (datetime.strptime(logical, "%Y-%m-%d") - timedelta(days=179)).strftime("%Y-%m-%d")
    end_exclusive = get_next_day(logical)

    file_path = f"/tmp/{symbol}_{logical}.csv"
    print(f"Extracting {symbol} window {start}..{end_exclusive} (180 days)")
    save_stock_price_as_file(symbol, start, end_exclusive, file_path)
    return file_path

@task
def load(file_path: str, database: str, schema: str, table: str, symbol: str):
    """Load extracted CSV into Snowflake with transaction and idempotency."""
    if not os.path.exists(file_path):
        print(f"No file at {file_path}; skipping.")
        return

    df = pd.read_csv(file_path, parse_dates=["date"])
    if df.empty:
        print(f"Empty CSV for {symbol}; skipping.")
        return

    # Compute date range in file for safe deletion before insert
    min_d = df["date"].min().date()
    max_d = df["date"].max().date()

    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        # Create table if not exists
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} (
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume NUMBER,
                symbol VARCHAR
            )
        """)
        # Delete overlapping rows (idempotent load)
        cur.execute(f"""
            DELETE FROM {database}.{schema}.{table}
            WHERE symbol = '{symbol}' AND date BETWEEN '{min_d}' AND '{max_d}'
        """)
        # Load new data
        populate_table_via_stage(cur, database, schema, table, file_path)
        cur.execute("COMMIT;")
        print(f"Loaded {symbol}: {min_d}..{max_d}")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Load failed for {symbol}: {e}")
        raise
    finally:
        # Remove temp file
        try:
            os.remove(file_path)
        except FileNotFoundError:
            pass

# DAG Definition
with DAG(
    dag_id='YfinanceToSnowflake_ETL',
    start_date=datetime(2025, 10, 5),
    catchup=False,
    schedule='30 3 * * *',  # Daily at 3:30 AM
    tags=['ETL'],
    description='Loads last 180 days of OHLCV data for multiple tickers into Snowflake daily.'
) as dag:
    # Pull runtime parameters from Airflow Variables
    database = Variable.get("snowflake_db")       
    schema   = Variable.get("snowflake_schema")    
    table    = Variable.get("snowflake_table")     
    symbols  = ast.literal_eval(Variable.get("stock_symbols")) 

    # Build dynamic extract->load tasks for each symbol
    for sym in symbols:
        fp = extract.override(task_id=f"extract_{sym}")(sym)
        load.override(task_id=f"load_{sym}")(fp, database, schema, table, sym)
