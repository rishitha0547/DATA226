from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd


default_args = {
    'owner': 'Data226_lab1_Rishitha',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

RAW_TABLE = "USER_DB_FERRET.RAW.MSFT_GOOGL_RAW_DATA"
SNOWFLAKE_CONN_ID = "my_snowflake_conn"
STOCK_DAYS = 180
SYMBOLS = ['GOOGL', 'MSFT']


def fetch_and_load_stock_data(**kwargs):
    """
    Fetch recent stock prices for MSFT and GOOGL, then load them into Snowflake.

    Retrieves last 180 trading days from Yahoo Finance, formats and cleans data,
    and upserts to a Snowflake table.
    Args:
        **kwargs: Airflow context arguments (not used here).
    Returns:
        str: Summary of operation result.
    """
    frames = []

    for symbol in SYMBOLS:
        # Fetch historical stock data from Yahoo Finance API with buffer for non-trading days 
        df = yf.download(symbol, period=f"{STOCK_DAYS * 2 + 60}d", interval="1d",
                         auto_adjust=True, progress=False)

        if df is None or df.empty:
            # Skip if no data returned for symbol (invalid ticker or no recent data)
            continue

        # Handle MultiIndex columns if present (e.g :  from multiple tickers) 
        if isinstance(df.columns, pd.MultiIndex):
            df = df.xs(symbol, axis=1, level=-1)

        # Keep only the last 180 trading days
        df = df.sort_index().tail(STOCK_DAYS)

        # Reset index to turn date into a column
        df = df.reset_index()

        # Clean column names into uppercase and underscore format
        df.columns = [str(c).upper().replace(" ", "_") for c in df.columns]

        # Remove incomplete rows missing critical info
        df = df.dropna(subset=["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"])

        # Add symbol column for identification
        df["SYMBOL"] = symbol

        # Convert date to timezone-naive datetime for consistency 
        df["TRADE_DATETIME"] = pd.to_datetime(df["DATE"]).dt.tz_localize(None)

        # Collect columns required for Snowflake upload
        frames.append(df[["SYMBOL", "TRADE_DATETIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]])

    if not frames:
        return "No data fetched."

    # Combine data for all symbols into one DataFrame
    data = pd.concat(frames, ignore_index=True)

    # Format rows and serialize datetime for Snowflake insertion
    rows = [
        (
            r.SYMBOL,
            r.TRADE_DATETIME.isoformat(),
            float(r.OPEN),
            float(r.HIGH),
            float(r.LOW),
            float(r.CLOSE),
            int(r.VOLUME),
        )
        for r in data.itertuples(index=False)
    ]

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")

        # Create raw data table if not exists, with composite primary key
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
                SYMBOL STRING NOT NULL,
                TRADE_DATETIME TIMESTAMP_NTZ NOT NULL,
                OPEN FLOAT,
                HIGH FLOAT,
                LOW FLOAT,
                CLOSE FLOAT,
                VOLUME NUMBER,
                LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                CONSTRAINT PK_MSFT_GOOGL PRIMARY KEY (SYMBOL, TRADE_DATETIME)
            );
        """)

        # Use a temp table to stage new data
        cur.execute(f"CREATE OR REPLACE TEMP TABLE TMP_MSFT_GOOGL LIKE {RAW_TABLE};")

        # Bulk insert new rows into temp table 
        cur.executemany(
            f"""INSERT INTO TMP_MSFT_GOOGL
                (SYMBOL, TRADE_DATETIME, OPEN, HIGH, LOW, CLOSE, VOLUME)
                VALUES (%s, %s::TIMESTAMP_NTZ, %s, %s, %s, %s, %s)""",
            rows
        )

        # Upsert from temp table to main table
        cur.execute(f"""
            MERGE INTO {RAW_TABLE} T
            USING TMP_MSFT_GOOGL S
            ON T.SYMBOL = S.SYMBOL AND T.TRADE_DATETIME = S.TRADE_DATETIME
            WHEN MATCHED THEN UPDATE SET
                T.OPEN = S.OPEN,
                T.HIGH = S.HIGH,
                T.LOW = S.LOW,
                T.CLOSE = S.CLOSE,
                T.VOLUME = S.VOLUME,
                T.LOAD_TS = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (SYMBOL, TRADE_DATETIME, OPEN, HIGH, LOW, CLOSE, VOLUME)
            VALUES
                (S.SYMBOL, S.TRADE_DATETIME, S.OPEN, S.HIGH, S.LOW, S.CLOSE, S.VOLUME);
        """)

        cur.execute("COMMIT;")
        return f"Upserted {len(rows)} rows into {RAW_TABLE}"

    except Exception:
        cur.execute("ROLLBACK;")
        raise

    finally:
        cur.close()
        conn.close()

# Define the DAG and its tasks 
with DAG(
    'MSFT_GOOGLE_Stock_data_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    load_stock_data_task = PythonOperator(
        task_id='fetch_and_load_stock_data',
        python_callable=fetch_and_load_stock_data,
    )
