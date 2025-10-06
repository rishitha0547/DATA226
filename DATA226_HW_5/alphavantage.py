# Airflow, Snowflake, and Python library imports
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import requests
import pandas as pd

# function to connect to snowflake 
def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(symbol: str):
    """Fetch daily stock data for given symbol from Alpha Vantage API
    Args:
        symbol (str): Stock ticker symbol (e.g., 'AMZN')
    Returns:
        dict: Raw JSON data from API response
    ."""

    # Get API key from Airflow Variables
    api_key = Variable.get("alphavantage_api_key")  
    # Fetch data from Alpha Vantage API
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key,
        "outputsize": "compact"
    }
    # Make API request 
    response = requests.get(url, params=params)
    data = response.json()
    # Basic error handling for API response
    if "Time Series (Daily)" not in data:
        raise ValueError("Alpha Vantage API error or no data returned.")
    return data["Time Series (Daily)"]


@task
def transform(raw_data: dict, symbol: str):
    """Transform raw stock data into cleaned records for Snowflake insertion.
    Args:
        raw_data (dict): Raw JSON data from Alpha Vantage API
        symbol (str): Stock ticker symbol
    Returns:
        list: List of cleaned records as dictionaries
    """
    # Transform raw API data into a DataFrame
    df = pd.DataFrame.from_dict(raw_data, orient='index')
    # Rename columns to standard format
    df = df.rename(columns=lambda x: x.split(' ')[1])
    df.reset_index(inplace=True)
    # Rename index column to 'date'
    df.rename(columns={'index':'date'}, inplace=True)
    df['date'] = df['date'].astype(str)  # Convert dates to string for serialization
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert numeric columns reliably
    df.sort_values('date', inplace=True)
    df['symbol'] = symbol  # Add symbol column
    return df.to_dict(orient='records')  # Convert to list of dicts for insertion


@task
def load(records: list, target_table: str):
    """Load cleaned stock data records into Snowflake table.
    Args:
        records (list): List of cleaned records as dictionaries
        target_table (str): Target Snowflake table name (e.g., 'raw.amazon_stock_price')
    """
    cur = get_snowflake_cursor()
    try:
        # Start transaction
        cur.execute("BEGIN;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (   
                symbol STRING NOT NULL,
                date DATE NOT NULL,
                open FLOAT,
                close FLOAT,
                high FLOAT,
                low FLOAT,
                volume NUMBER,
                PRIMARY KEY(symbol, date)
            );
        """)  # Create table if not exists
        cur.execute(f"DELETE FROM {target_table};")  # Full refresh: delete existing records
        # Insert records in batch 
        insert_sql = f"""
            INSERT INTO {target_table} (symbol, date, open, close, high, low, volume) 
            VALUES (%(symbol)s, %(date)s, %(open)s, %(close)s, %(high)s, %(low)s, %(volume)s); 
        """
        
        cur.executemany(insert_sql, records)  # Batch insert for efficiency
        cur.execute("COMMIT;")  # Commit transaction if successful
    except Exception as e:
        cur.execute("ROLLBACK;")  # Rollback transaction on error
        raise e
    finally:
        cur.close()  # Close cursor

# Define dag, schedule, and task dependencies
with DAG(
    dag_id='alpha_vantage_etl',
    start_date=datetime(2025, 9, 30),
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False,
    tags=['homework4', 'ETL']
) as dag:
    symbol = "AMZN"  # Stock symbol to extract
    target_table = "raw.amazon_stock_price"  # Target Snowflake table
    
    # Define task dependencies for ETL process 
    raw = extract(symbol)
    cleaned = transform(raw, symbol)
    load(cleaned, target_table)
