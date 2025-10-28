from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def get_cursor():
    """Return a Snowflake cursor using the Airflow connection."""
    hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
    conn = hook.get_conn()
    return conn, conn.cursor()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 1),  
    "retries": 1,
}

with DAG(
    dag_id="snowflake_import_dag",
    default_args=default_args,
    schedule_interval="@daily", 
    catchup=False,
    tags=["snowflake", "etl"],
) as dag:

    @task
    def create_and_load_raw_tables():
        conn, cur = get_cursor()
        try:
            # Start transaction
            cur.execute("BEGIN;")

            # Create tables if not exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw.user_session_channel (
                    userId INT,
                    sessionId VARCHAR(64) PRIMARY KEY,
                    channel VARCHAR(64) DEFAULT 'direct'
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw.session_timestamp (
                    sessionId VARCHAR(64) PRIMARY KEY,
                    ts TIMESTAMP
                );
            """)

            # Create or replace stage
            cur.execute("""
                CREATE OR REPLACE STAGE raw.blob_stage
                URL='s3://s3-geospatial/readonly/'
                FILE_FORMAT=(TYPE=CSV, SKIP_HEADER=1, FIELD_OPTIONALLY_ENCLOSED_BY='"');
            """)

            # Load data using COPY INTO
            cur.execute("""
                COPY INTO raw.user_session_channel
                FROM @raw.blob_stage/user_session_channel.csv
                FILE_FORMAT=(TYPE=CSV, SKIP_HEADER=1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
                ON_ERROR='ABORT_STATEMENT';
            """)

            cur.execute("""
                COPY INTO raw.session_timestamp
                FROM @raw.blob_stage/session_timestamp.csv
                FILE_FORMAT=(TYPE=CSV, SKIP_HEADER=1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
                ON_ERROR='ABORT_STATEMENT';
            """)

            cur.execute("COMMIT;")

        except Exception as e:
            cur.execute("ROLLBACK;")
            raise

        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    create_and_load_raw_tables()
