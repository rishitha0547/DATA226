from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 26),
    "retries": 1
}


with DAG(
    dag_id="snowflake_import_dag",
    default_args=default_args,
    schedule_interval="@daily", 
    catchup=False,
    tags=["snowflake", "etl"],
) as dag:

    # Task 1: Truncate raw.user_session_channel
   
    truncate_user_session_channel = SnowflakeOperator(
        task_id="truncate_user_session_channel",
        snowflake_conn_id="my_snowflake_conn",
        sql=f"""
            TRUNCATE TABLE raw.user_session_channel;
        """,
    )

    # Task 2: Truncate raw.session_timestamp
   
    truncate_session_timestamp = SnowflakeOperator(
        task_id="truncate_session_timestamp",
        snowflake_conn_id="my_snowflake_conn",
        sql=f"""
            TRUNCATE TABLE raw.session_timestamp;
        """,
    )

    
    # Task 3: Load data into raw.user_session_channel

    copy_user_session_channel = SnowflakeOperator(
        task_id="copy_user_session_channel",
        snowflake_conn_id="my_snowflake_conn",
        sql=f"""
        
            COPY INTO raw.user_session_channel
            FROM @raw.blob_stage/user_session_channel.csv
            FILE_FORMAT=(TYPE=CSV, SKIP_HEADER=1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
            ON_ERROR='ABORT_STATEMENT';
        """,
    )

    
    # Task 4: Load data into raw.session_timestamp
    
    copy_session_timestamp = SnowflakeOperator(
        task_id="copy_session_timestamp",
        snowflake_conn_id="my_snowflake_conn",
        sql=f"""
            
            COPY INTO raw.session_timestamp
            FROM @raw.blob_stage/session_timestamp.csv
            FILE_FORMAT=(TYPE=CSV, SKIP_HEADER=1, FIELD_OPTIONALLY_ENCLOSED_BY='"')
            ON_ERROR='ABORT_STATEMENT';
        """,
    )

    
    # Task dependencies
    # Truncate tables first, then load new data

    truncate_user_session_channel >> copy_user_session_channel
    truncate_session_timestamp >> copy_session_timestamp
