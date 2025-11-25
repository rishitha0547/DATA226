from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# ---------- Snowflake helpers ----------

def get_conn_id() -> str:
    # Airflow Variable "snowflake_conn_id" should be set to "my_snowflake_conn"
    return Variable.get("snowflake_conn_id")

def get_cursor():
    hook = SnowflakeHook(snowflake_conn_id=get_conn_id())
    return hook.get_conn().cursor()

def get_forecast_function_name() -> str:
    # Use Airflow Variable "forecast_fn" (e.g., USER_DB_FERRET.MODEL.FORECAST_MODEL)
    return Variable.get("forecast_fn")

# ---------- Tasks ----------

@task
def train(train_input_table: str, train_view: str):
    """
    1) Create/replace view with DATE, CLOSE, SYMBOL
    2) Create/replace SNOWFLAKE.ML.FORECAST object
    3) Print evaluation metrics
    """
    cur = get_cursor()
    forecast_function_name = get_forecast_function_name()

    create_view_sql = f"""
        CREATE OR REPLACE VIEW {train_view} AS
        SELECT DATE, CLOSE, SYMBOL
        FROM {train_input_table};
    """

    create_model_sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA         => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME     => 'SYMBOL',
            TIMESTAMP_COLNAME  => 'DATE',
            TARGET_COLNAME     => 'CLOSE',
            CONFIG_OBJECT      => {{ 'ON_ERROR': 'SKIP' }}
        );
    """

    try:
        cur.execute("BEGIN;")
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        cur.execute("COMMIT;")

        # Log evaluation metrics
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
        for row in cur.fetchall():
            print(row)
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e

@task
def predict(train_input_table: str, forecast_table: str, final_table: str):
    """
    1) Call the forecast object for 7 days
    2) Store results in MODEL.FORECASTS
    3) Create final table combining actuals and forecasts
    """
    cur = get_cursor()
    forecast_function_name = get_forecast_function_name()

    try:
        # Run forecast and capture result set
        cur.execute(f"""
            CALL {forecast_function_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT       => {{ 'prediction_interval': 0.95 }}
            );
        """)
        qid = cur.sfqid  # query id for RESULT_SCAN

        # Save predictions
        cur.execute(f"""
            CREATE OR REPLACE TABLE {forecast_table} AS
            SELECT * FROM TABLE(RESULT_SCAN('{qid}'));
        """)

        # Build final analytics table
        cur.execute(f"""
            CREATE OR REPLACE TABLE {final_table} AS
            SELECT
                SYMBOL,
                DATE,
                'ACTUAL'  AS RECORD_TYPE,
                CLOSE     AS PRICE,
                NULL::FLOAT AS LOWER_BOUND,
                NULL::FLOAT AS UPPER_BOUND
            FROM {train_input_table}
            UNION ALL
            SELECT
                REPLACE(SERIES,'\"','') AS SYMBOL,
                TS::DATE                AS DATE,
                'FORECAST' AS RECORD_TYPE,
                FORECAST::FLOAT         AS PRICE,
                LOWER_BOUND::FLOAT      AS LOWER_BOUND,
                UPPER_BOUND::FLOAT      AS UPPER_BOUND
            FROM {forecast_table};
        """)
        print(f"Created final table: {final_table}")
    except Exception as e:
        raise e

# ---------- DAG ----------

with DAG(
    dag_id="ML_Forecast_DAG",
    start_date=datetime(2025, 10, 5),
    schedule="00 4 * * *",  # daily at 04:00
    catchup=False,
    tags=["ML", "ELT"],
    description="Train Snowflake ML Forecast daily and publish 7-day predictions."
) as dag:

    # Airflow Variables (fallback to sane defaults)
    etl_table      = Variable.get("etl_table",      default_var="USER_DB_FERRET.RAW.STOCK_PRICES")
    train_view     = Variable.get("train_view",     default_var="USER_DB_FERRET.ADHOC.MARKET_DATA_VIEW")
    forecast_table = Variable.get("forecast_table", default_var="USER_DB_FERRET.MODEL.FORECASTS")
    final_table    = Variable.get("final_table",    default_var="USER_DB_FERRET.ANALYTICS.FINAL_PRICES_FORECAST")

    t_train   = train(etl_table, train_view)
    t_predict = predict(etl_table, forecast_table, final_table)

    t_train >> t_predict  # ensure training completes before prediction
