# Start from the official Airflow image with Python 3.11
FROM apache/airflow:2.10.1-python3.11

# 1. Install packages that need or are compatible with Airflow constraints
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt" \
    yfinance \
    apache-airflow-providers-snowflake \
    sentence-transformers \
    pinecone-client

# 2. Install dbt packages WITHOUT the constraints flag to allow for newer dependencies (like protobuf)
RUN pip install --no-cache-dir \
    dbt-core==1.10.15 \
    dbt-snowflake==1.10.3