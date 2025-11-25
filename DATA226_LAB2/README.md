# DATA 226 — Stock Price Analytics & Forecasting (Airflow + Snowflake + dbt + Preset)

This repository contains the complete implementation of an **end-to-end data engineering and analytics pipeline** for stock price analysis and forecasting.

This system integrates **Apache Airflow**, **Snowflake**, **dbt**, **Snowflake ML**, and **Preset** to perform daily extraction, transformation, forecasting, testing, and visualization on stock market data.

---

## Project Overview

The pipeline automates the entire analytical lifecycle, from raw data ingestion to final business intelligence (BI) consumption:

1.  **Ingestion (Airflow ETL):** Daily stock data is downloaded from `yfinance`.
2.  **Transformation (dbt ELT):** Data is cleaned, standardized, and enriched with technical indicators (SMA, Volatility, ROC).
3.  **Forecasting (Snowflake ML):** A 7-day price prediction is generated using a model trained directly in Snowflake.
4.  **Visualization (Preset):** Final analytical tables are used to power dashboards for strategic decision-making.

---

## Technology Stack

| Tool | Role | Description |
| :--- | :--- | :--- |
| **Apache Airflow** | Orchestration | Manages the workflow dependencies (ETL, ELT/dbt, ML Forecast). |
| **Snowflake** | Cloud Data Warehouse | Serves as the central, scalable storage and compute layer for all data and transformations. |
| **dbt (Data Build Tool)** | Transformation/ELT | Manages complex SQL transformations, testing, and documentation inside Snowflake. |
| **Snowflake ML** | Forecasting | Used to create and run the 7-day stock price prediction model. |
| **Preset** | Business Intelligence | Visualization and dashboarding layer consuming the final Marts. |

---

## Repository Structure

### `dags/`
Contains all Apache Airflow workflows used in the project.

| DAG File | Purpose | Description |
| :--- | :--- | :--- |
| `yfinance_etl_dag.py` | **ETL Pipeline (Lab 1)** | Downloads 180 days of OHLCV data from `yfinance` and loads it into Snowflake (`RAW.STOCK_PRICES`). Designed to be **idempotent** and transaction-safe. |
| `built_elt_with_dbt.py` | **ELT Pipeline (Lab 2)** | Triggers the `dbt run`, `dbt test`, and `dbt snapshot` commands to build all transformation layers inside Snowflake. |
| `ml_forecast_dag.py` | **ML Forecasting DAG** | Creates a **Snowflake ML Forecast model**, generates 7-day price predictions, and writes the **ACTUAL + FORECAST** results into the `ANALYTICS` schema. |

### `dbt/`
Contains the complete dbt project for transformations.

| Directory/File | Purpose | Key Features |
| :--- | :--- | :--- |
| `dbt_project.yml` | Project Config | Defines dbt settings, folder structure, model configurations, and materializations. |
| `profiles.yml` | Connection Config | Stores project-specific connection details for Snowflake, used internally by dbt within Airflow. |
| `macros/generate_schema_name.sql` | Dynamic Schemas | Custom macro that dynamically assigns schema names for Staging, Intermediate, and Marts layers. |
| `snapshots/snapshot_stock_prices.sql` | History Tracking | Tracks historical changes in stock prices using the **check strategy**, writing data into the `SNAPSHOTS` schema. |

#### `dbt/models/` (Transformation Layers)

| Layer | Models | Purpose |
| :--- | :--- | :--- |
| **`Staging/`** | `staging_stock_prices.sql` | **Clean & Standardize.** Ensures correct datatypes and enforces a standard: one record per (symbol, date). |
| **`Intermediate/`** | `int_daily_returns.sql`, `int_roc_momentum.sql`, `int_sma.sql`, `int_volatility.sql` | **Compute Indicators.** Calculates technical indicators used in analytics and dashboarding (Daily Returns, 10-day ROC/Momentum, 20/50-day Moving Averages, 20-day Rolling Volatility). |
| **`Marts/`** | `final_stock_price_table.sql` | **Final BI Table.** Combines all computed indicators into a single, comprehensive analytical table for direct consumption by Preset. |

### `docker-compose.yml` & `Dockerfile`

These files define the local development and execution environment, ensuring consistency and reproducibility.

* **`docker-compose.yml`**: Defines the services required to run the environment locally: **Airflow webserver**, **scheduler**, and triggers for **dbt** inside the containers.
* **`Dockerfile`**: Extends the base Airflow image and installs all necessary dependencies, including `dbt-core`, `dbt-snowflake`, and Python libraries like `yfinance`.

---

## Documentation and Artifacts

The `images/` directory contains all screenshots used in the project report and dashboard, serving as visual documentation for the system's output and components:

* Airflow DAG screenshots (ETL, ML, dbt).
* dbt model outputs and lineage.
* Snapshot previews.
* Dashboard charts (SMA, Daily Returns, ROC/Momentum, Volatility).
* Final analytics table preview in Snowflake.