-- Model: int_roc_momentum
-- Purpose: Calculate 10-day Rate of Change (ROC) and Momentum
--          to measure the speed and direction of price movements.
-- Layer: Intermediate (INTERMEDIATE schema)


{{ config(materialized='view') }}

with base as (
  select
    symbol,
    date,
    close,
    lag(close, 10) over (partition by symbol order by date) as close_lag_10
  from {{ ref('staging_stock_prices') }}
)

select
  symbol,
  date,
  case
    when close_lag_10 is null or close_lag_10 = 0 then null
    else ((close / close_lag_10) - 1) * 100.0
  end as roc_10,
  case
    when close_lag_10 is null then null
    else close - close_lag_10
  end as momentum_10
from base
