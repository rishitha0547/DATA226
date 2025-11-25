-- Model: int_daily_returns
-- Purpose: Calculate the daily percentage change in closing price
--          for each stock symbol using lag() over date order.
-- Layer: Intermediate (INTERMEDIATE schema)

{{ config(materialized='view') }}

with base as (
  select
    symbol,
    date,
    close,
    lag(close) over (partition by symbol order by date) as prev_close
  from {{ ref('staging_stock_prices') }}
)

select
  symbol,
  date,
  case
    when prev_close is null or prev_close = 0 then null
    else ((close - prev_close) / prev_close) * 100.0
  end as daily_return_pct
from base
