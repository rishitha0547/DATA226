
-- Model: int_volatility
-- Purpose: Calculate 20-day rolling volatility (standard deviation
--          of daily returns) as a measure of price fluctuation.
-- Layer: Intermediate (INTERMEDIATE schema)

{{ config(materialized='view') }}

-- Volatility as rolling 20-day stddev of daily returns
with dr as (
  select
    symbol,
    date,
    daily_return_pct
  from {{ ref('int_daily_returns') }}
)

select
  symbol,
  date,
  stddev_samp(daily_return_pct) over (
    partition by symbol
    order by date
    rows between 19 preceding and current row
  ) as vol_20d
from dr
