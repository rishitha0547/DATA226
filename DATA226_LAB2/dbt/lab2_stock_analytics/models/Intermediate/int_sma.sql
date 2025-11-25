-- Model: int_sma
-- Purpose: Compute simple moving averages (SMA-20 and SMA-50)
--          to identify short- and medium-term stock trends.
-- Layer: Intermediate (INTERMEDIATE schema)


{{ config(materialized='view') }}

select
  symbol,
  date,
  avg(close) over (
    partition by symbol
    order by date
    rows between 19 preceding and current row
  ) as sma_20,
  avg(close) over (
    partition by symbol
    order by date
    rows between 49 preceding and current row
  ) as sma_50
from {{ ref('staging_stock_prices') }}
