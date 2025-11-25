-- Model: fct_technical_indicators
-- Purpose: Combine all key technical indicators (daily returns,
--          moving averages, ROC, momentum, volatility) into a
--          single analytical table for BI dashboards.
-- Layer: Mart (ANALYTICS schema)

{{ config(materialized='table') }}

with
dr  as (select * from {{ ref('int_daily_returns') }}),
sma as (select * from {{ ref('int_sma') }}),
roc as (select * from {{ ref('int_roc_momentum') }}),
vol as (select * from {{ ref('int_volatility') }})

select
  coalesce(dr.symbol, sma.symbol, roc.symbol, vol.symbol) as symbol,
  coalesce(dr.date,   sma.date,   roc.date,   vol.date)   as date,
  dr.daily_return_pct,
  sma.sma_20, sma.sma_50,
  roc.roc_10, roc.momentum_10,
  vol.vol_20d,
  {{ dbt_utils.generate_surrogate_key(['coalesce(dr.symbol, sma.symbol, roc.symbol, vol.symbol)',
                                       'coalesce(dr.date,   sma.date,   roc.date,   vol.date)']) }} as unique_key
from dr
full outer join sma using (symbol, date)
full outer join roc using (symbol, date)
full outer join vol using (symbol, date)
