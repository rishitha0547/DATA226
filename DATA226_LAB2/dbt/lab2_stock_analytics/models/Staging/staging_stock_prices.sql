-- models/Staging/staging_stock_prices.sql
{{ config(materialized='view') }}

with sp as (
  select
    date::date   as date,
    open::float  as open,
    high::float  as high,
    low::float   as low,
    close::float as close,
    volume::number as volume,
    upper(symbol) as symbol
  from {{ source('raw', 'STOCK_PRICES') }}
)

select * from sp
