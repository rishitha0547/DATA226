{% snapshot snapshot_stock_prices %}

{{
  config(
    target_database='USER_DB_FERRET',
    target_schema='SNAPSHOTS',
    unique_key="symbol || '-' || to_char(date, 'YYYY-MM-DD')",
    strategy='check',
    check_cols=['open', 'high', 'low', 'close', 'volume']
  )
}}

select
  symbol,
  date,
  open,
  high,
  low,
  close,
  volume
from {{ source('raw', 'STOCK_PRICES') }}

{% endsnapshot %}
