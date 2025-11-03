{% snapshot snapshot_session_summary %}
{{
  config(
    target_schema='SNAPSHOT',
    target_database='USER_DB_FERRET',
    unique_key='sessionid',
    strategy='timestamp',
    updated_at='ts',
    invalidate_hard_deletes=True
  )
}}
select userid, sessionid, channel, ts
from {{ ref('session_summary') }}
{% endsnapshot %}
