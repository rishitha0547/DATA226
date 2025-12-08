{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('raw', 'OPENF1_RACE_CONTROL_REALTIME') }}
    where "meeting_key" is not null
      and "session_key" is not null
      and "date"        is not null
      and "message"     is not null 
),

renamed as (
    select
        cast("meeting_key"   as int)        as meeting_key,
        cast("session_key"   as int)        as session_key,
        cast("date"          as timestamp)  as event_timestamp,

        -- clean "None" -> NULL for numeric fields
        cast(nullif("driver_number", 'None') as int) as driver_number,
        cast(nullif("lap_number",    'None') as int) as lap_number,

        -- clean "None" -> NULL for text labels
        nullif("category", 'None')                       as category,
        nullif("flag",     'None')                       as flag,
        nullif("scope",    'None')                       as scope,

        -- clean "None" -> NULL before casting to float
        cast(nullif("sector", 'None') as float)          as sector,

        "message"                                        as message,

        true                                             as is_realtime
    from source
)

select * from renamed
