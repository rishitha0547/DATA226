{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('raw', 'OPENF1_RACE_CONTROL_HISTORICAL') }}
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

        -- clean "None" strings → NULL, then cast to INT
        cast(nullif("driver_number", 'None') as int) as driver_number,
        cast(nullif("lap_number",    'None') as int) as lap_number,

        -- clean "None" text labels → NULL first
        nullif("category", 'None')                       as category,
        nullif("flag",     'None')                       as flag,
        nullif("scope",    'None')                       as scope,

        -- convert 'nan' / 'None' / '' → NULL, else cast to float
        case
            when trim(lower("sector")) in ('nan', 'none', '') then null
            else try_to_double("sector")
        end                                              as sector,

        -- message is always text
        "message"                                        as message,

        false                                            as is_realtime
    from source
)

select *
from renamed

