{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'OPENF1_POSITION_REALTIME') }}
     where "meeting_key"   is not null
      and "session_key"   is not null
      and "driver_number" is not null
      and "date"          is not null
),

renamed as (
    select
        cast("date"          as timestamp)  as event_timestamp,
        cast("session_key"   as int)        as session_key,
        cast("meeting_key"   as int)        as meeting_key,
        cast("driver_number" as int)        as driver_number,
        cast("position"      as int)        as race_position,
        cast("year"          as int)        as season_year,

        true                                  as is_realtime
    from source
)

select * from renamed
