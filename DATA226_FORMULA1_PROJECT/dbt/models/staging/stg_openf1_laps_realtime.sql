{{ config(materialized='view') }}

with source as (

    select
        *,
        row_number() over (
            partition by "meeting_key", "session_key", "driver_number", "lap_number"
            order by "date_start" desc
        ) as rn
    from {{ source('raw', 'OPENF1_LAPS_REALTIME') }}
    where "meeting_key"   is not null
      and "session_key"   is not null
      and "driver_number" is not null
      and "lap_number"    is not null

),

filtered as (

    -- keep only the latest realtime record per lap
    select *
    from source
    where rn = 1

),

renamed as (

    select
        cast("meeting_key"       as int)        as meeting_key,
        cast("session_key"       as int)        as session_key,
        cast("driver_number"     as int)        as driver_number,
        cast("lap_number"        as int)        as lap_number,
        cast("date_start"        as timestamp)  as lap_start_time,

        cast("duration_sector_1" as float)      as sector1_time,
        cast("duration_sector_2" as float)      as sector2_time,
        cast("duration_sector_3" as float)      as sector3_time,
        cast("lap_duration"      as float)      as lap_time,

        cast("i1_speed"          as float)      as i1_speed_kph,
        cast("i2_speed"          as float)      as i2_speed_kph,
        cast("st_speed"          as float)      as st_speed_kph,

        cast("is_pit_out_lap"    as boolean)    as is_pit_out_lap,
        cast("year"              as int)        as season_year,

        "segments_sector_1"                      as segments_sector_1,
        "segments_sector_2"                      as segments_sector_2,
        "segments_sector_3"                      as segments_sector_3,

        true                                     as is_realtime
    from filtered

)

select * from renamed
