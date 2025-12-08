{{ config(materialized='view') }}

with unioned as (

    -- HISTORICAL LAPS
    select
        meeting_key,
        session_key,
        driver_number,
        lap_number,
        lap_start_time,
        sector1_time,
        sector2_time,
        sector3_time,
        lap_time,
        i1_speed_kph,
        i2_speed_kph,
        st_speed_kph,
        is_pit_out_lap,
        season_year,
        segments_sector_1,
        segments_sector_2,
        segments_sector_3,
        -- session_name,
        is_realtime
    from {{ ref('stg_openf1_laps_historical') }}

    union all

    -- REALTIME LAPS
    select
        meeting_key,
        session_key,
        driver_number,
        lap_number,
        lap_start_time,
        sector1_time,
        sector2_time,
        sector3_time,
        lap_time,
        i1_speed_kph,
        i2_speed_kph,
        st_speed_kph,
        is_pit_out_lap,
        season_year,
        segments_sector_1,
        segments_sector_2,
        segments_sector_3,
        -- session_name,
        is_realtime
    from {{ ref('stg_openf1_laps_realtime') }}

),

deduped as (
    select
        *,
        row_number() over (
            partition by meeting_key, session_key, driver_number, lap_number
            order by is_realtime desc, lap_start_time desc
        ) as rn
    from unioned
)

select
    meeting_key,
    session_key,
    driver_number,
    lap_number,
    lap_start_time,
    lap_time,
    sector1_time,
    sector2_time,
    sector3_time,
    i1_speed_kph,
    i2_speed_kph,
    st_speed_kph,
    is_pit_out_lap,
    season_year,
    segments_sector_1,
    segments_sector_2,
    segments_sector_3,
    is_realtime
from deduped
where rn = 1
