{{ config(materialized='view') }}

with laps as (

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
        is_realtime
    from {{ ref('int_openf1_laps_all') }}

),

positions as (

    select
        meeting_key,
        session_key,
        driver_number,
        event_timestamp,
        race_position,
        season_year,
        is_realtime
    from {{ ref('int_openf1_position_all') }}

),

joined as (

    select
        l.meeting_key,
        l.session_key,
        l.driver_number,
        l.lap_number,
        l.lap_start_time,

        l.lap_time,
        l.sector1_time,
        l.sector2_time,
        l.sector3_time,
        l.i1_speed_kph,
        l.i2_speed_kph,
        l.st_speed_kph,
        l.is_pit_out_lap,
        l.season_year,
        l.is_realtime,

        p.race_position,
        p.event_timestamp as position_timestamp,

        row_number() over (
            partition by l.meeting_key,
                         l.session_key,
                         l.driver_number,
                         l.lap_number
            order by p.event_timestamp desc
        ) as rn
    from laps l
    left join positions p
      on  l.meeting_key   = p.meeting_key
      and l.session_key   = p.session_key
      and l.driver_number = p.driver_number
      and p.event_timestamp <= l.lap_start_time
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
    is_realtime,
    race_position,
    position_timestamp
from joined
where rn = 1
