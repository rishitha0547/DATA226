{{ config(materialized='table') }}

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

    -- features
    prev_lap_time,
    next_lap_time,
    rolling_avg_5_laps,
    pace_momentum,
    pace_stability_index,
    degradation_index,
    position_momentum,
    performance_score_raw,
    pace_state,
    track_position_state

from {{ ref('int_driver_lap_features') }}
