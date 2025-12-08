{{ config(materialized='table') }}

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
        is_realtime,
        race_position,

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
),

agg as (
    select
        meeting_key,
        session_key,
        driver_number,

        min(lap_number)             as first_lap,
        max(lap_number)             as last_lap,

        min(race_position)          as best_position,
        max(race_position)          as worst_position,

        min(lap_time)               as best_lap_time,
        avg(lap_time)               as avg_lap_time,

        avg(pace_stability_index)   as avg_psi,
        avg(degradation_index)      as avg_degradation,
        avg(performance_score_raw)  as avg_performance_score,

        sum(case when is_pit_out_lap then 1 else 0 end) as pit_stop_count
    from laps
    group by
        meeting_key,
        session_key,
        driver_number
),

final as (
    select
        l.*,
        a.first_lap,
        a.last_lap,
        a.best_position,
        a.worst_position,
        a.best_lap_time,
        a.avg_lap_time,
        a.avg_psi,
        a.avg_degradation,
        a.avg_performance_score,
        a.pit_stop_count
    from laps l
    left join agg a
        on  l.meeting_key   = a.meeting_key
        and l.session_key   = a.session_key
        and l.driver_number = a.driver_number
)

select *
from final
