{{ config(materialized='table') }}

with laps as (
    select * from {{ ref('int_driver_lap_features') }}
),

agg as (
    select
        meeting_key,
        session_key,
        driver_number,

        min(lap_number)          as first_lap,
        max(lap_number)          as last_lap,

        min(race_position)       as best_position,
        max(race_position)       as worst_position,

        min(lap_time)            as best_lap_time,
        avg(lap_time)            as avg_lap_time,

        avg(pace_stability_index)   as avg_psi,
        avg(degradation_index)      as avg_degradation,
        avg(performance_score_raw)  as avg_performance_score,

        sum(case when is_pit_out_lap then 1 else 0 end) as pit_stop_count
    from laps
    group by 1,2,3
)

select * from agg
