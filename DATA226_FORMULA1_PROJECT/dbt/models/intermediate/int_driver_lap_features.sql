{{ config(materialized='view') }}

with base as (

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
        -- meeting_name,
        -- session_name,
        is_realtime,
        race_position,

        -- previous + next lap times for same driver/session
        lag(lap_time) over (
            partition by meeting_key, session_key, driver_number
            order by lap_number
        ) as prev_lap_time,

        lead(lap_time) over (
            partition by meeting_key, session_key, driver_number
            order by lap_number
        ) as next_lap_time,

        -- rolling avg + stddev over last 5 laps (pace stability)
        avg(lap_time) over (
            partition by meeting_key, session_key, driver_number
            order by lap_number
            rows between 4 preceding and current row
        ) as rolling_avg_5_laps,

        stddev_samp(lap_time) over (
            partition by meeting_key, session_key, driver_number
            order by lap_number
            rows between 4 preceding and current row
        ) as rolling_stddev_5_laps,

        -- personal best lap in this session
        min(lap_time) over (
            partition by meeting_key, session_key, driver_number
        ) as best_lap_time_driver,

        -- best lap in the whole session (any driver)
        min(lap_time) over (
            partition by meeting_key, session_key
        ) as best_lap_time_session,

        -- total drivers in session (for position-based scaling)
        count(distinct driver_number) over (
            partition by meeting_key, session_key
        ) as driver_count_in_session

    from {{ ref('int_session_driver_laps') }}
),

features as (

    select
        *,
        -- 1) Pace momentum (lap-to-lap improvement; positive = faster)
        (prev_lap_time - lap_time) as pace_momentum,

        -- 2) Pace stability index (PSI) = stddev over last 5 laps
        rolling_stddev_5_laps      as pace_stability_index,

        -- 3) Degradation index: how much slower than own best lap
        (lap_time - best_lap_time_driver) as degradation_index,

        -- 4) Position momentum: +ve means gaining places
        (
            lag(race_position) over (
                partition by meeting_key, session_key, driver_number
                order by lap_number
            ) - race_position
        ) as position_momentum,

        -- 5) Performance score (0–100-ish)
        --    combine how close to session best lap + race position
        (
            -- pace component (relative to session best)
            coalesce(best_lap_time_session / nullif(lap_time, 0), 0) * 60
            +
            -- position component (front of field -> higher score)
            case
                when race_position is not null and driver_count_in_session > 1
                then
                    ( (driver_count_in_session - race_position)::float
                      / (driver_count_in_session - 1) ) * 40
                else 0
            end
        ) as performance_score_raw

    from base
),

labels as (

    select
        *,
        -- Pace state labels – high-level pace / tyre picture
        case
            when pace_momentum > 0.3 and degradation_index < 1.0
                then 'ATTACKING_PACE'           -- pushing, good tyres
            when degradation_index > 2.5
                then 'HIGH_TYRE_DEGRADATION'    -- pace off vs personal best
            when pace_stability_index < 0.15
                then 'HIGHLY_CONSISTENT_PACE'   -- very repeatable laps
            else 'BASELINE_RACE_PACE'
        end as pace_state,

        -- Track position state (field movement)
        case
            when position_momentum > 0  then 'GAINING_POSITIONS'
            when position_momentum < 0  then 'LOSING_POSITIONS'
            else                           'HOLDING_POSITION'
        end as track_position_state,

        -- Pace momentum interpretation (more human readable)
        case
            when pace_momentum > 0.25  then 'Strong Pace Gain'
            when pace_momentum > 0.05  then 'Pace Improving'
            when pace_momentum > -0.05 then 'Pace Stable'
            when pace_momentum > -0.25 then 'Pace Dropping'
            else                           'Significant Pace Loss'
        end as pace_momentum_label,

        -- Tyre degradation interpretation
        case
            when degradation_index < 1.0 then 'Tyres Fresh'
            when degradation_index < 2.5 then 'Low Degradation'
            when degradation_index < 4.0 then 'Moderate Degradation'
            else                              'Severe Degradation'
        end as tyre_state,

        -- Consistency interpretation
        case
            when pace_stability_index < 0.10 then 'Very Consistent'
            when pace_stability_index < 0.25 then 'Consistent'
            else                                  'Variable Pace'
        end as consistency_label,

        -- Position momentum interpretation
        case
            when position_momentum > 0 then 'Gaining Positions'
            when position_momentum < 0 then 'Losing Positions'
            else                           'Holding Position'
        end as position_trend_label,

        -- Overall performance rating
        case
            when performance_score_raw >= 80 then 'Excellent'
            when performance_score_raw >= 60 then 'Good'
            when performance_score_raw >= 40 then 'Average'
            else                                  'Poor'
        end as performance_rating

    from features
)

select * from labels
