{{ config(materialized='view') }}

with unioned as (

    select
        meeting_key,
        session_key,
        driver_number,
        event_timestamp,
        race_position,
        season_year,
        is_realtime
    from {{ ref('stg_openf1_position_historical') }}

    union all

    select
        meeting_key,
        session_key,
        driver_number,
        event_timestamp,
        race_position,
        season_year,
        is_realtime
    from {{ ref('stg_openf1_position_realtime') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by meeting_key, session_key, driver_number, event_timestamp
            order by is_realtime desc
        ) as rn
    from unioned
)

select
    meeting_key,
    session_key,
    driver_number,
    event_timestamp,
    race_position,
    season_year,
    is_realtime
from deduped
where rn = 1
