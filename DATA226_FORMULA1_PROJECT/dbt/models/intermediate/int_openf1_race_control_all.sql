{{ config(materialized='view') }}

with unioned as (

    select
        meeting_key,
        session_key,
        event_timestamp,
        driver_number,
        lap_number,
        category,
        flag,
        scope,
        sector,
        message,
        is_realtime
    from {{ ref('stg_openf1_race_control_historical') }}

    union all

    select
        meeting_key,
        session_key,
        event_timestamp,
        driver_number,
        lap_number,
        category,
        flag,
        scope,
        sector,
        message,
        is_realtime
    from {{ ref('stg_openf1_race_control_realtime') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by meeting_key, session_key, event_timestamp, message
            order by is_realtime desc
        ) as rn
    from unioned
)

select
    meeting_key,
    session_key,
    event_timestamp,
    driver_number,
    lap_number,
    category,
    flag,
    scope,
    sector,
    message,
    is_realtime
from deduped
where rn = 1
