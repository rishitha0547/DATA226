-- Input model built as CTEs
with src as (
    select
        userId,
        sessionId,
        channel
    from {{ source('raw', 'user_session_channel') }}
),
clean as (
    select
        userId,
        sessionId,
        channel
    from src
    where sessionId is not null
)
select * from clean
