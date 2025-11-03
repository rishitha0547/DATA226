-- Input model built as CTEs
with src as (
    select
        sessionId,
        ts
    from {{ source('raw', 'session_timestamp') }}
),
clean as (
    select
        sessionId,
        ts
    from src
    where sessionId is not null
)
select * from clean
