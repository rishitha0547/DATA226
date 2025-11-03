-- models/output/session_summary.sql
with u as (
  select userid, sessionid, channel
  from {{ ref('user_session_channel') }}
  where sessionid is not null
),
st as (
  select sessionid, ts
  from {{ ref('session_timestamp') }}
  where sessionid is not null
)
select
  u.userid,
  u.sessionid,
  u.channel,
  st.ts
from u
join st on u.sessionid = st.sessionid
