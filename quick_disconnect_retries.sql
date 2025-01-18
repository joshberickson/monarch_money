with accounts_created as (
select
    c.user_id
  , c.institution_name
  , c.data_provider
  , s.source
  , to_timestamp(s.timestamp, 'M/d/yy H:mm') as connected_at
  , to_timestamp(c.timestamp, 'M/d/yy H:mm') as created_at
  , c.event_id
  , c.credential_id
from dbt_dev.dbt_jerickson.selected s
join dbt_dev.dbt_jerickson.created c
  on s.user_id = c.user_id
  and s.institution_name = c.institution_name
  and s.data_provider = c.data_provider
  and to_timestamp(s.timestamp, 'M/d/yy H:mm') < to_timestamp(c.timestamp, 'M/d/yy H:mm')
where 1=1
group by 1,2,3,4,5,6,7,8
)
, dedupe_accts as (
select
    *
  , row_number() over(partition by credential_id order by connected_at desc) as rk
from accounts_created
qualify rk = 1
)
, disconnections as (
select
    accts.credential_id
  , min(to_timestamp(d.timestamp, 'M/d/yy H:mm')) as disconnected_at
from dedupe_accts accts
join dbt_dev.dbt_jerickson.disconnected d
  on accts.credential_id = d.credential_id
  and to_timestamp(d.timestamp, 'M/d/yy H:mm') > accts.created_at
group by 1
)
, data as (
select
    c.user_id
  , c.created_at
  , c.source
  , c.institution_name
  , c.data_provider
  , c.event_id
  , c.credential_id
  , d.disconnected_at
  , timestampdiff(hour,c.created_at,d.disconnected_at) as hrs_diff
  , case when d.disconnected_at is null or timestampdiff(hour,c.created_at,d.disconnected_at) > 24 then 1 else 0 end as successful_connection
from dedupe_accts c
left join disconnections d
  on c.credential_id = d.credential_id
order by 1,2 desc
)
, quick_disconnect_users as (
select *
from data
where 1=1
  and successful_connection = 0
)
, retry_users as (
select
    qd.user_id
  , qd.created_at
  , qd.source
  , qd.institution_name
  , qd.data_provider
  , qd.credential_id
  , qd.disconnected_at
  , min(to_timestamp(s.timestamp, 'M/d/yy H:mm')) as retry_at
  , max(case when s.user_id is not null then 1 else 0 end) as retry_after_qd
from quick_disconnect_users qd
left join dbt_dev.dbt_jerickson.selected s
  on qd.user_id = s.user_id
  and to_timestamp(s.timestamp, 'M/d/yy H:mm') > qd.disconnected_at
group by 1,2,3,4,5,6,7
order by 1
)
select
    r.user_id
  , r.created_at
  , r.source
  , r.institution_name
  , r.data_provider
  , r.credential_id
  , r.disconnected_at
  , r.retry_at
  , r.retry_after_qd
  , max(case when c.user_id is not null then 1 else 0 end) as connection_after_retry
from retry_users r
left join dbt_dev.dbt_jerickson.created c
  on r.user_id = c.user_id
  and r.institution_name = c.institution_name
  and r.data_provider = c.data_provider
  and to_timestamp(c.timestamp, 'M/d/yy H:mm') > r.retry_at
group by 1,2,3,4,5,6,7,8,9
order by user_id,institution_name,data_provider
