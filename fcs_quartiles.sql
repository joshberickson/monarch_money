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
--find the connection attempt event closest to the created event
--in the event that a user attempts to connect an institution on web, abandons the flow, then tries again mobile
--we want to attribute the creation to the latest (mobile) event
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
, metrics as (
select
    institution_name
  , data_provider
  , count(distinct event_id) as connections
  , sum(successful_connection) as successful_connections
  , 1.0*successful_connections / connections as first_connection_success
from data
group by 1,2
)
, fcs as (
select
    m.institution_name
  , sum(successful_connections) / sum(connections) as pct_fcs
from metrics m
where 1=1
  and m.connections >= 100
group by 1
)
, ntiles as (
select
    institution_name
  , pct_fcs
  , ntile(4) over(order by pct_fcs) as quartile
from fcs
group by 1,2
)
select
    quartile
  , avg(pct_fcs) as avg_fcs
  , count(distinct institution_name) as institutions
from ntiles
group by 1
order by 1 desc
