with accounts_created as (
select
    c.user_id,
    c.institution_name,
    c.data_provider,
    s.source,
    to_timestamp(s.timestamp, 'M/d/yy H:mm') as connected_at,
    to_timestamp(c.timestamp, 'M/d/yy H:mm') as created_at,
    c.event_id,
    c.credential_id
from dbt_dev.dbt_jerickson.selected s
join dbt_dev.dbt_jerickson.created c
    on s.user_id = c.user_id
    and s.institution_name = c.institution_name
    and s.data_provider = c.data_provider
    and to_timestamp(s.timestamp, 'M/d/yy H:mm') < to_timestamp(c.timestamp, 'M/d/yy H:mm')
),
--find the connection attempt event closest to the created event
--in the event that a user attempts to connect an institution on web, abandons the flow, then tries again mobile
--we want to attribute the creation to the latest (mobile) event
dedupe_accts as (
select
    *,
    row_number() over(partition by credential_id order by connected_at desc) as rk
from accounts_created
qualify rk = 1
),
disconnections as (
select
    accts.credential_id,
    min(to_timestamp(d.timestamp, 'M/d/yy H:mm')) as disconnected_at
from dedupe_accts accts
join dbt_dev.dbt_jerickson.disconnected d
    on accts.credential_id = d.credential_id
    and to_timestamp(d.timestamp, 'M/d/yy H:mm') > accts.created_at
group by 1
),
data as (
select
    c.user_id,
    c.created_at,
    c.source,
    c.institution_name,
    c.data_provider,
    c.event_id,
    c.credential_id,
    d.disconnected_at,
    timestampdiff(hour, c.created_at, d.disconnected_at) as hrs_diff,
    case when d.disconnected_at is null or timestampdiff(hour, c.created_at, d.disconnected_at) > 24 then 1 else 0 end as successful_connection
from dedupe_accts c
left join disconnections d
    on c.credential_id = d.credential_id
order by 1,2 desc
),
metrics as (
select
    institution_name,
    data_provider,
    count(distinct event_id) as connections,
    count(distinct user_id) as users,
    sum(successful_connection) as successful_connections,
    1.0 * successful_connections / connections as first_connection_success
from data
group by 1,2
),
institution_cnt as (
select
    institution_name,
    count(data_provider) as num_rows
from metrics
where 1=1
    and connections >= 100
group by 1
)
select
    m.*,
    inst.num_rows,
    dense_rank() over(order by m.first_connection_success desc) as rk_desc,
    dense_rank() over(order by m.first_connection_success) as rk_asc,
    dense_rank() over(order by m.users desc) as rk_users_desc,
    dense_rank() over(order by m.users) as rk_users_acc
from metrics m
join institution_cnt inst
    on m.institution_name = inst.institution_name
where 1=1
    and m.connections >= 100
order by 1,2
