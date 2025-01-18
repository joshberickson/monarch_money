--are there created events that don't have a corresponding connect event

with created as (
select
    'created' as type
  , count(distinct c.event_id) as events
  , count(distinct case when s.user_id is null then c.event_id else null end) as disregard_events
from dbt_dev.dbt_jerickson.created c
left join dbt_dev.dbt_jerickson.selected s
  on c.user_id = s.user_id
  and c.institution_name = s.institution_name
  and c.data_provider = s.data_provider
  and s.type = 'connect'
  and to_timestamp(c.timestamp, 'M/d/yy H:mm') > to_timestamp(s.timestamp, 'M/d/yy H:mm')
where 1=1
  and c.type = 'created'
group by 1
)
, disconnected as (
select
    'disconnected' as type
  , count(distinct d.event_id) as events
  , count(distinct case when c.credential_id is null then d.event_id else null end) as disregard_events
from dbt_dev.dbt_jerickson.disconnected d
left join dbt_dev.dbt_jerickson.created c
  on d.credential_id = c.credential_id
  and c.type = 'created'
  and to_timestamp(d.timestamp, 'M/d/yy H:mm') > to_timestamp(c.timestamp, 'M/d/yy H:mm')
where 1=1
  and d.type = 'disconnected'
group by 1
)
select *
from created c
union all
select *
from disconnected d


/*
individual data checks for the data returned above

--65938	8/1/24 0:11	Aurora Capital Group	plaid
select *
from dbt_dev.dbt_jerickson.selected
where user_id = '65938'

--60767	8/1/24 0:40	Maple Leaf Trust	finicity	created
select *
from dbt_dev.dbt_jerickson.selected
where user_id = '60767'

--82023	8/1/24 2:31	Tideway Bank	mx
select *
from dbt_dev.dbt_jerickson.selected
where user_id = '82023'
*/