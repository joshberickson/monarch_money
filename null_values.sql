--do any records have NULL values for the field type?

select
    'connect' as type
  , count(s.event_id) as events
  , count(distinct s.event_id) as distinct_events
  , count(distinct case when s.type is null then s.event_id else null end) as null_rows
from dbt_dev.dbt_jerickson.selected s
group by 1
union all
select
    'created' as type
  , count(c.event_id) as events
  , count(distinct c.event_id) as distinct_events
  , count(distinct case when c.type is null then c.event_id else null end) as null_rows
from dbt_dev.dbt_jerickson.created c
group by 1
union all
select
    'disconnected' as type
  , count(d.event_id) as events
  , count(distinct d.event_id) as distinct_events
  , count(distinct case when d.type is null then d.event_id else null end) as null_rows
from dbt_dev.dbt_jerickson.disconnected d
group by 1
