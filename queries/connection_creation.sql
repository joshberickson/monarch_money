select
    s.source,
    s.data_provider,
    s.institution_name,
    count(distinct s.event_id) as selected,
    count(distinct c.user_id) as created
from dbt_dev.dbt_jerickson.selected s
left join dbt_dev.dbt_jerickson.created c
    on s.user_id = c.user_id
    and s.institution_name = c.institution_name
    and s.data_provider = c.data_provider
group by 1, 2, 3
order by 4 desc
