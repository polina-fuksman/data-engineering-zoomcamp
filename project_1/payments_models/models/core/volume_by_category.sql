{{
    config(
        materialized='table'
    )
}}

with volume_by_category as (
    select *
    from {{ ref('payment_data') }}
)
select 
category,
ROUND(SUM(amount),2) as amount,
ROUND(SUM(amount)/(select SUM(amount) from {{ ref('payment_data') }} where fraud = 0) * 100,2) as percentage
from volume_by_category
where fraud = 0
group by category
order by amount desc
