{{
    config(
        materialized='table'
    )
}}

with volume_by_merchants as (
    select
    merchant,
    ROUND(SUM(amount),2) as amount
    from {{ ref('payment_data') }}
    where fraud = 0
    group by merchant
)
select
    merchant, 
    amount,
    ROUND(amount / (select SUM(amount) from {{ ref('payment_data') }} where fraud = 0) *100, 2) as share,
    DENSE_RANK() Over (Order by amount desc) as merchant_rank
from volume_by_merchants
order by merchant_rank
 