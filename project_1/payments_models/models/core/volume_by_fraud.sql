{{
    config(
        materialized='table'
    )
}}

with volume_by_fraud as (
    select *
    from {{ ref('payment_data') }}
)
select 
fraud,
ROUND(SUM(amount),2) as amount,
ROUND(SUM(amount)/(select SUM(amount) from {{ ref('payment_data') }}) * 100,2) as percentage
from volume_by_fraud
group by fraud
