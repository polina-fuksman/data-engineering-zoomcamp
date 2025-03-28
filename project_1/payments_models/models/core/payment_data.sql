{{
    config(
        materialized='table'
    )
}}

with payment_data as (
    select *
    from {{ ref('stg_bs') }}
)
select 
    step,
    customer,
    age,
    gender,
    merchant,
    category,
    fraud,
    amount
from payment_data