{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
    select 
    -- Revenue grouping 
    {{ dbt.date_trunc("year", "pickup_datetime") }} as revenue_year, 
    {{ dbt.date_trunc("quarter", "pickup_datetime") }} as revenue_quarter,


    -- Revenue calculation 
    sum(total_amount) as revenue_monthly_total_amount,
    revenue_monthly_total_amount - LAG(revenue_monthly_total_amount, offset 4) OVER(ORDER BY revenue_year,revenue_quarter) AS change

    from trips_data
    group by 1,2