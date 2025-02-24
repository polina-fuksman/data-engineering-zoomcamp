{{ config(materialized='table') }}

with trips_data as (
    select
    service_type,
    extract(year from pickup_datetime) as r_year,
    extract(quarter from pickup_datetime) as r_quarter,
    sum(total_amount) as revenue,
    
    from `taxi-rides-ny-448612`.`hw4_2019_2020`.`fact_trips`

    where extract(year from pickup_datetime) between 2019 and 2020
    group by
    service_type,
    r_year,
    r_quarter
    )
select 
service_type,
r_year, 
r_quarter,
revenue,
revenue / LAG(revenue,4) OVER (partition by service_type order by service_type, r_year, r_quarter)*100 as changes
from trips_data
order by service_type desc