-- Create a new model fct_taxi_trips_monthly_fare_p95.sql
-- Filter out invalid entries (fare_amount > 0, trip_distance > 0, and payment_type_description in ('Cash', 'Credit Card'))
-- Compute the continous percentile of fare_amount partitioning by service_type, year and and month
-- Now, what are the values of p97, p95, p90 for Green Taxi and Yellow Taxi, in April 2020?


{{ config(materialized='table') }}

with trips_data as (
    select
    service_type,
    PERCENTILE_CONT(fare_amount, 0.97)
    OVER (PARTITION BY service_type, extract(year from pickup_datetime), extract(month from pickup_datetime)) AS p97,
    PERCENTILE_CONT(fare_amount, 0.95)
    OVER (PARTITION BY service_type, extract(year from pickup_datetime), extract(month from pickup_datetime)) AS p95,
    PERCENTILE_CONT(fare_amount, 0.90)
    OVER (PARTITION BY service_type, extract(year from pickup_datetime), extract(month from pickup_datetime)) AS p90

    from `taxi-rides-ny-448612`.`hw4_2019_2020`.`fact_trips`

    where 
    fare_amount > 0
    and 
    trip_distance > 0
    and
    payment_type_description in ('Cash', 'Credit Card')
    and
    extract(year from pickup_datetime) = 2020
    and
    extract(month from pickup_datetime) = 4
    )
select distinct service_type, p97, p95, p90

from trips_data