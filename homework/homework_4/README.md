# Module 4 Homework

## Question 1: Understanding dbt model resolution

**Answer:** select * from myproject.raw_nyc_tripdata.ext_green_taxi (because DBT_BIGQUERY_SOURCE_DATASET != DBT_BIGQUERY_DATASET I hope)

## Question 2: dbt Variables & Dynamic Models

**Answer:** Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY

## Question 3: dbt Data Lineage and Execution

**Answer:** dbt run --select models/staging/+

## Question 4: dbt Macros and Jinja

**Answer:** 
- Setting a value for  DBT_BIGQUERY_TARGET_DATASET env var is mandatory, or it'll fail to compile
- When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET
- When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET
- When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET

## Question 5: Taxi Quarterly Revenue Growth

<<<<<<< HEAD
fct_taxi_trips_quarterly_revenue.sql file


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

**Answer:** green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}

## Question 6: P97/P95/P90 Taxi Monthly Fare

fct_taxi_trips_monthly_fare_p95.sql file

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

**Answer:** green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

## Question 7: Top #Nth longest P90 travel time Location for FHV

fct_fhv_monthly_zone_traveltime_p90.sql file

{{ config(materialized='table') }}

with trips_data as (
    select
    *,
    timestamp_diff(dropOff_datetime, pickup_datetime, second) as trip_duration,
    PERCENTILE_CONT((timestamp_diff(dropOff_datetime, pickup_datetime, second)), 0.90) OVER (PARTITION BY pu_year, pu_month, pickup_locationid, dropoff_locationid) as p90

    from `taxi-rides-ny-448612.zoomcamp.dim_fhv_trips`

    )
select distinct
pickup_zone,
dropoff_zone,
p90
from trips_data
where 
pickup_zone in ('Newark Airport', 'SoHo','Yorkville East')
and 
pu_year = 2019
and 
pu_month = 11

**Answer:** LaGuardia Airport, Chinatown, Garment District
=======
**Answer:**

## Question 6: P97/P95/P90 Taxi Monthly Fare

**Answer:**

## Question 7: Top #Nth longest P90 travel time Location for FHV

**Answer:**
>>>>>>> 929cd5da026be5f9839083a4e019af0abe1db654
