--For each record in dim_fhv_trips.sql, compute the timestamp_diff in seconds between 
--dropoff_datetime and pickup_datetime - we'll call it trip_duration for this exercise
--Compute the continous p90 of trip_duration partitioning by year, month, 
--pickup_location_id, and dropoff_location_id
-- For the Trips that respectively started from Newark Airport, SoHo, and Yorkville East, 
-- in November 2019, what are dropoff_zones with the 2nd longest p90 trip_duration ?

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