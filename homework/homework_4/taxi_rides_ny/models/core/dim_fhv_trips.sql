-- Create a core model for FHV Data (dim_fhv_trips.sql) joining with dim_zones. 
-- Similar to what has been done here
-- Add some new dimensions year (e.g.: 2019) and month (e.g.: 1, 2, ..., 12), 
-- based on pickup_datetime, to the core model to facilitate filtering for your queries


{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_trips') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
unique_row_id,
filename,
dispatching_base_num,
pickup_datetime,
dropOff_datetime,
pickup_locationid,
dropoff_locationid,
SR_Flag,
Affiliated_base_number,
pickup_zone.borough as pickup_borough, 
pickup_zone.zone as pickup_zone, 
dropoff_zone.borough as dropoff_borough, 
dropoff_zone.zone as dropoff_zone,
extract(year from pickup_datetime) as pu_year,
extract(month from pickup_datetime) as pu_month

from fhv_tripdata

inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid