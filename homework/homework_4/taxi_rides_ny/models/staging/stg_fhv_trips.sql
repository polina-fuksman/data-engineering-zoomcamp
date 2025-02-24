-- Create a staging model for FHV Data (2019), 
-- and DO NOT add a deduplication step, just filter out the entries where where dispatching_base_num is not null
-- Create a core model for FHV Data (dim_fhv_trips.sql) joining with dim_zones. 
-- Similar to what has been done here
-- Add some new dimensions year (e.g.: 2019) and month (e.g.: 1, 2, ..., 12), 
-- based on pickup_datetime, to the core model to facilitate filtering for your queries

{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata') }}
  where dispatching_base_num is not null 
)
select
    -- identifiers
    unique_row_id,
    filename,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    Affiliated_base_number,
    dispatching_base_num,
    SR_Flag

from tripdata


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}