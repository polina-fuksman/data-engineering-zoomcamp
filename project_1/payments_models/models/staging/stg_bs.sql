{{
    config(
        materialized='view'
    )
}}

with payment_data as
(
  select *
  from {{ source('staging','bs') }}
  where unique_row_id is not null 
)
select
    -- identifiers
    unique_row_id,
    filename,
    
    -- payment info
    {{ dbt.safe_cast("step", api.Column.translate_type("integer")) }} as step,
    customer,
    age,
    gender,
    zipcodeOri,
    merchant,
    zipMerchant,
    category,
    {{ dbt.safe_cast("fraud", api.Column.translate_type("integer")) }} as fraud,

    -- payment amount
    cast(amount as numeric) as amount

from payment_data


-- -- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}