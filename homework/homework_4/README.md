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

**Answer:**

## Question 6: P97/P95/P90 Taxi Monthly Fare

**Answer:**

## Question 7: Top #Nth longest P90 travel time Location for FHV

**Answer:**