## Module 2 Homework

### 1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?

Answer: 128.3 MB

### 2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?

Answer: 
"{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"
green_tripdata_2020-04.csv

### 3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

SELECT count(*)  
FROM `{project_id}.zoomcamp.yellow_tripdata` 
WHERE  filename like '%2020%'

Answer: 24,648,499

### 4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?

SELECT count(*)  
FROM `{project_id}.zoomcamp.green_tripdata` 
WHERE  filename like '%2020%'

Answer: 1,734,051

### 5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

1,925,152

### 6. How would you configure the timezone to New York in a Schedule trigger?

A schedule that runs daily at midnight US Eastern time.

yaml
triggers:
  - id: daily
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "@daily"
    timezone: America/New_York

Answer: Add a timezone property set to America/New_York in the Schedule trigger configuration
