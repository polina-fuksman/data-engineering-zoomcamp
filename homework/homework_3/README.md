# Homework 3: Data Warehousing

## Question 1. Count of records for the 2024 Yellow Taxi Data

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny-448612.hw3_ny_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://hw3_ny_taxi_bucket/yellow_tripdata_2024-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE taxi-rides-ny-448612.hw3_ny_taxi.yellow_tripdata_non_partitioned AS
SELECT * FROM taxi-rides-ny-448612.hw3_ny_taxi.external_yellow_tripdata;

Answer: 20,332,093

## Question 2. Estimated amount of data 

-- A query to count the distinct number of PULocationIDs for the entire dataset on both the tables
select count(distinct PULocationID)
from taxi-rides-ny-448612.hw3_ny_taxi.external_yellow_tripdata;

select count(distinct PULocationID)
from taxi-rides-ny-448612.hw3_ny_taxi.yellow_tripdata_non_partitioned;

Answer: 0 B and 155.12 MB

## Question 3. Why are the estimated number of Bytes different? 

-- A query to retrieve the PULocationID from the table (not the external table) in BigQuery. 
-- Then a query to retrieve the PULocationID and DOLocationID on the same table.

select PULocationID
from taxi-rides-ny-448612.hw3_ny_taxi.yellow_tripdata_non_partitioned
limit 100;

select PULocationID, DOLocationID
from taxi-rides-ny-448612.hw3_ny_taxi.yellow_tripdata_non_partitioned
limit 100;

Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

## Question 4. How many records have a fare_amount of 0?

select count(fare_amount)
from taxi-rides-ny-448612.hw3_ny_taxi.yellow_tripdata_non_partitioned
where fare_amount = 0;

Answer: 8333

## Question 5. The best strategy to make an optimized table in Big Query

CREATE OR REPLACE TABLE taxi-rides-ny-448612.hw3_ny_taxi.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi-rides-ny-448612.hw3_ny_taxi.external_yellow_tripdata;

Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

## Question 6. Estimated processed bytes

SELECT count(distinct VendorID)
FROM taxi-rides-ny-448612.hw3_ny_taxi.yellow_tripdata_non_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT count(distinct VendorID)
FROM taxi-rides-ny-448612.hw3_ny_taxi.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

Answer: 310.24 MB and  26.84 MB

## Question 7. Where is the data for external tables stored?

Answer: GCP Bucket

## Question 8. Always clustering

Answer: False