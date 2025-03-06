# Module 5 Homework

## Question 1: Install Spark and PySpark

**Answer:** '3.4.4'

## Question 2: Yellow October 2024

```
-rw-r--r-- 1 polina polina   0 Mar  6 19:31 _SUCCESS
-rw-r--r-- 1 polina polina 23M Mar  6 19:31 part-00000-3c872932-8baa-4c49-b208-e2936e493fb7-c000.snappy.parquet
-rw-r--r-- 1 polina polina 23M Mar  6 19:31 part-00001-3c872932-8baa-4c49-b208-e2936e493fb7-c000.snappy.parquet
-rw-r--r-- 1 polina polina 23M Mar  6 19:31 part-00002-3c872932-8baa-4c49-b208-e2936e493fb7-c000.snappy.parquet
-rw-r--r-- 1 polina polina 23M Mar  6 19:31 part-00003-3c872932-8baa-4c49-b208-e2936e493fb7-c000.snappy.parquet
```

**Answer:** ~ 25MB

## Question 3: Count records

```
df_result = spark.sql("""
SELECT 
    count(*)
FROM 
    yellow_2024_10
WHERE
    tpep_pickup_datetime >= '2024-10-15 00:00:00'
    AND
    tpep_pickup_datetime < '2024-10-16 00:00:00'
;
""").show()
```

**Answer:** ~ 125,567 (128,893)

## Question 4: Longest trip

```
df_longest_trip = spark.sql("""
SELECT 
    MAX(TIMESTAMPDIFF(hour,tpep_pickup_datetime,tpep_dropoff_datetime)) AS longest_trip
FROM 
    yellow_2024_10
;
""").show()
```

**Answer:** 162

## Question 5: User Interface

**Answer:** 4040

## Question 6: Least frequent pickup location zone

```
df_least_frequent = spark.sql("""
SELECT 
    PULocationID,
    Zone,
    count(*)
FROM 
    with_zones
GROUP BY
    1,2
ORDER BY
    count(*) asc
LIMIT
    1
;
""").show()
```

**Answer:** Governor's Island/Ellis Island/Liberty Island