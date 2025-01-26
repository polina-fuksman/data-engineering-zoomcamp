# Data Engineering Zoomcamp 2025

## Module 1 Homework: Docker & SQL


### Question 1. Understanding docker first run

Command: 
        docker run -it --entrypoint=bash python:3.12.8

**Answer:** 24.3.1


### Question 2. Understanding Docker networking and docker-compose

**Answer:** postgres:5432


### Question 3. Trip Segmentation Count

sql query:

        select count(lpep_pickup_datetime), 'Up to 1 mile' as question
        from green_taxi_data
        where 
        (trip_distance <= 1.0)
        and
        (DATE(lpep_pickup_datetime) >= '2019-10-01' and DATE(lpep_dropoff_datetime) >= '2019-10-01')
        and
        (DATE(lpep_pickup_datetime) < '2019-11-01' and DATE(lpep_dropoff_datetime) < '2019-11-01')

        UNION

        select count(lpep_pickup_datetime), 'In between 1 (exclusive) and 3 miles (inclusive)' as question
        from green_taxi_data
        where 
        trip_distance > 1.0 and trip_distance <= 3.0
        and 
        (DATE(lpep_pickup_datetime) >= '2019-10-01' and DATE(lpep_dropoff_datetime) >= '2019-10-01')
        and
        (DATE(lpep_pickup_datetime) < '2019-11-01' and DATE(lpep_dropoff_datetime) < '2019-11-01')

        UNION

        select count(lpep_pickup_datetime), 'In between 3 (exclusive) and 7 miles (inclusive)' as question
        from green_taxi_data
        where 
        trip_distance > 3.0 and trip_distance <= 7.0
        and 
        (DATE(lpep_pickup_datetime) >= '2019-10-01' and DATE(lpep_pickup_datetime) >= '2019-10-01')
        and
        (DATE(lpep_pickup_datetime) < '2019-11-01' and DATE(lpep_dropoff_datetime) < '2019-11-01')

        UNION

        select count(lpep_pickup_datetime), 'In between 7 (exclusive) and 10 miles (inclusive)' as question
        from green_taxi_data
        where 
        trip_distance > 7.0 and trip_distance <= 10.0
        and 
        (DATE(lpep_pickup_datetime) >= '2019-10-01' and DATE(lpep_pickup_datetime) >= '2019-10-01')
        and
        (DATE(lpep_pickup_datetime) < '2019-11-01' and DATE(lpep_dropoff_datetime) < '2019-11-01')

        UNION

        select count(lpep_pickup_datetime), 'Over 10 miles' as question
        from green_taxi_data
        where 
        trip_distance > 10.0
        and 
        (DATE(lpep_pickup_datetime) >= '2019-10-01' and DATE(lpep_pickup_datetime) >= '2019-10-01')
        and
        (DATE(lpep_pickup_datetime) < '2019-11-01' and DATE(lpep_dropoff_datetime) < '2019-11-01')
        ;

**answer:**

104802	"Up to 1 mile"

198924	"In between 1 (exclusive) and 3 miles (inclusive)"

109603	"In between 3 (exclusive) and 7 miles (inclusive)"

27678	"In between 7 (exclusive) and 10 miles (inclusive)"

35189	"Over 10 miles"


### Question 4. Longest trip for each day

sql query:

        select DATE(lpep_pickup_datetime) as pick_up_date, MAX(trip_distance) as max_dis
        from green_taxi_data
        where
        DATE(lpep_pickup_datetime) in
        (
        '2019-10-11',
        '2019-10-24',
        '2019-10-26',
        '2019-10-31')
        group by DATE(lpep_pickup_datetime)
        order by max_dis DESC
        limit 1
        ;

**answer** "2019-10-31"	515.89


### Question 5. Three biggest pickup zones

sql query:

        select "Zone", sum(total_amount)
        from green_taxi_data t
        left join taxi_zones z on t."PULocationID" = z."LocationID"
        where DATE(lpep_pickup_datetime) = '2019-10-18'
        group by "Zone"
        having sum(total_amount) > 13000
        order by sum(total_amount) desc;

**answer:**

"East Harlem North"	18686.680000000088

"East Harlem South"	16797.260000000075

"Morningside Heights"	13029.790000000039


### Question 6. Largest tip

sql query:

        select zpu."Zone", zdo."Zone" as drop_off_zone, tip_amount
        from green_taxi_data t
        left join taxi_zones zpu on t."PULocationID" = zpu."LocationID"
        left join taxi_zones zdo on t."DOLocationID" = zdo."LocationID"
        where DATE(lpep_pickup_datetime) BETWEEN '2019-10-01' and '2019-10-31'
        and zpu."Zone" like 'East Harlem North'
        order by tip_amount desc
        limit 1;

**answer:** "East Harlem North"	--> "JFK Airport"	87.3


### Question 7. Terraform Workflow

**answer:** terraform init, terraform apply -auto-approve, terraform destroy