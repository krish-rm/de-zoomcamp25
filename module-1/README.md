# Module 1 Homework: Docker & SQL

### Question 1. Understanding docker first run

Run Docker with the python:3.12.8 image in interactive mode

```bash
docker run -it --entrypoint bash python:3.12.8
```

Check the version of pip

```bash
root@2e7bb3d40ae4:/# pip --version
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
```

ANSWER: **pip 24.3.1**


### Question 2. Understanding Docker networking and docker-compose

The hostname for the PostgreSQL service will be the name of the service, which is defined as db.
Inside the container, the PostgreSQL service will be running on the default PostgreSQL port 5432.

ANSWER: **db:5432**

### Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

Up to 1 mile
In between 1 (exclusive) and 3 miles (inclusive),
In between 3 (exclusive) and 7 miles (inclusive),
In between 7 (exclusive) and 10 miles (inclusive),
Over 10 mile

```sql
SELECT
    SUM(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) AS "Up to 1 mile",
    SUM(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 ELSE 0 END) AS "Between 1 and 3 miles",
    SUM(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 ELSE 0 END) AS "Between 3 and 7 miles",
    SUM(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 ELSE 0 END) AS "Between 7 and 10 miles",
    SUM(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) AS "Over 10 miles"
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01'
  AND lpep_pickup_datetime < '2019-11-01';
```

ANSWER: **104,838; 199,013; 109,645; 27,688; 35,202**


### Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.
Tip: For every day, we only care about one single trip with the longest distance.

```sql
SELECT DATE(lpep_pickup_datetime) AS pickup_date , trip_distance
FROM green_taxi_trips
ORDER BY trip_distance DESC
LIMIT 1;

```

ANSWER: **2019-10-31         515.89**


### Question 5. Three biggest pickup zones
Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?
Consider only lpep_pickup_datetime when filtering by date.

```sql
SELECT tz."Zone", tz."Borough", SUM(gtt.total_amount) AS total_amount
FROM green_taxi_trips gtt
JOIN taxi_zones tz ON gtt."PULocationID" = tz."LocationID"
WHERE DATE(gtt.lpep_pickup_datetime) = '2019-10-18'
GROUP BY tz."Zone", tz."Borough"
HAVING SUM(gtt.total_amount) > 13000
ORDER BY total_amount DESC
LIMIT 3;
```

ANSWER: **East Harlem North, East Harlem South, Morningside Heights**


### Question 6. Largest tip
For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?
Note: it's tip , not trip
We need the name of the zone, not the ID.

```sql
SELECT tz."Zone" AS dropoff_zone, MAX(gtt.tip_amount) AS max_tip
FROM green_taxi_trips gtt
JOIN taxi_zones tz ON gtt."DOLocationID" = tz."LocationID"
JOIN taxi_zones pickup_zone ON gtt."PULocationID" = pickup_zone."LocationID"
WHERE pickup_zone."Zone" = 'East Harlem North'
AND DATE(gtt.lpep_pickup_datetime) BETWEEN '2019-10-01' AND '2019-10-31'
GROUP BY tz."Zone"
ORDER BY max_tip DESC
LIMIT 1;
```

ANSWER: **JFK Airport 87.3**


### Question 7. Terraform Workflow
Which of the following sequences, respectively, describes the workflow for:
Downloading the provider plugins and setting up backend,
Generating proposed changes and auto-executing the plan
Remove all resources managed by terraform`

ANSWER: **terraform init, terraform apply -auto-approve, terraform destroy**