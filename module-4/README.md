# Module 4 Homework: dbt Analytics

### Quarter 5: Taxi Quarterly Revenue Growth

```sql
WITH RankedData AS (
    SELECT 
        service_type, 
        year_quarter, 
        yoy_growth_percentage,
        RANK() OVER(PARTITION BY service_type ORDER BY yoy_growth_percentage DESC) AS best_rank,
        RANK() OVER(PARTITION BY service_type ORDER BY yoy_growth_percentage ASC) AS worst_rank
    FROM `focus-heuristic-448107-a5.nytaxi_hw4.fct_taxi_monthly_zone_revenue`
    WHERE service_type IN ('Yellow', 'Green')
      AND CAST(SUBSTR(year_quarter, 1, 4) AS INT) = 2020
)
SELECT * FROM RankedData
ORDER BY service_type, best_rank;
```



### Question 6: P97/P95/P90 Taxi Monthly Fare

```sql
SELECT DISTINCT service_type, year, month, 
       PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97,
       PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95,
       PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90
FROM `focus-heuristic-448107-a5.nytaxi_hw4.dim_taxi_trips`
WHERE service_type = 'Green' AND year = 2020 AND month = 4
LIMIT 1;

SELECT DISTINCT service_type, year, month, 
       PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97,
       PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95,
       PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90
FROM `focus-heuristic-448107-a5.nytaxi_hw4.dim_taxi_trips`
WHERE service_type = 'Yellow' AND year = 2020 AND month = 4
LIMIT 1;
```


### Question 7: Top #Nth longest P90 travel time Location for FHV

```sql
SELECT DISTINCT
pickup_zone,
dropoff_zone,
p90
FROM `focus-heuristic-448107-a5.nytaxi_hw4.fct_fhv_monthly_zone_traveltime_p90`
WHERE pickup_zone = 'Newark Airport' AND
fhv_year = 2019 AND
fhv_month = 11
ORDER BY p90 DESC;

SELECT DISTINCT
pickup_zone,
dropoff_zone,
p90
FROM `focus-heuristic-448107-a5.nytaxi_hw4.fct_fhv_monthly_zone_traveltime_p90`
WHERE pickup_zone = 'SoHo' AND
fhv_year = 2019 AND
fhv_month = 11
ORDER BY p90 DESC;

SELECT DISTINCT
pickup_zone,
dropoff_zone,
p90
FROM `focus-heuristic-448107-a5.nytaxi_hw4.fct_fhv_monthly_zone_traveltime_p90`
WHERE pickup_zone = 'Yorkville East' AND
fhv_year = 2019 AND
fhv_month = 11
ORDER BY p90 DESC;
```