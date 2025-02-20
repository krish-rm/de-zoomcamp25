{{ config(materialized='table') }}

WITH filtered_trips AS (
    SELECT 
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount
    FROM `focus-heuristic-448107-a5.nytaxi_hw4.dim_taxi_trips`
    WHERE fare_amount > 0 
      AND trip_distance > 0
      AND payment_type_description IN ('Cash', 'Credit Card')
),

percentile_fares AS (
    SELECT 
        service_type,
        year,
        month,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(90)] AS p90_fare,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(95)] AS p95_fare,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(97)] AS p97_fare
    FROM filtered_trips
    GROUP BY service_type, year, month
)

SELECT * FROM percentile_fares
ORDER BY service_type, year, month;
