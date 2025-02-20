{{
    config(
        materialized='table'
    )
}}

SELECT 
    year,
    month,
    pickup_zone,
    dropoff_zone,
    PERCENTILE_CONT(
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND), 
        0.90
    ) OVER (
        PARTITION BY year, month, pickup_zone, dropoff_zone
    ) AS trip_duration_p90
FROM {{ ref("dim_fhv_trips") }}
