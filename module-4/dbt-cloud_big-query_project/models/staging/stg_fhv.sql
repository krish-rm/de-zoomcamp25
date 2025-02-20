{{
    config(
        materialized='view'
    )
}}

SELECT
    pulocationid,
    dolocationid,
    pickup_datetime,
    dropoff_datetime,
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    EXTRACT(MONTH FROM pickup_datetime) AS month
FROM {{ source('staging', 'external_fhv') }}
WHERE dispatching_base_num IS NOT NULL