{{
    config(
        materialized='table'
    )
}}

SELECT
    t.year,
    t.month,
    t.pickup_datetime,
    t.dropoff_datetime,
    pu.zone AS pickup_zone,
    do.zone AS dropoff_zone
FROM {{ ref("stg_fhv") }} AS t
INNER JOIN {{ ref('taxi_zone_lookup') }} AS pu
    ON t.pulocationid = pu.locationid
INNER JOIN {{ ref('taxi_zone_lookup') }} AS do
    ON t.dolocationid = do.locationid

