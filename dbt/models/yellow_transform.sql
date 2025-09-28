{{ config(materialized='table', schema='main') }}
--jinja command

--Create a CTE named emissions, pulling all rows from the vehicle_emissions table
WITH emissions AS (
    SELECT * FROM {{ ref('vehicle_emissions') }}
),
yellow AS (SELECT * FROM {{ ref('yellow_clean')}}
)

SELECT
    y.*,
    --Calculating Co2 per trip in kg
    (y.trip_distance * e.co2_grams_per_mile / 1000) AS trip_co2_kgs,

    --average mph
    CASE
        WHEN EXTRACT(epoch from (y.tpep_dropoff_datetime - y.tpep_pickup_datetime)) / 3600.0 > 0
        THEN y.trip_distance / (EXTRACT(epoch from (y.tpep_dropoff_datetime - y.tpep_pickup_datetime)) / 3600.0)
        ELSE NULL
    END AS avg_mph,

    --Trip Hour
    EXTRACT(hour FROM y.tpep_pickup_datetime) AS hour_of_day,

    --Trip Day of Week
    EXTRACT(dow FROM y.tpep_pickup_datetime) AS day_of_week,

    --Week of year
    EXTRACT(week FROM y.tpep_pickup_datetime) AS week_of_year,

    --Month of Year
    EXTRACT(month FROM y.tpep_pickup_datetime) AS month_of_year

FROM yellow y
LEFT JOIN emissions e
    ON e.vehicle_type = 'yellow_taxi'
