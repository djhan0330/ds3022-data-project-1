{{ config(materialized='table', schema='main') }}

WITH emissions AS (
    SELECT * FROM {{ source('main', 'vehicle_emissions') }}
),
green AS (
    SELECT * FROM {{ source('main', 'green_clean') }}
)

SELECT
    g.*,
    -- Calculating CO₂ per trip in kg
    (g.trip_distance * e.co2_grams_per_mile / 1000.0) AS trip_co2_kgs,

    -- Average MPH
    CASE
        WHEN EXTRACT(EPOCH FROM (g.lpep_dropoff_datetime - g.lpep_pickup_datetime)) / 3600.0 > 0
        THEN g.trip_distance / (EXTRACT(EPOCH FROM (g.lpep_dropoff_datetime - g.lpep_pickup_datetime)) / 3600.0)
        ELSE NULL
    END AS avg_mph,

    -- Time features
    EXTRACT(HOUR FROM g.lpep_pickup_datetime) AS hour_of_day,
    EXTRACT(DOW FROM g.lpep_pickup_datetime) AS day_of_week,
    EXTRACT(WEEK FROM g.lpep_pickup_datetime) AS week_of_year,
    EXTRACT(MONTH FROM g.lpep_pickup_datetime) AS month_of_year

FROM green g
LEFT JOIN emissions e
    ON e.vehicle_type = 'green_taxi'
