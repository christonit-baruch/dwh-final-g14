WITH source_times AS (
    -- Get distinct times from 311 complaints
    SELECT DISTINCT
        EXTRACT(HOUR FROM created_date) AS hour,
        EXTRACT(MINUTE FROM created_date) AS minute
    FROM {{ ref('stg_311_street_complaint') }}
    WHERE created_date IS NOT NULL

    UNION DISTINCT

    -- Get distinct times from motor vehicle collisions
    SELECT DISTINCT
        EXTRACT(HOUR FROM crash_date) AS hour,
        EXTRACT(MINUTE FROM crash_date) AS minute
    FROM {{ ref('stg_motor_vehicle_collisions') }}
    WHERE crash_date IS NOT NULL
),

time_dimension AS (
    SELECT
        -- Surrogate key: HHMM integer format (e.g., 8:05 AM = 805, 4:30 PM = 1630)
        CAST(
            FORMAT('%02d%02d', hour, minute) AS INT64
        ) AS time_sgkey,

        hour,
        minute,

        -- Time of day grouping
        CASE
            WHEN hour >= 5  AND hour < 12 THEN 'Morning'
            WHEN hour >= 12 AND hour < 17 THEN 'Afternoon'
            WHEN hour >= 17 AND hour < 21 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,

        -- NYC rush hour flag (7–10am and 4–7pm)
        CASE
            WHEN (hour >= 7 AND hour < 10)
              OR (hour >= 16 AND hour < 19)
            THEN TRUE
            ELSE FALSE
        END AS rush_hour_flag

    FROM source_times
)

SELECT * FROM time_dimension