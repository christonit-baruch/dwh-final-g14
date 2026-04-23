WITH time_spine AS (
    -- Generate an array from 0 to 1439 (the total number of minutes in a 24-hour day)
    SELECT offset_minute
    FROM UNNEST(GENERATE_ARRAY(0, 1439)) AS offset_minute
),

time_dimension AS (
    SELECT
        -- Smart Surrogate Key: We format hour and minute into an integer like HHMM. 
        -- e.g., 8:05 AM = 805. 4:30 PM = 1630. This ensures it matches the ERD's 'integer' type.
        CAST(FORMAT('%02d%02d', DIV(offset_minute, 60), MOD(offset_minute, 60)) AS INT64) AS time_sgkey,
        
        -- Calculate the hour (0-23)
        DIV(offset_minute, 60) AS hour,
        
        -- Calculate the minute (0-59)
        MOD(offset_minute, 60) AS minute,
        
        -- Group into logical times of day
        CASE
            WHEN DIV(offset_minute, 60) >= 5 AND DIV(offset_minute, 60) < 12 THEN 'Morning'
            WHEN DIV(offset_minute, 60) >= 12 AND DIV(offset_minute, 60) < 17 THEN 'Afternoon'
            WHEN DIV(offset_minute, 60) >= 17 AND DIV(offset_minute, 60) < 21 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        
        -- Rush hour flag based on typical NYC transit peaks (7am-10am and 4pm-7pm)
        CASE
            WHEN (DIV(offset_minute, 60) >= 7 AND DIV(offset_minute, 60) < 10) 
              OR (DIV(offset_minute, 60) >= 16 AND DIV(offset_minute, 60) < 19) 
            THEN TRUE
            ELSE FALSE
        END AS rush_hour_flag

    FROM time_spine
)

SELECT * FROM time_dimension