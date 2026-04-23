-- Check 311 Complaint date ranges
SELECT 
    CAST(MIN(created_date) AS DATE) AS earliest_complaint,
    CAST(MAX(created_date) AS DATE) AS latest_complaint
FROM {{ source('raw','311_street_complaint_raw_data') }}

UNION ALL
-- Check Collision date ranges
SELECT 
   CAST(MIN(crash_date) AS DATE) AS earliest_crash,
   CAST( MAX(crash_date) AS DATE)AS latest_crash
FROM {{ source('raw','motor_vehicle_collisions_raw_data') }}