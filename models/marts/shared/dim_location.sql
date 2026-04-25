WITH locations AS (
   -- Get dates (dates, no time included) from 311 requests
   SELECT DISTINCT borough, 
           incident_zip AS zip_code
   FROM {{ ref('stg_311_street_complaint') }}
   WHERE borough IS NOT NULL

   UNION DISTINCT

   -- Get dates from restaurant applications
   SELECT DISTINCT borough, zip_code
   FROM {{ ref('stg_motor_vehicle_collisions') }}
   WHERE borough IS NOT NULL
),
 location_dimension AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY zip_code) as location_sgkey,
        borough,
        zip_code
    FROM locations
)

SELECT * FROM location_dimension

