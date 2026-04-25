-- Date dimension shared by both restaurant applications and 311 requests

WITH all_dates AS (
   -- Get dates (dates, no time included) from 311 requests
   SELECT DISTINCT CAST(created_date AS DATE) AS full_date
   FROM {{ ref('stg_311_street_complaint') }}
   WHERE created_date IS NOT NULL

   UNION DISTINCT

   -- Get dates from restaurant applications
   SELECT DISTINCT CAST(crash_date AS DATE) AS full_date
   FROM {{ ref('stg_motor_vehicle_collisions') }}
   WHERE crash_date IS NOT NULL
),

date_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key(['full_date']) }} AS date_key,

       full_date,
       EXTRACT(YEAR FROM full_date) AS year,
       EXTRACT(MONTH FROM full_date) AS month,
       EXTRACT(QUARTER FROM full_date) AS quarter,
       FORMAT_DATE('%B', full_date) AS month_name,
       FORMAT_DATE('%Y-%m', full_date) AS month_year,
       EXTRACT(DAY FROM full_date) AS day_of_month,
       EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week,
       FORMAT_DATE('%A', full_date) AS day_name,
       EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) AS is_weekend,


   FROM all_dates
)

SELECT * FROM date_dimension