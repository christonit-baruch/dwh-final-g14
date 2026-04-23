WITH source AS (
    SELECT * FROM {{ source('raw', 'motor_vehicle_collisions_raw_data') }}
),

cleaned AS (
    SELECT
        -- Grab every single column from the source EXCEPT the three we are about to clean
        * EXCEPT (
            collision_id,
            borough,
            zip_code
        ),
        
        -- Re-add the columns with their EXACT original names, but cleaned up
        CAST(collision_id AS STRING) AS collision_id,
        
        CASE
            WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
            WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
            WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
            WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
            WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
            ELSE 'UNKNOWN or CITYWIDE'
        END AS borough,
        
        CASE
            WHEN UPPER(TRIM(CAST(zip_code AS STRING))) IN ('N/A', 'NA', '') THEN NULL
            WHEN LENGTH(TRIM(CAST(zip_code AS STRING))) = 5 THEN TRIM(CAST(zip_code AS STRING))
            WHEN LENGTH(TRIM(CAST(zip_code AS STRING))) = 9 THEN TRIM(CAST(zip_code AS STRING))
            ELSE NULL
        END AS zip_code,
        
        -- Add the metadata column
        CURRENT_TIMESTAMP() AS stg_loaded_at
        
    FROM source
    WHERE collision_id IS NOT NULL
)

SELECT *
FROM cleaned
-- Deduplicate using the collision_id, keeping the most recent crash_date if there are duplicates
QUALIFY ROW_NUMBER() OVER (PARTITION BY collision_id ORDER BY crash_date DESC) = 1