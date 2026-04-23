WITH source AS (
    SELECT * FROM {{ source('raw', '311_street_complaint_raw_data') }}
),

cleaned AS (
    SELECT
        -- Grab every single column from the source EXCEPT the four we are about to clean
        * EXCEPT (
            unique_key,
            created_date,
            borough,
            incident_zip
        ),
        
        -- Re-add the 4 columns with their EXACT original names, but cleaned up
        CAST(unique_key AS STRING) AS unique_key,
        
        CAST(created_date AS TIMESTAMP) AS created_date,
        
        CASE
            WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
            WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
            WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
            WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
            WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
            ELSE 'UNKNOWN or CITYWIDE'
        END AS borough,
        
        CASE
            WHEN UPPER(TRIM(CAST(incident_zip AS STRING))) IN ('N/A', 'NA', '') THEN NULL
            WHEN LENGTH(TRIM(CAST(incident_zip AS STRING))) = 5 THEN TRIM(CAST(incident_zip AS STRING))
            WHEN LENGTH(TRIM(CAST(incident_zip AS STRING))) = 9 THEN TRIM(CAST(incident_zip AS STRING))
            ELSE NULL
        END AS incident_zip,
        
        -- Add the metadata column
        CURRENT_TIMESTAMP() AS stg_loaded_at
        
    FROM source
    WHERE unique_key IS NOT NULL
)

SELECT *
FROM cleaned
-- Deduplicate using the original unique_key name
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY created_date DESC) = 1