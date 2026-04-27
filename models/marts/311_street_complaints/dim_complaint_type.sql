WITH complaint_types AS (
    SELECT DISTINCT
        complaint_type,
        descriptor        AS complaint_description,
        complaint_category
    FROM {{ ref('stg_nyc_311_dot') }}
    WHERE complaint_type IS NOT NULL
),
dim_complaint_type AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY complaint_type) AS complaint_sgkey,
        complaint_type,
        complaint_description,
        complaint_category
    FROM complaint_types
)

SELECT * FROM dim_complaint_type