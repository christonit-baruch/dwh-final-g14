WITH complaint_statuses AS (
    SELECT DISTINCT
        status,
        agency,
        agency_name
    FROM {{ ref('stg_311_street_complaint') }}
    WHERE status IS NOT NULL
),
dim_complaint_status AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY status) AS status_sgkey,
        status,
        agency,
        agency_name
    FROM complaint_statuses
)

SELECT * FROM dim_complaint_status