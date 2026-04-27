WITH complaints AS (
    SELECT *
    FROM {{ ref('stg_311_street_complaint') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['complaints.unique_key']) }} AS request_sgkey,
        complaints.unique_key,

        complaint_type.complaint_sgkey,
        complaint_status.status_sgkey, 

        created_date.date_sgkey AS created_date_sgkey,
        closed_date.date_sgkey AS closed_date_sgkey,
        location.location_sgkey,

        CASE
            WHEN UPPER(TRIM(complaints.status)) = 'CLOSED' THEN TRUE
            ELSE FALSE
        END AS is_closed,

        CASE
            WHEN complaints.closed_date IS NOT NULL
            THEN DATE_DIFF(CAST(complaints.closed_date AS DATE), CAST(complaints.created_date AS DATE), DAY)
            ELSE NULL
        END AS days_to_close,

        created_time.time_sgkey AS created_time_sgkey,
        closed_time.time_sgkey AS closed_time_sgkey

    FROM complaints

    LEFT JOIN {{ ref('dim_complaint_type') }} AS complaint_type
        ON complaints.complaint_type = complaint_type.complaint_type
        AND complaints.descriptor = complaint_type.complaint_description

    LEFT JOIN {{ ref('dim_complaint_status') }} AS complaint_status
        ON UPPER(TRIM(complaints.status)) = UPPER(TRIM(complaint_status.status))
        AND complaints.agency = complaint_status.agency
        AND complaints.agency_name = complaint_status.agency_name

    LEFT JOIN {{ ref('dim_date') }} AS created_date
        ON CAST(complaints.created_date AS DATE) = created_date.full_date

    LEFT JOIN {{ ref('dim_date') }} AS closed_date
        ON CAST(complaints.closed_date AS DATE) = closed_date.full_date

    LEFT JOIN {{ ref('dim_location') }} AS location
        ON complaints.borough = location.borough

    LEFT JOIN {{ ref('dim_time') }} AS created_time
        ON CAST(FORMAT_TIMESTAMP('%H%M', complaints.created_date) AS INT64) = created_time.time_sgkey

    LEFT JOIN {{ ref('dim_time') }} AS closed_time
        ON CAST(FORMAT_TIMESTAMP('%H%M', complaints.closed_date) AS INT64) = closed_time.time_sgkey
)

SELECT *
FROM final