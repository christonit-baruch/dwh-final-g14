WITH source AS (
    SELECT *
    FROM {{ ref('stg_motor_vehicle_collisions') }}
),

dim_date AS (
    SELECT *
    FROM {{ ref('dim_date') }}
),

dim_time AS (
    SELECT *
    FROM {{ ref('dim_time') }}
),

dim_location AS (
    SELECT *
    FROM {{ ref('dim_location') }}
),

dim_crash_report AS (
    SELECT *
    FROM {{ ref('dim_crash_report') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['source.collision_id']) }} AS crash_skey,

        source.collision_id,

        dim_crash_report.crash_report_skey,

        (
            COALESCE(source.num_persons_injured, 0)
            + COALESCE(source.num_persons_killed, 0)
            + COALESCE(source.num_pedestrians_injured, 0)
            + COALESCE(source.num_pedestrians_killed, 0)
            + COALESCE(source.num_cyclists_injured, 0)
            + COALESCE(source.num_cyclists_killed, 0)
            + COALESCE(source.num_motorists_injured, 0)
            + COALESCE(source.num_motorists_killed, 0)
        ) AS total_affected,

        dim_time.time_skey,
        dim_location.location_skey,
        dim_date.date_skey,

        source.latitude,
        source.longitude

    FROM source

    LEFT JOIN dim_date
        ON CAST(source.crash_date AS DATE) = dim_date.full_date

    LEFT JOIN dim_time
        ON source.hour = dim_time.hour
       AND source.minute = dim_time.minute

    LEFT JOIN dim_location
        ON source.borough = dim_location.borough
       AND source.zip_code = dim_location.zip_code

    LEFT JOIN dim_crash_report
        ON source.num_persons_injured = dim_crash_report.num_persons_injured
       AND source.num_persons_killed = dim_crash_report.num_persons_killed
       AND source.num_pedestrians_injured = dim_crash_report.num_pedestrians_injured
       AND source.num_pedestrians_killed = dim_crash_report.num_pedestrians_killed
       AND source.num_cyclists_injured = dim_crash_report.num_cyclists_injured
       AND source.num_cyclists_killed = dim_crash_report.num_cyclists_killed
       AND source.num_motorists_injured = dim_crash_report.num_motorists_injured
       AND source.num_motorists_killed = dim_crash_report.num_motorists_killed
)

SELECT *
FROM final