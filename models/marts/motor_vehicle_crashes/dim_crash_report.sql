WITH source AS (
    SELECT *
    FROM {{ ref('stg_motor_vehicle_collisions') }}
),
dim_date AS (
    SELECT *
    FROM {{ ref('dim_date') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['collision_id']) }} AS crash_skey,

        collision_id,
        dim_date.date_key,

        -- TODO: add dimension foreign keys when teammates finish dimensions
    
        NULL AS location_skey,
        NULL AS time_skey,

        source.latitude,
        source.longitude,

        source.number_of_persons_injured,
        source.number_of_persons_killed,
        source.number_of_pedestrians_injured,
        source.number_of_pedestrians_killed,
        source.number_of_cyclist_injured,
        source.number_of_cyclist_killed,
        source.number_of_motorist_injured,
        source.number_of_motorist_killed

    FROM source

    LEFT JOIN dim_date
        ON CAST(source.crash_date AS DATE) = dim_date.full_date
)

SELECT *
FROM final