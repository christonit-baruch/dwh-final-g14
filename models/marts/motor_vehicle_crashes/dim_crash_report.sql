WITH source AS (
    SELECT *
    FROM {{ ref('stg_motor_vehicle_collisions') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['collision_id']) }} AS crash_key,

        collision_id,

        -- temporary NULL keys until dimensions are successfully built
        NULL AS date_key,
        NULL AS time_key,
        NULL AS location_key,

        borough,
        zip_code,
        latitude,
        longitude,

        number_of_persons_injured AS num_persons_injured,
        number_of_persons_killed AS num_persons_killed,
        number_of_pedestrians_injured AS num_pedestrians_injured,
        number_of_pedestrians_killed AS num_pedestrians_killed,
        number_of_cyclist_injured AS num_cyclists_injured,
        number_of_cyclist_killed AS num_cyclists_killed,
        number_of_motorist_injured AS num_motorists_injured,
        number_of_motorist_killed AS num_motorists_killed

    FROM source
)

SELECT *
FROM final