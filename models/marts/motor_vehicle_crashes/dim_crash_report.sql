WITH source AS (
    SELECT *
    FROM {{ ref('stg_motor_vehicle_collisions') }}
),
final AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY COLLISION_ID) as crash_report_sgkey,
        source.number_of_persons_injured AS num_persons_injured,
        source.number_of_persons_killed AS num_persons_killed,
        source.number_of_pedestrians_injured AS num_pedestrians_injured,
        source.number_of_pedestrians_killed AS num_pedestrians_killed,
        source.number_of_cyclist_injured AS num_cyclist_injured,
        source.number_of_cyclist_killed AS num_cyclist_killed,
        source.number_of_motorist_injured AS num_motorist_injured,
        source.number_of_motorist_killed AS num_motorist_killed
    FROM source
)

SELECT *
FROM final