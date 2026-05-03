WITH source AS (
    SELECT *
    FROM {{ ref('stg_motor_vehicle_collisions') }}
),
<<<<<<< HEAD

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

=======
combinations AS (
    SELECT DISTINCT
        COALESCE(source.number_of_persons_injured, 0) AS num_persons_injured,
        COALESCE(source.number_of_persons_killed, 0) AS num_persons_killed,
        COALESCE(source.number_of_pedestrians_injured, 0) AS num_pedestrians_injured,
        COALESCE(source.number_of_pedestrians_killed, 0) AS num_pedestrians_killed,
        COALESCE(source.number_of_cyclist_injured, 0) AS num_cyclists_injured,
        COALESCE(source.number_of_cyclist_killed, 0) AS num_cyclists_killed,
        COALESCE(source.number_of_motorist_injured, 0) AS num_motorists_injured,
        COALESCE(source.number_of_motorist_killed, 0) AS num_motorists_killed
>>>>>>> 9d19cd3c35400589c2be7a993840519bafea88bc
    FROM source
)

SELECT 
    ROW_NUMBER() OVER (
        ORDER BY 
            num_persons_injured,
            num_persons_killed,
            num_pedestrians_injured,
            num_pedestrians_killed,
            num_cyclists_injured,
            num_cyclists_killed,
            num_motorists_injured,
            num_motorists_killed
    ) as crash_report_sgkey,
    *
    FROM combinations
