WITH source AS (
    SELECT *
    FROM {{ ref('stg_motor_vehicle_collisions') }}
),
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
