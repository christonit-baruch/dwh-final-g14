with
    mvc as (select * from {{ ref("stg_motor_vehicle_collisions") }}),
    time as (select * from {{ ref("dim_time") }}),
    location as (select * from {{ ref("dim_location") }}),
    date as (select * from {{ ref("dim_date") }}),
    crash_report as (select * from {{ ref("dim_crash_report") }})

select
   row_number() over (order by collision_id) as crash_sgkey,
    collision_id,
    cr.crash_report_sgkey,
    (
        coalesce(number_of_persons_injured, 0)
        + coalesce(number_of_persons_killed, 0)
        + coalesce(number_of_pedestrians_injured, 0)
        + coalesce(number_of_pedestrians_killed, 0)
        + coalesce(number_of_cyclist_injured, 0)
        + coalesce(number_of_cyclist_killed, 0)
        + coalesce(number_of_motorist_injured, 0)
        + coalesce(number_of_motorist_killed, 0)
    ) as total_affected,
    date.date_sgkey,
    time.time_sgkey,
    COALESCE(by_zip.location_sgkey, by_borough.location_sgkey, 1) AS location_sgkey,
    latitude,
    longitude
from mvc
LEFT JOIN location AS by_zip
    ON mvc.zip_code = by_zip.zip_code
-- Join #2: Try to get the "Borough-level" record (where zip_code IS NULL in the dimension)
LEFT JOIN location AS by_borough
    ON mvc.borough = by_borough.borough
    AND by_borough.zip_code IS NULL
{#-- Join 3: stuff for the crash report #}
LEFT JOIN crash_report AS cr
    ON  
    mvc.number_of_persons_injured     = cr.num_persons_injured
    AND mvc.number_of_persons_killed       = cr.num_persons_killed
    AND mvc.number_of_pedestrians_injured  = cr.num_pedestrians_injured
    AND mvc.number_of_pedestrians_killed   = cr.num_pedestrians_killed
    AND mvc.number_of_cyclist_injured      = cr.num_cyclists_injured
    AND mvc.number_of_cyclist_killed       = cr.num_cyclists_killed
    AND mvc.number_of_motorist_injured     = cr.num_motorists_injured
    AND mvc.number_of_motorist_killed      = cr.num_motorists_killed#}
LEFT JOIN date
    ON date.full_date = mvc.crash_date
LEFT JOIN time
    ON CAST(REPLACE(mvc.crash_time, ':', '') AS INT64) = time.time_sgkey
