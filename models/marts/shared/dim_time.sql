with
    time_spine as (
        -- Generate an array from 0 to 1439 (the total number of minutes in a 24-hour
        -- day)
        select offset_minute from unnest(generate_array(0, 1439)) as offset_minute
    ),

    time_dimension as (
        select
            -- Smart Surrogate Key: We format hour and minute into an integer like
            -- HHMM.
            -- e.g., 8:05 AM = 805. 4:30 PM = 1630. This ensures it matches the ERD's
            -- 'integer' type.
            cast(
                format(
                    '%02d%02d', div(offset_minute, 60), mod(offset_minute, 60)
                ) as int64
            ) as time_sgkey,

            -- Calculate the hour (0-23)
            div(offset_minute, 60) as hour,

            -- Calculate the minute (0-59)
            mod(offset_minute, 60) as minute,

            -- Group into logical times of day
            case
                when div(offset_minute, 60) >= 5 and div(offset_minute, 60) < 12
                then 'Morning'
                when div(offset_minute, 60) >= 12 and div(offset_minute, 60) < 17
                then 'Afternoon'
                when div(offset_minute, 60) >= 17 and div(offset_minute, 60) < 21
                then 'Evening'
                else 'Night'
            end as time_of_day,

            -- Rush hour flag based on typical NYC transit peaks (7am-10am and 4pm-7pm)
            case
                when
                    (div(offset_minute, 60) >= 7 and div(offset_minute, 60) < 10)
                    or (div(offset_minute, 60) >= 16 and div(offset_minute, 60) < 19)
                then true
                else false
            end as rush_hour_flag

        from time_spine
    )

select *
from time_dimension
