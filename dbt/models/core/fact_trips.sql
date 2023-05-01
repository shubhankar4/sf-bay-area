{{ config(materialized="table") }}

select
    start_station_name,
    date(started_at) as start_date,
    extract(hour from started_at) as start_hr,
    member_casual,
    rideable_type,
    count(*) as num_trips,
    round(avg(trip_duration_mins), 2) as avg_trip_duration_mins
from {{ ref('stg_tripdata') }}
where start_station_name is not null
group by start_station_name, start_date, start_hr, member_casual, rideable_type
order by start_station_name, start_date, start_hr, member_casual, rideable_type