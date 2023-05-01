{{ config(materialized="view") }}

with tripdata as
(
    select *,
        row_number() over(partition by ride_id, started_at) as rn
    from {{ source('staging', 'trips_data_partitioned') }}
    where ride_id is not null
)
select
    -- Ride info
    {{ dbt_utils.generate_surrogate_key(['ride_id', 'started_at']) }} as trip_id,
    cast(ride_id as string) as ride_id,
    cast(rideable_type as string) as rideable_type,
    cast(member_casual as string) as member_casual,

    -- Temporal info
    cast(started_at as timestamp) as started_at,
    cast(ended_at as timestamp) as ended_at,
    round(
        (unix_seconds(ended_at) - unix_seconds(started_at)) / 60.0, 2
    ) as trip_duration_mins,

    -- Stations info
    cast(start_station_id as string) as start_station_id,
    cast(end_station_id as string) as end_station_id,
    cast(start_station_name as string) as start_station_name,
    cast(end_station_name as string) as end_station_name,

    -- Geographic info
    round(cast(start_lat as numeric), 4) as start_lat,
    round(cast(start_lng as numeric), 4) as start_lng,
    round(cast(end_lat as numeric), 4) as end_lat,
    round(cast(end_lat as numeric), 4) as end_lng
from tripdata
where rn = 1