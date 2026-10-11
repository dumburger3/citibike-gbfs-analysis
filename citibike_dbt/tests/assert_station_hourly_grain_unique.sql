-- fct_station_hourly must have exactly one row per station x weekend flag x hour.
select station_id, is_weekend, hour_et, count(*) as n
from {{ ref('fct_station_hourly') }}
group by 1, 2, 3
having count(*) > 1
