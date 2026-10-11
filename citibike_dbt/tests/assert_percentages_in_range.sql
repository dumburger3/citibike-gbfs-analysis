select *
from {{ ref('fct_station_hourly') }}
where pct_time_empty not between 0 and 1
   or pct_time_full  not between 0 and 1
   or pct_time_stocked_out not between 0 and 1
