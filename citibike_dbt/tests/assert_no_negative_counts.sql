select *
from {{ ref('stg_station_status') }}
where bikes_available < 0
   or ebikes_available < 0
   or docks_available < 0
   or bikes_disabled < 0
   or docks_disabled < 0
