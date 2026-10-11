-- E-bikes are a subset of num_bikes_available, so classic = bikes - ebikes.
-- If this ever returns rows, that assumption broke upstream and every
-- classic-bike number downstream is wrong.
select *
from {{ ref('stg_station_status') }}
where ebikes_available > bikes_available
