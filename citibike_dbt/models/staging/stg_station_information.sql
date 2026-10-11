-- One row per station: its most recent reference record.
-- station_information is snapshotted daily but barely changes, so we keep the
-- latest version of each station rather than a daily history.

with flat as (

    select
        s.fetched_at_utc,
        st.value:station_id::string   as station_id,
        st.value:name::string         as station_name,
        st.value:short_name::string   as short_name,
        st.value:lat::float           as lat,
        st.value:lon::float           as lon,
        st.value:capacity::int        as capacity,
        st.value:region_id::string    as region_id
    from {{ source('gbfs_raw', 'station_information_raw') }} s,
         lateral flatten(input => s.payload:data:stations) st

)

select
    station_id,
    station_name,
    short_name,
    lat,
    lon,
    capacity,
    region_id,
    fetched_at_utc as info_as_of_utc
from flat
qualify row_number() over (partition by station_id order by fetched_at_utc desc) = 1
