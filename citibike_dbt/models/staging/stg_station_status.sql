-- One row per station per snapshot.
-- Flattens the raw JSON, casts every field out of VARIANT, and nulls out the
-- 86400 "never reported" sentinel. No rows are filtered here — staging cleans
-- and types; deciding what counts as a "real" station happens downstream.
--
-- Incremental: each run only flattens snapshots newer than what's already in
-- the table, instead of re-exploding millions of rows every time.

{{ config(
    materialized='incremental',
    on_schema_change='fail'
) }}

with snapshots as (

    select source_file, fetched_at_utc, payload
    from {{ source('gbfs_raw', 'station_status_raw') }}

    {% if is_incremental() %}
    where fetched_at_utc > (select max(fetched_at_utc) from {{ this }})
    {% endif %}

)

select
    s.fetched_at_utc,
    s.source_file,
    st.value:station_id::string                 as station_id,

    st.value:num_bikes_available::int           as bikes_available,
    st.value:num_ebikes_available::int          as ebikes_available,
    st.value:num_bikes_disabled::int            as bikes_disabled,
    st.value:num_docks_available::int           as docks_available,
    st.value:num_docks_disabled::int            as docks_disabled,

    st.value:is_installed::int = 1              as is_installed,
    st.value:is_renting::int   = 1              as is_renting,
    st.value:is_returning::int = 1              as is_returning,

    -- 86400 (= one day of seconds) is the feed's placeholder for "never
    -- reported". Anything before 2001 is not a real timestamp.
    case
        when st.value:last_reported::number > 1000000000
            then to_timestamp_ntz(st.value:last_reported::number)
    end                                         as last_reported_utc

from snapshots s,
     lateral flatten(input => s.payload:data:stations) st
