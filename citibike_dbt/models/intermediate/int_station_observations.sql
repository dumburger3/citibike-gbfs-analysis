-- One row per LIVE station per snapshot, enriched with local time, stockout
-- flags, and a time weight.
--
-- Two decisions live here:
--
-- 1. What counts as a stockout. Only stations that are installed AND renting.
--    A decommissioned station sitting at zero bikes is not a failure; an active
--    station drained to zero by riders is.
--
-- 2. How to handle irregular sampling. GitHub Actions cron fires unevenly, so
--    some hours have 12 snapshots and some have 1. Counting rows would let
--    densely-sampled hours dominate. Instead each observation is weighted by
--    the minutes until the station's next observation, capped at 15 so a
--    collection gap doesn't let one snapshot speak for an hour it never saw.
--    Downstream percentages are "share of observed time", not "share of rows".

with live as (

    select *
    from {{ ref('stg_station_status') }}
    where is_installed and is_renting

),

weighted as (

    select
        station_id,
        fetched_at_utc,
        convert_timezone('UTC', 'America/New_York', fetched_at_utc) as observed_at_et,

        bikes_available,
        ebikes_available,
        bikes_available - ebikes_available                          as classic_bikes_available,
        docks_available,
        bikes_available + docks_available                           as usable_slots,

        bikes_available = 0                                         as is_empty,
        docks_available = 0                                         as is_full,

        datediff(
            'second',
            lag(fetched_at_utc) over (partition by station_id order by fetched_at_utc),
            fetched_at_utc
        ) / 60.0                                                    as minutes_since_prev_obs,

        -- Net change in bikes since the previous snapshot. Summed over time
        -- this is an activity proxy: busy stations churn, dead ones don't.
        -- It undercounts (a rental and a return in the same window cancel
        -- out) and includes rebalancing-truck moves, so it's a relative
        -- measure of activity, not a ride count.
        abs(
            bikes_available
            - lag(bikes_available) over (partition by station_id order by fetched_at_utc)
        )                                                           as bikes_changed,

        least(
            coalesce(
                datediff(
                    'second',
                    fetched_at_utc,
                    lead(fetched_at_utc) over (partition by station_id order by fetched_at_utc)
                ) / 60.0,
                5           -- last observation for a station: assume one normal interval
            ),
            15
        )                                                           as weight_minutes

    from live

)

select
    w.*,

    date(w.observed_at_et)                          as date_et,
    hour(w.observed_at_et)                          as hour_et,
    -- ISO weekday (Mon=1 ... Sun=7) doesn't depend on session settings,
    -- unlike DAYOFWEEK.
    dayofweekiso(w.observed_at_et) in (6, 7)        as is_weekend,

    -- Fullness from observed slots, NOT declared capacity: ~35% of live
    -- stations don't reconcile to capacity (see DECISIONS.md).
    iff(w.usable_slots > 0, w.bikes_available / w.usable_slots, null) as fullness_ratio,

    i.station_name,
    i.lat,
    i.lon,
    i.capacity

from weighted w
left join {{ ref('stg_station_information') }} i
    using (station_id)
