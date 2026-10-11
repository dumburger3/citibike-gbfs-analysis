-- Grain: one row per stockout episode — a continuous run of snapshots where a
-- station was empty (no bikes) or full (no docks).
--
-- This answers what an hourly percentage can't: HOW LONG a station stays
-- broken once it breaks. 30% empty could be one 7-hour outage or forty
-- 10-minute blips, and those need completely different responses.
--
-- Technique: gaps-and-islands. CONDITIONAL_CHANGE_EVENT increments a counter
-- every time a station's state changes, so each unbroken run of the same
-- state gets its own number. Group by that number and each group is an episode.

with states as (

    select
        station_id,
        fetched_at_utc,
        observed_at_et,
        weight_minutes,
        is_weekend,
        hour_et,
        case
            when is_empty then 'empty'
            when is_full  then 'full'
            else 'ok'
        end as state,
        minutes_since_prev_obs
    from {{ ref('int_station_observations') }}

),

islands as (

    select
        *,
        conditional_change_event(state)
            over (partition by station_id order by fetched_at_utc) as state_run
    from states

)

select
    station_id || '-' || state_run                                  as episode_id,
    station_id,
    state                                                           as stockout_type,

    min(observed_at_et)                                             as started_at_et,
    max(observed_at_et)                                             as last_seen_at_et,
    min_by(hour_et, fetched_at_utc)                                 as start_hour_et,
    min_by(is_weekend, fetched_at_utc)                              as started_on_weekend,

    -- First to last snapshot, plus the weight of the last snapshot (the time
    -- it represents before the next check).
    datediff('second', min(fetched_at_utc), max(fetched_at_utc)) / 60.0
        + max_by(weight_minutes, fetched_at_utc)                    as duration_minutes,

    count(*)                                                        as observations,

    -- If a collection gap (>15 min) falls inside the episode, we never saw
    -- the station during it — it may have recovered and failed again.
    -- Flag those so analyses can exclude or caveat them.
    coalesce(max(iff(observations_index > 1, minutes_since_prev_obs, null)), 0) > 15
                                                                    as spans_collection_gap,

    -- If the snapshot after the episode's last one came 15+ minutes later
    -- (or never), we don't know when the episode actually ended, so
    -- duration_minutes is a capped guess. Exclude these from duration stats.
    max_by(weight_minutes, fetched_at_utc) >= 15                    as end_unobserved

from (
    select
        *,
        row_number() over (partition by station_id, state_run order by fetched_at_utc)
            as observations_index
    from islands
)
where state <> 'ok'
group by station_id, state_run, state
