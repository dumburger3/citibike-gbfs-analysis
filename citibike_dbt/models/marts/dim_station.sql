-- Grain: one row per station.
-- Reference attributes plus each station's overall reliability profile, and a
-- rebalancing priority rank: "where should the truck go first?"

with obs as (

    select
        station_id,
        count(distinct date_et)                                         as days_observed,
        sum(weight_minutes)                                             as observed_minutes,
        sum(iff(is_empty, weight_minutes, 0)) / nullif(sum(weight_minutes), 0)
                                                                        as pct_time_empty,
        sum(iff(is_full,  weight_minutes, 0)) / nullif(sum(weight_minutes), 0)
                                                                        as pct_time_full,
        avg(fullness_ratio)                                             as avg_fullness,

        -- Weekday commute windows, where a stockout costs the most riders.
        sum(iff(not is_weekend and hour_et between 7 and 9 and is_empty, weight_minutes, 0))
            / nullif(sum(iff(not is_weekend and hour_et between 7 and 9, weight_minutes, 0)), 0)
                                                                        as pct_empty_am_peak,
        sum(iff(not is_weekend and hour_et between 16 and 19 and is_full, weight_minutes, 0))
            / nullif(sum(iff(not is_weekend and hour_et between 16 and 19, weight_minutes, 0)), 0)
                                                                        as pct_full_pm_peak,

        -- Activity: net bikes moved per hour, measured only across normal
        -- sampling intervals (<=15 min) so collection gaps don't distort it.
        60 * sum(iff(minutes_since_prev_obs <= 15, bikes_changed, 0))
            / nullif(sum(iff(minutes_since_prev_obs <= 15, minutes_since_prev_obs, 0)), 0)
                                                                        as bikes_moved_per_hour
    from {{ ref('int_station_observations') }}
    group by station_id

),

episodes as (

    select
        station_id,
        count(*)                                                        as stockout_episodes,
        -- Durations only from episodes whose end we actually observed;
        -- the rest are capped guesses. MEDIAN/MAX skip the NULLs.
        median(iff(not end_unobserved, duration_minutes, null))         as median_episode_minutes,
        max(iff(not end_unobserved, duration_minutes, null))            as longest_episode_minutes,
        count_if(not end_unobserved)                                    as episodes_with_observed_duration
    from {{ ref('fct_stockout_episodes') }}
    where not spans_collection_gap
    group by station_id

),

joined as (

    select
        i.station_id,
        i.short_name,
        i.station_name,
        i.lat,
        i.lon,
        i.capacity,

        o.days_observed,
        o.observed_minutes,
        o.pct_time_empty,
        o.pct_time_full,
        o.pct_time_empty + o.pct_time_full                              as pct_time_stocked_out,
        o.avg_fullness,
        o.pct_empty_am_peak,
        o.pct_full_pm_peak,
        o.bikes_moved_per_hour,

        -- Stocked-out share of time x how busy the station is. A full station
        -- nobody rides to barely matters; a full station in heavy use turns
        -- riders away constantly. Higher = more riders hurt.
        (o.pct_time_empty + o.pct_time_full) * o.bikes_moved_per_hour   as stockout_impact_score,

        coalesce(e.stockout_episodes, 0)                                as stockout_episodes,
        e.stockout_episodes / nullif(o.days_observed, 0)                as stockout_episodes_per_day,
        e.median_episode_minutes,
        e.longest_episode_minutes,
        e.episodes_with_observed_duration,

        case
            when o.pct_time_empty >= 0.20 and o.pct_time_full >= 0.20 then 'both'
            when o.pct_time_empty >= 0.20                             then 'chronically empty'
            when o.pct_time_full  >= 0.20                             then 'chronically full'
            else 'healthy'
        end                                                             as station_profile

    from {{ ref('stg_station_information') }} i
    inner join obs o using (station_id)
    left join episodes e using (station_id)

)

select
    *,
    -- Only rank stations with at least a week of history; a station seen for
    -- two days shouldn't top the list on noise.

    -- Naive rank: share of time stocked out. Kept for comparison — it favors
    -- quiet stations that sit full because nobody rides them.
    iff(
        days_observed >= 7,
        rank() over (partition by (days_observed >= 7) order by pct_time_stocked_out desc),
        null
    )                                                                   as stockout_time_rank,

    -- Priority rank: weighted by activity. "Where does the truck go first?"
    iff(
        days_observed >= 7,
        rank() over (partition by (days_observed >= 7) order by stockout_impact_score desc nulls last),
        null
    )                                                                   as rebalancing_priority_rank
from joined
