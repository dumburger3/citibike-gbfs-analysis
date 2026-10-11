-- Grain: one row per station x hour-of-day (ET) x weekday/weekend.
-- "When does this station fail?" — the profile a dispatcher plans a shift around.

select
    station_id,
    is_weekend,
    hour_et,

    count(*)                                                        as observations,
    sum(weight_minutes)                                             as observed_minutes,

    sum(iff(is_empty, weight_minutes, 0)) / nullif(sum(weight_minutes), 0)
                                                                    as pct_time_empty,
    sum(iff(is_full,  weight_minutes, 0)) / nullif(sum(weight_minutes), 0)
                                                                    as pct_time_full,
    sum(iff(is_empty or is_full, weight_minutes, 0)) / nullif(sum(weight_minutes), 0)
                                                                    as pct_time_stocked_out,

    avg(fullness_ratio)                                             as avg_fullness,
    avg(bikes_available)                                            as avg_bikes_available,
    avg(docks_available)                                            as avg_docks_available

from {{ ref('int_station_observations') }}
group by station_id, is_weekend, hour_et
