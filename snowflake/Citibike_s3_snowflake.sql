USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS citibike_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60          -- seconds idle before shutting off. Default is 600.
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS citibike;

CREATE SCHEMA IF NOT EXISTS citibike.raw;      
CREATE SCHEMA IF NOT EXISTS citibike.staging;  
CREATE SCHEMA IF NOT EXISTS citibike.marts;    

USE WAREHOUSE citibike_wh;
USE DATABASE citibike;
USE SCHEMA raw;

SHOW SCHEMAS IN DATABASE citibike;

--- cost tracker
CREATE RESOURCE MONITOR citibike_monthly_cap
  WITH CREDIT_QUOTA = 10
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE citibike_wh SET RESOURCE_MONITOR = citibike_monthly_cap;

SHOW RESOURCE MONITORS;
SHOW WAREHOUSES;


--- S3 integration with Snowflake
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION citibike_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<my role arn>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://citibike-gbfs-raw-danny/raw/');

DESC INTEGRATION citibike_s3_integration;


--- testing connection
CREATE FILE FORMAT IF NOT EXISTS json_gz
  TYPE = JSON
  COMPRESSION = GZIP;

CREATE STAGE IF NOT EXISTS citibike_raw_stage
  STORAGE_INTEGRATION = citibike_s3_integration
  URL = 's3://citibike-gbfs-raw-danny/raw/'
  FILE_FORMAT = json_gz;

LIST @citibike_raw_stage/station_status/;


--- loading
-- station status
CREATE TABLE IF NOT EXISTS citibike.raw.station_status_raw (
  source_file    STRING,
  fetched_at_utc TIMESTAMP_NTZ,
  payload        VARIANT,
  loaded_at      TIMESTAMP_NTZ DEFAULT SYSDATE()
);

COPY INTO citibike.raw.station_status_raw (source_file, fetched_at_utc, payload)
FROM (
  SELECT
    METADATA$FILENAME,
    TO_TIMESTAMP_NTZ(
      REGEXP_SUBSTR(METADATA$FILENAME, '(\\d{8}T\\d{6}Z)'),
      'YYYYMMDD"T"HH24MISS"Z"'
    ),
    $1
  FROM @citibike_raw_stage/station_status/
)
FILE_FORMAT = (FORMAT_NAME = json_gz)
ON_ERROR = 'ABORT_STATEMENT';



-- station information
CREATE TABLE IF NOT EXISTS citibike.raw.station_information_raw (
  source_file    STRING,
  fetched_at_utc TIMESTAMP_NTZ,
  payload        VARIANT,
  loaded_at      TIMESTAMP_NTZ DEFAULT SYSDATE()
);

COPY INTO citibike.raw.station_information_raw (source_file, fetched_at_utc, payload)
FROM (
  SELECT
    METADATA$FILENAME,
    TO_TIMESTAMP_NTZ(
      REGEXP_SUBSTR(METADATA$FILENAME, '(\\d{8}T\\d{6}Z)'),
      'YYYYMMDD"T"HH24MISS"Z"'
    ),
    $1
  FROM @citibike_raw_stage/station_information/
)
FILE_FORMAT = (FORMAT_NAME = json_gz)
ON_ERROR = 'ABORT_STATEMENT';


-- copy check
SELECT
  COUNT(*)                                       AS snapshots,
  MIN(fetched_at_utc)                            AS first_obs,
  MAX(fetched_at_utc)                            AS last_obs,
  COUNT(DISTINCT DATE(fetched_at_utc))           AS days_covered,
  ROUND(AVG(ARRAY_SIZE(payload:data:stations)))  AS avg_stations
FROM citibike.raw.station_status_raw;

SELECT 
  COUNT(*), 
  MIN(fetched_at_utc), 
  MAX(fetched_at_utc)
FROM citibike.raw.station_information_raw;

-- data shape check
WITH latest_status AS (
  SELECT st.value:station_id::STRING        AS station_id,
         st.value:num_bikes_available::INT  AS bikes,
         st.value:num_bikes_disabled::INT   AS bikes_disabled,
         st.value:num_docks_available::INT  AS docks,
         st.value:num_docks_disabled::INT   AS docks_disabled,
         st.value:num_ebikes_available::INT AS ebikes,
         st.value:is_installed::INT         AS is_installed
  FROM citibike.raw.station_status_raw s,
       LATERAL FLATTEN(input => s.payload:data:stations) st
  WHERE s.fetched_at_utc = (SELECT MAX(fetched_at_utc) FROM citibike.raw.station_status_raw)
),
latest_info AS (
  SELECT st.value:station_id::STRING AS station_id,
         st.value:capacity::INT      AS capacity
  FROM citibike.raw.station_information_raw s,
       LATERAL FLATTEN(input => s.payload:data:stations) st
  WHERE s.fetched_at_utc = (SELECT MAX(fetched_at_utc) FROM citibike.raw.station_information_raw)
)
SELECT
  COUNT(*)                                     AS matched_stations,
  COUNT_IF(s.ebikes > s.bikes)                 AS ebikes_exceed_total,
  COUNT_IF(i.capacity = 0)                     AS zero_capacity,
  COUNT_IF(s.bikes + s.bikes_disabled + s.docks + s.docks_disabled <> i.capacity) AS capacity_mismatch
FROM latest_status s
JOIN latest_info i USING (station_id)
WHERE s.is_installed = 1;


-- 
WITH obs AS (
  SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', s.fetched_at_utc) AS ts_et,
         st.value:num_bikes_available::INT AS bikes,
         st.value:num_docks_available::INT AS docks,
         st.value:is_installed::INT        AS is_installed,
         st.value:is_renting::INT          AS is_renting
  FROM citibike.raw.station_status_raw s,
       LATERAL FLATTEN(input => s.payload:data:stations) st
)
SELECT HOUR(ts_et)                                      AS hour_et,
       DAYOFWEEK(ts_et) IN (0, 6)                       AS is_weekend,
       COUNT(*)                                         AS observations,
       ROUND(100.0 * COUNT_IF(bikes = 0) / COUNT(*), 2) AS pct_no_bikes,
       ROUND(100.0 * COUNT_IF(docks = 0) / COUNT(*), 2) AS pct_no_docks
FROM obs
WHERE is_installed = 1 AND is_renting = 1
GROUP BY 1, 2
ORDER BY 2, 1;



--- dbt setup
SELECT CURRENT_USER();

ALTER USER <YOUR_USERNAME> SET RSA_PUBLIC_KEY = '<YOUR_RSA_PUBLIC_KEY>';


SELECT rebalancing_priority_rank AS new_rank,
       stockout_time_rank        AS old_rank,
       station_name, station_profile,
       ROUND(pct_time_stocked_out * 100, 1) AS pct_stocked_out,
       ROUND(bikes_moved_per_hour, 1)       AS bikes_per_hr,
       median_episode_minutes
FROM citibike.marts.dim_station
WHERE rebalancing_priority_rank <= 20
ORDER BY rebalancing_priority_rank;

