-- =============================================================================
-- TfL Live Tube Tracker — Dynamic Table & Task
-- =============================================================================
-- Run AFTER setup.sql and after reference data has been loaded.
-- These objects depend on data being present in RAW_ARRIVALS and REF_* tables.
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE TFL_DEMO;
USE SCHEMA PUBLIC;

-- -----------------------------------------------------------------------------
-- Dynamic Table: TRAIN_POSITIONS
-- Interpolates geographic position for each active train based on
-- currentLocation text from TfL and station reference coordinates.
-- Target lag: 1 minute (minimum for Dynamic Tables).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE TRAIN_POSITIONS
    TARGET_LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH
AS
WITH latest_per_vehicle AS (
    -- For each vehicle+line, keep only the prediction with the smallest
    -- timeToStation from the most recent ingestion batch.
    -- This gives us the train's "next stop" which is closest to its position.
    SELECT *
    FROM RAW_ARRIVALS
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY VEHICLE_ID, LINE_ID
        ORDER BY TIMESTAMP_UTC DESC, TIME_TO_STATION ASC
    ) = 1
),
parsed AS (
    SELECT
        a.VEHICLE_ID,
        a.LINE_ID,
        a.LINE_NAME,
        a.DIRECTION,
        a.DESTINATION_NAME,
        a.CURRENT_LOCATION,
        a.TIME_TO_STATION,
        a.STATION_NAME,
        a.STATION_NAPTAN_ID,
        a.TOWARDS,
        a.TIMESTAMP_UTC,
        -- Classify the location type
        CASE
            WHEN a.CURRENT_LOCATION LIKE 'At %' THEN 'AT_STATION'
            WHEN a.CURRENT_LOCATION LIKE 'Between %' THEN 'BETWEEN'
            WHEN a.CURRENT_LOCATION LIKE 'Approaching %' THEN 'APPROACHING'
            WHEN a.CURRENT_LOCATION LIKE 'Left %'
              OR a.CURRENT_LOCATION LIKE 'Departed %' THEN 'DEPARTED'
            ELSE 'OTHER'
        END AS LOCATION_TYPE,
        -- Extract station names from "Between {A} and {B}"
        REGEXP_SUBSTR(a.CURRENT_LOCATION,
            'Between (.+) and (.+)', 1, 1, 'e', 1) AS FROM_STATION_NAME,
        REGEXP_SUBSTR(a.CURRENT_LOCATION,
            'Between (.+) and (.+)', 1, 1, 'e', 2) AS TO_STATION_NAME,
        -- Extract station name from "At {station}"
        REGEXP_SUBSTR(a.CURRENT_LOCATION,
            '^At (.+)$', 1, 1, 'e', 1) AS AT_STATION_NAME,
        -- Extract station name from "Approaching {station}"
        REGEXP_SUBSTR(a.CURRENT_LOCATION,
            '^Approaching (.+)$', 1, 1, 'e', 1) AS APPROACHING_STATION_NAME
    FROM latest_per_vehicle a
),
positioned AS (
    SELECT
        p.*,
        -- Calculate latitude
        CASE
            WHEN p.LOCATION_TYPE = 'AT_STATION' THEN s_at.LATITUDE
            WHEN p.LOCATION_TYPE = 'APPROACHING' THEN
                COALESCE(s_approaching.LATITUDE, s_dest.LATITUDE)
            WHEN p.LOCATION_TYPE = 'BETWEEN' THEN
                -- Linear interpolation: assume ~180s between stations
                -- fraction = how far along (0 = at from_station, 1 = at to_station)
                s_from.LATITUDE + (
                    (LEAST(GREATEST(180.0 - p.TIME_TO_STATION, 0), 180.0) / 180.0)
                    * (s_to.LATITUDE - s_from.LATITUDE)
                )
            WHEN p.LOCATION_TYPE = 'DEPARTED' THEN s_dest.LATITUDE
            ELSE s_dest.LATITUDE
        END AS LATITUDE,
        -- Calculate longitude
        CASE
            WHEN p.LOCATION_TYPE = 'AT_STATION' THEN s_at.LONGITUDE
            WHEN p.LOCATION_TYPE = 'APPROACHING' THEN
                COALESCE(s_approaching.LONGITUDE, s_dest.LONGITUDE)
            WHEN p.LOCATION_TYPE = 'BETWEEN' THEN
                s_from.LONGITUDE + (
                    (LEAST(GREATEST(180.0 - p.TIME_TO_STATION, 0), 180.0) / 180.0)
                    * (s_to.LONGITUDE - s_from.LONGITUDE)
                )
            WHEN p.LOCATION_TYPE = 'DEPARTED' THEN s_dest.LONGITUDE
            ELSE s_dest.LONGITUDE
        END AS LONGITUDE
    FROM parsed p
    -- "At {station}" — match by name substring within the same line
    LEFT JOIN REF_STATIONS s_at
        ON s_at.STATION_NAME ILIKE '%' || p.AT_STATION_NAME || '%'
        AND s_at.LINE_ID = p.LINE_ID
    -- "Approaching {station}"
    LEFT JOIN REF_STATIONS s_approaching
        ON s_approaching.STATION_NAME ILIKE '%' || p.APPROACHING_STATION_NAME || '%'
        AND s_approaching.LINE_ID = p.LINE_ID
    -- Fallback: next station (the station this prediction is for)
    LEFT JOIN REF_STATIONS s_dest
        ON s_dest.NAPTAN_ID = p.STATION_NAPTAN_ID
        AND s_dest.LINE_ID = p.LINE_ID
    -- "Between A and B" — from station
    LEFT JOIN REF_STATIONS s_from
        ON s_from.STATION_NAME ILIKE '%' || p.FROM_STATION_NAME || '%'
        AND s_from.LINE_ID = p.LINE_ID
    -- "Between A and B" — to station
    LEFT JOIN REF_STATIONS s_to
        ON s_to.STATION_NAME ILIKE '%' || p.TO_STATION_NAME || '%'
        AND s_to.LINE_ID = p.LINE_ID
)
SELECT
    p.VEHICLE_ID,
    p.LINE_ID,
    p.LINE_NAME,
    p.DIRECTION,
    p.DESTINATION_NAME,
    p.CURRENT_LOCATION,
    p.LOCATION_TYPE,
    p.TIME_TO_STATION,
    p.STATION_NAME,
    p.TOWARDS,
    p.TIMESTAMP_UTC,
    p.LATITUDE,
    p.LONGITUDE,
    l.COLOUR_HEX,
    l.COLOUR_R,
    l.COLOUR_G,
    l.COLOUR_B
FROM positioned p
LEFT JOIN REF_LINES l ON l.LINE_ID = p.LINE_ID
WHERE p.LATITUDE IS NOT NULL
  AND p.LONGITUDE IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Task: Trigger ingestion every minute
-- NOTE: Replace <account> and TFL API key values in the spec before running.
-- -----------------------------------------------------------------------------
-- CREATE OR REPLACE TASK TFL_INGEST_TASK
--     WAREHOUSE = COMPUTE_WH
--     SCHEDULE = '1 MINUTE'
-- AS
--     EXECUTE SERVICE
--         IN COMPUTE POOL TFL_POOL
--         FROM SPECIFICATION $$
--         spec:
--           containers:
--             - name: tfl-ingest
--               image: /tfl_demo/public/images/tfl_ingest:latest
--               env:
--                 TFL_API_KEY: "<your-key>"
--                 SNOWFLAKE_ACCOUNT: "<your-account>"
--                 SNOWFLAKE_HOST: "<your-account>.snowflakecomputing.com"
--                 SNOWFLAKE_ROLE: "SYSADMIN"
--                 SNOWFLAKE_DATABASE: "TFL_DEMO"
--                 SNOWFLAKE_SCHEMA: "PUBLIC"
--                 PIPE_NAME: "RAW_ARRIVALS_PIPE"
--                 CHANNEL_NAME: "tfl-ingest-channel"
--               resources:
--                 requests:
--                   memory: 256M
--                   cpu: "0.5"
--                 limits:
--                   memory: 512M
--                   cpu: "1"
--           endpoints: []
--         capabilities:
--           securityContext:
--             enableCustomCredentials: true
--         $$
--         EXTERNAL_ACCESS_INTEGRATIONS = (TFL_API_ACCESS);
--
-- ALTER TASK TFL_INGEST_TASK RESUME;
