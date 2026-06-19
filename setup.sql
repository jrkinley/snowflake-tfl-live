-- =============================================================================
-- TfL Live Tube Tracker — Snowflake Setup
-- =============================================================================
-- Run this script to create all required objects.
-- Prerequisites: SYSADMIN (or equivalent) role with CREATE DATABASE privilege.
-- =============================================================================

USE ROLE SYSADMIN;

-- -----------------------------------------------------------------------------
-- Database and schema
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS TFL_DEMO;
USE DATABASE TFL_DEMO;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- -----------------------------------------------------------------------------
-- Raw landing table (Snowpipe Streaming target)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ARRIVALS (
    VEHICLE_ID              VARCHAR,
    LINE_ID                 VARCHAR,
    LINE_NAME               VARCHAR,
    STATION_NAPTAN_ID       VARCHAR,
    STATION_NAME            VARCHAR,
    PLATFORM_NAME           VARCHAR,
    DIRECTION               VARCHAR,
    DESTINATION_NAME        VARCHAR,
    DESTINATION_NAPTAN_ID   VARCHAR,
    CURRENT_LOCATION        VARCHAR,
    TOWARDS                 VARCHAR,
    TIME_TO_STATION         INTEGER,
    EXPECTED_ARRIVAL        TIMESTAMP_NTZ,
    TIMESTAMP_UTC           TIMESTAMP_NTZ,
    INGESTION_ID            VARCHAR
);

-- -----------------------------------------------------------------------------
-- Pipe for Snowpipe Streaming v2 (high-performance architecture)
-- -----------------------------------------------------------------------------
CREATE PIPE IF NOT EXISTS RAW_ARRIVALS_PIPE
AS COPY INTO RAW_ARRIVALS
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- -----------------------------------------------------------------------------
-- Reference tables
-- -----------------------------------------------------------------------------

-- Station coordinates (one row per station per line for interchange disambiguation)
CREATE TABLE IF NOT EXISTS REF_STATIONS (
    NAPTAN_ID       VARCHAR,
    STATION_NAME    VARCHAR,
    LINE_ID         VARCHAR,
    LATITUDE        FLOAT,
    LONGITUDE       FLOAT
);

-- Line metadata and colours
CREATE TABLE IF NOT EXISTS REF_LINES (
    LINE_ID     VARCHAR,
    LINE_NAME   VARCHAR,
    COLOUR_HEX  VARCHAR,
    COLOUR_R    INTEGER,
    COLOUR_G    INTEGER,
    COLOUR_B    INTEGER
);

-- Line route polylines for map rendering
CREATE TABLE IF NOT EXISTS REF_LINE_ROUTES (
    LINE_ID         VARCHAR,
    DIRECTION       VARCHAR,
    COORDINATES     VARIANT
);

-- -----------------------------------------------------------------------------
-- SPCS infrastructure
-- -----------------------------------------------------------------------------

-- Compute pool (XS is sufficient for API polling)
CREATE COMPUTE POOL IF NOT EXISTS TFL_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS;

-- Image repository
CREATE IMAGE REPOSITORY IF NOT EXISTS TFL_DEMO.PUBLIC.IMAGES;

-- Network rule for TfL API egress
CREATE NETWORK RULE IF NOT EXISTS TFL_API_RULE
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = ('api.tfl.gov.uk:443');

-- External access integration
CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS TFL_API_ACCESS
    ALLOWED_NETWORK_RULES = (TFL_DEMO.PUBLIC.TFL_API_RULE)
    ENABLED = TRUE;

-- Secret for TfL API key (replace with your actual key)
CREATE SECRET IF NOT EXISTS TFL_API_KEY
    TYPE = GENERIC_STRING
    SECRET_STRING = '<REPLACE_WITH_YOUR_TFL_API_KEY>';

-- -----------------------------------------------------------------------------
-- Streamlit stage
-- -----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- -----------------------------------------------------------------------------
-- Grants (adjust role as needed)
-- -----------------------------------------------------------------------------
GRANT USAGE ON DATABASE TFL_DEMO TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA TFL_DEMO.PUBLIC TO ROLE SYSADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA TFL_DEMO.PUBLIC TO ROLE SYSADMIN;
GRANT ALL ON ALL PIPES IN SCHEMA TFL_DEMO.PUBLIC TO ROLE SYSADMIN;
