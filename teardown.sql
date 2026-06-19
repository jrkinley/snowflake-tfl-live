-- =============================================================================
-- TfL Live Tube Tracker — Teardown
-- =============================================================================
-- Run this script to cleanly remove all objects created by setup.sql.
-- =============================================================================

USE ROLE SYSADMIN;

-- Suspend task first
ALTER TASK IF EXISTS TFL_DEMO.PUBLIC.TFL_INGEST_TASK SUSPEND;

-- Drop task
DROP TASK IF EXISTS TFL_DEMO.PUBLIC.TFL_INGEST_TASK;

-- Drop dynamic table
DROP DYNAMIC TABLE IF EXISTS TFL_DEMO.PUBLIC.TRAIN_POSITIONS;

-- Drop Streamlit app
DROP STREAMLIT IF EXISTS TFL_DEMO.PUBLIC.TUBE_TRACKER;

-- Drop SPCS objects
DROP SERVICE IF EXISTS TFL_DEMO.PUBLIC.TFL_INGEST_SERVICE;
ALTER COMPUTE POOL IF EXISTS TFL_POOL STOP ALL;
DROP COMPUTE POOL IF EXISTS TFL_POOL;

-- Drop integrations and network rules
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS TFL_API_ACCESS;
DROP NETWORK RULE IF EXISTS TFL_DEMO.PUBLIC.TFL_API_RULE;

-- Drop secret
DROP SECRET IF EXISTS TFL_DEMO.PUBLIC.TFL_API_KEY;

-- Drop image repository
DROP IMAGE REPOSITORY IF EXISTS TFL_DEMO.PUBLIC.IMAGES;

-- Drop stage
DROP STAGE IF EXISTS TFL_DEMO.PUBLIC.STREAMLIT_STAGE;

-- Drop pipe
DROP PIPE IF EXISTS TFL_DEMO.PUBLIC.RAW_ARRIVALS_PIPE;

-- Drop tables
DROP TABLE IF EXISTS TFL_DEMO.PUBLIC.RAW_ARRIVALS;
DROP TABLE IF EXISTS TFL_DEMO.PUBLIC.REF_STATIONS;
DROP TABLE IF EXISTS TFL_DEMO.PUBLIC.REF_LINES;
DROP TABLE IF EXISTS TFL_DEMO.PUBLIC.REF_LINE_ROUTES;

-- Drop database (uncomment if you want full cleanup)
-- DROP DATABASE IF EXISTS TFL_DEMO;
