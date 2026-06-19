---
name: tfl-live-tube-tracker
description: >
  Deploy and run the TfL Live Tube Tracker demo end-to-end on Snowflake.
  Creates the database, loads reference data from TfL API, builds and pushes
  the SPCS ingestion container, creates the Dynamic Table for position
  interpolation, deploys the Streamlit-in-Snowflake dashboard, and starts
  the ingestion pipeline. Includes full teardown. Triggers: tfl demo, tube
  tracker, run tfl demo, deploy tube tracker, clean up tfl demo, teardown
  tfl demo.
tools:
  - SnowflakeSqlExecute
  - Bash
  - FileWrite
  - FileRead
  - AskUserQuestion
---

## When to use

- Deploy the TfL Live Tube Tracker demo from scratch
- Run the end-to-end pipeline: ingestion, enrichment, and visualisation
- Tear down the demo and clean up all Snowflake objects
- Troubleshoot issues with the ingestion or dashboard

## What this skill provides

A fully automated deployment of a real-time London Underground train tracker:

1. **Snowflake infrastructure** — database, tables, PIPE, compute pool, network rules, secrets
2. **Reference data** — station coordinates and line route polylines from TfL API
3. **SPCS ingestion** — container built and pushed, streaming via Snowpipe Streaming v2
4. **Dynamic Table** — interpolates train positions between stations in real time
5. **Streamlit dashboard** — live geographic map of all tube trains
6. **Scheduled ingestion** — Snowflake Task triggers the container every minute
7. **Full teardown** — clean removal of all objects

## Prerequisites

- Snowflake account with SPCS enabled (compute pools, image repositories)
- TfL API key (register free at https://api-portal.tfl.gov.uk/)
- Snowflake CLI (`snow`) installed locally
- Docker installed (for building the ingestion container)
- Python 3.11+ with `snowflake-snowpark-python`, `requests`, and `aiohttp`

## Important context

### Project location
The project source code is at: `/Users/jkinley/code/snowflake-tfl-live`
GitHub: https://github.com/jrkinley/snowflake-tfl-live

### Database
All objects are created in `TFL_DEMO.PUBLIC`.

### TfL API
- Endpoint: `https://api.tfl.gov.uk/Line/{line}/Arrivals`
- Returns ~150-500 arrival predictions per line
- All 11 tube lines: bakerloo, central, circle, district, hammersmith-city, jubilee, metropolitan, northern, piccadilly, victoria, waterloo-city
- `currentLocation` field provides train position as free text (e.g. "Between Stockwell and Oval", "At Victoria", "Approaching Green Park")

### Architecture

```
TfL API → SPCS Container (Python/aiohttp) → SSv2 PIPE → RAW_ARRIVALS
                                                              │
REF_STATIONS + REF_LINES ──────────────────────────┐         │
                                                   ▼         ▼
                                          Dynamic Table: TRAIN_POSITIONS
                                                   │
                                                   ▼
                                    Streamlit-in-Snowflake (pydeck map)
```

## Instructions

After determining the user's intent, follow the relevant path. For full deployment, execute all steps sequentially. For targeted actions, jump to the relevant step.

Ask the user for confirmation before executing any SQL that creates or replaces objects.

### Step 0 — Present capabilities and gather context

Present this overview to the user:

---

Here's what I can help you with for the TfL Live Tube Tracker:

**Deploy**
- Full end-to-end deployment from scratch
- Create Snowflake infrastructure (database, tables, SPCS objects)
- Load reference data (stations, routes, line colours)
- Build and push the ingestion container
- Deploy the Streamlit dashboard

**Operate**
- Start/stop the ingestion pipeline
- Manually trigger a single ingestion run
- Check pipeline health and data freshness

**Troubleshoot**
- Verify data is flowing into RAW_ARRIVALS
- Check Dynamic Table refresh status
- Debug position interpolation issues

**Teardown**
- Remove all Snowflake objects cleanly

---

Wait for the user to indicate what they want before proceeding.

### Step 1 — Create Snowflake infrastructure

Run `setup.sql` to create all required objects:

```bash
cd /Users/jkinley/code/snowflake-tfl-live
snow sql -f setup.sql
```

Then set the TfL API key:

```sql
ALTER SECRET TFL_DEMO.PUBLIC.TFL_API_KEY
    SET SECRET_STRING = '<TFL_API_KEY>';
```

Ask the user for their TfL API key if not already known.

Verify:
```sql
SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_CATALOG = 'TFL_DEMO';
```

### Step 2 — Load reference data

Reference data is loaded via a stored procedure. First, upload the Python source:

```bash
cd /Users/jkinley/code/snowflake-tfl-live
snow stage copy reference/load_reference_data.py @TFL_DEMO.PUBLIC.CODE_STAGE/reference/ --overwrite
```

Then create and call the procedure:

```sql
CREATE OR REPLACE PROCEDURE TFL_DEMO.PUBLIC.LOAD_REFERENCE_DATA()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python', 'requests')
    IMPORTS = ('@TFL_DEMO.PUBLIC.CODE_STAGE/reference/load_reference_data.py')
    EXTERNAL_ACCESS_INTEGRATIONS = (TFL_API_ACCESS)
    SECRETS = ('tfl_api_key' = TFL_DEMO.PUBLIC.TFL_API_KEY)
    HANDLER = 'load_reference_data.run'
    EXECUTE AS CALLER;

CALL TFL_DEMO.PUBLIC.LOAD_REFERENCE_DATA();
-- Expected output: "Reference data loaded: 384 stations, 66 routes, 11 lines"
```

Verify:
```sql
SELECT COUNT(*) AS station_count FROM TFL_DEMO.PUBLIC.REF_STATIONS;
-- Expected: ~384 rows

SELECT * FROM TFL_DEMO.PUBLIC.REF_LINES;
-- Expected: 11 rows (one per tube line)

SELECT LINE_ID, DIRECTION, LENGTH(COORDINATES) AS coord_length
FROM TFL_DEMO.PUBLIC.REF_LINE_ROUTES
LIMIT 5;
```

### Step 3 — Build and push the ingestion container

```bash
cd /Users/jkinley/code/snowflake-tfl-live/ingest

# Get image registry URL
REGISTRY=$(snow spcs image-registry url)
echo "Registry: $REGISTRY"

# Build for linux/amd64 (required for SPCS)
docker build --platform linux/amd64 -t tfl_ingest:latest .

# Tag
docker tag tfl_ingest:latest ${REGISTRY}/tfl_demo/public/images/tfl_ingest:latest

# Login using Snowflake CLI (username/password does NOT work)
snow spcs image-registry login

# Push
docker push ${REGISTRY}/tfl_demo/public/images/tfl_ingest:latest
```

Verify the image is in the repository:
```sql
SHOW IMAGES IN IMAGE REPOSITORY TFL_DEMO.PUBLIC.IMAGES;
```

### Step 4 — Create the Dynamic Table

Run the Dynamic Table creation from `pipeline.sql`:

```bash
cd /Users/jkinley/code/snowflake-tfl-live
snow sql -f pipeline.sql
```

Note: The Dynamic Table will show 0 rows until data flows into RAW_ARRIVALS.

Verify:
```sql
SHOW DYNAMIC TABLES LIKE 'TRAIN_POSITIONS' IN SCHEMA TFL_DEMO.PUBLIC;
```

### Step 5 — Test a single ingestion run

Before setting up the scheduled Task, test the ingestion locally:

```bash
cd /Users/jkinley/code/snowflake-tfl-live/ingest
pip install snowpipe-streaming aiohttp
export TFL_API_KEY=<key>
export SNOWFLAKE_ACCOUNT=<account_identifier>
export SNOWFLAKE_HOST=<account>.snowflakecomputing.com
python tfl_ingest.py
```

Or execute as an SPCS service:

```sql
EXECUTE SERVICE
    IN COMPUTE POOL TFL_POOL
    FROM SPECIFICATION $$
    spec:
      containers:
        - name: tfl-ingest
          image: /tfl_demo/public/images/tfl_ingest:latest
          env:
            TFL_API_KEY: "<key>"
            SNOWFLAKE_ACCOUNT: "<account>"
            SNOWFLAKE_HOST: "<account>.snowflakecomputing.com"
            SNOWFLAKE_ROLE: "SYSADMIN"
            SNOWFLAKE_DATABASE: "TFL_DEMO"
            SNOWFLAKE_SCHEMA: "PUBLIC"
            PIPE_NAME: "RAW_ARRIVALS_PIPE"
            CHANNEL_NAME: "tfl-ingest-channel"
          resources:
            requests:
              memory: 256M
              cpu: "0.5"
            limits:
              memory: 512M
              cpu: "1"
      endpoints: []
    capabilities:
      securityContext:
        enableCustomCredentials: true
    $$
    EXTERNAL_ACCESS_INTEGRATIONS = (TFL_API_ACCESS);
```

Verify data landed:
```sql
SELECT COUNT(*) FROM TFL_DEMO.PUBLIC.RAW_ARRIVALS;
-- Expected: ~3000-5000 rows per ingestion cycle

SELECT LINE_ID, COUNT(*) AS predictions
FROM TFL_DEMO.PUBLIC.RAW_ARRIVALS
GROUP BY LINE_ID
ORDER BY predictions DESC;
```

### Step 6 — Verify the Dynamic Table

After data lands in RAW_ARRIVALS, the Dynamic Table should refresh within ~1 minute:

```sql
-- Check train positions
SELECT COUNT(*) AS active_trains,
       COUNT(DISTINCT LINE_ID) AS lines_active
FROM TFL_DEMO.PUBLIC.TRAIN_POSITIONS;
-- Expected: ~100-200 active trains across 11 lines

-- Sample positioned trains
SELECT LINE_NAME, VEHICLE_ID, CURRENT_LOCATION, LOCATION_TYPE,
       LATITUDE, LONGITUDE, COLOUR_HEX
FROM TFL_DEMO.PUBLIC.TRAIN_POSITIONS
ORDER BY LINE_NAME, VEHICLE_ID
LIMIT 20;

-- Check for any trains without positions (debugging)
SELECT LOCATION_TYPE, COUNT(*) AS cnt
FROM TFL_DEMO.PUBLIC.TRAIN_POSITIONS
GROUP BY LOCATION_TYPE;
```

### Step 7 — Deploy the Streamlit dashboard

```bash
cd /Users/jkinley/code/snowflake-tfl-live/streamlit
snow streamlit deploy \
    --database TFL_DEMO \
    --schema PUBLIC \
    --warehouse COMPUTE_WH \
    --replace
```

Alternatively, deploy via SQL:
```sql
CREATE STAGE IF NOT EXISTS TFL_DEMO.PUBLIC.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Upload files (via snow stage copy or PUT)
PUT file:///Users/jkinley/code/snowflake-tfl-live/streamlit/streamlit_app.py
    @TFL_DEMO.PUBLIC.STREAMLIT_STAGE
    AUTO_COMPRESS = FALSE
    OVERWRITE = TRUE;

PUT file:///Users/jkinley/code/snowflake-tfl-live/streamlit/environment.yml
    @TFL_DEMO.PUBLIC.STREAMLIT_STAGE
    AUTO_COMPRESS = FALSE
    OVERWRITE = TRUE;

CREATE OR REPLACE STREAMLIT TFL_DEMO.PUBLIC.TUBE_TRACKER
    ROOT_LOCATION = '@TFL_DEMO.PUBLIC.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = COMPUTE_WH
    COMMENT = 'Live London Underground train tracker';
```

The dashboard URL will be shown in the output. It requires acknowledging External Offerings Terms for Mapbox tiles (one-time).

### Step 8 — Set up scheduled ingestion (Task)

```sql
CREATE OR REPLACE TASK TFL_DEMO.PUBLIC.TFL_INGEST_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
AS
    EXECUTE SERVICE
        IN COMPUTE POOL TFL_POOL
        FROM SPECIFICATION $$
        spec:
          containers:
            - name: tfl-ingest
              image: /tfl_demo/public/images/tfl_ingest:latest
              env:
                TFL_API_KEY: "<key>"
                SNOWFLAKE_ACCOUNT: "<account>"
                SNOWFLAKE_HOST: "<account>.snowflakecomputing.com"
                SNOWFLAKE_ROLE: "SYSADMIN"
                SNOWFLAKE_DATABASE: "TFL_DEMO"
                SNOWFLAKE_SCHEMA: "PUBLIC"
                PIPE_NAME: "RAW_ARRIVALS_PIPE"
                CHANNEL_NAME: "tfl-ingest-channel"
              resources:
                requests:
                  memory: 256M
                  cpu: "0.5"
                limits:
                  memory: 512M
                  cpu: "1"
          endpoints: []
        capabilities:
          securityContext:
            enableCustomCredentials: true
        $$
        EXTERNAL_ACCESS_INTEGRATIONS = (TFL_API_ACCESS);

ALTER TASK TFL_DEMO.PUBLIC.TFL_INGEST_TASK RESUME;
```

Verify task is running:
```sql
SHOW TASKS LIKE 'TFL_INGEST_TASK' IN SCHEMA TFL_DEMO.PUBLIC;

-- Check task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TFL_INGEST_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
```

### Step 9 — Verification summary

Run all verification queries:

```sql
-- Infrastructure
SHOW TABLES IN SCHEMA TFL_DEMO.PUBLIC;
SHOW DYNAMIC TABLES IN SCHEMA TFL_DEMO.PUBLIC;
SHOW PIPES IN SCHEMA TFL_DEMO.PUBLIC;
SHOW TASKS IN SCHEMA TFL_DEMO.PUBLIC;

-- Data health
SELECT 'RAW_ARRIVALS' AS table_name, COUNT(*) AS rows FROM TFL_DEMO.PUBLIC.RAW_ARRIVALS
UNION ALL
SELECT 'REF_STATIONS', COUNT(*) FROM TFL_DEMO.PUBLIC.REF_STATIONS
UNION ALL
SELECT 'REF_LINES', COUNT(*) FROM TFL_DEMO.PUBLIC.REF_LINES
UNION ALL
SELECT 'REF_LINE_ROUTES', COUNT(*) FROM TFL_DEMO.PUBLIC.REF_LINE_ROUTES
UNION ALL
SELECT 'TRAIN_POSITIONS', COUNT(*) FROM TFL_DEMO.PUBLIC.TRAIN_POSITIONS;

-- Freshness
SELECT MAX(TIMESTAMP_UTC) AS latest_ingestion,
       DATEDIFF(SECOND, MAX(TIMESTAMP_UTC), CURRENT_TIMESTAMP()) AS lag_seconds
FROM TFL_DEMO.PUBLIC.RAW_ARRIVALS;
```

### Step 10 — Teardown

To remove all demo objects:

```bash
cd /Users/jkinley/code/snowflake-tfl-live
snow sql -f teardown.sql
```

Or run manually:
```sql
-- Suspend and drop task
ALTER TASK IF EXISTS TFL_DEMO.PUBLIC.TFL_INGEST_TASK SUSPEND;
DROP TASK IF EXISTS TFL_DEMO.PUBLIC.TFL_INGEST_TASK;

-- Drop dynamic table
DROP DYNAMIC TABLE IF EXISTS TFL_DEMO.PUBLIC.TRAIN_POSITIONS;

-- Drop Streamlit
DROP STREAMLIT IF EXISTS TFL_DEMO.PUBLIC.TUBE_TRACKER;

-- Drop SPCS objects
DROP SERVICE IF EXISTS TFL_DEMO.PUBLIC.TFL_INGEST_SERVICE;
ALTER COMPUTE POOL IF EXISTS TFL_POOL STOP ALL;
DROP COMPUTE POOL IF EXISTS TFL_POOL;

-- Drop integrations
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS TFL_API_ACCESS;
DROP NETWORK RULE IF EXISTS TFL_DEMO.PUBLIC.TFL_API_RULE;
DROP SECRET IF EXISTS TFL_DEMO.PUBLIC.TFL_API_KEY;
DROP IMAGE REPOSITORY IF EXISTS TFL_DEMO.PUBLIC.IMAGES;

-- Drop pipe and tables
DROP PIPE IF EXISTS TFL_DEMO.PUBLIC.RAW_ARRIVALS_PIPE;
DROP STAGE IF EXISTS TFL_DEMO.PUBLIC.STREAMLIT_STAGE;
DROP TABLE IF EXISTS TFL_DEMO.PUBLIC.RAW_ARRIVALS;
DROP TABLE IF EXISTS TFL_DEMO.PUBLIC.REF_STATIONS;
DROP TABLE IF EXISTS TFL_DEMO.PUBLIC.REF_LINES;
DROP TABLE IF EXISTS TFL_DEMO.PUBLIC.REF_LINE_ROUTES;

-- Optionally drop the database entirely
-- DROP DATABASE IF EXISTS TFL_DEMO;
```

## Common issues

### Network policy blocks SSv2 from SPCS (error 390422)

If your account has an account-level network policy, the SSv2 ingest endpoint
(`*.ingest.lhrasq.snowflakecomputing.com`) will reject SPCS container requests with:
`"Incoming request with IP/Token 10.x.x.x is not allowed to access Snowflake"`

**Workarounds that do NOT work:**
- Adding `10.0.0.0/8` to the policy's allowed list
- Setting a permissive user-level network policy on the service owner user

**Working workaround:** Unset the account-level network policy:
```sql
ALTER ACCOUNT UNSET NETWORK_POLICY;
```

This is a known product gap — SPCS internal traffic to the SSv2 ingest endpoint is not
exempted from account-level network policies. Reported to the SSv2/SPCS team.

### SNOWFLAKE_HOST DNS resolution fails inside SPCS

Use the **regional locator format** for `SNOWFLAKE_HOST`, not the org-account format:
- Correct: `mr31655.eu-west-2.aws.snowflakecomputing.com`
- Wrong: `sfseeurope-jkinley_aws.snowflakecomputing.com` (underscore breaks DNS)

Find your locator: `SELECT CURRENT_ACCOUNT()` and check `SYSTEM$ALLOWLIST()` for the
`SNOWFLAKE_DEPLOYMENT` entry.

### Docker registry login fails with username/password

Use the Snowflake CLI instead:
```bash
snow spcs image-registry login
```

### EAI requires ALLOWED_AUTHENTICATION_SECRETS

When using secrets in stored procedures with an EAI, you must include:
```sql
CREATE EXTERNAL ACCESS INTEGRATION TFL_API_ACCESS
    ALLOWED_NETWORK_RULES = (TFL_API_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (TFL_API_KEY)  -- Required!
    ENABLED = TRUE;
```

Without this, `CREATE PROCEDURE ... SECRETS = (...)` will fail with
"Integrations do not allow secret".

### "Compute pool not ready"
The compute pool may take 1-2 minutes to start on first use. Check status:
```sql
DESCRIBE COMPUTE POOL TFL_POOL;
```

### "Network rule blocked" (TfL API egress)
Ensure the External Access Integration is enabled and the network rule allows `api.tfl.gov.uk:443`.

### "No data in TRAIN_POSITIONS"
1. Check RAW_ARRIVALS has data
2. Check Dynamic Table refresh status: `SHOW DYNAMIC TABLES LIKE 'TRAIN_POSITIONS'`
3. Check for station name matching issues (ILIKE joins may miss variations)

### "Map shows no trains"
1. Verify TRAIN_POSITIONS has rows with non-null LATITUDE/LONGITUDE
2. Check the Streamlit app can query the Dynamic Table
3. Ensure External Offerings Terms are acknowledged for Mapbox

### "TfL API rate limited"
The free tier allows 500 requests/minute. Fetching 11 lines = 11 requests per cycle.
With a 1-minute Task schedule this is well within limits.

## Notes

- The Dynamic Table uses a 180-second assumed inter-station journey time for interpolation. This is approximate but produces good visual results.
- Station name matching uses ILIKE with substring matching which may produce duplicates at interchanges. The QUALIFY clause handles deduplication.
- The Streamlit app auto-refreshes every 15 seconds by default. This can be adjusted in the sidebar.
- TfL operates reduced services overnight (approx 00:30-05:00). The map will show fewer/no trains during these hours.
