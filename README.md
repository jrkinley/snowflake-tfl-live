# snowflake-tfl-live

Live London Underground train tracker built on Snowflake.

Polls the TfL Unified API for real-time arrival predictions, streams them into Snowflake
via Snowpipe Streaming v2, computes interpolated train positions with a Dynamic Table,
and renders a live geographic map in Streamlit-in-Snowflake.

## Architecture

```
TfL Unified API (all 11 tube lines)
        │
        ▼  (fetched every ~60s)
┌──────────────────────────────┐
│  SPCS Container (Python)     │
│  - aiohttp concurrent fetch  │
│  - snowpipe-streaming SDK    │
└──────────────┬───────────────┘
               │  Snowpipe Streaming v2 (PIPE → RAW_ARRIVALS)
               ▼
┌──────────────────────────────────────────────────────┐
│  Snowflake: TFL_DEMO.PUBLIC                          │
│                                                      │
│  RAW_ARRIVALS ──┐                                    │
│                 ├──▶ Dynamic Table: TRAIN_POSITIONS   │
│  REF_STATIONS ──┘    (interpolated lat/lng)          │
│  REF_LINES ─────────────────────┐                    │
│  REF_LINE_ROUTES ───────────────┤                    │
└─────────────────────────────────┼────────────────────┘
                                  │
                                  ▼
                    Streamlit-in-Snowflake
                    (st.pydeck_chart — PathLayer + ScatterplotLayer)
```

## Prerequisites

- Snowflake account with SPCS enabled
- TfL API key (register at https://api-portal.tfl.gov.uk/)
- Snowflake CLI (`snow`) installed locally
- Docker (for building the ingestion container)
- No account-level network policy blocking SPCS internal IPs (see Known Issues below)

## Setup

### 1. Create Snowflake objects

```bash
snow sql -f setup.sql
```

Update the TfL API key secret:
```sql
ALTER SECRET TFL_DEMO.PUBLIC.TFL_API_KEY SET SECRET_STRING = '<your-key>';
```

### 2. Load reference data

Reference data is loaded via a stored procedure that fetches from TfL API:

```sql
-- First, upload the Python source to the code stage:
-- snow stage copy reference/load_reference_data.py @TFL_DEMO.PUBLIC.CODE_STAGE/reference/ --overwrite

-- Create the procedure (see setup.sql for full DDL)
-- Then call it:
CALL TFL_DEMO.PUBLIC.LOAD_REFERENCE_DATA();
```

This fetches station coordinates and line route polylines from TfL and loads them
into `REF_STATIONS` (384 stations), `REF_LINES` (11 lines), and `REF_LINE_ROUTES` (66 routes).

### 3. Build and push the ingestion container

```bash
cd ingest

# Get your image registry URL
snow spcs image-registry url
# e.g. org-account.registry.snowflakecomputing.com

# Build for linux/amd64 (required for SPCS)
docker build --platform linux/amd64 -t tfl_ingest:latest .
docker tag tfl_ingest:latest <registry>/tfl_demo/public/images/tfl_ingest:latest

# Login using Snowflake CLI (not username/password)
snow spcs image-registry login

# Push
docker push <registry>/tfl_demo/public/images/tfl_ingest:latest
```

### 4. Create the Dynamic Table

```bash
snow sql -f pipeline.sql
```

### 5. Run ingestion

```sql
EXECUTE JOB SERVICE
    IN COMPUTE POOL TFL_POOL
    NAME = TFL_INGEST_JOB
    EXTERNAL_ACCESS_INTEGRATIONS = (TFL_API_ACCESS)
    ASYNC = TRUE
    FROM SPECIFICATION $$
spec:
  containers:
    - name: tfl-ingest
      image: /tfl_demo/public/images/tfl_ingest:latest
      env:
        TFL_API_KEY: "<your-key>"
        SNOWFLAKE_ACCOUNT: "<org-account>"
        SNOWFLAKE_HOST: "<locator>.<region>.aws.snowflakecomputing.com"
        SNOWFLAKE_ROLE: "SYSADMIN"
        SNOWFLAKE_DATABASE: "TFL_DEMO"
        SNOWFLAKE_SCHEMA: "PUBLIC"
        PIPE_NAME: "RAW_ARRIVALS_PIPE"
      resources:
        requests:
          memory: 256M
          cpu: "0.5"
        limits:
          memory: 512M
          cpu: "1"
capabilities:
  securityContext:
    enableCustomCredentials: true
$$;
```

**Important:** Use the regional host format (e.g. `mr31655.eu-west-2.aws.snowflakecomputing.com`),
not the org-account format with underscores (which fails DNS resolution inside SPCS).

### 6. Deploy the Streamlit dashboard

```bash
cd streamlit
snow streamlit deploy \
    --database TFL_DEMO \
    --schema PUBLIC \
    --warehouse COMPUTE_WH \
    --replace
```

## Verification

```sql
-- Check raw data is flowing
SELECT COUNT(*), COUNT(DISTINCT INGESTION_ID) AS batches
FROM TFL_DEMO.PUBLIC.RAW_ARRIVALS;

-- Check dynamic table has positioned trains
SELECT COUNT(*), COUNT(DISTINCT LINE_ID) AS lines
FROM TFL_DEMO.PUBLIC.TRAIN_POSITIONS;

-- Sample positioned trains
SELECT LINE_NAME, VEHICLE_ID, CURRENT_LOCATION, LATITUDE, LONGITUDE
FROM TFL_DEMO.PUBLIC.TRAIN_POSITIONS
WHERE LINE_ID = 'victoria'
LIMIT 10;
```

## Teardown

```bash
snow sql -f teardown.sql
```

## Project structure

```
snowflake-tfl-live/
├── README.md
├── setup.sql              All Snowflake DDL (idempotent)
├── pipeline.sql           Dynamic Table + Task definitions
├── teardown.sql           Clean removal of all objects
├── SKILL.md               CoCo skill for end-to-end deployment
├── ingest/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── spec.yaml          SPCS service specification
│   └── tfl_ingest.py      Ingestion script (TfL → SSv2)
├── reference/
│   ├── line_colours.json  Static line colour metadata
│   └── load_reference_data.py  Stored procedure source
└── streamlit/
    ├── environment.yml    Snowflake Anaconda dependencies
    └── streamlit_app.py   Dashboard with pydeck map
```

## Known Issues

### Network policy blocks SSv2 from SPCS

If your account has an account-level network policy with an IP allowlist, the SSv2 ingest
endpoint (`*.ingest.lhrasq.snowflakecomputing.com`) will reject requests from SPCS containers
with error 390422 ("IP/Token not allowed to access Snowflake").

**Workarounds attempted (none work):**
- Adding `10.0.0.0/8` to the policy's allowed list
- Setting a permissive user-level network policy on the service owner

**Current workaround:** Unset the account-level network policy (`ALTER ACCOUNT UNSET NETWORK_POLICY`).

This has been reported to the SSv2/SPCS team for a proper fix.

### Docker registry login

Use `snow spcs image-registry login` for authentication — username/password login does not work.
The CLI handles token-based auth automatically.

### SNOWFLAKE_HOST format

Inside SPCS, use the regional locator format for `SNOWFLAKE_HOST`:
- Correct: `mr31655.eu-west-2.aws.snowflakecomputing.com`
- Wrong: `sfseeurope-jkinley_aws.snowflakecomputing.com` (underscore fails DNS)

Get your locator with: `SELECT CURRENT_ACCOUNT()` and check `SYSTEM$ALLOWLIST()` for the
`SNOWFLAKE_DEPLOYMENT` host entry.

### External Access Integration requires ALLOWED_AUTHENTICATION_SECRETS

When creating an EAI that will be used with stored procedures referencing secrets, you must
include `ALLOWED_AUTHENTICATION_SECRETS` in the integration definition:

```sql
CREATE EXTERNAL ACCESS INTEGRATION TFL_API_ACCESS
    ALLOWED_NETWORK_RULES = (TFL_API_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (TFL_API_KEY)  -- Required!
    ENABLED = TRUE;
```

## Future work

- Streamlit-in-Snowflake deployment (pydeck map)
- Scheduled Task for continuous ingestion (every 60s)
- Semantic model over `TRAIN_POSITIONS` for Cortex Analyst
- Cortex Agent for natural language tube queries and journey planning
- Historical analytics (delay patterns, service reliability)
