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
- Python 3.11+ with `snowflake-snowpark-python` and `requests`

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

```bash
cd reference
export TFL_API_KEY=<your-key>
python load_reference_data.py
```

This fetches station coordinates and line route polylines from TfL and loads them
into `REF_STATIONS`, `REF_LINES`, and `REF_LINE_ROUTES`.

### 3. Build and push the ingestion container

```bash
cd ingest

# Get your image registry URL
snow spcs image-registry url
# e.g. org-account.registry.snowflakecomputing.com

docker build --platform linux/amd64 -t tfl_ingest:latest .
docker tag tfl_ingest:latest <registry>/tfl_demo/public/images/tfl_ingest:latest
docker login <registry>
docker push <registry>/tfl_demo/public/images/tfl_ingest:latest
```

### 4. Create the Dynamic Table and Task

Edit `pipeline.sql` to fill in your account details, then:
```bash
snow sql -f pipeline.sql
```

### 5. Deploy the Streamlit dashboard

```bash
cd streamlit
snow streamlit deploy \
    --database TFL_DEMO \
    --schema PUBLIC \
    --warehouse COMPUTE_WH \
    --replace
```

Or create manually:
```sql
CREATE STAGE IF NOT EXISTS TFL_DEMO.PUBLIC.STREAMLIT_STAGE DIRECTORY = (ENABLE = TRUE);
-- Upload files to stage, then:
CREATE OR REPLACE STREAMLIT TFL_DEMO.PUBLIC.TUBE_TRACKER
    ROOT_LOCATION = '@TFL_DEMO.PUBLIC.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = COMPUTE_WH;
```

### 6. Start the ingestion task

Uncomment the task in `pipeline.sql` and run it, or execute manually:
```sql
EXECUTE SERVICE
    IN COMPUTE POOL TFL_POOL
    FROM SPECIFICATION $$ ... $$
    EXTERNAL_ACCESS_INTEGRATIONS = (TFL_API_ACCESS);
```

## Verification

```sql
-- Check raw data is flowing
SELECT COUNT(*) FROM TFL_DEMO.PUBLIC.RAW_ARRIVALS;

-- Check dynamic table has positioned trains
SELECT COUNT(*), COUNT(DISTINCT VEHICLE_ID)
FROM TFL_DEMO.PUBLIC.TRAIN_POSITIONS;

-- Sample positioned trains
SELECT LINE_NAME, VEHICLE_ID, CURRENT_LOCATION, LATITUDE, LONGITUDE
FROM TFL_DEMO.PUBLIC.TRAIN_POSITIONS
LIMIT 20;
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
├── ingest/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── spec.yaml          SPCS service specification
│   └── tfl_ingest.py      Ingestion script (TfL → SSv2)
├── reference/
│   ├── line_colours.json  Static line colour metadata
│   └── load_reference_data.py  One-time reference data loader
└── streamlit/
    ├── environment.yml    Snowflake Anaconda dependencies
    └── streamlit_app.py   Dashboard with pydeck map
```

## Future work

- Semantic model over `TRAIN_POSITIONS` for Cortex Analyst
- Cortex Agent for natural language tube queries and journey planning
- Historical analytics (delay patterns, service reliability)
