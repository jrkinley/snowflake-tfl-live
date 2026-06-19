"""
TfL Live Ingestion Service — SPCS Container.

Polls TfL Unified API for all tube line arrivals and streams predictions
into Snowflake via Snowpipe Streaming v2 (high-performance architecture).

Designed to run as a one-shot SPCS job triggered by a Snowflake Task.
"""

import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import aiohttp
from snowpipe_streaming import SnowpipeStreamingClient


# Configuration
TFL_API_KEY = os.environ.get("TFL_API_KEY", "")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "TFL_DEMO")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
PIPE_NAME = os.environ.get("PIPE_NAME", "RAW_ARRIVALS_PIPE")
CHANNEL_NAME = os.environ.get("CHANNEL_NAME", "tfl-ingest-channel")

TFL_BASE_URL = "https://api.tfl.gov.uk"

TUBE_LINES = [
    "bakerloo",
    "central",
    "circle",
    "district",
    "hammersmith-city",
    "jubilee",
    "metropolitan",
    "northern",
    "piccadilly",
    "victoria",
    "waterloo-city",
]


def get_snowflake_url() -> str:
    """Build Snowflake URL from account identifier."""
    host = os.environ.get("SNOWFLAKE_HOST", "")
    if host:
        return f"https://{host}"
    return f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"


async def fetch_arrivals(
    session: aiohttp.ClientSession, line_id: str
) -> list[dict]:
    """Fetch arrival predictions for a single tube line."""
    url = f"{TFL_BASE_URL}/Line/{line_id}/Arrivals"
    params = {}
    if TFL_API_KEY:
        params["app_key"] = TFL_API_KEY

    async with session.get(url, params=params, timeout=30) as resp:
        resp.raise_for_status()
        return await resp.json()


async def fetch_all_lines() -> list[dict]:
    """Fetch arrivals for all tube lines concurrently."""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_arrivals(session, line) for line in TUBE_LINES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_predictions = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"  Warning: failed to fetch {TUBE_LINES[i]}: {result}")
            continue
        all_predictions.extend(result)

    return all_predictions


def transform_prediction(prediction: dict, ingestion_id: str, timestamp: str) -> dict:
    """Transform a TfL prediction into a flat row for Snowflake."""
    expected = prediction.get("expectedArrival", "")
    return {
        "VEHICLE_ID": prediction.get("vehicleId", ""),
        "LINE_ID": prediction.get("lineId", ""),
        "LINE_NAME": prediction.get("lineName", ""),
        "STATION_NAPTAN_ID": prediction.get("naptanId", ""),
        "STATION_NAME": prediction.get("stationName", ""),
        "PLATFORM_NAME": prediction.get("platformName", ""),
        "DIRECTION": prediction.get("direction", ""),
        "DESTINATION_NAME": prediction.get("destinationName", ""),
        "DESTINATION_NAPTAN_ID": prediction.get("destinationNaptanId", ""),
        "CURRENT_LOCATION": prediction.get("currentLocation", ""),
        "TOWARDS": prediction.get("towards", ""),
        "TIME_TO_STATION": prediction.get("timeToStation", 0),
        "EXPECTED_ARRIVAL": expected.replace("Z", "") if expected else None,
        "TIMESTAMP_UTC": timestamp,
        "INGESTION_ID": ingestion_id,
    }


def create_streaming_client() -> SnowpipeStreamingClient:
    """Create SSv2 client using SPCS workload-identity auth."""
    return SnowpipeStreamingClient(
        account=SNOWFLAKE_ACCOUNT,
        url=get_snowflake_url(),
        role=SNOWFLAKE_ROLE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        pipe_name=PIPE_NAME,
        authorization_type="SPCS",
    )


async def main():
    ingestion_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    print(f"[{timestamp}] Starting TfL ingestion (id={ingestion_id[:8]})")
    print(f"  Fetching arrivals for {len(TUBE_LINES)} lines...")

    predictions = await fetch_all_lines()
    print(f"  Received {len(predictions)} predictions.")

    if not predictions:
        print("  No data to ingest. Exiting.")
        return

    rows = [
        transform_prediction(p, ingestion_id, timestamp) for p in predictions
    ]

    print(f"  Streaming {len(rows)} rows via SSv2...")
    client = create_streaming_client()
    channel = client.open_channel(CHANNEL_NAME)
    channel.append_rows(rows)
    client.close()

    print(f"  Done. Ingested {len(rows)} rows successfully.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)
