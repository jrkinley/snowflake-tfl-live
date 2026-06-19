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
import tempfile
import uuid
from datetime import datetime, timezone

import aiohttp
from snowflake.ingest.streaming import StreamingIngestClient

os.environ["SS_LOG_LEVEL"] = "info"

# Configuration
TFL_API_KEY = os.environ.get("TFL_API_KEY", "")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_HOST = os.environ.get("SNOWFLAKE_HOST", "")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "TFL_DEMO")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
PIPE_NAME = os.environ.get("PIPE_NAME", "RAW_ARRIVALS_PIPE")

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


def write_streaming_profile() -> str:
    """Write an SSv2 SDK profile.json using SPCS workload-identity token auth."""
    profile = {
        "authorization_type": "SPCS",
        "url": f"https://{SNOWFLAKE_HOST}",
        "account": SNOWFLAKE_ACCOUNT,
        "role": SNOWFLAKE_ROLE,
        "spcs_token_path": "/snowflake/session/token",
    }
    profile_path = os.path.join(tempfile.gettempdir(), "profile.json")
    with open(profile_path, "w") as f:
        json.dump(profile, f)
    return profile_path


async def fetch_arrivals(
    session: aiohttp.ClientSession, line_id: str
) -> list[dict]:
    """Fetch arrival predictions for a single tube line."""
    url = f"{TFL_BASE_URL}/Line/{line_id}/Arrivals"
    params = {"app_key": TFL_API_KEY} if TFL_API_KEY else {}
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
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

    rows = [transform_prediction(p, ingestion_id, timestamp) for p in predictions]

    # Stream via SSv2
    profile_path = write_streaming_profile()
    client_name = f"TFL_INGEST_{uuid.uuid4().hex[:8]}"

    print(f"  Streaming {len(rows)} rows via SSv2...")

    with StreamingIngestClient(
        client_name=client_name,
        db_name=SNOWFLAKE_DATABASE,
        schema_name=SNOWFLAKE_SCHEMA,
        pipe_name=PIPE_NAME,
        profile_json=profile_path,
    ) as client:
        channel_name = f"tfl-channel-{uuid.uuid4().hex[:8]}"
        with client.open_channel(channel_name)[0] as channel:
            for i, row in enumerate(rows):
                channel.append_row(row, str(i + 1))

            print(f"  All {len(rows)} rows submitted. Waiting for commit...")

            total = len(rows)

            def all_committed(token):
                return token is not None and int(token) >= total

            channel.wait_for_commit(all_committed, timeout_seconds=60)

            status = channel.get_channel_status()
            print(f"  Ingestion complete!")
            print(f"    Committed offset: {status.latest_committed_offset_token}")
            print(f"    Rows inserted:    {status.rows_inserted_count}")
            print(f"    Rows errored:     {status.rows_error_count}")

            if status.rows_error_count > 0:
                print(f"    Last error: {status.last_error_message}")
                sys.exit(1)

    print(f"  Done. Ingested {len(rows)} rows successfully.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)
