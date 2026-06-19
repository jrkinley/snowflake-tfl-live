"""
Load reference data from TfL API into Snowflake.

Fetches station coordinates, line route polylines, and loads line colour metadata.
Run this once (or on-demand) to populate REF_STATIONS, REF_LINES, REF_LINE_ROUTES.

Usage:
    pip install snowflake-snowpark-python requests
    python load_reference_data.py
"""

import json
import os
from pathlib import Path

import requests
from snowflake.snowpark import Session
from snowflake.snowpark.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    VariantType,
)

TFL_API_KEY = os.environ.get("TFL_API_KEY", "")
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

DATABASE = "TFL_DEMO"
SCHEMA = "PUBLIC"


def get_session() -> Session:
    """Create Snowpark session from environment or default connection."""
    return Session.builder.configs({"connection_name": "default"}).create()


def fetch_json(path: str) -> dict | list:
    """Fetch JSON from TfL API."""
    url = f"{TFL_BASE_URL}{path}"
    params = {"app_key": TFL_API_KEY} if TFL_API_KEY else {}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def load_stations(session: Session) -> int:
    """Fetch station data for all tube lines and load into REF_STATIONS."""
    rows = []
    for line_id in TUBE_LINES:
        print(f"  Fetching stations for {line_id}...")
        stops = fetch_json(f"/Line/{line_id}/StopPoints")
        for stop in stops:
            rows.append({
                "NAPTAN_ID": stop["naptanId"],
                "STATION_NAME": stop["commonName"],
                "LINE_ID": line_id,
                "LATITUDE": stop["lat"],
                "LONGITUDE": stop["lon"],
            })

    schema = StructType([
        StructField("NAPTAN_ID", StringType()),
        StructField("STATION_NAME", StringType()),
        StructField("LINE_ID", StringType()),
        StructField("LATITUDE", FloatType()),
        StructField("LONGITUDE", FloatType()),
    ])

    df = session.create_dataframe(rows, schema=schema)
    df.write.mode("overwrite").save_as_table(f"{DATABASE}.{SCHEMA}.REF_STATIONS")
    print(f"  Loaded {len(rows)} station records.")
    return len(rows)


def load_line_routes(session: Session) -> int:
    """Fetch route polylines for all tube lines and load into REF_LINE_ROUTES."""
    rows = []
    for line_id in TUBE_LINES:
        for direction in ["inbound", "outbound"]:
            print(f"  Fetching route for {line_id} ({direction})...")
            try:
                data = fetch_json(
                    f"/Line/{line_id}/Route/Sequence/{direction}"
                    "?serviceTypes=Regular"
                )
                for line_string in data.get("lineStrings", []):
                    coords = json.loads(line_string) if isinstance(line_string, str) else line_string
                    rows.append({
                        "LINE_ID": line_id,
                        "DIRECTION": direction,
                        "COORDINATES": json.dumps(coords),
                    })
            except requests.HTTPError as e:
                print(f"    Warning: {e}")
                continue

    schema = StructType([
        StructField("LINE_ID", StringType()),
        StructField("DIRECTION", StringType()),
        StructField("COORDINATES", StringType()),
    ])

    df = session.create_dataframe(rows, schema=schema)

    # Write as variant by using parse_json in a view, or just store as string
    # and parse in Streamlit. For simplicity, store raw JSON string.
    df.write.mode("overwrite").save_as_table(f"{DATABASE}.{SCHEMA}.REF_LINE_ROUTES")
    print(f"  Loaded {len(rows)} route records.")
    return len(rows)


def load_line_colours(session: Session) -> int:
    """Load line colour metadata from local JSON file into REF_LINES."""
    colours_path = Path(__file__).parent / "line_colours.json"
    with open(colours_path) as f:
        colours = json.load(f)

    rows = []
    for line_id, info in colours.items():
        rows.append({
            "LINE_ID": line_id,
            "LINE_NAME": info["name"],
            "COLOUR_HEX": info["hex"],
            "COLOUR_R": info["r"],
            "COLOUR_G": info["g"],
            "COLOUR_B": info["b"],
        })

    schema = StructType([
        StructField("LINE_ID", StringType()),
        StructField("LINE_NAME", StringType()),
        StructField("COLOUR_HEX", StringType()),
        StructField("COLOUR_R", IntegerType()),
        StructField("COLOUR_G", IntegerType()),
        StructField("COLOUR_B", IntegerType()),
    ])

    df = session.create_dataframe(rows, schema=schema)
    df.write.mode("overwrite").save_as_table(f"{DATABASE}.{SCHEMA}.REF_LINES")
    print(f"  Loaded {len(rows)} line colour records.")
    return len(rows)


def main():
    print("Connecting to Snowflake...")
    session = get_session()
    session.sql(f"USE DATABASE {DATABASE}").collect()
    session.sql(f"USE SCHEMA {SCHEMA}").collect()

    print("\n[1/3] Loading station coordinates...")
    load_stations(session)

    print("\n[2/3] Loading line route polylines...")
    load_line_routes(session)

    print("\n[3/3] Loading line colours...")
    load_line_colours(session)

    print("\nDone! Reference data loaded successfully.")
    session.close()


if __name__ == "__main__":
    main()
