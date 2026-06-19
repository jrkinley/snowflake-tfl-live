"""
Load reference data from TfL API into Snowflake.

Deployed as a stored procedure: CALL TFL_DEMO.PUBLIC.LOAD_REFERENCE_DATA();
Uses External Access Integration TFL_API_ACCESS for egress to api.tfl.gov.uk.

Can also be run locally:
    pip install snowflake-snowpark-python requests
    export TFL_API_KEY=<key>
    python load_reference_data.py
"""

import json
import os

import _snowflake
import requests
from snowflake.snowpark import Session

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

LINE_COLOURS = {
    "bakerloo":         {"name": "Bakerloo",           "hex": "#B36305", "r": 179, "g": 99,  "b": 5},
    "central":          {"name": "Central",            "hex": "#E32017", "r": 227, "g": 32,  "b": 23},
    "circle":           {"name": "Circle",             "hex": "#FFD300", "r": 255, "g": 211, "b": 0},
    "district":         {"name": "District",           "hex": "#00782A", "r": 0,   "g": 120, "b": 42},
    "hammersmith-city":  {"name": "Hammersmith & City", "hex": "#F3A9BB", "r": 243, "g": 169, "b": 187},
    "jubilee":          {"name": "Jubilee",            "hex": "#A0A5A9", "r": 160, "g": 165, "b": 169},
    "metropolitan":     {"name": "Metropolitan",       "hex": "#9B0056", "r": 155, "g": 0,   "b": 86},
    "northern":         {"name": "Northern",           "hex": "#000000", "r": 0,   "g": 0,   "b": 0},
    "piccadilly":       {"name": "Piccadilly",         "hex": "#003688", "r": 0,   "g": 54,  "b": 136},
    "victoria":         {"name": "Victoria",           "hex": "#0098D4", "r": 0,   "g": 152, "b": 212},
    "waterloo-city":    {"name": "Waterloo & City",    "hex": "#95CDBA", "r": 149, "g": 205, "b": 186},
}


def get_api_key() -> str:
    """Get TfL API key from Snowflake secret or environment."""
    try:
        return _snowflake.get_generic_secret_string("tfl_api_key")
    except Exception:
        return os.environ.get("TFL_API_KEY", "")


def fetch_json(path: str, api_key: str) -> dict | list:
    """Fetch JSON from TfL API."""
    url = f"{TFL_BASE_URL}{path}"
    params = {"app_key": api_key} if api_key else {}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def load_stations(session: Session, api_key: str) -> int:
    """Fetch station data for all tube lines and load into REF_STATIONS."""
    rows = []
    for line_id in TUBE_LINES:
        stops = fetch_json(f"/Line/{line_id}/StopPoints", api_key)
        for stop in stops:
            rows.append([
                stop["naptanId"],
                stop["commonName"],
                line_id,
                float(stop["lat"]),
                float(stop["lon"]),
            ])

    df = session.create_dataframe(
        rows,
        schema=["NAPTAN_ID", "STATION_NAME", "LINE_ID", "LATITUDE", "LONGITUDE"],
    )
    df.write.mode("overwrite").save_as_table("REF_STATIONS")
    return len(rows)


def load_line_routes(session: Session, api_key: str) -> int:
    """Fetch route polylines for all tube lines and load into REF_LINE_ROUTES."""
    rows = []
    for line_id in TUBE_LINES:
        for direction in ["inbound", "outbound"]:
            try:
                data = fetch_json(
                    f"/Line/{line_id}/Route/Sequence/{direction}"
                    "?serviceTypes=Regular",
                    api_key,
                )
                for line_string in data.get("lineStrings", []):
                    coords = (
                        json.loads(line_string)
                        if isinstance(line_string, str)
                        else line_string
                    )
                    rows.append([line_id, direction, json.dumps(coords)])
            except requests.HTTPError:
                continue

    df = session.create_dataframe(
        rows, schema=["LINE_ID", "DIRECTION", "COORDINATES"]
    )
    df.write.mode("overwrite").save_as_table("REF_LINE_ROUTES")
    return len(rows)


def load_line_colours(session: Session) -> int:
    """Load line colour metadata into REF_LINES."""
    rows = []
    for line_id, info in LINE_COLOURS.items():
        rows.append([
            line_id,
            info["name"],
            info["hex"],
            info["r"],
            info["g"],
            info["b"],
        ])

    df = session.create_dataframe(
        rows,
        schema=["LINE_ID", "LINE_NAME", "COLOUR_HEX", "COLOUR_R", "COLOUR_G", "COLOUR_B"],
    )
    df.write.mode("overwrite").save_as_table("REF_LINES")
    return len(rows)


def run(session: Session) -> str:
    """Main entry point for the stored procedure."""
    api_key = get_api_key()

    station_count = load_stations(session, api_key)
    route_count = load_line_routes(session, api_key)
    line_count = load_line_colours(session)

    return (
        f"Reference data loaded: "
        f"{station_count} stations, "
        f"{route_count} routes, "
        f"{line_count} lines"
    )


# Allow local execution
if __name__ == "__main__":
    session = Session.builder.configs({"connection_name": "default"}).create()
    session.sql("USE DATABASE TFL_DEMO").collect()
    session.sql("USE SCHEMA PUBLIC").collect()
    print(run(session))
    session.close()
