"""
TfL Live Tube Tracker — Streamlit Dashboard

Displays real-time London Underground train positions on a geographic map
using data from the TRAIN_POSITIONS dynamic table.

Deploy to Snowflake:
    snow streamlit deploy --replace
"""

import json
import time

import pydeck as pdk
import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide")

# -- Snowpark session --
session = get_active_session()

# -- Constants --
LONDON_CENTER = {"lat": 51.51, "lng": -0.12}
REFRESH_INTERVAL_SECONDS = 15

# -- Sidebar --
st.sidebar.title("London Tube Tracker")
st.sidebar.markdown("Real-time train positions from TfL")

# Line filter
all_lines = session.sql(
    "SELECT LINE_ID, LINE_NAME, COLOUR_HEX FROM TFL_DEMO.PUBLIC.REF_LINES ORDER BY LINE_NAME"
).to_pandas()

selected_lines = st.sidebar.multiselect(
    "Filter lines",
    options=all_lines["LINE_ID"].tolist(),
    default=all_lines["LINE_ID"].tolist(),
    format_func=lambda x: all_lines[all_lines["LINE_ID"] == x]["LINE_NAME"].iloc[0],
)

auto_refresh = st.sidebar.toggle("Auto-refresh", value=True)

# -- Fetch train positions --
if selected_lines:
    line_filter = ", ".join(f"'{l}'" for l in selected_lines)
    trains_df = session.sql(f"""
        SELECT
            VEHICLE_ID, LINE_ID, LINE_NAME, DIRECTION,
            DESTINATION_NAME, CURRENT_LOCATION, LOCATION_TYPE,
            TIME_TO_STATION, TOWARDS, TIMESTAMP_UTC,
            LATITUDE, LONGITUDE,
            COLOUR_HEX, COLOUR_R, COLOUR_G, COLOUR_B
        FROM TFL_DEMO.PUBLIC.TRAIN_POSITIONS
        WHERE LINE_ID IN ({line_filter})
    """).to_pandas()
else:
    trains_df = None

# -- Fetch route polylines --
if selected_lines:
    routes_df = session.sql(f"""
        SELECT LINE_ID, DIRECTION, COORDINATES
        FROM TFL_DEMO.PUBLIC.REF_LINE_ROUTES
        WHERE LINE_ID IN ({line_filter})
          AND DIRECTION = 'inbound'
    """).to_pandas()
else:
    routes_df = None

# -- Header stats --
col1, col2, col3 = st.columns(3)
if trains_df is not None and not trains_df.empty:
    col1.metric("Active Trains", len(trains_df))
    col2.metric("Lines Active", trains_df["LINE_ID"].nunique())
    last_update = trains_df["TIMESTAMP_UTC"].max()
    col3.metric("Last Update", str(last_update)[:19] if last_update else "N/A")
else:
    col1.metric("Active Trains", 0)
    col2.metric("Lines Active", 0)
    col3.metric("Last Update", "No data")

# -- Build pydeck layers --
layers = []

# Route paths layer
if routes_df is not None and not routes_df.empty:
    route_data = []
    for _, row in routes_df.iterrows():
        coords = row["COORDINATES"]
        if isinstance(coords, str):
            coords = json.loads(coords)

        # Get line colour
        line_info = all_lines[all_lines["LINE_ID"] == row["LINE_ID"]]
        if not line_info.empty:
            hex_colour = line_info.iloc[0]["COLOUR_HEX"]
            r = int(hex_colour[1:3], 16)
            g = int(hex_colour[3:5], 16)
            b = int(hex_colour[5:7], 16)
        else:
            r, g, b = 128, 128, 128

        # coords is [[lng, lat], ...] — pydeck PathLayer wants [[lng, lat], ...]
        if coords and isinstance(coords[0], list) and isinstance(coords[0][0], list):
            # Nested array (multiple segments)
            for segment in coords:
                route_data.append({"path": segment, "color": [r, g, b, 180]})
        else:
            route_data.append({"path": coords, "color": [r, g, b, 180]})

    if route_data:
        layers.append(
            pdk.Layer(
                "PathLayer",
                data=route_data,
                get_path="path",
                get_color="color",
                width_scale=4,
                width_min_pixels=2,
                pickable=False,
            )
        )

# Train positions layer
if trains_df is not None and not trains_df.empty:
    trains_df["color"] = trains_df.apply(
        lambda row: [int(row["COLOUR_R"]), int(row["COLOUR_G"]), int(row["COLOUR_B"]), 220],
        axis=1,
    )

    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=trains_df,
            get_position=["LONGITUDE", "LATITUDE"],
            get_color="color",
            get_radius=120,
            pickable=True,
        )
    )

# -- Render map --
view_state = pdk.ViewState(
    latitude=LONDON_CENTER["lat"],
    longitude=LONDON_CENTER["lng"],
    zoom=11,
    pitch=0,
)

deck = pdk.Deck(
    layers=layers,
    initial_view_state=view_state,
    tooltip={
        "text": "{LINE_NAME} → {TOWARDS}\n{CURRENT_LOCATION}\nVehicle: {VEHICLE_ID}"
    },
    map_style="mapbox://styles/mapbox/dark-v11",
)

st.pydeck_chart(deck, use_container_width=True)

# -- Per-line train count table --
if trains_df is not None and not trains_df.empty:
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Trains per line**")
    line_counts = (
        trains_df.groupby("LINE_NAME")
        .size()
        .reset_index(name="Trains")
        .sort_values("Trains", ascending=False)
    )
    st.sidebar.dataframe(line_counts, hide_index=True, use_container_width=True)

# -- Auto-refresh --
if auto_refresh:
    time.sleep(REFRESH_INTERVAL_SECONDS)
    st.rerun()
