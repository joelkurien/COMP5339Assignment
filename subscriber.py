import json
import pandas as pd
import streamlit as st
import folium
from streamlit_folium import st_folium
import paho.mqtt.client as mqtt
import numpy as np
from streamlit_autorefresh import st_autorefresh
from queue import Queue
import threading
import warnings
import time

warnings.filterwarnings("ignore", category=DeprecationWarning)

# Load facility coordinate data
@st.cache_data
def load_facilities(path="power_with_geo.csv"):
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    rename_map = {}
    for c in df.columns:
        if c.lower() == "facility name":
            rename_map[c] = "Facility Name"
        elif c.lower() == "latitude":
            rename_map[c] = "latitude"
        elif c.lower() == "longitude":
            rename_map[c] = "longitude"
    df.rename(columns=rename_map, inplace=True)
    df = df.drop_duplicates(subset=["Facility Name"])
    df = df.dropna(subset=["latitude", "longitude"])
    df["Fuel Type"] = df.get("Fuel Type", "Unknown")
    return df

# @st.cache_data
# def load_market(path="market_data.csv"):
#     df = pd.read_csv(path)
#     df.columns = [c.strip() for c in df.columns]
#     if "timestamp" in df.columns:
#         df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
#     return df

FACILITIES = load_facilities("power_with_geo.csv")

# Sidebar: MQTT Settings
st.sidebar.header("🔌 MQTT Connection Settings")
broker_host = st.sidebar.text_input("Broker Host", "test.mosquitto.org")
broker_port = st.sidebar.number_input("Port", 1883, step=1)
topic_facility = st.sidebar.text_input("Facility Topic", "facilities/metrics_info")
st.sidebar.divider()

# Global state
if "fac_buffer" not in st.session_state:
    st.session_state["fac_buffer"] = {}

msg_queue = Queue()

FUEL_COLOR = {
    "Coal": "black", "Gas": "orange", "Battery": "blue",
    "Solar": "yellow", "Wind": "green", "Hydro": "cyan",
    "Bioenergy": "brown", "Unknown": "gray"
}

# MQTT callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        #print(f"Connected to MQTT broker {broker_host}:{broker_port}")
        client.subscribe(topic_facility)
    else:
        print(f" Connection failed with code {rc}")

def on_message(client, userdata, msg):
    """Receive and push messages into queue"""
    try:
        #print("📩 Raw message received:", msg.payload[:200])
        payload = json.loads(msg.payload.decode())
        msg_queue.put(payload)
    except Exception as e:
        print("⚠️ MQTT decode error:", e)

# MQTT background thread
def start_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker_host, int(broker_port), 60)
    client.loop_forever()

threading.Thread(target=start_mqtt, daemon=True).start()

# Global state
if "msg_queue" not in st.session_state:
    from queue import Queue
    st.session_state["msg_queue"] = Queue()
if "fac_buffer" not in st.session_state:
    st.session_state["fac_buffer"] = {}
if "mqtt_started" not in st.session_state:
    threading.Thread(target=start_mqtt, daemon=True).start()
    st.session_state["mqtt_started"] = True

msg_queue = st.session_state["msg_queue"]
fac_buffer = st.session_state["fac_buffer"]

# Streamlit Layout (Left: Map + Facility metrics, Right: Market)
st.set_page_config(page_title="NEM Real-Time Dashboard", layout="wide")
st.title("⚡ NEM Real-Time Dashboard (Live MQTT Data)")

st_autorefresh(interval=3000, key="refresh_counter")

# MQTT data
while not msg_queue.empty():
    payload = msg_queue.get()
    records = payload.get("data", [])
    for rec in records:
        name = rec.get("Facility Name")
        if not name:
            continue
        ts = rec.get("Timestamp") or payload.get("timestamp")
        power = rec.get("Power(MW)") or rec.get("Power") or rec.get("POWER") or rec.get("Value")
        emis = rec.get("Emissions(t)") or rec.get("EMISSIONS") or None
        fuel = rec.get("Fuel Type", "Unknown")
        st.session_state["fac_buffer"][name] = {
            "Timestamp": ts,
            "Power(MW)": power,
            "Emissions(t)": emis,
            "Fuel Type": fuel
        }

fac_buffer = st.session_state["fac_buffer"]

left_col, right_col = st.columns([1, 1])

# map
with left_col:
    st.markdown("### Facility Map")

    if "map_obj" not in st.session_state:
        fmap = folium.Map(location=[-25, 134], zoom_start=4, tiles="CartoDB positron")
        for _, row in FACILITIES.iterrows():
            lat, lon = row["latitude"], row["longitude"]
            if np.isnan(lat) or np.isnan(lon):
                continue
            name = row["Facility Name"]
            fuel = row.get("Fuel Type", "Unknown")
            color = FUEL_COLOR.get(fuel, "gray")
            popup_html = f"""
                <div style="width:180px; font-size:13px; line-height:1.4;">
                    <b>{name}</b><br>
                    <b>Fuel Type:</b> {fuel}<br>
                    <b>Timestamp:</b> -<br>
                    <b>Power:</b> N/A MW<br>
                    <b>Emissions:</b> N/A tCO₂
                </div>
            """

            folium.CircleMarker(
                location=[lat, lon],
                radius=6,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.9,
                popup=popup_html,
                tooltip=f"{name}"
            ).add_to(fmap)
        st.session_state["map_obj"] = fmap

    fmap = st.session_state["map_obj"]
    map_display = st_folium(fmap, width=700, height=500, returned_objects=["last_object_clicked"])


    # click event
    if "selected_facility" not in st.session_state:
        st.session_state["selected_facility"] = None

    if map_display and map_display.get("last_object_clicked"):
        lat_c, lon_c = map_display["last_object_clicked"]["lat"], map_display["last_object_clicked"]["lng"]
        distances = (FACILITIES["latitude"] - lat_c).abs() + (FACILITIES["longitude"] - lon_c).abs()
        if not distances.empty:
            new_selection = FACILITIES.loc[distances.idxmin(), "Facility Name"]
            st.session_state["selected_facility"] = new_selection

    selected_fac = st.session_state["selected_facility"]

    # real time data
    st.markdown("### Live Facility Data")

    if selected_fac:
        metric_placeholder = st.empty()

        rec = next((v for k, v in st.session_state["fac_buffer"].items()
                    if k.strip().lower() == selected_fac.strip().lower()), None)

        if rec:
            ts = rec.get("Timestamp", "-")
            power = rec.get("Power(MW)", "N/A")
            emis = rec.get("Emissions(t)", "N/A")
            fuel = rec.get("Fuel Type", "Unknown")

            if isinstance(power, (int, float)):
                power = f"{float(power):,.2f}"
            if isinstance(emis, (int, float)):
                emis = f"{float(emis):,.3f}"

            with metric_placeholder.container():
                st.markdown(f"""
                <div style="padding:15px; border-radius:10px;
                            border:1px solid #ddd; width:90%;">
                    <h4 style="margin-bottom:0.3rem;">{selected_fac}</h4>
                    <p style="color:#666; margin-top:0;">Fuel Type: <b>{fuel}</b></p>
                    <hr style="margin:0.3rem 0;">
                    <p style="margin:0.3rem 0;">⏱ <b>Timestamp:</b> {ts}</p>
                    <p style="margin:0.3rem 0;"><b>Power:</b> {power} MW</p>
                    <p style="margin:0.3rem 0;"><b>Emissions:</b> {emis} tCO₂</p>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("Waiting for live MQTT messages...")

    else:
        st.info("🕹️ Click a marker on the map to start live monitoring.")

#  Market Data
with right_col:
    st.markdown("### Market Data (All Regions)")

    # load market_data.csv
    @st.cache_data
    def load_market(path="market_data.csv"):
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]

        time_cols = [c for c in df.columns if "time" in c.lower() or "date" in c.lower()]
        if time_cols:
            time_col = time_cols[0]
            df.rename(columns={time_col: "timestamp"}, inplace=True)
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        else:
            st.warning("⚠️ No timestamp-like column found in market_data.csv.")
            df["timestamp"] = pd.NaT

        return df

    market_df = load_market("market_data.csv")

    if selected_fac:
        rec = next((v for k, v in st.session_state["fac_buffer"].items()
                    if k.strip().lower() == selected_fac.strip().lower()), None)

        if rec and rec.get("Timestamp"):
            ts_str = rec.get("Timestamp")
            try:
                ts = pd.to_datetime(ts_str)
            except Exception:
                ts = pd.to_datetime(ts_str, errors="coerce")

            subset = market_df[
                market_df["timestamp"].between(ts - pd.Timedelta(minutes=5),
                                               ts + pd.Timedelta(minutes=5))
            ]
            if subset.empty:
                st.warning(f"No market data found near {ts_str}")
            else:
                closest_time = subset["timestamp"].iloc[(subset["timestamp"] - ts).abs().argsort()].iloc[0]
                same_time_data = market_df[market_df["timestamp"] == closest_time]

                st.markdown(f"**Timestamp:** {closest_time.strftime('%Y-%m-%d %H:%M')}")

                # show all region
                display_df = same_time_data[["region", "Price($/MWh)", "Demand(MW)"]].copy()
                display_df.columns = ["Region", "Price ($/MWh)", "Demand (MW)"]
                display_df = display_df.sort_values("Region")

                st.dataframe(display_df.style.format({
                    "Price ($/MWh)": "{:.2f}",
                    "Demand (MW)": "{:,.0f}"
                }), use_container_width=True)

                avg_price = display_df["Price ($/MWh)"].mean()
                total_demand = display_df["Demand (MW)"].sum()
                st.markdown(f"""
                **Average Price:** {avg_price:.2f} $/MWh  
                **Total Demand:** {total_demand:,.0f} MW
                """)
        else:
            st.info("Waiting for facility timestamp...")
    else:
        st.info("🕹️ Click a marker to start monitoring.")
