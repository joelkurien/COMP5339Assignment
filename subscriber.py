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
        #print(f"✅ Connected to MQTT broker {broker_host}:{broker_port}")
        client.subscribe(topic_facility)
    else:
        print(f"❌ Connection failed with code {rc}")

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

# Streamlit UI
st.set_page_config(page_title="MQTT Facility Dashboard", layout="wide")
st.title("⚡ Real-Time Facility Map (Live MQTT Data)")
st.caption(f"Broker: `{broker_host}` | Port: `{broker_port}` | Topic: `{topic_facility}`")

st_autorefresh(interval=3000, key="refresh_counter")

# Update buffer from Queue
#print(f"🧮 Queue size: {msg_queue.qsize()}")
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

# Build map once
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
            <b>{name}</b><br>
            Fuel: {fuel}<br>
            Timestamp: -<br>
            Power: N/A MW<br>
            Emissions: N/A tCO₂
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

# map area only render once
map_placeholder = st.empty()
fmap = st.session_state["map_obj"]
map_display = st_folium(fmap, width=550, height=325, returned_objects=["last_object_clicked"])

# Facility selection logic
if "selected_facility" not in st.session_state:
    st.session_state["selected_facility"] = None

if map_display and map_display.get("last_object_clicked"):
    lat_c, lon_c = map_display["last_object_clicked"]["lat"], map_display["last_object_clicked"]["lng"]
    distances = (FACILITIES["latitude"] - lat_c).abs() + (FACILITIES["longitude"] - lon_c).abs()
    if not distances.empty:
        new_selection = FACILITIES.loc[distances.idxmin(), "Facility Name"]
        st.session_state["selected_facility"] = new_selection

selected_fac = st.session_state["selected_facility"]

# Real-time metric display (update only numbers)
st.markdown("### 🔄 Live Data Monitor")

if selected_fac:
    st.write(f"**Facility:** {selected_fac}")

    metric_placeholder = st.empty()

    for _ in range(60):  # update each 3 secs for 3 minutes
        rec = next((v for k, v in st.session_state["fac_buffer"].items()
                    if k.strip().lower() == selected_fac.strip().lower()), None)

        if rec:
            ts = rec.get("Timestamp", "-")
            power = rec.get("Power(MW)", "N/A")
            emis = rec.get("Emissions(t)", "N/A")
            fuel = rec.get("Fuel Type", "Unknown")

            with metric_placeholder.container():
                col1, col2, col3 = st.columns(3)
                col1.metric("Timestamp", ts)
                col2.metric("Power (MW)", power)
                col3.metric("Emissions (tCO₂)", emis)
                st.caption(f"Fuel Type: {fuel}")
        else:
            metric_placeholder.info("Waiting for live MQTT messages...")

        time.sleep(3)

else:
    st.info("🕹️ Click a marker to start live monitoring.")