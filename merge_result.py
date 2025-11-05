import json
import pandas as pd
import streamlit as st
import folium
from streamlit_folium import st_folium
import paho.mqtt.client as mqtt
import numpy as np
from streamlit_autorefresh import st_autorefresh

# ======================================
# Load facility coordinate data
# ======================================
@st.cache_data
def load_facilities(path="power_with_geo.csv"):
    """Load all unique facilities with coordinates."""
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
    print(f"✅ Loaded {len(df)} unique facilities from {path}")
    return df

FACILITIES = load_facilities("power_with_geo.csv")

# ======================================
# Global state
# ======================================
fac_buffer = {}
mqtt_client = None

FUEL_COLOR = {
    "Coal": "black", "Gas": "orange", "Battery": "blue",
    "Solar": "yellow", "Wind": "green", "Hydro": "cyan",
    "Bioenergy": "brown", "Unknown": "gray"
}

# ======================================
# Sidebar MQTT setup
# ======================================
st.sidebar.header("🔌 MQTT Connection Settings")
broker_host = st.sidebar.text_input("Broker Host", "test.mosquitto.org")
broker_port = st.sidebar.number_input("Port", 1883, step=1)
topic_facility = st.sidebar.text_input("Facility Topic", "facilities/metrics_info")
st.sidebar.divider()

# ======================================
# MQTT callbacks
# ======================================
def on_connect(client, userdata, flags, rc, properties):
    """Callback when connected to MQTT broker."""
    if rc == 0:
        print(f"✅ Connected to MQTT broker {broker_host}:{broker_port}")
        client.subscribe(topic_facility)
        st.session_state["mqtt_status"] = f"Connected to {broker_host}:{broker_port}"
    else:
        print("❌ Connection failed:", rc)
        st.session_state["mqtt_status"] = f"Failed ({rc})"

fmap = folium.Map(location=[-25, 134], zoom_start=4, tiles="CartoDB positron")

def on_message(client, userdata, msg):
    """Receive live facility metrics from publisher."""
    global fac_buffer
    try:
        payload = json.loads(msg.payload.decode())
        records = payload.get("data", [])
        if records:
            print(f"📩 Received MQTT message with {len(records)} records.")
        for rec in records:
            name = rec.get("Facility Name")
            if not name:
                continue
            fac_buffer[name] = {
                "Timestamp": rec.get("Timestamp"),
                "Power(MW)": rec.get("Power(MW)") or rec.get("Value"),
                "Emissions(t)": rec.get("Emissions(t)"),
                "Fuel Type": rec.get("Fuel Type", "Unknown")
            }


        metric_view = st.radio("Select metric for color display:", ["Power", "Emissions"], horizontal=True)

        # Build map

        for _, row in FACILITIES.iterrows():
            lat, lon = row["latitude"], row["longitude"]
            if np.isnan(lat) or np.isnan(lon):
                continue
            name = row["Facility Name"]
            fuel = row.get("Fuel Type", "Unknown")
            color = FUEL_COLOR.get(fuel, "gray")

            # Get live MQTT data if available
            rec = fac_buffer.get(name, {})
            ts = rec.get("Timestamp", "-")
            power = rec.get("Power(MW)", "N/A")
            emis = rec.get("Emissions(t)", "N/A")

            popup_html = f"""
                <b>{name}</b><br>
                Fuel: {fuel}<br>
                Timestamp: {ts}<br>
                Power: {power} MW<br>
                Emissions: {emis} tCO₂
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

        event = st_folium(fmap, width=1100, height=650, returned_objects=["last_object_clicked"])

        # ======================================
        # Live detail panel (and terminal print)
        # ======================================
        selected_fac = None
        if event and event.get("last_object_clicked"):
            lat_c, lon_c = event["last_object_clicked"]["lat"], event["last_object_clicked"]["lng"]
            distances = (FACILITIES["latitude"] - lat_c).abs() + (FACILITIES["longitude"] - lon_c).abs()
            if not distances.empty:
                selected_fac = FACILITIES.loc[distances.idxmin(), "Facility Name"]
                st.session_state["selected_facility"] = selected_fac
        else:
            selected_fac = st.session_state.get("selected_facility")

        if selected_fac:
            st.markdown(f"### 🔄 Live Data — {selected_fac}")
            rec = fac_buffer.get(selected_fac)

            if rec:
                ts = rec.get("Timestamp", "-")
                power = rec.get("Power(MW)", "N/A")
                emis = rec.get("Emissions(t)", "N/A")
                fuel = rec.get("Fuel Type", "Unknown")

                # ---- Print live data to terminal ----
                print(f"📡 [{selected_fac}] Timestamp={ts}, Power={power}, Emissions={emis}, Fuel={fuel}")

                # ---- Update dashboard ----
                col1, col2, col3 = st.columns(3)
                col1.metric("Timestamp", ts)
                col2.metric("Power (MW)", power)
                col3.metric("Emissions (tCO₂)", emis)
                st.caption(f"Fuel Type: {fuel}")
            else:
                st.info("Waiting for MQTT messages...")
        else:
            st.info("🕹️ Click a marker to start live monitoring.")
    except Exception as e:
        print("⚠️ MQTT decode error:", e)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

# Connect to the server using the IP address and connection port
client.connect("test.mosquitto.org", 1883, 60)
# This loop will keep the client listening for messages
client.loop_forever()

# ======================================
# Streamlit main
# ======================================
st.set_page_config(page_title="MQTT Facility Dashboard", layout="wide")
st.title("⚡ Real-Time Facility Map (Live MQTT Data)")
st.caption(f"Broker: `{broker_host}` | Port: `{broker_port}` | Topic: `{topic_facility}`")
st_autorefresh(interval=1000, limit=None, key="refresh_counter")

if "selected_facility" not in st.session_state:
    st.session_state["selected_facility"] = None

else:
    st.info("🕹️ Click a facility marker to start live monitoring.")


