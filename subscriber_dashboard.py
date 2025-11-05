

# ======================================
# 5️⃣ Streamlit map + interactive refresh
# ======================================
metric_view = st.radio("Select metric for color display:", ["Power", "Emissions"], horizontal=True)

# # Build map
# fmap = folium.Map(location=[-25, 134], zoom_start=4, tiles="CartoDB positron")

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
# 6️⃣ Live detail panel (and terminal print)
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
