import os
os.environ["OPENELECTRICITY_API_KEY"] = "oe_3ZLTaoN86mVzNAEFwMjaxR8u"

from openelectricity import OEClient
from openelectricity.types import DataMetric, NetworkCode, DataInterval, UnitStatusType, MarketMetric
from datetime import datetime
import json
import paho.mqtt.publish as publish
import pandas as pd
import numpy as np
import time
import requests
from dotenv import load_dotenv
import duckdb
import re
from rapidfuzz import process, fuzz

load_dotenv()
nemClient = OEClient()

# OpenElectricity data extraction and cleaning
def get_facility_codes():
    """Fetch or load cached facility codes from file."""
    facility_codes_file = 'facility_codes.json'
    if os.path.exists(facility_codes_file) and os.path.getsize(facility_codes_file) > 0:
        with open(facility_codes_file, 'r') as file:
            facility_codes = json.load(file)
    else:
        fac_data_response = nemClient.get_facilities(
            status_id=[UnitStatusType.OPERATING, UnitStatusType.COMMITTED],
            network_id=["NEM"]
        )
        facility_codes = {facility.name: facility.code for facility in fac_data_response.data}
        with open(facility_codes_file, "w") as file:
            json.dump(facility_codes, file, indent=4)
        print("Facility Codes Cached")
    return facility_codes


def extract_power_and_emissions_data(facilities, f_codes, n, slice_size, obs_date):
    """Extract power and emission metrics per facility."""
    date_start, date_end = obs_date['start'], obs_date['end']
    data = []
    for i in range(0, n, slice_size):
        fac_metrics_response = nemClient.get_facility_data(
            network_code="NEM",
            facility_code=f_codes[i:i + slice_size],
            metrics=[DataMetric.POWER, DataMetric.EMISSIONS],
            interval="5m",
            date_start=date_start,
            date_end=date_end
        )

        for facility in facilities.data:
            for series in fac_metrics_response.data:
                for result in series.results:
                    if result.columns.unit_code in [unit.code for unit in facility.units]:
                        for data_pt in result.data:
                            unit = next(u for u in facility.units if u.code == result.columns.unit_code)
                            data.append({
                                'Facility Name': facility.name,
                                'Facility Code': facility.code,
                                'Unit Code': unit.code,
                                'Fuel Type': unit.fueltech_id.value.replace("_", " ").title(),
                                'Timestamp': data_pt.timestamp.strftime("%Y-%m-%d %H:%M"),
                                'Metric': series.metric,
                                'Value': data_pt.value,
                                'Measure Unit': series.unit
                            })
    return data


def extract_market_data(obs_date):
    """Extract 5-minute market data (price & demand)."""
    date_start, date_end = obs_date['start'], obs_date['end']
    response = nemClient.get_market(
        network_code="NEM",
        metrics=[MarketMetric.PRICE, MarketMetric.DEMAND],
        interval="5m",
        date_start=date_start,
        date_end=date_end,
        primary_grouping="network_region"
    )

    records = []
    for timeseries in response.data:
        for result in timeseries.results:
            region = result.name.split("_")[-1]
            for dp in result.data:
                records.append({
                    "timestamp": dp.timestamp,
                    "region": region,
                    "metric": timeseries.metric,
                    "value": dp.value,
                    "unit": timeseries.unit
                })
    return pd.DataFrame(records)


def get_facility_metrics(obs_date):
    """Get facility-level power/emission metrics and save to CSV."""
    facility_codes = get_facility_codes()
    f_codes = [facility_codes[name] for name in facility_codes]
    facilities = nemClient.get_facilities(network_id=["NEM"])

    n = len(facilities.data)
    slice_size = max(1, n // 20)
    data = extract_power_and_emissions_data(facilities, f_codes, n, slice_size, obs_date)

    df = pd.DataFrame(data)
    df.to_csv("power_df.csv", index=False)
    print(f"✅ Saved power_df.csv with {len(df)} rows.")


def get_market_data(obs_date):
    """Get market metrics and save to CSV."""
    df = extract_market_data(obs_date)
    df.to_csv("market_data.csv", index=False)
    print(f"✅ Saved market_data.csv with {len(df)} rows.")


def csv_cleaning():
    """Clean and reformat extracted CSVs for downstream use."""
    power_df = pd.read_csv("power_df.csv")
    market_df = pd.read_csv("market_data.csv")

    power_temp = power_df[power_df['Metric'] == 'power'].copy()
    market_temp = market_df[market_df['metric'] == 'price'].copy()

    power_temp["Power(MW)"] = power_df.query("Metric == 'power'")['Value'].values
    power_temp["Emissions(t)"] = power_df.query("Metric == 'emissions'")['Value'].values
    power_temp = power_temp[~((power_temp["Power(MW)"] == 0) & (power_temp["Emissions(t)"] == 0))]
    power_temp.drop(columns=["Value", "Metric", "Measure Unit"], inplace=True)

    market_temp["Price($/MWh)"] = market_df.query("metric == 'price'")['value'].values
    market_temp["Demand(MW)"] = market_df.query("metric == 'demand'")['value'].values
    market_temp.drop(columns=["Unnamed: 0"], errors="ignore", inplace=True)
    market_temp["timestamp"] = market_temp["timestamp"].astype(str).str.replace(r"\+10:00$", "", regex=True)

    power_temp.to_csv("power_df.csv", index=False)
    market_temp.to_csv("market_data.csv", index=False)
    print("🧹 Cleaned and updated CSVs successfully.")

# Fuzzy merge with DuckDB coordinates
def normalize_name(name: str) -> str:
    """Normalize facility/project names for better fuzzy matching."""
    if pd.isna(name):
        return ""
    name = re.sub(r"[^A-Za-z0-9 ]+", "", str(name)).lower()
    for term in [
        "solar farm", "wind farm", "power station", "power plant", "energy",
        "battery", "ps", "wf", "sf", "farm", "plant", "station"
    ]:
        name = name.replace(term, "")
    return re.sub(r"\s+", " ", name).strip()


def merge_with_duckdb_location_fuzzy(
    db_path="assignment1.duckdb",
    csv_path="power_df.csv",
    output_path="power_with_geo.csv",
    min_score_primary=60,
    min_score_secondary=30
):
    """Fuzzy match facility names with DuckDB project coordinates."""
    conn = duckdb.connect(database=db_path)
    conn.execute("INSTALL spatial; LOAD spatial;")
    df_db = conn.execute("""
        SELECT projectName, ST_Y(location) AS latitude, ST_X(location) AS longitude
        FROM powerStation_fact_table
        JOIN powerStation_dim_table USING (station_id)
        WHERE location IS NOT NULL
    """).fetchdf()
    conn.close()
    print(f"✅ Loaded {len(df_db)} facilities from DuckDB")

    df_power = pd.read_csv(csv_path)
    df_power["Facility Name"] = df_power["Facility Name"].astype(str).str.strip()

    df_db["clean_name"] = df_db["projectName"].apply(normalize_name)
    df_power["clean_name"] = df_power["Facility Name"].apply(normalize_name)
    names_db = df_db["clean_name"].tolist()

    latitudes, longitudes, scores = [], [], []
    for name in df_power["clean_name"]:
        if not name:
            latitudes.append(None)
            longitudes.append(None)
            scores.append(None)
            continue

        match = process.extractOne(name, names_db, scorer=fuzz.partial_ratio)
        if match and match[1] >= min_score_primary:
            matched_row = df_db[df_db["clean_name"] == match[0]].iloc[0]
        else:
            match = process.extractOne(name, names_db, scorer=fuzz.token_sort_ratio)
            if match and match[1] >= min_score_secondary:
                matched_row = df_db[df_db["clean_name"] == match[0]].iloc[0]
            else:
                matched_row = None

        if matched_row is not None:
            latitudes.append(matched_row["latitude"])
            longitudes.append(matched_row["longitude"])
            scores.append(match[1])
        else:
            latitudes.append(None)
            longitudes.append(None)
            scores.append(None)

    df_power["latitude"], df_power["longitude"], df_power["match_score"] = latitudes, longitudes, scores
    matched = df_power["latitude"].notna().sum()
    print(f"✅ Fuzzy matched facilities: {matched}/{len(df_power)} ({matched/len(df_power)*100:.2f}%)")

    df_power.to_csv(output_path, index=False)
    print(f"💾 Saved merged dataset → {output_path}")
    print(df_power.head(5)[["Facility Name", "latitude", "longitude", "match_score"]])
    return df_power

# MQTT Publisher
def mqtt_publisher():
    """Publish facility metrics to MQTT broker."""
    MQTT_BROKER = "test.mosquitto.org"
    MQTT_PORT = 1883
    MQTT_TOPIC = "facilities/metrics_info"

    df = pd.read_csv("power_with_geo.csv")
    timestamp_grouped = df.groupby("Timestamp")

    while True:
        for timestamp, group in timestamp_grouped:
            data = group.to_dict(orient="records")
            payload = {"timestamp": str(timestamp), "data": data}
            publish.single(MQTT_TOPIC, json.dumps(payload), hostname=MQTT_BROKER, port=MQTT_PORT)
            print(f"📡 Published {len(data)} records for {timestamp}")
            time.sleep(0.1)
        time.sleep(60)

#  Main workflow
# obs_date = {"start": datetime(2025, 10, 1), "end": datetime(2025, 10, 8)}
# get_facility_metrics(obs_date)
# get_market_data(obs_date)
# csv_cleaning()
# merge_with_duckdb_location_fuzzy()
mqtt_publisher()
