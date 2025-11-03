import os
os.environ['OPENELECTRICITY_API_KEY'] = 'oe_3ZLTaoN86mVzNAEFwMjaxR8u'

from openelectricity import OEClient
from openelectricity.types import DataMetric, NetworkCode, DataInterval, UnitStatusType, MarketMetric
from datetime import datetime, timedelta
import json
import paho.mqtt.publish as publish
import pandas as pd
import numpy as np
import time
import requests

nemClient = OEClient()
    
#get facility codes
def get_facility_codes():
    facility_codes = None
    facility_codes_file = 'facility_codes.json'
    if os.path.exists(facility_codes_file) and os.path.getsize(facility_codes_file) > 0:
            with open(facility_codes_file, 'r') as file:
                facility_codes = json.load(file)
    else:
        fac_data_response = nemClient.get_facilities(
            status_id=[UnitStatusType.OPERATING, UnitStatusType.COMMITTED],
            network_id=["NEM"]
        )

        facility_codes = { facility.name: facility.code for facility in fac_data_response.data }
        with open(facility_codes_file, "w") as file:
            json.dump(facility_codes, file, indent=4)
        print("Facility Codes Cached")
    return facility_codes

#extract per-facility power and emissions information
def extract_power_and_emissions_data(facilities, f_codes, n, slice, obs_date):
    date_start = obs_date['start']
    date_end = obs_date['end']
    
    data = []
    for i in range(0, n, slice):
        fac_metrics_response = nemClient.get_facility_data(
            network_code="NEM",
            facility_code=f_codes[i:i+slice],
            metrics=[
                DataMetric.POWER, 
                DataMetric.EMISSIONS
            ],
            interval="5m",
            date_start = date_start,
            date_end = date_end
        )
        
        for facility in facilities.data:
            for series in fac_metrics_response.data:
                for result in series.results:
                    if result.columns.unit_code in [unit.code for unit in facility.units]:
                        for data_pt in result.data:
                            facility_name = facility.name
                            unit = next(u for u in facility.units if u.code == result.columns.unit_code)
                            unit_code = unit.code
                            fueltype = unit.fueltech_id.value.replace("_", " ").title()
                            timestamp = data_pt.timestamp.strftime("%Y-%m-%d %H:%M")
                            data.append({
                                'Facility Name': facility_name,
                                'Unit Code': unit_code,
                                'Fuel Type': fueltype,
                                'Timestamp': timestamp,
                                'Metric': series.metric,
                                'Value': data_pt.value,
                                'Measure Unit': series.unit  
                            })
    return data

#extract per-facility market info
def extract_market_data(obs_date):
    nemClient = OEClient()
    
    date_start = obs_date['start']
    date_end = obs_date['end']
    
    response = nemClient.get_market(
        network_code="NEM",
        metrics=[
            MarketMetric.PRICE,
            MarketMetric.DEMAND
        ],
        interval="5m",
        date_start = date_start,
        date_end = date_end,
        primary_grouping="network_region"
    )
    
    data = []
    for timeseries in response.data:
        for result in timeseries.results:
            region = result.name.split("_")[-1]  # Extract region from name
            for data_point in result.data:
                data.append({
                    "timestamp": data_point.timestamp,
                    "region": region,
                    "metric": timeseries.metric,
                    "value": data_point.value,
                    "unit": timeseries.unit
                })

    df = pd.DataFrame(data)
    return df

#general functions to call the extraction methods safely
def get_facility_metrics(obs_date):
    facility_codes = get_facility_codes()
    f_codes = [facility_codes[name] for name in facility_codes]
    facilities = nemClient.get_facilities(network_id=["NEM"])
    data = []
    
    n = len(facilities.data)
    
    slice = n//20
    
    data = extract_power_and_emissions_data(facilities, f_codes, n, slice, obs_date)
    
    df = pd.DataFrame(data)
    df.to_csv("power_df.csv", index=False)

def get_market_data(obs_date):
    data = extract_market_data(obs_date)
    data.to_csv("market_data.csv")

#control function for task 1       
def get_metric_main():
    obs_date = {
        'start': datetime(2025, 10, 1),
        'end': datetime(2025, 10, 8)
    }
    
    get_facility_metrics(obs_date)
    get_market_data(obs_date)

#csv cleaning function for task 2
def csv_cleaning():
    power_df = pd.read_csv("power_df.csv")
    market_df = pd.read_csv("market_data.csv")

    power_temp = power_df[power_df['Metric'] == 'power']
    market_temp = market_df[market_df['metric'] == 'price'] 

    power_temp = power_temp.copy()  
    power_temp.loc[:, 'Power(MW)'] = power_df.query("Metric == 'power'")['Value'].values
    power_temp.loc[:, 'Emissions(t)'] = power_df.query("Metric == 'emissions'")['Value'].values

    market_temp = market_temp.copy()
    market_temp.loc[:, 'Price($/MWh)'] = market_df.query("metric == 'price'")['value'].values
    market_temp.loc[:, 'Demand(MW)'] = market_df.query("metric == 'demand'")['value'].values
    market_temp = market_temp.drop(columns=['Unnamed: 0'])
    market_temp['timestamp'] = market_temp['timestamp'].str.replace(r'\+10:00$', '', regex=True)

    power_temp.to_csv('power_df.csv', index=False)
    market_temp.to_csv('market_data.csv', index=False)
    print("CSV's have been cleaned and updated")

#mqtt publisher on localhost for the two topics of facility metrics and region markets
def mqtt_publisher():
    MQTT_BROKER = "localhost"  
    MQTT_PORT = 1883
    MQTT_TOPIC = "facitilies/metrics"
    
    topics = {
        "power_df.csv": "facilities/metrics_info",
        "market_data.csv": "market/price_demand"
    }
    
    
    while True:
        for file, topic in topics.items():
            df = pd.read_csv(file)

            for index, row in df.iterrows():
                record = row.to_dict()
                json_payload = json.dumps(record)

                publish.single(topic, json_payload, hostname=MQTT_BROKER, port=MQTT_PORT)

                print(f"✅ Published record {index + 1}/{len(df)} to topic {topic}")
                time.sleep(0.1)
        time.sleep(60)
    

get_metric_main()
csv_cleaning()
mqtt_publisher()

