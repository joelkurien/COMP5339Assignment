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
   
def get_facility_metrics(obs_date):
    if not os.path.exists("power_df.csv") or os.path.getsize("power_df.csv") == 0:
        facility_codes = get_facility_codes()
        f_codes = [facility_codes[name] for name in facility_codes]
        facilities = nemClient.get_facilities(network_id=["NEM"])
        data = []
        
        n = len(facilities.data)
        
        slice = n//20
        
        data = extract_power_and_emissions_data(facilities, f_codes, n, slice, obs_date)
        
        df = pd.DataFrame(data)
        df.to_csv("power_df.csv", index=False)
    else:
        print("Power and Emission information is alread loaded in a CSV")

def get_market_data(obs_date):
    if not os.path.exists("market_data.csv") or os.path.getsize("market_data.csv") == 0:
        data = extract_market_data(obs_date)
        
        data.to_csv("market_data.csv")
    else:
        print("Market Information for each region has been already loaded")
        
def get_metric_main():
    obs_date = {
        'start': datetime(2025, 10, 1),
        'end': datetime(2025, 10, 8)
    }
    
    get_facility_metrics(obs_date)
    get_market_data(obs_date)
    

def mqtt_publisher():
    MQTT_BROKER = "localhost"  
    MQTT_PORT = 1883
    MQTT_TOPIC = "facitilies/metrics"
    
    df = pd.read_csv("power_df.csv")
    json_payload = df.to_json(orient="records") 
    payload = json.loads(json_payload)  
    
    
    for index, row in df.iterrows():
        record = row.to_dict()
        
        json_payload = json.dumps(record)
        publish.single(MQTT_TOPIC, json_payload, hostname=MQTT_BROKER, port=MQTT_PORT)
        
        print(f"Published record {index+1}: {json_payload}")
        time.sleep(0.1)

    print("All records published.")
    

get_metric_main()
mqtt_publisher()

