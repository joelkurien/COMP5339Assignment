import os
os.environ['OPENELECTRICITY_API_KEY'] = 'oe_3ZLTaoN86mVzNAEFwMjaxR8u'

from openelectricity import OEClient
from openelectricity.types import DataMetric, NetworkCode, DataInterval, UnitStatusType
from datetime import datetime, timedelta
import json
import paho.mqtt.publish as publish
import pandas as pd
import numpy as np
import time

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

def format_power(value: float) -> str:
    """Format power values in MW or GW."""
    if abs(value) >= 1000:
        return f"{value / 1000:.2f} GW"
    return f"{value:.2f} MW"

def extract_emissions_data(facilities, f_codes, n, slice):
    data = []
    for i in range(0, n, slice):
        fac_metrics_response = nemClient.get_facility_data(
            network_code="NEM",
            facility_code=f_codes[i:i+slice],
            metrics=[DataMetric.POWER],
            interval="5m",
            date_start=datetime(2025, 10, 1),
            date_end=datetime(2025, 10, 7)
        )
        
        for facility in facilities.data:
            for series in fac_metrics_response.data:
                for result in series.results:
                    if result.columns.unit_code in [unit.code for unit in facility.units]:
                        for values in result.data:
                            facility_name = facility.name
                            unit = next(u for u in facility.units if u.code == result.columns.unit_code)
                            unit_code = unit.code
                            fueltype = unit.fueltech_id.value.replace("_", " ").title()
                            timestamp = values.timestamp.strftime("%Y-%m-%d %H:%M")
                            emissions = values.value
                            data.append({
                                'Facility Name': facility_name,
                                'Unit Code': unit_code,
                                'Fuel Type': fueltype,
                                'Timestamp': timestamp,
                                'POWER': emissions,
                                'Power': None  
                            })
    return data

def get_facility_metrics():
    facility_codes = get_facility_codes()
    f_codes = [facility_codes[name] for name in facility_codes]
    facilities = nemClient.get_facilities(network_id=["NEM"])
    data = []
    
    n = len(facilities.data)
    
    slice = n//20
    
    data = extract_emissions_data(facilities, f_codes, n, slice)
    
    df = pd.DataFrame(data)
    df.to_csv("power_df.csv", index=False)
    
def get_market_data(facility_codes):
    pass

def mqtt_publisher():
    MQTT_BROKER = "mqtt.eclipse.org"  
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
    

mqtt_publisher()
