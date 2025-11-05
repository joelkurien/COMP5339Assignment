import os

import pandas as pd
import numpy as np
import requests
import json
import time
import googlemaps
from tqdm import tqdm
from bs4 import BeautifulSoup
from io import StringIO

# web scraping general function
def get_html(url):
    webpage = requests.get(url).text
    content = None
    if webpage != None:
        content = BeautifulSoup(webpage, 'html.parser')
    return content


# get response from a url
def get_response(url):
    while True:
        try:
            response = requests.get(url)
            if response.text != None:
                return response.text
        except:
            print("Failure in connection")


# general database functions
import duckdb


def open_connection():
    conn = duckdb.connect(database='assignment1.duckdb')
    try:
        conn.execute("INSTALL spatial; LOAD spatial;")
        print("✅ Spatial extension loaded successfully.")
    except Exception as e:
        print("⚠️ Spatial extension unavailable, fallback to basic mode:", e)
    return conn


conn = open_connection()


# web scraping cer
def find_cer_dataset(url, base_url, options):
    soup = get_html(url)
    csv_paths = []
    for item in soup.find_all('div', 'vue-datavis-wrapper'):
        for sub_item in item.find_all('div', 'cer-accordion__body'):
            a = sub_item.find(
                'a',
                href=lambda x: x and any(opt in x for opt in options))
            if a:
                csv_paths.append(base_url + a['href'])
    return csv_paths


def get_datasets(csv_paths):
    power_dfs = []
    for path in csv_paths:
        csv_response = get_response(path)
        df = pd.read_csv(StringIO(csv_response))
        power_dfs.append(df)
    return power_dfs


PS_URL = "https://cer.gov.au/markets/reports-and-data/large-scale-renewable-energy-data"
CER_PAGE_URL = "https://cer.gov.au"
station_options = {'accredited', 'committed', 'probable'}
power_station_paths = find_cer_dataset(PS_URL, CER_PAGE_URL, station_options)
power_dfs = get_datasets(power_station_paths)


def cer_integration(power_dfs):
    ac, cm, pr = power_dfs
    rename_map = {
        "Power station name": "Project Name",
        "Installed capacity (MW)": "MW Capacity",
        "Fuel Source (s)": "Fuel Source",
        "State": "State",
        "State ": "State"
    }

    def standardize(df, status):
        df = df.rename(columns=rename_map)
        df = df[["Project Name", "State", "MW Capacity", "Fuel Source"]].copy()
        df["status"] = status
        return df

    ac_core = standardize(ac, "accredited")
    cm_core = standardize(cm, "committed")
    pr_core = standardize(pr, "probable")

    return pd.concat([ac_core, cm_core, pr_core], ignore_index=True)


def cer_cleaning(df, export=True):
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    df["Project Name"] = df["Project Name"].str.split("-").str[0].str.strip()
    keys = ['Project Name', 'State']
    dupmask = df.duplicated(keys, keep=False)
    duplicates = df.loc[dupmask].sort_values(keys)
    # aggregate duplicates
    numeric_cols = df.select_dtypes(include="number").columns.difference(keys)
    non_numeric_cols = df.columns.difference(numeric_cols.union(keys))
    agg_rules = {**{col: "first" for col in non_numeric_cols},
                 **{col: "mean" for col in numeric_cols}}
    df_clean = df.groupby(keys, as_index=False).agg(agg_rules)
    if export:
        df_clean.to_csv("core_summary.csv", index=False)
    return df_clean


core_summary = cer_integration(power_dfs)
core_summary = cer_cleaning(core_summary)
core_summary.head()

# data augmentation
gmaps = googlemaps.Client(key="AIzaSyCXe4WX_VTezOiWHQYSxqXxp3tphr1nqpQ")


def get_coordinates_google(address):
    try:
        geocode_result = gmaps.geocode(address)
        if geocode_result:
            location = geocode_result[0]['geometry']['location']
            lat = location['lat']
            lon = location['lng']

            region = None
            for comp in geocode_result[0]['address_components']:
                if "locality" in comp['types']:
                    region = comp['long_name']
                    break

            return lat, lon
    except Exception as e:
        print(f"Error on {address}: {e}")
    return None, None, None


def geocode_projects(core_summary, sleep=0.05):
    latitudes, longitudes, regions = [], [], []

    for addr in tqdm(core_summary["Project Name"] + ", " +
                     core_summary["State"] + ", Australia",
                     desc="Google Geocoding", unit="site"):
        lat, lon = get_coordinates_google(addr)
        latitudes.append(lat)
        longitudes.append(lon)
        time.sleep(sleep)

    core_summary = core_summary.copy()
    core_summary["latitude"] = latitudes
    core_summary["longitude"] = longitudes

    return core_summary


core_summary = geocode_projects(core_summary)


# CER dataset transformation and loading into database
def create_powerStation_schema(df):
    for table in ['powerStation_fact_table', 'powerStation_dim_table']:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    for seq in ['station_id_seq']:
        conn.execute(f"DROP SEQUENCE IF EXISTS {seq}")
    conn.execute("CREATE SEQUENCE station_id_seq START 1")

    conn.execute("""
                 CREATE TABLE powerStation_dim_table
                 (
                     station_id  INTEGER DEFAULT nextval('station_id_seq'),
                     projectName VARCHAR NOT NULL,
                     state       VARCHAR NOT NULL,
                     fuel_source VARCHAR NOT NULL,
                     status      VARCHAR NOT NULL,
                     UNIQUE (projectName, state),
                     PRIMARY KEY (station_id)
                 )
                 """)

    conn.execute("""
                 CREATE TABLE powerStation_fact_table
                 (
                     station_id INTEGER,
                     mw_capacity DOUBLE,
                     location   GEOMETRY,
                     FOREIGN KEY (station_id) REFERENCES powerStation_dim_table (station_id),
                     PRIMARY KEY (station_id)
                 )
                 """)

    dim_cols = ['Project Name', 'State', 'Fuel Source', 'status']
    dim_df = df[dim_cols].drop_duplicates().reset_index(drop=True)

    conn.register("powerStation_dim_df", dim_df)
    conn.execute("""
                 INSERT INTO powerStation_dim_table (projectName, state, fuel_source, status)
                 SELECT *
                 FROM powerStation_dim_df
                 """)

    dim_table = conn.execute("SELECT * FROM powerStation_dim_table").fetchdf()
    powerStation_fact_df = df.merge(
        dim_table,
        left_on=['Project Name', 'State'],
        right_on=['projectName', 'state'],
        how='inner'
    )

    powerStation_fact_df = powerStation_fact_df[['station_id', 'MW Capacity', 'longitude', 'latitude']]
    powerStation_fact_df.rename(columns={"MW Capacity": "mw_capacity"}, inplace=True)
    conn.register("powerStation_fact_df", powerStation_fact_df)
    print(powerStation_fact_df.columns)
    conn.execute("""
                 INSERT INTO powerStation_fact_table (station_id, mw_capacity, location)
                 SELECT station_id, mw_capacity, ST_Point2D(longitude, latitude)
                 FROM powerStation_fact_df
                 """)

    return "Successfully created PowerStation fact and dimension tables with spatial location in fact table."


create_powerStation_schema(core_summary)
conn.execute("SELECT * FROM powerStation_fact_table LIMIT 5").fetchdf()

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