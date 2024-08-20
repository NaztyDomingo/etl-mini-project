from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

import os
import requests
from datetime import datetime
import json
import pandas as pd
import configparser

CURR_PATH = os.path.dirname(os.path.realpath(__file__))
TARGET_PATH_SMHI = os.path.join(CURR_PATH, 'smhi_weather_data.json')
TARGET_PATH_MET = os.path.join(CURR_PATH, 'met_weather_data.json')

locations = [
    {'lat': 57.7, 'lon': 11.9}, # Göteborg
    {'lat': 55.6, 'lon': 13.0}, # Malmö
    {'lat': 59.9, 'lon': 10.7}, # Oslo
    {'lat': 59.3, 'lon': 18.0}, # Stockholm
    {'lat': 60.1, 'lon': 24.9} # Helsinki
]


def _create_smhi_url(lat, lon ):
    return f"https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/{lon}/lat/{lat}/data.json"


def _create_met_url(lat, lon):
    return f'https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={lat}&lon={lon}'

def get_smhi_data(lat, lon):
    smhi_url = _create_smhi_url(lat, lon)
    smhi_response = requests.get(smhi_url)
    
    if smhi_response.status_code == 200:
        data = smhi_response.json()
        print('SMHI data retrieved successfully')
        return data
    else:
        print(f"Failed to retrieve SMHI data: HTTP {smhi_response.status_code}")
        return None

def get_met_data(lat, lon):
    met_url = _create_met_url(lat, lon)
    headers = {'User-Agent': 'your-email@example.com'}  # Ensure you add a User-Agent
    met_response = requests.get(met_url, headers=headers)

    if met_response.status_code == 200:
        data = met_response.json()
        print('MET data retrieved successfully')
        return data
    else:
        print(f"Failed to retrieve MET data: HTTP {met_response.status_code}")
        return None

def smhi_to_dataframe(smhi_data):
    rows = []
    for item in smhi_data['timeSeries']:
        row = {
            'time': item['validTime'],
            'temperature': item['parameters'][10]['values'][0],  # Example: Temperature
            'wind_speed': item['parameters'][14]['values'][0],  # Example: Wind Speed
        }
        rows.append(row)
    return pd.DataFrame(rows)

def met_to_dataframe(met_data):
    rows = []
    for timeseries in met_data['properties']['timeseries']:
        row = {
            'time': timeseries['time'],
            'temperature': timeseries['data']['instant']['details']['air_temperature'],  # Example: Temperature
            'wind_speed': timeseries['data']['instant']['details']['wind_speed'],  # Example: Wind Speed
        }
        rows.append(row)
    return pd.DataFrame(rows)

def process_weather_data(locations):
    for location in locations:
        lat = location['lat']
        lon = location['lon']

        smhi_data = get_smhi_data(lat, lon)
        if smhi_data:
            smhi_df = smhi_to_dataframe(smhi_data)
            smhi_df.to_csv(f'smhi_weather_data_{lat}_{lon}.csv', index=False)

        met_data = get_met_data(lat, lon)
        if met_data:
            met_df = met_to_dataframe(met_data)
            met_df.to_csv(f'met_weather_data_{lat}_{lon}.csv', index=False)


def fetch_api_data():
    process_weather_data(locations)
    for location in locations:
        lat = location['lat']
        lon = location['lon']
        
        smhi_data = get_smhi_data(lat, lon)
        if smhi_data:
            with open(TARGET_PATH_SMHI, 'w') as f:
                json.dump(smhi_data, f)

        met_data = get_met_data(lat, lon)
        if met_data:
            with open(TARGET_PATH_MET, 'w') as f:
                json.dump(met_data, f)


with DAG(
    "weather_data_pipeline",
    start_date=datetime(2021, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    
    fetch_api_data_task = PythonOperator(
        task_id='fetch_api_data',
        python_callable= fetch_api_data
    )
    