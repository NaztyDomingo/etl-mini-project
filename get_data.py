from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import os
import requests
from datetime import datetime
import json
import pandas as pd
import configparser

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
    headers = {'User-Agent': 'weather@brightstraining.com'}  # Ensure you add a User-Agent
    met_response = requests.get(met_url, headers=headers)

    if met_response.status_code == 200:
        data = met_response.json()
        print('MET data retrieved successfully')
        return data
    else:
        print(f"Failed to retrieve MET data: HTTP {met_response.status_code}")
        return None