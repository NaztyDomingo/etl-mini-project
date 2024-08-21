# Description: This file contains the Airflow DAG for the miniweather project.
import requests
import os
import json
import pandas as pd
import psycopg2 as ps
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

CURR_PATH = os.path.dirname(os.path.realpath(__file__))
TARGET_PATH_SMHI = os.path.join(CURR_PATH, 'smhi_weather_data.json')
TARGET_PATH_MET = os.path.join(CURR_PATH, 'met_weather_data.json')
TARGET_PATH_SMHI_CSV = os.path.join(CURR_PATH, 'smhi_weather_data.csv')
TARGET_PATH_MET_CSV = os.path.join(CURR_PATH, 'met_weather_data.csv')


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
    headers = {'User-Agent': 'weather@brightstraining.com'}
    met_response = requests.get(met_url, headers=headers)

    if met_response.status_code == 200:
        data = met_response.json()
        print('MET data retrieved successfully')
        return data
    else:
        print(f"Failed to retrieve MET data: HTTP {met_response.status_code}")
        return None
    


def process_weather_data(locations):
    for location in locations:
        lat = location['lat']
        lon = location['lon']
        location_name = f"{lat}_{lon}"


        smhi_data = get_smhi_data(lat, lon)
        if smhi_data:
            smhi_df = smhi_to_dataframe(smhi_data)
            smhi_df['location'] = location_name
            smhi_df = transforming_smhi(smhi_df)
            if os.path.exists(TARGET_PATH_SMHI_CSV):
                smhi_df.to_csv(TARGET_PATH_SMHI_CSV, mode='a', header=False, index=False) 
            else:    
                smhi_df.to_csv(TARGET_PATH_SMHI_CSV, mode='w', index=False) 

       
        met_data = get_met_data(lat, lon)
        if met_data:
            met_df = met_to_dataframe(met_data)
            met_df['location'] = location_name
            met_df = transforming_met(met_df)
            if os.path.exists(TARGET_PATH_MET_CSV):
                met_df.to_csv(TARGET_PATH_MET_CSV, mode='a', header=False, index=False)  
            else:
                met_df.to_csv(TARGET_PATH_MET_CSV, mode='w', index=False) 


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



def smhi_to_dataframe(smhi_data):
    rows = []
    for item in smhi_data['timeSeries']:
        row = {
            'time': item['validTime'],
            'temperature': next(param['values'][0] for param in item['parameters'] if param['name'] == 't'),  
            'cloud_coverage': next(param['values'][0] for param in item['parameters'] if param['name'] == 'tcc_mean'),  
            'relative_humidity': next(param['values'][0] for param in item['parameters'] if param['name'] == 'r'),  
            'wind_speed': next(param['values'][0] for param in item['parameters'] if param['name'] == 'ws')  
        }
        rows.append(row)
    return pd.DataFrame(rows)


def met_to_dataframe(met_data):
    rows = []
    for timeseries in met_data['properties']['timeseries']:
        row = {
            'time': timeseries['time'],
            'temperature': timeseries['data']['instant']['details']['air_temperature'],    
            'cloud_coverage': timeseries['data']['instant']['details']['cloud_area_fraction'],
            'relative_humidity': timeseries['data']['instant']['details']['relative_humidity'],
            'wind_speed': timeseries['data']['instant']['details']['wind_speed']
        }
        rows.append(row)
    return pd.DataFrame(rows)

def transforming_smhi(smhi_df):

    smhi_df['source'] = 'SMHI'

    smhi_df['location'] = smhi_df['location'].replace({
        '57.7_11.9': 'Gothenburg',
        '55.6_13.0': 'Malmo',
        '59.9_10.7': 'Oslo',
        '59.3_18.0': 'Stockholm',
        '60.1_24.9': 'Helsinki'
        })
    
    smhi_df['cloud_coverage'] = (smhi_df['cloud_coverage'] / 8) * 100

    smhi_df['time'] = pd.to_datetime(smhi_df['time'])
    smhi_df['day_of_week'] = pd.to_datetime(smhi_df['time']).dt.day_name()
    smhi_df['hour_of_day'] = pd.to_datetime(smhi_df['time']).dt.hour
    smhi_df['time'] = smhi_df['time'].dt.strftime('%Y-%m-%d')

    smhi_df = smhi_df.rename(columns={'time': 'date'})

    return smhi_df

def transforming_met(met_df):

    met_df['source'] = 'MET'

    met_df['location'] = met_df['location'].replace({
        '57.7_11.9': 'Gothenburg',
        '55.6_13.0': 'Malmo',
        '59.9_10.7': 'Oslo',
        '59.3_18.0': 'Stockholm',
        '60.1_24.9': 'Helsinki'
        })
    
    met_df['time'] = pd.to_datetime(met_df['time'])
    met_df['day_of_week'] = pd.to_datetime(met_df['time']).dt.day_name()
    met_df['hour_of_day'] = pd.to_datetime(met_df['time']).dt.hour
    met_df['time'] = met_df['time'].dt.strftime('%Y-%m-%d')

    met_df = met_df.rename(columns={'time': 'date'})

    return met_df

def connect_to_db():
    return ps.connect(
        dbname='miniweather_db', 
        user='nazelisvera', 
        password='12345',
        host='localhost',
        port=5432
        )


def db_template():
    try:
        conn = connect_to_db()
        cur = conn.cursor()

        cur.execute('DROP VIEW IF EXISTS compare_sources, mondays_vs_fridays;')
        cur.execute('DROP TABLE IF EXISTS weather_data;')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS weather_data(
                ID SERIAL PRIMARY KEY,
                date DATE,
                temperature NUMERIC(4, 2),
                cloud_coverage NUMERIC(5, 2),
                relative_humidity NUMERIC(4, 2),
                wind_speed NUMERIC(4, 2),
                location TEXT,
                source TEXT,
                day_of_week TEXT,
                hour_of_day INT
            );
        ''')

        cur.execute('''
            CREATE VIEW compare_sources AS
            SELECT 
                smhi.date AS smhi_date,
                smhi.hour_of_day AS smhi_hour,
                smhi.location AS smhi_location,
                smhi.temperature AS smhi_temperature,
                smhi.cloud_coverage AS smhi_cloud_coverage,
                smhi.relative_humidity AS smhi_relative_humidity,
                smhi.wind_speed AS smhi_wind_speed,
                met.temperature AS met_temperature,
                met.cloud_coverage AS met_cloud_coverage,
                met.relative_humidity AS met_relative_humidity,
                met.wind_speed AS met_wind_speed
            FROM 
                weather_data smhi
            JOIN 
                weather_data met
            ON 
                smhi.date = met.date
                AND smhi.hour_of_day = met.hour_of_day
                AND smhi.location = met.location
            WHERE 
                smhi.source = 'SMHI' 
                AND met.source = 'MET';
        ''')

        cur.execute('''
            CREATE VIEW Mondays_vs_Fridays AS
            SELECT 
                mon.date AS monday_date,
                mon.hour_of_day AS monday_hour,
                mon.location AS monday_location,
                mon.temperature AS monday_temperature,
                mon.cloud_coverage AS monday_cloud_coverage,
                mon.relative_humidity AS monday_relative_humidity,
                mon.wind_speed AS monday_wind_speed,
                fri.date AS friday_date,
                fri.hour_of_day AS friday_hour,
                fri.location AS friday_location,
                fri.temperature AS friday_temperature,
                fri.cloud_coverage AS friday_cloud_coverage,
                fri.relative_humidity AS friday_relative_humidity,
                fri.wind_speed AS friday_wind_speed
            FROM 
                weather_data mon
            JOIN 
                weather_data fri
            ON 
                mon.date = fri.date
                AND mon.hour_of_day = fri.hour_of_day
                AND mon.location = fri.location
            WHERE 
                mon.day_of_week = 'Monday'
                AND fri.day_of_week = 'Friday';
        ''')

        conn.commit()
        print('Templates created successfully')

    except Exception as e:
        print('Template creation failed')
        print(f'Error: {e}')
    
    finally:
        cur.close()
        conn.close()
    
    

def load_to_db():
    postgres_engine = create_engine(
        url="postgresql+psycopg2://localhost",
        creator=connect_to_db
    )


    smhi_weather_data_path = os.path.join(CURR_PATH, "smhi_weather_data.csv")
    met_weather_data_path = os.path.join(CURR_PATH, "met_weather_data.csv")


    smhi_weather_data = pd.read_csv(smhi_weather_data_path, sep=",")
    met_weather_data = pd.read_csv(met_weather_data_path, sep=",")


    columns = ['date', 'temperature', 'cloud_coverage', 'relative_humidity', 'wind_speed', 'location', 'source', 'day_of_week', 'hour_of_day']
    smhi_weather_data.columns = columns
    met_weather_data.columns = columns


    weather_data = pd.concat([smhi_weather_data, met_weather_data], ignore_index=True)


    weather_data.to_sql(name="weather_data", con=postgres_engine, if_exists="replace", index=False)




def plot_weather():

    smhi_weather_data = pd.read_csv(TARGET_PATH_SMHI_CSV, sep=",")
    met_weather_data = pd.read_csv(TARGET_PATH_MET_CSV, sep=",")
    weather_data = pd.concat([smhi_weather_data, met_weather_data], ignore_index=True)

    
    fig, ax = plt.subplots(2, 2, figsize=(15, 10))

    for city in weather_data['location'].unique():
        city_df = weather_data[weather_data['location'] == city]
        city_df = city_df.pivot_table(index=['date'], values=['temperature', 'cloud_coverage', 'relative_humidity', 'wind_speed'], aggfunc='mean').round(2)
        city_df['temperature'].plot(ax=ax[0, 0], label=city)
        city_df['cloud_coverage'].plot(ax=ax[0, 1], label=city)
        city_df['relative_humidity'].plot(ax=ax[1, 0], label=city)
        city_df['wind_speed'].plot(ax=ax[1, 1], label=city)

    ax[0, 0].set_title('Temperature')
    ax[0, 0].set_ylabel('Temperature (°C)')
    ax[0, 0].legend()

    ax[0, 1].set_title('Cloud Coverage')
    ax[0, 1].set_ylabel('Cloud Coverage (%)')
    ax[0, 1].legend()

    ax[1, 0].set_title('Relative Humidity')
    ax[1, 0].set_ylabel('Relative Humidity (%)')
    ax[1, 0].legend()

    ax[1, 1].set_title('Wind Speed')
    ax[1, 1].set_ylabel('Wind Speed (m/s)')
    ax[1, 1].legend()

    plt.tight_layout()
    plt.savefig('weather_plot.png')



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for weather data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_api_data,
    dag=dag,
)

create_connection_task = BranchPythonOperator(
    task_id='create_db_connection',
    python_callable=connect_to_db,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_psql_table',
    python_callable=db_template,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_to_db,
    dag=dag,
)

plot_data_task = PythonOperator(
    task_id='plot_data',
    python_callable=plot_weather,
    dag=dag,
)

fetch_data_task >> create_connection_task >> create_table_task >> load_data_task >> plot_data_task
