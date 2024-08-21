import os
import pandas as pd
from create_connection import connect_to_db
from sqlalchemy import create_engine

CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))

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
        "postgresql+psycopg2://localhost",
        creator=connect_to_db
    )

    smhi_weather_data_path = os.path.join(CURR_DIR_PATH, "smhi_weather_data.csv")
    met_weather_data_path = os.path.join(CURR_DIR_PATH, "met_weather_data.csv")

    smhi_weather_data = pd.read_csv(smhi_weather_data_path)
    met_weather_data = pd.read_csv(met_weather_data_path)

    columns = ['date', 'temperature', 'cloud_coverage', 'relative_humidity', 'wind_speed', 'location', 'source', 'day_of_week', 'hour_of_day']
    smhi_weather_data.columns = columns
    met_weather_data.columns = columns

    weather_data = pd.concat([smhi_weather_data, met_weather_data], ignore_index=True)

    weather_data.to_sql(name="weather_data", con=postgres_engine, if_exists="replace", index=False)


