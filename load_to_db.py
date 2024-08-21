import os
import pandas as pd
from create_connection import connect_to_db 
from sqlalchemy import create_engine


CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))


conn = connect_to_db()
cur = conn.cursor()


# cur.execute('DROP VIEWS IF EXISTS compare_sources, mondays_vs_fridays, overview')
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


conn.commit()


cur.close()
conn.close()


postgres_engine = create_engine(
     url="postgresql+psycopg2://localhost",
     creator=connect_to_db
)


smhi_weather_data_path = os.path.join(CURR_DIR_PATH, "smhi_weather_data.csv")
met_weather_data_path = os.path.join(CURR_DIR_PATH, "met_weather_data.csv")


smhi_weather_data = pd.read_csv(smhi_weather_data_path, sep=",")
met_weather_data = pd.read_csv(met_weather_data_path, sep=",")


columns = ['date', 'temperature', 'cloud_coverage', 'relative_humidity', 'wind_speed', 'location', 'source', 'day_of_week', 'hour_of_day']
smhi_weather_data.columns = columns
met_weather_data.columns = columns


weather_data = pd.concat([smhi_weather_data, met_weather_data], ignore_index=True)


weather_data.to_sql(name="weather_data", con=postgres_engine, if_exists="replace", index=False)


