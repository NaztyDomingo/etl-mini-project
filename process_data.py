import os
from get_data import get_met_data, get_smhi_data
from transform_data import met_to_dataframe, smhi_to_dataframe, transforming_smhi, transforming_met
import json

CURR_PATH = os.path.dirname(os.path.realpath(__file__))
TARGET_PATH_SMHI = os.path.join(CURR_PATH, 'smhi_weather_data.json')
TARGET_PATH_MET = os.path.join(CURR_PATH, 'met_weather_data.json')
TARGET_PATH_SMHI_CSV = os.path.join( CURR_PATH, 'smhi_weather_data.csv')
TARGET_PATH_MET_CSV = os.path.join( CURR_PATH, 'met_weather_data.csv')

locations = [
    {'lat': 57.7, 'lon': 11.9}, # Göteborg
    {'lat': 55.6, 'lon': 13.0}, # Malmö
    {'lat': 59.9, 'lon': 10.7}, # Oslo
    {'lat': 59.3, 'lon': 18.0}, # Stockholm
    {'lat': 60.1, 'lon': 24.9} # Helsinki
]


def process_weather_data(locations):
    for location in locations:
        lat = location['lat']
        lon = location['lon']
        location_name = f"{lat}_{lon}"


        smhi_data = get_smhi_data(lat, lon)
        if smhi_data:
            smhi_df = smhi_to_dataframe(smhi_data)
            smhi_df['location'] = location_name
            smhi_df = transforming_smhi(smhi_df) # NEW TO TRY  
            if os.path.exists(TARGET_PATH_SMHI_CSV):
                smhi_df.to_csv(TARGET_PATH_SMHI_CSV, mode='a', header=False, index=False) 
            else:    
                smhi_df.to_csv(TARGET_PATH_SMHI_CSV, mode='w', index=False) 

       
        met_data = get_met_data(lat, lon)
        if met_data:
            met_df = met_to_dataframe(met_data)
            met_df['location'] = location_name
            met_df = transforming_met(met_df) # NEW TO TRY  
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


fetch_api_data()