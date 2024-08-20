import pandas as pd


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