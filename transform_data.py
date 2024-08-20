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
            'cloud_coverage': timeseries['data']['instant']['details']['cloud_area_fraction'],  # Corrected key
            'relative_humidity': timeseries['data']['instant']['details']['relative_humidity'],  # Corrected key
            'wind_speed': timeseries['data']['instant']['details']['wind_speed']  # Corrected key
        }
        rows.append(row)
    return pd.DataFrame(rows)
