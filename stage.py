import psycopg2

def load_to_db():

	conn = psycopg2.connect('dbname=miniweather_db user=nazelisvera password=miniproject')
	cur = conn.cursor()

	cur.execute('DROP VIEWS IF EXISTS compare_sources, mondays_vs_fridays, overview')
	cur.execute('DROP TABLE IF EXISTS weather_data;')

	cur.execute('''
		CREATE TABLE IF NOT EXISTS weather_data(
		ID SERIAL PRIMARY KEY,
		date DATE,
		temperature NUMERIC(2, 1),
		cloud_coverage NUMERIC(3, 1),
		relative_humidity NUMERIC(2, 1),
		wind_speed NUMERIC(2, 1),
		location TEXT,
		source TEXT,
		day_of_week TEXT,
		hour_of_day INT
		);
	''')

	conn.commit()

	with open('smhi_weather_data.csv', 'r') as f:
		next(f)
		cur.copy_from(f, 'weather_data', sep=',', columns=('date', 'temperature', 'cloud_coverage', 'relative_humidity', 'wind_speed', 'location', 'source', 'day_of_week', 'hour_of_day'))


	conn.commit()

	cur.close()
	conn.close()
