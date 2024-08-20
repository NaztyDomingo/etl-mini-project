import psycopg2 as ps

def connect_to_db():
	return ps.connect(
		dbname='miniweather_db', 
		user='nazelisvera', 
		password='12345',
		host='localhost',
		port=5432		
		)