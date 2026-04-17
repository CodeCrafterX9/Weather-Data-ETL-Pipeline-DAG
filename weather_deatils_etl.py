from airflow import DAG
import psycopg2
from airflow.decorators import task
from datetime import datetime
import requests
import json

# latitude and longitude for the desired location
LATITUDE = '50.5074'
LONGITUDE = '-02.1278'
POSTGRES_CONN_ID ='postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {'owner':'airflow','start_date':datetime(2026,1,1)}

# write code for DAG 
with DAG (dag_id = 'weather_etl_pipeline', 
          default_args = default_args , 
          schedule = '@daily' , 
          catchup =False) as dags : 
    
    @task()

    def extract_weather_data (): 
        """ Extract Weather Data Using OPEN METEO API using Airflow Connection."""

        # Use HTTP Hook to get the weather Data
        url = "https://api.open-meteo.com/v1/forecast"

        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "current_weather": True
        }

        response = requests.get(url, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API failed: {response.status_code}")


    @task

    def transform_weather_data(weather_data):
        """ Transform Weather Data"""
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude' : LATITUDE,
            'longitude': LONGITUDE,
            'temperature' : current_weather["temperature"],
            'windspeed' : current_weather["windspeed"],
            'winddirection' : current_weather["winddirection"],
            'weathercode' : current_weather["weathercode"]
        }

        return transformed_data 
    
# PUSH DATA TO DATABASE
    @task
    def load_weather_data(transformed_data) : 
        """ Load Transformed data into Postgres SQL"""
        
        conn = psycopg2.connect(
        host="postgres",
        database="postgres",
        user="postgres",
        password="postgres",
        port=5432
        )


        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection VARCHAR,
            weathercode VARCHAR );"""
            )
        
        cursor.execute(
            """
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                transformed_data['latitude'],
                transformed_data['longitude'],
                transformed_data['temperature'],
                transformed_data['windspeed'],
                transformed_data['winddirection'],
                transformed_data['weathercode']
            )
        )

        conn.commit()
        cursor.close()

    # LETS CREATE DATA WORFLOW 
    # ORDER OD DAG

    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)