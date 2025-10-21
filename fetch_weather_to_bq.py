from dotenv import load_dotenv
import os
import requests
from google.cloud import bigquery
import datetime


load_dotenv()


GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
WEATHERAPI_KEY = os.getenv('WEATHERAPI_KEY')
PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_ID = os.getenv('DATASET_ID')


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS


cities = ['Barcelona', 'Brussels', 'Florence', 'Madrid']

def fetch_weather(city):
    url = f"http://api.weatherapi.com/v1/current.json?key={WEATHERAPI_KEY}&q={city}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f" Failed to fetch weather data for {city}: {response.status_code}")
        return None

def load_to_bigquery(data, city_name):
    if data is None:
        print(f" No data to load for {city_name}.")
        return

    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.live_weather_data"  

   
    try:
        table = client.get_table(table_id)
        print(f" Table {table_id} exists.")
    except Exception as e:
        print(" Table not found. Creating table...")
        
        schema = [
            bigquery.SchemaField("city_name", "STRING"),
            bigquery.SchemaField("temperature", "FLOAT"),
            bigquery.SchemaField("humidity", "INTEGER"),
            bigquery.SchemaField("weather_description", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ]

        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f" Created table {table_id}")

    
    table = client.get_table(table_id)

   
    row_to_insert = {
        "city_name": data.get("location", {}).get("name"),
        "temperature": data.get("current", {}).get("temp_c"),
        "humidity": data.get("current", {}).get("humidity"),
        "weather_description": data.get("current", {}).get("condition", {}).get("text"),
        "timestamp": datetime.datetime.fromtimestamp(
            data.get("current", {}).get("last_updated_epoch")
        ).isoformat()
    }

    errors = client.insert_rows_json(table, [row_to_insert])

    if not errors:
        print(f" Successfully inserted weather data for {city_name} into BigQuery!")
    else:
        print(f" Errors while inserting {city_name}: {errors}")

if __name__ == "__main__":
    for city in cities:
        weather_data = fetch_weather(city)
        load_to_bigquery(weather_data, city)
