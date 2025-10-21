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
TABLE_ID = os.getenv('TABLE_ID')
CITY_NAME = os.getenv('CITY_NAME')


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

def fetch_weather():
    url = f"http://api.weatherapi.com/v1/current.json?key={WEATHERAPI_KEY}&q={CITY_NAME}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f" Failed to fetch weather data: {response.status_code}")
        return None

def load_to_bigquery(data):
    if data is None:
        print(" No data to load.")
        return

    client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    
    try:
        table = client.get_table(table_ref)
        print(" Table exists.")
    except Exception as e:
        print(" Table not found. Creating table...")
        
        schema = [
            bigquery.SchemaField("city_name", "STRING"),
            bigquery.SchemaField("temperature", "FLOAT"),
            bigquery.SchemaField("humidity", "INTEGER"),
            bigquery.SchemaField("weather_description", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ]

        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f" Created table {table_ref}")

   
    table = client.get_table(table_ref)

 
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
        print(" Successfully inserted row into BigQuery!")
    else:
        print(f" Errors while inserting: {errors}")


    
if __name__ == "__main__":
    weather_data = fetch_weather()
    load_to_bigquery(weather_data)
