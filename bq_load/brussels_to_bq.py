from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

# Google credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

#  BigQuery
client = bigquery.Client()

#  project, dataset, and table
PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_ID = os.getenv('DATASET_ID')
TABLE_ID = "brussels_raw" 

table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


csv_file_path = r"F:\DM2\data\brussels_cleaned.csv"  


job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  
    autodetect=True,      
)


with open(csv_file_path, "rb") as source_file:
    load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

load_job.result() 

print(f" Successfully loaded {load_job.output_rows} rows into {table_ref}.")
