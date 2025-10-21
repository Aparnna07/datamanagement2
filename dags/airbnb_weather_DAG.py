from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests


DBT_ACCOUNT_ID = "70471823456965"
DBT_JOB_ID = "70471823459291"
DBT_API_TOKEN = "dbtu_YyA49SMD6yD_axpgggydxo8fNHx3w9QQ5tcDl86eQt-tz4Dxo8"


def trigger_dbt_cloud_job():
    url = f"https://cloud.getdbt.com/api/v2/accounts/70471823456965/jobs/70471823459291/run/"
    headers = {
        "Authorization": f"Token {DBT_API_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "cause": "Triggered by Airflow via Cloud Composer"
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    run_id = response.json().get("data", {}).get("id")
    print(f" Triggered dbt Cloud job successfully. Run ID: {run_id}")


default_args = {
    "start_date": datetime(2024, 5, 1),
    "retries": 1
}

with DAG(
    dag_id="trigger_dbt_cloud_job",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Trigger dbt Cloud job from Airflow",
    tags=["dbt", "airbnb", "weather"],
) as dag:

    trigger_dbt = PythonOperator(
        task_id="trigger_dbt_job_in_dbt_cloud",
        python_callable=trigger_dbt_cloud_job
    )
