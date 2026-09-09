from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.models import Variable
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import pandas as pd
import requests
import json
import random
from datetime import datetime, timedelta

POSTGRES_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG('ride_hailing_lakehouse', default_args=default_args, schedule='@hourly', catchup=False)

def generate_mock_logistics():
    AZURE_CONN_STR = Variable.get("azure_conn_str", default_var="SILAKAN_ISI_DI_UI_AIRFLOW")
    data = []
    for _ in range(250):
        status = random.choice(['COMPLETED', 'COMPLETED', 'DELAYED', 'CANCELLED', None])
        delay_mins = random.randint(15, 60) if status == 'DELAYED' else 0
        
        order = {
            "order_id": f"ORD-{random.randint(10000,99999)}",
            "driver_id": f"DRV-{random.randint(1,100)}",
            "timestamp": str(datetime.now() - timedelta(minutes=random.randint(0, 59))),
            "latitude": -6.2088 + random.uniform(-0.05, 0.05),
            "longitude": 106.8456 + random.uniform(-0.05, 0.05),
            "status": status,
            "delay_minutes": delay_mins
        }
        data.append(order)
    
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    blob_client = blob_service.get_blob_client(container="bronze", blob=f"logistics/orders_{datetime.now().strftime('%Y%m%d%H')}.json")
    blob_client.upload_blob(json.dumps(data), overwrite=True)

def fetch_weather_data():
    AZURE_CONN_STR = Variable.get("azure_conn_str", default_var="SILAKAN_ISI_DI_UI_AIRFLOW")
    url = "https://api.open-meteo.com/v1/forecast?latitude=-6.2088&longitude=106.8456&current_weather=true"
    response = requests.get(url).json()
    
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    blob_client = blob_service.get_blob_client(container="bronze", blob=f"weather/weather_{datetime.now().strftime('%Y%m%d%H')}.json")
    blob_client.upload_blob(json.dumps(response), overwrite=True)

def load_gold_to_postgres():
    AZURE_CONN_STR = Variable.get("azure_conn_str", default_var="SILAKAN_ISI_DI_UI_AIRFLOW")
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    container_client = blob_service.get_container_client("gold")
    
    blobs = list(container_client.list_blobs(name_starts_with="hourly_summary"))
    if not blobs:
        print("Belum ada data di Gold layer.")
        return
        
    latest_blob = sorted(blobs, key=lambda b: b.creation_time)[-1].name
    blob_client = blob_service.get_blob_client(container="gold", blob=latest_blob)
    
    file_path = "/tmp/gold_data.csv"
    with open(file_path, "wb") as f:
        f.write(blob_client.download_blob().readall())
        
    df = pd.read_csv(file_path)
    engine = create_engine(POSTGRES_CONN)
    df.to_sql('ride_hailing_summary', engine, if_exists='append', index=False)
    print(f"Berhasil meload {len(df)} baris ke PostgreSQL.")

task_ingest_logistics = PythonOperator(task_id='ingest_logistics', python_callable=generate_mock_logistics, dag=dag)
task_ingest_weather = PythonOperator(task_id='ingest_weather', python_callable=fetch_weather_data, dag=dag)

notebook_task = {
    'existing_cluster_id': 'JOB_ID',
    'notebook_task': {
        'notebook_path': '/Users/EMAIL/ride_hailing_transformation',
    }
}

task_run_databricks = DatabricksRunNowOperator(
    task_id='transform_silver_gold',
    databricks_conn_id='databricks_conn',
    job_id=708018447892694, 
    dag=dag
)

task_serve_postgres = PythonOperator(task_id='serve_to_postgres', python_callable=load_gold_to_postgres, dag=dag)

# DAG Dependencies
[task_ingest_logistics, task_ingest_weather] >> task_run_databricks >> task_serve_postgres
