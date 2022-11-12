import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_data import main

import pyarrow.csv as pv
import pyarrow.parquet as pq

from datetime import datetime


dataset_file = "yellow_tripdata_2022-01.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DB = os.getenv('PG_DB')

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id = "LocalIngestion",
    #schedule_interval = "0 3 6 * *",
    schedule_interval = "@daily",
    default_args=default_args
) as dag:
    
    get_data = BashOperator(
        task_id = "GetDataFromSource",
        bash_command = f'curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}'
        #bash_command = f'echo "{{ execution_date.strftime(\'%Y-%m\') }}"'
    )
    
    ingest_data = PythonOperator(
        task_id="ingestDataToDB",
        python_callable=main,
        op_kwargs={
            'user':PG_USER,
            'password':PG_PASSWORD,
            'host':PG_HOST,
            'port':PG_PORT,
            'db':PG_DB,
            'table_name':"???",
            'file':f"{path_to_local_home}/{dataset_file}"
        }
    )
    
    get_data >> ingest_data
    