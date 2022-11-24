import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator, BigQueryDeleteTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = "strong-keyword-364715"
BUCKET = "dtc_data_lake_strong-keyword-364715"
#https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.parquet
#https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet
dataset_file = "green_tripdata_2022-01.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
#parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="gcs_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de']
) as dag:
    
    get_parqet_file_to_local = BashOperator(
        task_id = "GetDataToLocalSystem",
        bash_command = f"wget {dataset_url} -O {path_to_local_home}/{dataset_file}"
    )
    
    parquet_file_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "FileToGCS",
        src = f"{path_to_local_home}/{dataset_file}",
        dst = f"taxi_data/green/{dataset_file}",
        bucket = BUCKET
    )
    
    gcs_to_bq = BigQueryCreateExternalTableOperator(
        task_id = "CreateExternalTableInBigQuery",
        table_resource = {
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "green_taxi_table_2022"
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/taxi_data/green/{dataset_file}"],
            }
        }
    )
    
    BIGQUERY_PARTITION_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.green_taxi_part_table_2022 \
          PARTITION BY DATE(lpep_pickup_datetime) AS \
          SELECT * FROM {BIGQUERY_DATASET}.green_taxi_table_2022;"
    )
    
    create_part_table = BigQueryInsertJobOperator(
        task_id = "CreatePartitionedTable",
        configuration = {
            "query": {
               "query": BIGQUERY_PARTITION_QUERY,
               "useLegacySql": False,
            }
        }
    )
    
    del_external_table = BigQueryDeleteTableOperator(
        task_id="DeleteExternalTable",
        deletion_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.green_taxi_table_2022"
    )
    
    get_parqet_file_to_local >> parquet_file_to_gcs >> gcs_to_bq >> create_part_table >> del_external_table
    
    
