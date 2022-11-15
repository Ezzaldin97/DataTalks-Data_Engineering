import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

#https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet
PROJECT_ID = "strong-keyword-364715"
BUCKET = "dtc_data_lake_strong-keyword-364715"
#https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.parquet
dataset_file = "yellow_tripdata_2022-01.parquet"
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
    
    gcs_to_gcs_task = GCSToGCSOperator(
    task_id='GCSToGCS',
    source_bucket=BUCKET,
    source_object=f"raw/{dataset_file}",
    destination_bucket=BUCKET,
    destination_object=f'taxi_data/yellow/{dataset_file}',
    move_object = True
    #gcp_conn_id=google_cloud_conn_id
)
    
    bq_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="BigQueryExternalTable",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "yellow_taxi_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/taxi_data/yellow/{dataset_file}"],
            },
        },
    )
    CREATE_PARTITIONED_TABLE_QUERY =( 
    f" CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.yellow_taxi_part_table \
        PARTITION BY DATE(tpep_pickup_datetime) AS \
    SELECT * FROM {BIGQUERY_DATASET}.yellow_taxi_table;"
    )
    bq_partition_table_task = BigQueryInsertJobOperator(
    task_id="InsertIntoTable",
    configuration={
        "query": {
            "query": CREATE_PARTITIONED_TABLE_QUERY,
            "useLegacySql": False,
        }
    }
)
    
    
    gcs_to_gcs_task >> bq_external_table_task >> bq_partition_table_task
    
    
    