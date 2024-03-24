# QUESTION 8
import os
import logging
import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
  BigQueryCreateExternalTableOperator,
  BigQueryInsertJobOperator
)

from google.cloud import storage
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

def format_to_parquet(src_file, dst_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dst_file)


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    
"""_summary_
Check if the file extension is .csv file
Then execute task 'format_to_parqute_task'
else then do nothing
"""
def branch_check_extension(src_file):
    if not src_file is None:
        return "download_csv_dataset_task"
    else:
        return "download_parquet_dataset_task"

def donwload_parquetize_upload_dag(
    dag,
    url_template,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        
        branch_check_extension_task = BranchPythonOperator(
            task_id="branch_check_extension_task",
            python_callable=branch_check_extension,
            op_kwargs={
                "src_file": local_csv_path_template,
            },
        )
        
        download_csv_dataset_task = BashOperator(
            task_id="download_csv_dataset_task",
            bash_command=f"curl -sSL {url_template} > {local_csv_path_template}"
        )
        
        download_parquet_dataset_task = BashOperator(
            task_id="download_parquet_dataset_task",
            bash_command=f"curl -sSL {url_template} > {local_parquet_path_template}"
        )
        
        extract_csv_dataset_task = BashOperator(
            task_id="extract_csv_dataset_task",
            bash_command=f"gzip -df {local_csv_path_template}"
        )
        
        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{local_csv_path_template.replace('.csv.gz', '.csv')}",
                "dst_file": local_parquet_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            trigger_rule='one_success',
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        clean_disk_task = BashOperator(
            task_id="clean_disk_task",
            bash_command=f"rm  -rf {local_csv_path_template} {local_csv_path_template.replace('.csv.gz', '.csv')} {local_parquet_path_template} || true"
        )

        branch_check_extension_task >> [download_csv_dataset_task, download_parquet_dataset_task]
        download_csv_dataset_task >> extract_csv_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> clean_disk_task
        download_parquet_dataset_task >> local_to_gcs_task >> clean_disk_task


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "retries": 1,
    "catchup": True,
    "max_active_runs": 3,
    "tags": ['dtc-de'],
}

"""_summary_
This DAG is designed to transfer FHV data to GCP bucket
"""
URL_PREFIX = r'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/'
FHV_CSV_FILE = 'fhv_tripdata_{{ data_interval_start.strftime(\'%Y-%m\') }}.csv.gz'
FHV_PARQUET_FILE = f"{FHV_CSV_FILE.replace('.csv.gz', '.parquet')}"
FHV_GCS_PATH = f"raw/fhv/{FHV_PARQUET_FILE}"

fhv = DAG(
    dag_id="data_ingestion_fhv",
    start_date=datetime.datetime(2019, 1, 1),
    end_date=datetime.datetime(2019, 12, 31),
    schedule_interval="0 6 2 * *",
    default_args=default_args,
)

donwload_parquetize_upload_dag(
    dag = fhv,
    url_template = f"{URL_PREFIX}/{FHV_CSV_FILE}",
    local_csv_path_template = f"{AIRFLOW_HOME}/{FHV_CSV_FILE}",
    local_parquet_path_template = f"{AIRFLOW_HOME}/{FHV_PARQUET_FILE}",
    gcs_path_template = FHV_GCS_PATH,
)

"""_summary_
This DAG is designed to transfer Green taxi data to GCP bucket
"""
URL_PREFIX = r'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
GREEN_TRIP_CSV_FILE = 'green_tripdata_{{ data_interval_start.strftime(\'%Y-%m\') }}.csv.gz'
GREEN_TRIP_PARQUET_FILE = f"{GREEN_TRIP_CSV_FILE.replace('.csv.gz', '.parquet')}"
GREEN_TRIP_GCS_PATH = f"raw/green_trip/{GREEN_TRIP_PARQUET_FILE}"

green_trip = DAG(
    dag_id="data_ingestion_green_taxi",
    start_date=datetime.datetime(2019, 1, 1),
    end_date=datetime.datetime(2020, 12, 31),
    schedule_interval="0 6 2 * *",
    default_args=default_args,
)

donwload_parquetize_upload_dag(
    dag = green_trip,
    url_template = f"{URL_PREFIX}/{GREEN_TRIP_CSV_FILE}",
    local_csv_path_template = f"{AIRFLOW_HOME}/{GREEN_TRIP_CSV_FILE}",
    local_parquet_path_template = f"{AIRFLOW_HOME}/{GREEN_TRIP_PARQUET_FILE}",
    gcs_path_template = GREEN_TRIP_GCS_PATH,
)

"""_summary_
  Create an External and BigQuery Table
"""
with DAG(
  dag_id="fhv_create_table",
  start_date=datetime.datetime(2020, 1, 1),
  schedule_interval="@once",
  default_args=default_args,
) as create_table:
  EXTERNAL_TABLE_NAME = f"{BIGQUERY_DATASET}.external_fhv_data_airflow"
  PARTITIONED_CLUSTERED_TABLE_NAME = f"{BIGQUERY_DATASET}.fhv_data_partitioned_clustered_airflow"
  LOCATION = "europe-west3"
  
  create_external_table = BigQueryCreateExternalTableOperator(
    task_id="create_external_table",
    destination_project_dataset_table=EXTERNAL_TABLE_NAME,
    bucket=f"{BUCKET}",
    source_objects=[f"raw/fhv/fhv_tripdata_2019-*.parquet"],
    source_format="PARQUET",
    schema_fields=[
      {"name": "dispatching_base_num", "type": "STRING", "mode": "NULLABLE"},  
      {"name": "pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},  
      {"name": "dropOff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
      {"name": "PUlocationID", "type": "INT64", "mode": "NULLABLE"},    
      {"name": "DOlocationID", "type": "INT64", "mode": "NULLABLE"},
      {"name": "SR_Flag", "type": "INT64", "mode": "NULLABLE"},    
      {"name": "Affiliated_base_number", "type": "STRING", "mode": "NULLABLE"}
    ]
  )
  
  CREATE_BQ_TABLE_QUERY = (
    f"""CREATE OR REPLACE TABLE `{PARTITIONED_CLUSTERED_TABLE_NAME}`
        PARTITION BY DATE(pickup_datetime)
        CLUSTER BY Affiliated_base_number AS
        SELECT * FROM `{EXTERNAL_TABLE_NAME}`;"""
  )

  # BigQueryExecuteQueryOperator is deprecated. 
  # Please use BigQueryInsertJobOperator instead.
  create_bq_table_task = BigQueryInsertJobOperator(
    task_id="create_bq_table_task", 
    configuration={
      "query": {
        "query": CREATE_BQ_TABLE_QUERY,
        "useLegacySql": "False",
      }
    },
    location=LOCATION
  )

  create_external_table >> create_bq_table_task
