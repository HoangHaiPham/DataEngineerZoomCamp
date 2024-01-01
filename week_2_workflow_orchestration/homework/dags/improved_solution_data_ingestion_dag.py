import os
import logging
import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from google.cloud import storage
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = r'https://d37ci6vzurychx.cloudfront.net/trip-data'


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
        
        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": local_csv_path_template,
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
            bash_command=f"rm  -rf {local_csv_path_template} {local_parquet_path_template} || true"
        )

        branch_check_extension_task >> [download_csv_dataset_task, download_parquet_dataset_task]
        download_csv_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> clean_disk_task
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
This DAG is designed to transfer YELLOW_TAXI data to GCP bucket
"""
YELLOW_TAXI_PARQUET_FILE = 'yellow_tripdata_{{ data_interval_start.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCP_PATH = f"raw-new/yellow_trip/{YELLOW_TAXI_PARQUET_FILE}"

yellow_taxi = DAG(
    dag_id="data_ingestion_yellow_taxi",
    start_date=datetime.datetime(2019, 1, 1),
    end_date=datetime.datetime(2020, 12, 31),
    schedule_interval="0 6 2 * *",
    default_args=default_args,
)

donwload_parquetize_upload_dag(
    dag = yellow_taxi,
    url_template = f"{URL_PREFIX}/{YELLOW_TAXI_PARQUET_FILE}",
    local_csv_path_template = None,
    local_parquet_path_template = f"{AIRFLOW_HOME}/{YELLOW_TAXI_PARQUET_FILE}",
    gcs_path_template = YELLOW_TAXI_GCP_PATH,
)


"""_summary_
This DAG is designed to transfer FHV data to GCP bucket
"""
FHV_PARQUET_FILE = 'fhv_tripdata_{{ data_interval_start.strftime(\'%Y-%m\') }}.parquet'
FHV_GCS_PATH = f"raw-new/fhv/{FHV_PARQUET_FILE}"

fhv = DAG(
    dag_id="data_ingestion_fhv",
    start_date=datetime.datetime(2019, 1, 1),
    end_date=datetime.datetime(2019, 12, 31),
    schedule_interval="0 6 2 * *",
    default_args=default_args,
)

donwload_parquetize_upload_dag(
    dag = fhv,
    url_template = f"{URL_PREFIX}/{FHV_PARQUET_FILE}",
    local_csv_path_template = None,
    local_parquet_path_template = f"{AIRFLOW_HOME}/{FHV_PARQUET_FILE}",
    gcs_path_template = FHV_GCS_PATH,
)


"""_summary_
This DAG is designed to transfer ZONES data to GCP bucket
"""
ZONE_URL_PREFIX = r'https://d37ci6vzurychx.cloudfront.net/misc/'
ZONES_CSV_FILE = 'taxi+_zone_lookup.csv'
ZONES_PARQUET_FILE = f"{ZONES_CSV_FILE.replace('.csv', '.parquet')}"
ZONES_GCP_PATH = f"raw-new/zones/{ZONES_PARQUET_FILE}"
zones = DAG(
    dag_id="data_ingestion_zones",
    start_date=datetime.datetime(2019, 1, 1),
    schedule_interval="@once",
    default_args=default_args,
)

donwload_parquetize_upload_dag(
    dag = zones,
    url_template = f"{ZONE_URL_PREFIX}/{ZONES_CSV_FILE}",
    local_csv_path_template = f"{AIRFLOW_HOME}/{ZONES_CSV_FILE}",
    local_parquet_path_template = f"{AIRFLOW_HOME}/{ZONES_PARQUET_FILE}",
    gcs_path_template = ZONES_GCP_PATH,
)