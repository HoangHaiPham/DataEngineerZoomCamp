import os
import logging
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_script import ingest_callable

# The reason why save the downloaded file to AIRFLOW_HOME is because
# the file will be saved in the default location which is /tmp 
# all these files will be removed after the task finishes.
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

URL_PREFIX = r'https://d37ci6vzurychx.cloudfront.net/trip-data'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FILEPATH_PARQUET = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FILEPATH_CSV = FILEPATH_PARQUET.replace('.parquet', '.csv')
TABLE_NAME_TEMPLATE = 'yellow_tripdata_{{ execution_date.strftime(\'%Y_%m\') }}'

def format_parquet_to_csv(src_file, dst_file):
  if not src_file.endswith('.parquet'):
      logging.error("Can only accept source files in PARQUET format, for the moment")
      return
  data = pd.read_parquet(src_file)
  data.to_csv(dst_file, index=False)


local_workflow = DAG(
  "LocalIngestionDag",
  schedule_interval = "0 6 2 * *", # At 06:00 on day-of-month 2
  start_date = datetime(2021, 1, 1)
)

with local_workflow:
  wget_task = BashOperator(
    task_id = "wget",
    # bash_command = f"""curl -sSL {dataset_url} > {AIRFLOW_HOME}/output.csv.gz && gzip -f -d {AIRFLOW_HOME}/output.csv.gz"""  
    bash_command = f"curl -sSL {URL_TEMPLATE} > {FILEPATH_PARQUET}"
    # bash_command = 'echo "{{ (execution_date - macros.timedelta(days=30)).strftime(\'%Y-%m\') }}" {{ execution_date.strftime(\'%Y-%m\') }}'
  )
  
  format_parquet_to_csv_task = PythonOperator(
    task_id = "format_parquet_to_csv",
    python_callable = format_parquet_to_csv,
    op_kwargs={
        "src_file": f"{FILEPATH_PARQUET}",
        "dst_file": f"{FILEPATH_CSV}",
    }
  )
  
  ingest_task = PythonOperator(
    task_id = 'ingest',
    python_callable = ingest_callable,
    op_kwargs = dict(
      user = PG_USER,
      password = PG_PASSWORD,
      host = PG_HOST,
      port = PG_PORT,
      db = PG_DATABASE,
      table_name = TABLE_NAME_TEMPLATE,
      csv_file = FILEPATH_CSV,
      # don't need to pass the execution_date
      # Airflow will inject this variable if
      # we specify it in the ingest_callable function
    ),
  )
  
  wget_task >> format_parquet_to_csv_task >> ingest_task