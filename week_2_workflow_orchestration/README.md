# With Airflow

# [DE Zoomcamp 2.1.1 - Data Lake](https://www.youtube.com/watch?v=W3Zm6rjOq70&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=20)

- What is a Data Lake
- ELT vs. ETL
- Alternatives to components (S3/HDFS, Redshift, Snowflake etc.)

### What is a Data Lake?

- A Data Lake is a central repository that holds big data from many sources.
- The data in a Data Lake could either be structured, unstructured or a mix of both.
- The main goal behind a Data Lake is being able to ingest data as quickly as possible and making it available to the other team members.
- A Data Lake should be: Secure, Scalable, Able to run on inexpensive hardware

### Data Lake vs Data Warehouse

There are several differences between Data Lake & Data Warehouse:

- Data Lake (DL)
  - The data is raw and has undergone minimal processing. The data is generally unstructured.
  - It stores huge amount of data (upto petabytes).
  - Use cases: stream processing, machine learning, and real-time analytics.
- Data Warehouse (DW)
  - The data is refined; it has been cleaned, pre-processed and structured for specific use cases.
  - Data is small in comparison with DLs. Data is always preprocessed before ingestion and may be purged periodically.
  - Use cases: batch processing, BI reporting.

How did DL start when everybody already has DW?

- Companies realized the value of data.
- Store and access data quickly.
- Cannot always define structure of data.
- Usefulness of data being realized later in the project lifecycle.
- Increase in data scientists.
- R&D on data products.
- Need for Cheap storage of Big data.

### ETL vs ELT

- Export Transform and Load (ETL)
  - ETL is mainly used for a small amount of data (DW solution).
  - ETL (schema on write) means the data is transformed (preprocessed, etc), defined schema & relationships before arriving to its final destination.
- Export Load and Transform (ELT).
  - ELT is used for large amount of data (DL solution).
  - ELT (schema on read) means the data is directly stored without any transformations and any schemas are determined when reading the data.

### Gotcha of Data Lake

Data Lakes are only useful if data can be easily processed from it. However, if DL converts into Data Swamp, which makes it very hard to be useful. There are serveral reasons that convert DL into Data Swamp:

- No versioning.
- Incompatible schemas for same data without versioning.
- No metadata associated.
- Joins between different datasets are not possible.

### Cloud provider Data Lake

- GCP - cloud storage
- AWS - S3
- Azure - Azure blob

# [DE Zoomcamp 2.2.1 - Introduction to Workflow orchestration](https://www.youtube.com/watch?v=0yK7LXwYeD0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=18)

- What is an Orchestration Pipeline?
- What is a DAG?

Workflow orchestration means governing your data flow in a way that respects orchestration rules and your business logic.

Workflow orchestration helps to turn any code into a workflow that you can schedule, run & observe.

In the previous lesson we saw the definition of data pipeline and we created a pipeline script that downloaded a CSV and processed it so that we could ingest it to Postgres.

![single-data-pipeline](./images/single-data-pipeline.png)

The script we created is an example of how NOT to create a pipeline, because it contains 2 steps which could otherwise be separated (downloading and processing). The reason is that if our internet connection is slow or if we're simply testing the script, it will have to download the CSV file every single time that we run the script, which is less than ideal.

Ideally, each of these steps would be contained as separate entities, like for example 2 separate scripts. For our pipeline, that would look like this:

> (web) → DOWNLOAD → (csv) → INGEST → (Postgres)

![data-pipeline](./images/data-pipeline.png)

We have now separated our pipeline into a DOWNLOAD script and a INGEST script.

In this lesson we will create a more complex pipeline:

![data-workflow](./images/data-workflow.png)

`Parquet` is a columnar storage datafile format which is more effective way of storing data than CSV.

This Data Workflow has more steps and even branches. This type of workflow is often called a `Directed Acyclic Graph (DAG)` because it lacks any loops and the data flow is well defined.

- Directed: the jobs need to be executed in order, we know which job depends on which one.
- Acyclic: means there are no cycles, no loops.

The steps in capital letters are our jobs and the objects in between are the jobs' outputs, which behave as dependencies for other jobs. Each job may have its own set of parameters and there may also be global parameters which are the same for all of the jobs.

A Workflow Orchestration Tool allows us to define data workflows and parametrize them; it also provides additional tools such as history and logging.

The tool we will focus on in this course is `Apache Airflow`, but there are many others such as Luigi, Prefect, Argo, etc.

# [DE Zoomcamp 2.3.1 - Setup Airflow Environment with Docker-Compose](https://www.youtube.com/watch?v=lqDMzReAtrw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=19)

### Airflow Architecture

![arch-diag-airflow.pnd](./images/arch-diag-airflow.png)

Ref: https://airflow.apache.org/docs/apache-airflow/stable/concepts/overview.html

- `Web server`: GUI to inspect, trigger and debug the behaviour of DAGs and tasks. Available at http://localhost:8080.
- `Scheduler`: Responsible for scheduling jobs. Handles both triggering & scheduled workflows, submits Tasks to the executor to run, monitors all tasks and DAGs, and then triggers the task instances once their dependencies are complete.
- `Executor` handles running tasks. In a default installation, the executor runs everything inside the scheduler but most production-suitable executors push task execution out to workers.
- `Worker`: This component executes the tasks given by the scheduler.
- `Metadata database (postgres)`: Backend to the Airflow environment. Used by the scheduler, executor and webserver to store state.
- A `DAG directory`: a folder with DAG files which is read by the scheduler and the executor (an by extension by any worker the executor might have)
- Other components (seen in docker-compose services):
  - `redis`: Message broker that forwards messages from scheduler to worker.
  - `flower`: The flower app for monitoring the environment. It is available at http://localhost:5555.
  - `airflow-init`: initialization service (customized as per this design)

All these services allow you to run Airflow with CeleryExecutor.

If you want to run a lighter version of Airflow with fewer services, check this [video](https://www.youtube.com/watch?v=A1p5LQ0zzaQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb). It reduces the laptop memory usage!

### Airflow will create a folder Project Structure when running:

- `./dags` - DAG_FOLDER for DAG files (use ./dags_local for the local ingestion DAG).
- `./logs` - contains logs from task execution and scheduler.
- `./plugins` - for custom plugins.

### Workflow components

- `DAG`: Directed acyclic graph, specifies the dependencies between a set of tasks with explicit execution order, and has a beginning as well as an end. (Hence, “acyclic”)

  - DAG Structure: DAG Definition, Tasks (eg. Operators), Task Dependencies (control flow: >> or << )

- `Task`: a defined unit of work (aka, operators in Airflow). The Tasks themselves describe what to do, be it fetching data, running analysis, triggering other systems, or more. Common Types of tasks are:

  - Operators (used in this workshop) are predefined tasks. They're the most common.
  - Sensors are a subclass of operator which wait for external events to happen.
  - TaskFlow decorators (subclasses of Airflow's BaseOperator) are custom Python functions packaged as tasks.

- `DAG Run`: individual execution/run of a DAG. A run may be scheduled or triggered.

- `Task Instance`: an individual run of a single task. Task instances also have an indicative state, which could be “running”, “success”, “failed”, “skipped”, “up for retry”, etc.

  - Ideally, a task should flow from `none`, to `scheduled`, to `queued`, to `running`, and finally to `success`.

### Pre-requisites

1. For the skae of standardization across this tutorial's config:

- rename the `gcp-service-accounts-credentials` file to `google_credentials.json`.
- Store it ini `$HOME` directory.

```bash
cd ~ && mkdir -p ~/.google/credentials/
cp /Users/hoang.hai.pham/Documents/code/Tutorials/DataEngineer/data/dtc-de-0201-8eee0a0ef1ac.json ~/.google/credentials/google_credentials.json

```

2. Upgrade docker-compose version to v2.x+ & set the memory for Docker Engine to minimum 5GB (ideally 8GB). If enough memory is not allocated, it might lead to airflow-webserver continuously restarting. On Docker Desktop this can be changed in `Setting > Resources > Advanced`.

### Setup (full version)

Please follow these instructions for deploying the "full" Airflow with Docker. Instructions for a "lite" version are provided in the next section but you must follow these steps first.

1. Create a new sub-directory called `airflow` in your project dir.

2. Import the official image & setup from the latest Airflow version:

   > curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.8.0/docker-compose.yaml'

   - The official docker-compose.yaml file is quite complex and contains [several service definitions](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

3. Setup the Airflow user:

- On Linux, the quick start needs to know your host user=id and needs to have group id set to 0. Otherwise the files created in `dags`, `logs`, and `plugins` will be created with root user. You have to make sure to configure them for the docker-compose
  > mkdir -p ./dags ./logs ./plugins (for blanks setup) \
  > echoe -e "AIRFLOW_UID=$(id -u)" > .env
- For MacOS, create a new `.env` in the same folder as the docker-compose.yaml file with the content below:

4. The base Airflow Docker image won't work with GCP, so we need to [customize it](https://airflow.apache.org/docs/docker-stack/build.html) to suit our needs. You may download a GCP-ready [Airflow Dockerfile](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2022/week_2_data_ingestion/airflow/Dockerfile). A few things of note:

- Use the base Apache Airflow image as the base.
- Install the GCP SDK CLI tool so that Airflow can communicate with our GCP project.
- We also need to provide a requirements.txt file to install Python dependencies. The dependencies are:
  - `apache-airflow-providers-google` so that Airflow can use the GCP SDK.
  - `pyarrow`, a library to work with parquet files.
- You may find a modified docker-compose.yaml file in this [link](./airflow/Dockerfile).

5. Alter the `x-airflow-common` service definition inside `docker-compose.yaml` file as follows:

- We need to point to our custom Docker image. At the beginning, comment or delete the image field and uncomment the build line, or arternatively, use the following (make sure you respect YAML indentation):
  ```yaml
  build:
    context: .
    dockerfile: ./Dockerfile
  ```
- Add a volume and point it to the folder where you stored the credentials json file. Assuming you complied with the pre-requisites and moved and renamed your credentials, add the following line after all the other volumes:
  ```yaml
  - ~/.google/credentials/:/.google/credentials:ro
  ```
- Add 2 new environment variables right after the others: `GOOGLE_APPLICATION_CREDENTIALS` and `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`:
  ```yaml
  GOOGLE_APPLICATION_CREDENTIALS: /.google/credentials/google_credentials.json
  AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: "google-cloud-platform://?extra__google_cloud_platform__key_path=/.google/credentials/google_credentials.json"
  ```
- Add 2 new additional environment variables for your GCP project ID and the GCP bucket that Terraform should have created in the previous lesson. You can find this info in your GCP project's dashboard.
  ```yaml
  GCP_PROJECT_ID: "<your_gcp_project_id>"
  GCP_GCS_BUCKET: "<your_bucket_id>"
  ```
- Change the `AIRFLOW__CORE__LOAD_EXAMPLES` value to `'false'`. This will prevent Airflow from populating its interface with DAG examples.

- You may find a modified docker-compose.yaml file in this [link](./airflow/docker-compose.yaml).

6. Download files:

- [`data_ingestion_gcs_data.py`](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2022/week_2_data_ingestion/airflow/dags/data_ingestion_gcs_dag.py) into `dags` folder.
- [`data_ingestion_local.py`](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2022/week_2_data_ingestion/airflow/dags_local/data_ingestion_local.py) & [`ingest_script.py`](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2022/week_2_data_ingestion/airflow/dags_local/ingest_script.py) into `dags_local`.

7. Additional notes:
   The YAML file uses CeleryExecutor as its executor type, which means that tasks will be pushed to workers (external Docker containers) rather than running them locally (as regular processes). You can change this setting by modifying the `AIRFLOW__CORE__EXECUTOR` environment variable under the x-airflow-common environment definition.

You may now skip to the [Execution section](#execution) to deploy Airflow, or continue reading to modify your `docker-compose.yaml` file further for a less resource-intensive Airflow deployment.

### Execution

1. Build the image. It may take several minutes You only need to do this the first time you run Airflow or if you modified the Dockerfile or the `requirements.txt` file.
   ```bash
   docker-compose build
   ```
2. Initialize the Airflow scheduler, DB, and other config:
   ```bash
   docker-compose up airflow-init
   ```
3. Run Airflow
   ```bash
   docker-compose up -d
   ```
4. In another terminal, run docker-compose ps to see which containers are up & running (there should be 7, matching with the services in your docker-compose file).
5. You may now access the Airflow GUI by browsing to `localhost:8080`. Username and password are both `airflow` .
   > **_IMPORTANT_**: this is **_NOT_** a production-ready setup! The username and password for Airflow have not been modified in any way; you can find them by searching for `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` inside the `docker-compose.yaml` file.
6. On finishing your run or to shut down the container/s:

   > docker-compose down

   To stop and delete containers, delete volumes with database data, and download images, run:

   > docker-compose down --volumes --rmi all

   or

   > docker-compose down --volumes --remove-orphans
