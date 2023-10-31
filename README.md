# Data Engineer ZoomCamp

### Course UI

Alternatively, you can access this course using the provided UI app, the app provides a user-friendly interface for navigating through the course material.

- Visit the following link: [DE Zoomcamp UI](https://dezoomcamp.streamlit.app/)

![dezoomcamp-ui](https://github.com/DataTalksClub/data-engineering-zoomcamp/assets/66017329/4466d2bc-3728-4fca-8e9e-b1c6be30a430)

## Syllabus

> **Note:** NYC TLC changed the format of the data we use to parquet. But you can still access
> the csv files [here](https://github.com/DataTalksClub/nyc-tlc-data).

### [Week 1: Introduction & Prerequisites](week_1_basics_n_setup)

- Course overview
- Introduction to GCP
- Docker and docker-compose
- Running Postgres locally with Docker
- Setting up infrastructure on GCP with Terraform
- Preparing the environment for the course
- Homework

### [Week 2: Workflow Orchestration](week_2_workflow_orchestration/)

- Data Lake
- Workflow orchestration
- Workflow orchestration with Mage
- Homework

### [Week 3: Data Warehouse](week_3_data_warehouse)

- Data Warehouse
- BigQuery
- Partitioning and clustering
- BigQuery best practices
- Internals of BigQuery
- Integrating BigQuery with Airflow
- BigQuery Machine Learning

### [Week 4: Analytics engineering](week_4_analytics_engineering/)

- Basics of analytics engineering
- dbt (data build tool)
- BigQuery and dbt
- Postgres and dbt
- dbt models
- Testing and documenting
- Deployment to the cloud and locally
- Visualizing the data with google data studio and metabase

### [Week 5: Batch processing](week_5_batch_processing)

- Batch processing
- What is Spark
- Spark Dataframes
- Spark SQL
- Internals: GroupBy and joins

### [Week 6: Streaming](week_6_stream_processing)

- Introduction to Kafka
- Schemas (avro)
- Kafka Streams
- Kafka Connect and KSQL

### [Week 7, 8 & 9: Project](week_7_project)

Putting everything we learned to practice

- Week 7 and 8: working on your project
- Week 9: reviewing your peers

### Technologies

- _Google Cloud Platform (GCP)_: Cloud-based auto-scaling platform by Google
  - _Google Cloud Storage (GCS)_: Data Lake
  - _BigQuery_: Data Warehouse
- _Terraform_: Infrastructure-as-Code (IaC)
- _Docker_: Containerization
- _SQL_: Data Analysis & Exploration
- _Prefect_: Workflow Orchestration
- _dbt_: Data Transformation
- _Spark_: Distributed Processing
- _Kafka_: Streaming

## Tools

For this course, you'll need to have the following software installed on your computer:

- Docker and Docker-Compose
- Python 3 (e.g. via [Anaconda](https://www.anaconda.com/products/individual))
- Google Cloud SDK
- Terraform
