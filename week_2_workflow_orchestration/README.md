# With Airflow

# [DE Zoomcamp 2.1.1 - Data Lake](https://www.youtube.com/watch?v=W3Zm6rjOq70&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=20)

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
