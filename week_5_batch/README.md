# What will be covered in this week?

- What is batch processing?
- Tool for batch processing: Spark
- What is Spark?
- Why do we need it?
- How to install?
- Features of Spark?
- RDDs concept
- How to use Docker to run Spark? or locally?
- Running Spark in the Cloud (GCP)
- Connecting Spark to DW

# Table of contents

- [DE Zoomcamp 5.1.1 - Introduction to Batch processing](#de-zoomcamp-511---introduction-to-batch-processing)
  - [Batch vs Stream processing](#batch-vs-stream-processing)
  - [Types of batch jobs](#types-of-batch-jobs)
  - [Workflow of batch jobs](#workflow-of-batch-jobs)
  - [Pros and cons of batch jobs](#pros-and-cons-of-batch-jobs)

# [DE Zoomcamp 5.1.1 - Introduction to Batch processing](https://www.youtube.com/watch?v=dcHe5Fl3MF8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=48)

### Batch vs Stream processing

There are 2 ways of processing data:

- `Batch processing`: processing _chunks_ of data at _regular intervals_.
  - Example: processing taxi trips each month.
    ![batch](./images/batch.png)
- `Stream processing`: processing data _on the fly_.
  - Example: processing a taxi trip as soon as it's generated.
    ![stream](./images/stream.png)

This lesson will cover `batch processing`. Next lesson will cover streaming.

### Types of batch jobs

A `batch job` is a **_job_** (a unit of work) that will process data in batches.

Batch jobs may be _scheduled_ in many ways:

- Weekly
- Daily (very common)
- Hourly (very common)
- X timnes per hous
- Every 5 minutes
- Etc...

Batch jobs may also be carried out using different technologies:

- Python scripts (like the `data pipelines in lesson 1`, getting csv file and ingesting to database with monthly intervals).
  - Python scripts can be run anywhere (Kubernets, AWS Batch, ...).
- SQL (like the `dbt models in lesson 4`), using SQL to transform data.
- Spark (what we will use for this lesson).
- Flink.
- Etc...

### Workflow of batch jobs

Batch jobs are commonly orchestrated with tools such as `Airflow` (week 2).

A common workflow for batch jobs may be the following:
![workflow-batch-job](./images/workflow-batch-job.png)

### Pros and cons of batch jobs

- Advantages:
  - Easy to manage. There are multiple tools to manage them (the technologies we already mentioned)
  - Re-executable. Jobs can be easily retried if they fail.
  - Scalable. Scripts can be executed in more capable machines; Spark can be run in bigger clusters, etc.
- Disadvantages (`main disadvantage of batch processing`):
  - Delay. Each task of the workflow in the previous section may take a few minutes; assuming the whole workflow takes 20 minutes, we would need to wait those 20 minutes until the data is ready for work.

However, the advantages of `batch jobs` often compensate for its shortcomings, and as a result most companies that deal with data tend to work with batch jobs mos of the time (probably 80% for batch, 20% for stream).
