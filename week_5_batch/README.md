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
- [DE Zoomcamp 5.1.2 - Introduction to Spark](#de-zoomcamp-512---introduction-to-spark)
  - [What is Spark?](#what-is-spark)
  - [Why do we need Spark?](#why-do-we-need-spark)
- [DE Zoomcamp 5.2.1 - (Optional) Installing Spark on Linux](#de-zoomcamp-521---optional-installing-spark-on-linux)

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

# [DE Zoomcamp 5.1.2 - Introduction to Spark](https://www.youtube.com/watch?v=FhaqbEOuQ8U&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=52)

### What is Spark?

[Apache Spark](https://spark.apache.org/) is an open-source `multi-language` unified analytics `engine` for large-scale data processing.

Spark is an `engine` because it _processes data_.

Example we have a database/data lake, Spark pulls the data to its machines to its executors -> then it does something with the data -> then outputs it to again data lake/data warehouse -> the processing happens in Spark -> that's why it's an `engine`.

![spark](./images/spark.png)

Spark can be ran in `clusters` with multiple `nodes`, each pulling and transforming data.

Spark is `multi-language` because we can use Java and Scala natively, and there are wrappers for Python, R and other languages.

The wrapper for Python is called [PySpark](https://spark.apache.org/docs/latest/api/python/).

Spark can deal with both `batches` and `streaming data`. The technique for streaming data is seeing a stream of data as a sequence of small batches and then applying similar techniques on them to those used on regular badges. We will cover streaming in detail in the next lesson.

### Why do we need Spark?

Spark is used for transforming data in a Data Lake. Since in Data Lake, you will have a bunch of files, then using SQL is not always easy => then you would go with Spark.

There are tools such as Hive, Presto or Athena (a AWS managed Presto) that allow you to express jobs as SQL queries on Data Lake.

However, there are times where you need to apply more complex manipulation which are very difficult or even impossible to express with SQL (such as ML models); in those instances, Spark is the tool to use.

![when-to-use-spark-1](./images/when-to-use-spark-1.png)

A typical workflow may combine both tools. Here's an example of a workflow involving Machine Learning:

![when-to-use-spark-2](./images/when-to-use-spark-2.png)

In this scenario, most of the preprocessing would be happening in Athena, so for everything that can be expressed with SQL, it's always a good idea to do so, but for everything else, there's Spark.

# [DE Zoomcamp 5.2.1 - (Optional) Installing Spark on Linux](https://www.youtube.com/watch?v=hqUbB9c8sKg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=53)

Follow this folder to install Spark based on the OS: [setup folder](./setup/)

After installing the appropiate JDK and Spark, make sure that you set up [PySpark by following these instructions](./setup/pyspark.md).

Create file `.bashrc` to set permanent environment variables

```bash
nano ~/.bash_profile

# Copy into .bash_profile file
export GOOGLE_APPLICATION_CREDENTIALS="${HOME}/.google/credentials/google_credentials.json"
export PATH="${HOME}/bin:${PATH}"

export JAVA_HOME="/usr/local/Cellar/openjdk@11/11.0.22"
export PATH="${JAVA_HOME}/bin/:${PATH}"

export SPARK_HOME="/usr/local/Cellar/apache-spark/3.5.0/libexec"
export PATH="${SPARK_HOME}/bin/:${PATH}"

export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```

Then save this file and run

```bash
source ~/.bash_profile
```

However, whenever the terminal is turned off, all the variables will be removed. In order to set it permanently, Open file `~/.zshrc` and paste this

```text
if [ -f ~/.bash_profile ]; then
  . ~/.bash_profile
fi
```

=> Save and close the file. Now, it should work.

Test your install by running this [Jupiter Notebook](./code/test-pyspark.ipynb).

To access `Interface of Spark master (Spark UI)`, using port 4040 => `localhost:4040`, there are all the jobs that we executed.
