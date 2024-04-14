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
- [DE Zoomcamp 5.3.1 - First Look at Spark/PySpark](#de-zoomcamp-531---first-look-at-sparkpyspark)
  - [Reading CSV files & construct data type](#reading-csv-files--construct-data-type)
  - [Partitions](#partitions)
- [DE Zoomcamp 5.3.2 - Spark DataFrames](#de-zoomcamp-532---spark-dataframes)
  - [Actions vs Transformations](#actions-vs-transformations)
  - [Functions in Spark](#functions-in-spark)
  - [User Defined Functions (UDFs)](#user-defined-functions-udfs)
- [DE Zoomcamp 5.3.3 - (Optional) Preparing Yellow and Green Taxi Data](#de-zoomcamp-533---optional-preparing-yellow-and-green-taxi-data)
- [DE Zoomcamp 5.3.4 - SQL with Spark](#de-zoomcamp-534---sql-with-spark)
- [DE Zoomcamp 5.4.1 - Anatomy of a Spark Cluster](#de-zoomcamp-541---anatomy-of-a-spark-cluster)
  - [Spark cluster is IN CONTRAST to Hadoop/HDFS](#spark-cluster-is-in-contrast-to-hadoophdfs)
- [DE Zoomcamp 5.4.2 - GroupBy in Spark](#de-zoomcamp-542---groupby-in-spark)
  - [Explain what is Spark doing when execute a query](#explain-what-is-spark-doing-when-execute-a-query)
  - [STAGE #1 OF GROUPBY](#stage-1-of-groupby)
  - [STAGE #2 OF GROUPBY (RESHUFFLING)](#stage-2-of-groupby-reshuffling)
- [DE Zoomcamp 5.4.3 - Join in Spark](#de-zoomcamp-543---join-in-spark)
  - [Joining 2 large tables](#joining-2-large-tables)
  - [Joining a large table and a small table](#joining-a-large-table-and-a-small-table)
- [DE Zoomcamp 5.5.1 - (Optional) Operations on Spark RDDs](#de-zoomcamp-551---optional-operations-on-spark-rdds)
- [DE Zoomcamp 5.5.2 - (Optional) Spark RDD mapPartition](#de-zoomcamp-552---optional-spark-rdd-mappartition)
- [DE Zoomcamp 5.6.1 - Connecting to Google Cloud Storage](#de-zoomcamp-561---connecting-to-google-cloud-storage)

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

To access `Interface of Spark master (Spark UI)`, using port 4041 => `localhost:4041`, there are all the jobs that we executed.

# [DE Zoomcamp 5.3.1 - First Look at Spark/PySpark](https://www.youtube.com/watch?v=r_Sf6fCB40c&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=54)

We can use Spark with Python code by means of PySpark. We will be using Jupyter Notebooks for this lesson.

We first need to import PySpark to our code:

```python
import pyspark
from pyspark.sql import SparkSession
```

We now need to instantiate a `Spark session`, an object that we use to interact with Spark.

```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```

- `SparkSession` is the class of the object that we instantiate. `builder` is the builder method.
- `master()` sets the Spark _master URL_ to connect to. The `local` string means that Spark will run on a local cluster. `[*]` means that Spark will run with as many CPU cores as possible.
- `appName()` defines the name of our application/session. This will show in the Spark UI.
- `getOrCreate()` will create the session or recover the object if it was previously created.

Once we've instantiated a session, we can access the Spark UI by browsing to `localhost:4041`. The UI will display all current jobs. Since we've just created the instance, there should be no jobs currently running.

### Reading CSV files & construct data type

Similarlly to Pandas, Spark can read CSV files into `dataframes`, a tabular data structure. Unlike Pandas, Spark can handle much bigger datasets but it's unable to infer the datatypes of each column.

> Note: Spark dataframes use custom data types; we cannot use regular Python types.

For this example we will use the [High Volume For-Hire Vehicle Trip Records for January 2021](https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet) available from the [NYC TLC Trip Record Data webiste](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The file should be about 720MB in size.

```bash
# Download file by command
!wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz

# Extract the file
!gzip -f -d fhvhv_tripdata_2021-01.csv.gz

# Check rows of extracted file
!wc -l fhvhv_tripdata_2021-01.csv
```

Let's read the file and create a dataframe:

```python
df = spark.read \
    .option("header", "true") \
    .csv('fhvhv_tripdata_2021-01.csv')
```

- `read()` reads the file.
- `option()` contains options for the `read` method. In this case, we're specifying that the first line of the CSV file contains the column names.
- `csv()` is for readinc CSV files.

You can see the contents of the dataframe with `df.show()` (only a few rows will be shown) or `df.head()`. You can also check the current schema with `df.schema`; you will notice that all values are strings.

We can use a trick with Pandas to infer the datatypes:

1. Create a smaller CSV file with the first 1000 records or so.

   ```bash
   # Get the first 1001 rows of the original dataset
   !head -n 1001 fhvhv_tripdata_2021-01.csv > head.csv

   # Check rows of file
   !wc -l head.csv
   ```

2. Import Pandas and create a Pandas dataframe. This dataframe will have inferred datatypes.

   ```python
   import pandas as pd
   df_pandas = pd.read_csv("./head.csv")
   df_pandas.dtypes
   ```

3. Create a Spark dataframe from the Pandas dataframe and check its schema.

   ```python
   spark.createDataFrame(df_pandas).schema
   ```

4. Based on the output of the previous method, import `types` from `pyspark.sql` and create a `StructType` containing a list of the datatypes.

   ```python
   from pyspark.sql import types
   schema = types.StructType([...])
   ```

   - `types` contains all of the available data types for Spark dataframes.

5. Create a new Spark dataframe and include the schema as an option.

   ```python
   df = spark.read \
       .option("header", "true") \
       .schema(schema) \
       .csv('fhvhv_tripdata_2021-01.csv')
   ```

You may find an example Jupiter Notebook file using this trick [in this link](../week_5_batch/code/04_pyspark.ipynb).

### Partitions

A `Spark cluster` is composed of multiple `Executors`. Each executor can process data independently in order to parallelize and speed up work.

In the previous example we read a single large CSV file. A file can only be read by a single executor, which means that the code we've written so far isn't parallelized and thus will only be run by a single executor rather than many at the same time. The other executors will be `idle` (not do anything).
![large-file-1-executor](./images/large-file-1-executor.png)

In order to solve this issue, we can `split one large file into multiple parts` so that each executor can take care of a part and have all executors working simultaneously. These splits are called `PARTITIONS`. File 7 & File 8 are left, so when the Executor 1 is done with processing File 1, it will pick File 7 to continue processing.
![multi-files-multi-executors](./images/multi-files-multi-executors.png)

=> We will now read the CSV file, partition the dataframe and parquetize it. This will create multiple files in parquet format.

> Note: converting to parquet is an expensive operation which may take several minutes.

```python
# create 24 partitions in our dataframe
df = df.repartition(24)
# parquetize and write to fhvhv/2021/01/ folder
df.write.parquet('../../../data/fhvhv_partitions/2021/01')
```

You may check the Spark UI at any time and see the progress of the current job, which is divided into stages which contain tasks. The tasks in a stage will not start until all tasks on the previous stage are finished.

When creating a dataframe, Spark creates as many partitions as CPU cores available by default, and each partition creates a task. Thus, assuming that the dataframe was initially partitioned into 6 partitions, the `write.parquet()` method will have 2 stages: the first with 6 tasks and the second one with 24 tasks.

Besides the 24 parquet files, you should also see a `_SUCCESS` file which should be empty. This file is created when the job finishes successfully.

Trying to write the files again will output an error because Spark will not write to a non-empty folder. You can force an overwrite with the `mode` argument:

```python
df.write.parquet('../../../data/fhvhv_partitions/2021/01', mode='overwrite')
```

The opposite of partitioning (joining multiple partitions into a single partition) is called `coalescing`.

# [DE Zoomcamp 5.3.2 - Spark DataFrames](https://www.youtube.com/watch?v=ti3aC1m3rE8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=55)

Spark works with `dataframes`.

We can create a dataframe from the parquet files we created in the previous section:

```python
df = spark.read.parquet('../../../data/fhvhv_partitions/2021/01')
```

Unlike CSV files, parquet files contain the schema of the dataset, so there is no need to specify a schema like we previously did when reading the CSV file. You can check the schema like this:

```python
df.printSchema()
```

(One of the reasons why parquet files are smaller than CSV files is because they store the data according to the datatypes, so integer values will take less space than long or string values.)

There are many Pandas-like operations that we can do on Spark dataframes, such as:

- Column selection - returns a dataframe with only the specified columns.
  ```python
  new_df = df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID')
  ```
- Filtering by value - returns a dataframe whose records match the condition stated in the filter.
  ```python
  new_df = df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID').filter(df.hvfhs_license_num == 'HV0003')
  ```
- And many more. The official Spark documentation website contains a [quick guide for dataframes](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html).

### Actions vs Transformations

Some Spark methods are "lazy", meaning that they are not executed right away. You can test this with the last instructions we run in the previous section: after running them, the Spark UI will not show any new jobs. However, running `df.show()` right after will execute right away and display the contents of the dataframe; the Spark UI will also show a new job.

These lazy commands are called `transformations` and the eager commands are called `actions`. Computations only happen when `actions are triggered`.

```python
df.select(...).filter(...).show()
```

Both `select()` and `filter()` are _transformations_, but `show()` is an _action_. The whole instruction gets evaluated only when the `show()` action is triggered.

![actions-vs-transformations](./images/actions-vs-transformations.png)

- List of `Transformations` (lazy):

  - Selecting columns
  - Filtering
  - Joins
  - Group by
  - Partitions
  - ...

- List of `Actions` - eager (executed immediately):

  - Show, take, head
  - Write, read
  - ...

### Functions in Spark

Besides the SQL and Pandas-like commands we've seen so far, Spark provides additional built-in functions that allow for more complex data manipulation. By convention, these functions are imported as follows:

```python
from pyspark.sql import functions as F
```

Here's an example of built-in function usage:

```python
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```

- `withColumn()` is a `transformation` that adds a new column to the dataframe.
  - **_IMPORTANT_**: adding a new column with the same name as a previously existing column will overwrite the existing column!
- `select()` is another `transformation` that selects the stated columns.
- `F.to_date()` is a built-in Spark function that converts a timestamp to date format (year, month and day only, no hour and minute).

A list of built-in functions is available [in the official Spark documentation page](https://spark.apache.org/docs/latest/api/sql/index.html).

### User Defined Functions (UDFs)

Besides these built-in functions, Spark allows us to create **_User Defined Functions_** (UDFs) with custom behavior for those instances where creating SQL queries for that behaviour becomes difficult both to manage and test.

UDFs are regular functions which are then passed as parameters to a special builder. Let's create one:

```python
# A crazy function that changes values when they're divisible by 7 or 3
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'

# Creating the actual UDF
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
```

- `F.udf()` takes a function (`crazy_stuff()` in this example) as parameter as well as a return type for the function (a string in our example).
- While `crazy_stuff()` is obviously non-sensical, UDFs are handy for things such as ML and other complex operations for which SQL isn't suitable or desirable. Python code is also easier to test than SQL.

We can then use our UDF in transformations just like built-in functions:

```python
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```

# [DE Zoomcamp 5.3.3 - (Optional) Preparing Yellow and Green Taxi Data](https://www.youtube.com/watch?v=CI3P4tAtru4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=56)

Create a simple bat script to download the data

```bash
#!/bin/bash
set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "donwloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}

  echo "compressing ${LOCAL_PATH}"
  gzip ${LOCAL_PATH}
done
```

> ./download_data.sh yellow 2021

- The script loops through 12 months and downloads the dataset for each month for the specified taxi type; we compress the csv files to zip for saving storage (both Pandas and Spark can read zipped datasets).
- `set -e` means that the script will stop if any of the commands fail. This may happen with `wget` when we download files.
- We parametrize each part of the dataset URL. For the month, we need to convert the month numnber to 2-digits with leading zeros for single-digit months.
- `printf` is a shell built-in command available in bash and other shells which behaves very similar to C's `printf()` function. It can be used instead of `echo` for finer output control.
  - The syntax for the command is `printf [-v var] format [arguments]`
    - The `[-v var]` option is for assigning the output to a variable rather than printing it.
    - `format` is a string that may contain normal characters, backslash-escaped characters and conversion specifications for describing the format.
      - Conversion specifications follow this syntax: `%[flags][width][.precision]specifier`
    - `[arguments]` is a list of arguments of any length that will be passed to the `format` string.
  - `printf "%02d" ${MONTH}` means that the `${MONTH}` argument will be reformatted to show 2 digits with a leading 0 for single digit months.
    - `%` is the conversion specification character.
    - `0` is a flag for padding with leading zeroes.
    - `2` is a width directive; in our case, it means that the output should be of length 2.
    - `d` is the type conversion specifier for signed decimal integers.
  - You may learn more about the `printf` command [in this link](https://linuxize.com/post/bash-printf-command/).
- `mkdir -p` creates both the final directory and its parent directories if they do not exist.
- The `-O` option in `wget ${URL} -O ${LOCAL_PATH}` is for specifying the file name.
- `gzip ${LOCAL_PATH}` will compress the downloaded files to `gz` format.
  - By default, `gzip` deletes the original file after compressing it. CSV files will be removed.
  - `gz` files can be read by both Pandas and Spark.

Make the script executable with `chmod +x download_data.sh`. You may run the script with `./download_data.sh yellow 2020`. You may change yellow to `green` and `2020` to any year. For the rest of the lesson we will use the datasets for 2020 and 2021.

In order to have a look into the file, use this command

> zcat ../../../data/raw/yellow/2021/01/yellow_tripdata_2021-01.csv.gz | head -n 10

After running the script, you may check the final folder structure with `tree`, `df -h` or `du -h`.

The script can be found at [this link](./code/download_data.sh).

### Parquetize the datasets

We will use the same [Pandas trick we saw in DE Zoomcamp 5.3.2 - Spark DataFrames](#de-zoomcamp-532---spark-dataframes) to read the datasets, infer the schemas, partition the datasets and parquetize them.

The schema for yellow and green taxis will be the following:

```python
from pyspark.sql import types

green_schema = types.StructType([
  types.StructField('VendorID', types.IntegerType(), True),
  types.StructField('lpep_pickup_datetime', types.TimestampType(), True),
  types.StructField('lpep_dropoff_datetime', types.TimestampType(), True),
  types.StructField('store_and_fwd_flag', types.StringType(), True),
  types.StructField('RatecodeID', types.IntegerType(), True),
  types.StructField('PULocationID', types.IntegerType(), True),
  types.StructField('DOLocationID', types.IntegerType(), True),
  types.StructField('passenger_count', types.IntegerType(), True),
  types.StructField('trip_distance', types.DoubleType(), True),
  types.StructField('fare_amount', types.DoubleType(), True),
  types.StructField('extra', types.DoubleType(), True),
  types.StructField('mta_tax', types.DoubleType(), True),
  types.StructField('tip_amount', types.DoubleType(), True),
  types.StructField('tolls_amount', types.DoubleType(), True),
  types.StructField('ehail_fee', types.DoubleType(), True),
  types.StructField('improvement_surcharge', types.DoubleType(), True),
  types.StructField('total_amount', types.DoubleType(), True),
  types.StructField('payment_type', types.IntegerType(), True),
  types.StructField('trip_type', types.IntegerType(), True),
  types.StructField('congestion_surcharge', types.DoubleType(), True)
])

yellow_schema = types.StructType([
  types.StructField('VendorID', types.IntegerType(), True),
  types.StructField('tpep_pickup_datetime', types.TimestampType(), True),
  types.StructField('tpep_dropoff_datetime', types.TimestampType(), True),
  types.StructField('passenger_count', types.IntegerType(), True),
  types.StructField('trip_distance', types.DoubleType(), True),
  types.StructField('RatecodeID', types.IntegerType(), True),
  types.StructField('store_and_fwd_flag', types.StringType(), True),
  types.StructField('PULocationID', types.IntegerType(), True),
  types.StructField('DOLocationID', types.IntegerType(), True),
  types.StructField('payment_type', types.IntegerType(), True),
  types.StructField('fare_amount', types.DoubleType(), True),
  types.StructField('extra', types.DoubleType(), True),
  types.StructField('mta_tax', types.DoubleType(), True),
  types.StructField('tip_amount', types.DoubleType(), True),
  types.StructField('tolls_amount', types.DoubleType(), True),
  types.StructField('improvement_surcharge', types.DoubleType(), True),
  types.StructField('total_amount', types.DoubleType(), True),
  types.StructField('congestion_surcharge', types.DoubleType(), True)
])
```

The Jupyter Notebook script can be found at [this file](./code/05_taxi_schema.ipynb).

# [DE Zoomcamp 5.3.4 - SQL with Spark](https://www.youtube.com/watch?v=uAlp2VuZZPY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=57)

We already mentioned at the beginning that there are other tools for expressing batch jobs as SQL queries. However, Spark can also run SQL queries, which can come in handy if you already have a Spark cluster and setting up an additional tool for sporadic use isn't desirable.

Let's now load all of the yellow and green taxi data for 2020 and 2021 to Spark dataframes.

Assuning the parquet files for the datasets are stored on a `data/pq/color/year/month` folder structure:

```python
df_green = spark.read.parquet('../../../data/pg/green/*/*')
df_green = df_green. \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.parquet('../../../data/pg/yellow/*/*')
df_yellow = df_yellow \
        .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
```

- Because the pickup and dropoff column names don't match between the 2 datasets, we use the `withColumnRenamed` action to make them have matching names.

We will replicate the [`dm_monthyl_zone_revenue.sql`](../week_4_analytics_engineering/taxi_rides_ny/models/core/dm_monthly_zone_revenue.sql) model from `week_4_analytics_engineering` in Spark. This model makes use of `trips_data`, a combined table of yellow and green taxis, so we will create a combined dataframe with the common columns to both datasets.

We need to find out which are the common columns. We could do this:

```python
set(df_green.columns) & set(df_yellow.columns)
```

However, this command will not respect the column order. We can do this instead to respect the order:

```python
common_colums = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_colums.append(col)
```

Before we combine the datasets, we need to figure out how we will keep track of the taxi type for each record (the `service_type` field in `dm_monthyl_zone_revenue.sql`). We will add the `service_type` column to each dataframe.

```python
from pyspark.sql import functions as F

df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))
```

- `F.lit()` adds a _literal_ or constant to a dataframe. We use it here to fill the `service_type` column with a constant value, which is its corresponging taxi type.

Finally, let's combine both datasets:

```python
df_trips_data = df_green_sel.unionAll(df_yellow_sel)
```

We can also count the amount of records per taxi type:

```python
df_trips_data.groupBy('service_type').count().show()
```

We can make SQL queries with Spark with `spark.sql("SELECT * FROM ???")`. SQL expects a table for retrieving records, but a dataframe is not a table, so we need to **_register_** the dataframe as a table first:

```python
df_trips_data.registerTempTable('trips_data')
```

- This method creates a **_temporary table_** with the name `trips_data`.
- **_IMPORTANT_**: According to the [official docs](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.registerTempTable.html), this method is deprecated.

With our registered table, we can now perform regular SQL operations.

```python
spark.sql("""
SELECT
    service_type,
    count(1)
FROM
    trips_data
GROUP BY
    service_type
""").show()
```

- This query outputs the same as `df_trips_data.groupBy('service_type').count().show()`
- Note that the SQL query is wrapped with 3 double quotes (`"`).

The query output can be manipulated as a dataframe, which means that we can perform any queries on our table and manipulate the results with Python as we see fit.

We can now slightly modify the [`dm_monthyl_zone_revenue.sql`](../week_4_analytics_engineering/taxi_rides_ny/models/core/dm_monthly_zone_revenue.sql), and run it as a query with Spark and store the output in a dataframe:

```python
df_result = spark.sql("""
  SELECT
    -- Reveneue grouping
    PULocationID AS revenue_zone,
    --date_trunc('month', pickup_datetime) AS revenue_month,
    --Note: For BQ use instead: date_trunc(pickup_datetime, month) AS revenue_month,
    date_trunc('month', pickup_datetime) AS revenue_month,
    service_type,

    -- Revenue calculation
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    --SUM(ehail_fee) AS revenue_monthly_ehail_fee,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance

  FROM trips_data
  GROUP BY 1, 2, 3
""")
```

- We removed the `with` statement from the original query because it operates on an external table that Spark does not have access to.
- We removed the `count(tripid) as total_monthly_trips,` line in _Additional calculations_ because it also depends on that external table.
- We change the grouping from field names to references in order to avoid mistakes.

SQL queries are transformations, so we need an action to perform them such as `df_result.show()`.

Once we're happy with the output, we can also store it as a parquet file just like any other dataframe. We could run this:

```python
df_result.write.parquet('../../../data/report/revenue')
```

However, with our current dataset, this will create more than 200 parquet files of very small size, which isn't very desirable.

In order to reduce the amount of files, we need to reduce the amount of partitions of the dataset, which is done with the `coalesce()` method:

```python
df_result.coalesce(1).write.parquet('../../../data/report/revenue', mode='overwrite')
```

- This reduces the amount of partitions to just 1 file.

# [DE Zoomcamp 5.4.1 - Anatomy of a Spark Cluster](https://www.youtube.com/watch?v=68CipcZt7ZA&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=58)

Until now, we've used a `local cluster` to run our Spark code, but Spark clusters often contain multiple computers that behave as executors.

Spark clusters are managed by a `master`, which behaves similarly to an entry point of a Kubernetes cluster. A `driver` (an Airflow DAG, a computer running a local script, etc.) that wants to execute a Spark job will send the job to the master, which in turn will divide the work among the cluster's executors. If any executor fails and becomes offline for any reason, the master will reassign the task to another executor.

![spark-cluster](./images/spark-cluster.png)

Each executor will fetch a `dataframe partition` stored in a `Data Lake` (usually S3, GCS or a similar cloud provider), do something with it and then store it somewhere, which could be the same Data Lake or somewhere else. If there are more partitions than executors, executors will keep fetching partitions until every single one has been processed.

**_SUMMERIZE_**:

- A driver (operator in Airflow has a task does spark-submit, laptop, or something can submit a the job) submits a job to spark master.
- Master is the thing that coordinates everything. Executors are the machines that do actual computations. Master keeps track of which machines are healthy and if some machines become unhealthy, it reassign the work.
- The data is kept in cloud storage, data is read from cloud storage and written the results back to cloud storage

### Spark cluster is IN CONTRAST to [Hadoop/HDFS](https://hadoop.apache.org/)

Hadoop/HDFS is another data analytics engine, which is pretty popular. Executors locally store the data they process. Partitions in Hadoop are duplicated across several executors for redundancy, in case an executor fails for whatever reason (Hadoop is meant for clusters made of commodity hardware computers). The idea in Hadoop/HDFS is that instead of downloading data to the executors, only the code is downloaded to the executors which contain the data already.

In the past, this makes a lot of sense since the data is quite large (EX: the file is 100 MB, while the code is only 10 MB) -> make sens to send smaller thing instead of pulling a lot of data to the executors.

However, nowadays we have S3, GCP, Azure or other cloud providers. Cloud provider services & spark cluster are usually live in the same data center -> downloading 100 MB to an executor is very fast, it's `not` significantly slower compared to reading data from the local disk. Data locality has become less important as storage and data transfer costs have dramatically decreased and nowadays it's feasible to separate storage from computation.

Therefore, the executors now instead of keeping the data there, they can just pull the data from cloud storage bucket and process this and then save the results back to the data lake. => Hadoop/HDFS has fallen out of fashion.

# [DE Zoomcamp 5.4.2 - GroupBy in Spark](https://www.youtube.com/watch?v=9qrDsY_2COo&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=59)

Let's do the following query, [script 07_groupby_join.ipynb](./code/07_groupby_join.ipynb):

```python
df_green_revenue = spark.sql("""
  SELECT
    -- Reveneue grouping
    date_trunc('hour', lpep_pickup_datetime) AS hour,
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
  FROM green
  WHERE lpep_pickup_datetime >= '2020-01-01 00:00:00'
  GROUP BY 1, 2
  ORDER BY 1, 2
""")
```

### Explain what is Spark doing when execute a query

This query will output the total revenue and amount of trips per hour per zone. We need to group by hour and zones in order to do this.

The executor first does the filtering to discard data in the past, after that it does the `initial group by`. Why `initital`? Because one executor can only process one partition at a time.

Since the data is split along partitions, it's likely that we will need to group data which is in separate partitions, but executors only deal with individual partitions. Spark solves this issue by separating the grouping in 2 stages:

### STAGE #1 OF GROUPBY

In the first stage, each executor groups the results in the partition they're working on and outputs the results to a temporary partition. These temporary partitions are the `intermediate results`.

![spark-groupby-stage-1](./images/spark-groupby-stage-1.png)

### STAGE #2 OF GROUPBY (RESHUFFLING)

The second stage `shuffles` the data: Spark will put all records with the `same keys` (in this case, the `GROUP BY` keys which are hour and zone) in the `same partition`. The algorithm to do this is called `external merge sort`. Once the shuffling has finished, we can once again apply the `GROUP BY` to these new partitions and `reduce` the records to the **_final output_**.

- Note that the shuffled partitions may contain more than one key, but all records belonging to a key should end up in the same partition.

![spark-groupby-stage-2](./images/spark-groupby-stage-2.png)

Running the query should display the following DAG in the Spark UI:

![spark-dag](./images/spark-dag.png)

- The `Exchange` task refers to the shuffling.

If we were to add sorting to the query (adding a `ORDER BY 1,2` at the end), Spark would perform a very similar operation to `GROUP BY` after grouping the data. The resulting DAG would look liked this:

![spark-dag-with-orderby](./images/spark-dag-with-orderby.png)

By default, Spark will repartition the dataframe to 200 partitions after shuffling data. For the kind of data we're dealing with in this example this could be counterproductive because of the small size of each partition/file, but for larger datasets this is fine.

Shuffling is an `expensive operation`, so it's in our best interest to reduce the amount of data to shuffle when querying as least as possible.

- Keep in mind that repartitioning also involves shuffling data.

# [DE Zoomcamp 5.4.3 - Join in Spark](https://www.youtube.com/watch?v=lu7TrqAWuH4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=60)

Joining tables in Spark is implemented in a similar way to `GROUP BY` and `ORDER BY`, but there are 2 distinct cases:

- Joining 2 large tables.
- Joining a large table and a small table.

### Joining 2 large tables

Let's assume that we've created a `df_yellow_revenue` dataframe in the same manner as the `df_green_revenue` we created in the previous section. We want to join both tables, so we will create temporary dataframes with changed column names so that we can tell apart data from each original table:

```python
df_green_revenue_tmp = df_green_revenue \
    .withColumnRenamed('amount', 'green_amount') \
    .withColumnRenamed('number_records', 'green_number_records')

df_yellow_revenue_tmp = df_yellow_revenue \
    .withColumnRenamed('amount', 'yellow_amount') \
    .withColumnRenamed('number_records', 'yellow_number_records')
```

- Both of these queries are `transformations`; Spark doesn't actually do anything when we run them.

We will now perform an [outer join](https://dataschool.com/how-to-teach-people-sql/sql-join-types-explained-visually/) so that we can display the amount of trips and revenue per hour per zone for green and yellow taxis at the same time regardless of whether the hour/zone combo had one type of taxi trips or the other:

```python
df_join = df_green_revenue_tmp.join(df_yellow_revenue_tmp, on=['hour', 'zone'], how='outer')
```

- `on=` receives a list of columns by which we will join the tables. This will result in a **_primary composite key_** for the resulting table.
- `how=` specifies the type of `JOIN` to execute.

When we run either `show()` or `write()` on this query, Spark will have to create both the temporary dataframes and the join final dataframe. The DAG will look like this:

![spark-dag-join](./images/spark-dag-join.png)

Stages 1 and 2 belong to the creation of `df_green_revenue_tmp` and `df_yellow_revenue_tmp`.

For stage 3, given all records for yellow taxis `Y1, Y2, ... , Yn` and for green taxis `G1, G2, ... , Gn` and knowing that the resulting composite key is `key K = (hour H, zone Z)`, we can express the resulting complex records as `(Kn, Yn)` for yellow records and `(Kn, Gn)` for green records.

Spark will first `shuffle` the data like it did for grouping (using the `external merge sort algorithm`) and then it will `reduce` the records by joining yellow and green data for matching keys to show the final output.

![spark-join](./images/spark-join.png)

- Because we're doing an **_outer join_**, keys which only have yellow taxi or green taxi records will be shown with empty fields for the missing data, whereas keys with both types of records will show both yellow and green taxi data.
  - If we did an **_inner join_** instead, the records such as `(K1, Y1, Ø)` and `(K4, Ø, G3)` would be excluded from the final result.

### Joining a large table and a small table

> Note: this section assumes that you have run the code in [the test Jupyter Notebook](../5_batch_processing/03_test.ipynb) from the [Installing spark section](#installing-spark) and therefore have created a `zones` dataframe.

Let's now use the `zones` lookup table to match each zone ID to its corresponding name.

```python
df_zones = spark.read.parquet('zones/')

df_result = df_join.join(df_zones, df_join.zone == df_zones.LocationID)

df_result.drop('LocationID', 'zone').write.parquet('../../../data/tmp/revenue-zones')
```

- The default join type in Spark SQL is the inner join.
- Because we renamed the `LocationID` in the joint table to `zone`, we can't simply specify the columns to join and we need to provide a condition as criteria.
- We use the `drop()` method to get rid of the extra columns we don't need anymore, because we only want to keep the zone names and both `LocationID` and `zone` are duplicate columns with numeral ID's only.
- We also use `write()` instead of `show()` because `show()` might not process all of the data.

The `zones` table is actually very small and joining both tables with merge sort is unnecessary. What Spark does instead is `broadcasting`:

- Spark sends a `copy` of the small table ('zones' table in this EX) to all of the executors.
- Each executor then joins each partition of the big table in memory by performing a lookup on the local broadcasted table.

![spark-join-big-vs-small-tables](./images/spark-join-big-vs-small-tables.png)

Reshuffling isn't needed because each executor already has all of the necessary info to perform the join on each partition, thus speeding up the join operation by orders of magnitude. Because there is no reshuffling, there is just 1 stage => it's much faster.

# [DE Zoomcamp 5.5.1 - (Optional) Operations on Spark RDDs](https://www.youtube.com/watch?v=Bdu-xIrF3OM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=61)

### What are RDDs? How do they relate to dataframes?

`Resilient Distributed Datasets` (RDDs) are the main abstraction provided by Spark and consist of collection of elements partitioned accross the nodes of the cluster.

Dataframes are actually built on top of RDDs and contain a schema as well, which plain RDDs do not.

### From Dataframe to RDD

Spark dataframes contain a `rdd` field which contains the raw RDD of the dataframe. The RDD's objects used for the dataframe are called `rows`.

Let's take a look once again at the SQL query we saw in the [GROUP BY section](#group-by-in-spark):

```sql
SELECT
    date_trunc('hour', lpep_pickup_datetime) AS hour,
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
```

We can re-implement this query with RDD's instead:

1. We can re-implement the `SELECT` section by choosing the 3 fields from the RDD's rows.

   ```python
   rdd = df_green \
       .select('lpep_pickup_datetime', 'PULocationID', 'total_amount') \
       .rdd
   ```

1. We can implement the `WHERE` section by using the `filter()` and `take()` methods:

   - `filter()` returns a new RDD cointaining only the elements that satisfy a _predicate_, which in our case is a function that we pass as a parameter.
   - `take()` takes as many elements from the RDD as stated.

   ```python
   from datetime import datetime

   start = datetime(year=2020, month=1, day=1)

   def filter_outliers(row):
       return row.lpep_pickup_datetime >= start

   rdd.filter(filter_outliers).take(1)
   ```

The `GROUP BY` is more complex and makes use of special methods.

### Operations on RDDs: map, filter, reduceByKey

1. We need to generate _intermediate results_ in a very similar way to the original SQL query, so we will need to create the _composite key_ `(hour, zone)` and a _composite value_ `(amount, count)`, which are the 2 halves of each record that the executors will generate. Once we have a function that generates the record, we will use the `map()` method, which takes an RDD, transforms it with a function (our key-value function) and returns a new RDD.

![rdd-map](./images/rdd-map.png)

```python
def prepare_for_grouping(row):
    hour = row.lpep_pickup_datetime.replace(minute=0, second=0, microsecond=0)
    zone = row.PULocationID
    key = (hour, zone)

    amount = row.total_amount
    count = 1
    value = (amount, count)

    return (key, value)


rdd \
    .filter(filter_outliers) \
    .map(prepare_for_grouping) \
    .take(10)
```

2. We now need to use the `reduceByKey()` method, which will take all records with the same key and put them together in a single record by transforming all the different values according to some rules which we can define with a custom function. Since we want to count the total amount and the total number of records, we just need to add the values:

![rdd-reduceKey](./images/rdd-reduceKey.png)

```python
# we get 2 value tuples from 2 separate records as input
def calculate_revenue(left_value, right_value):
    # tuple unpacking
    left_amount, left_count = left_value
    right_amount, right_count = right_value

    output_amount = left_amount + right_amount
    output_count = left_count + right_count

    return (output_amount, output_count)

rdd \
    .filter(filter_outliers) \
    .map(prepare_for_grouping) \
    .reduceByKey(calculate_revenue)
```

1. The output we have is already usable but not very nice, so we map the output again in order to _unwrap_ it.

   ```python
   from collections import namedtuple
   RevenueRow = namedtuple('RevenueRow', ['hour', 'zone', 'revenue', 'count'])
   def unwrap(row):
       return RevenueRow(
           hour=row[0][0],
           zone=row[0][1],
           revenue=row[1][0],
           count=row[1][1]
       )

   rdd \
       .filter(filter_outliers) \
       .map(prepare_for_grouping) \
       .reduceByKey(calculate_revenue) \
       .map(unwrap)
   ```

   - Using `namedtuple` isn't necessary but it will help in the next step.

### From RDD to Dataframe

Finally, we can take the resulting RDD and convert it to a dataframe with `toDF()`. We will need to generate a schema first because we lost it when converting RDDs:

```python
from pyspark.sql import types

result_schema = types.StructType([
    types.StructField('hour', types.TimestampType(), True),
    types.StructField('zone', types.IntegerType(), True),
    types.StructField('revenue', types.DoubleType(), True),
    types.StructField('count', types.IntegerType(), True)
])

df_result = rdd \
    .filter(filter_outliers) \
    .map(prepare_for_grouping) \
    .reduceByKey(calculate_revenue) \
    .map(unwrap) \
    .toDF(result_schema)
```

- We can use `toDF()` without any schema as an input parameter, but Spark will have to figure out the schema by itself which may take a substantial amount of time. Using `namedtuple` in the previous step allows Spark to infer the column names but Spark will still need to figure out the data types; by passing a schema as a parameter we skip this step and get the output much faster.

![rdd-dag-2-stages](./images/rdd-dag-2-stages.png)

![rdd-2-stages](./images/rdd-2-stages.png)

As you can see, manipulating RDDs to perform SQL-like queries is complex and time-consuming. Ever since Spark added support for dataframes and SQL, manipulating RDDs in this fashion has become obsolete, but since dataframes are built on top of RDDs, knowing how they work can help us understand how to make better use of Spark.

# [DE Zoomcamp 5.5.2 - (Optional) Spark RDD mapPartition](https://www.youtube.com/watch?v=k3uB2K99roI&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=63)

The `mapPartitions()` function behaves similarly to `map()` in how it receives an RDD as input and transforms it into another RDD with a function that we define but it transforms partitions rather than elements. In other words: `map()` creates a new RDD by transforming every single element, whereas `mapPartitions()` transforms every partition to create a new RDD.

`mapPartitions()` is a convenient method for dealing with large datasets because it allows us to separate it into chunks that we can process more easily, which is handy for workflows such as Machine Learning.

![rdd-mapPartition](./images/rdd-mapPartition.png)

### Using `mapPartitions()` for ML

Let's demonstrate this workflow with an example. Let's assume we want to predict taxi travel length with the green taxi dataset. We will use `VendorID`, `lpep_pickup_datetime`, `PULocationID`, `DOLocationID` and `trip_distance` as our features. We will now create an RDD with these columns:

```python
columns = ['VendorID', 'lpep_pickup_datetime', 'PULocationID', 'DOLocationID', 'trip_distance']

duration_rdd = df_green \
    .select(columns) \
    .rdd
```

**_NOTE:_** The partitions are not super balanced and it not always a good thing, for instance [1141148, 436983, 433476, 292910]. As we can see the 4th partition has less rows and it will be processed quite fast, while the 1st partition takes the longest time to be processed. As a result, the other executors will have to wait so this is not a good thing. => To avoid this, we can do repartitioning (an expensive operation, which is not discussed in the scope of this section => need to learn more in practice).

```python
def apply_model_in_batch(partition):
  cnt = 0

  for row in partition:
    cnt = cnt + 1

  return [cnt]

duration_rdd.mapPartitions(apply_model_in_batch).collect()
```

Let's now create the method that `mapPartitions()` will use to transform the partitions. This method will essentially call our prediction model on the partition that we're transforming:

```python
import pandas as pd

def model_predict(df):
    # fancy ML code goes here
    (...)
    # predictions is a Pandas dataframe with the field predicted_duration in it
    return predictions

def apply_model_in_batch(rows):
    df = pd.DataFrame(rows, columns=columns)
    predictions = model_predict(df)
    df['predicted_duration'] = predictions

    for row in df.itertuples():
        yield row
```

- We're assuming that our model works with Pandas dataframes, so we need to import the library.
- We are converting the input partition into a dataframe for the model.
  - RDD's do not contain column info, so we use the `columns` param to name the columns because our model may need them.
  - Pandas will crash if the dataframe is too large for memory! We're assuming that this is not the case here, but you may have to take this into account when dealing with large partitions. You can use the [itertools package](https://docs.python.org/3/library/itertools.html) for slicing the partitions before converting them to dataframes.
- Our model will return another Pandas dataframe with a `predicted_duration` column containing the model predictions.
- `df.itertuples()` is an iterable that returns a tuple containing all the values in a single row, for all rows. Thus, `row` will contain a tuple with all the values for a single row.
- `yield` is a Python keyword that behaves similarly to `return` but returns a **_generator object_** instead of a value. This means that a function that uses `yield` can be iterated on. Spark makes use of the generator object in `mapPartitions()` to build the output RDD.
  - You can learn more about the `yield` keyword [in this link](https://realpython.com/introduction-to-python-generators/).

With our defined fuction, we are now ready to use `mapPartitions()` and run our prediction model on our full RDD:

```python
df_predicts = duration_rdd \
    .mapPartitions(apply_model_in_batch)\
    .toDF() \
    .drop('Index')

df_predicts.select('predicted_duration').show()
```

- We're not specifying the schema when creating the dataframe, so it may take some time to compute.
- We drop the `Index` field because it was created by Spark and it is not needed.

As a final thought, you may have noticed that the `apply_model_in_batch()` method does NOT operate on single elements, but rather it takes the whole partition and does something with it (in our case, calling a ML model). If you need to operate on individual elements then you're better off with `map()`.

# [DE Zoomcamp 5.6.1 - Connecting to Google Cloud Storage](https://www.youtube.com/watch?v=Yyz293hBVcQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=63)

So far we've seen how to run Spark locally and how to work with local data. In this section we will cover how to use Spark with remote data and run Spark in the cloud as well.

Google Cloud Storage is an _object store_, which means that it doesn't offer a fully featured file system. Spark can connect to remote object stores by using **_connectors_**; each object store has its own connector, so we will need to use [Google's Cloud Storage Connector](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) if we want our local Spark instance to connect to our Data Lake.

Before we do that, we will use `gsutil` to upload our local files to our Data Lake. `gsutil` is included with the GCP SDK, so you should already have it if you've followed the previous chapters.

### Uploading files to Cloud Storage with `gsutil`

Assuming you've got a bunch of parquet files you'd like to upload to Cloud Storage, run the following command to upload them:

```bash
gsutil -m cp -r <local_folder> gs://<bucket_name/destination_folder>

gsutil -m cp -r data/pg/ gs://dtc_data_lake_hpham-dtc-de/pg
```

- The `-m` option is for enabling multithreaded upload in order to speed it up.
- `cp` is for copying files.
- `-r` stands for _recursive_; it's used to state that the contents of the local folder are to be uploaded. For single files this option isn't needed.

### Configuring Spark with the GCS connector

1. **_IMPORTANT:_** Download the [Cloud Storage connector for Hadoop](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage). As the name implies, this `.jar` file is what essentially connects PySpark with GCS. The version tested for this lesson is version 2.2.21 for Hadoop 3; create a `lib` folder in your work directory and run the following command from it:

   ```bash
   gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.21.jar gcs-connector-hadoop3-2.2.21.jar
   ```

   This will download the connector to the local folder.

2. Move the `.jar` file to your Spark file directory. I installed Spark using homebrew on Mac => I had to create a `/jars` directory under directory "/usr/local/Cellar/apache-spark/3.5.0"

3. We now need to follow a few extra steps before creating the Spark session in our notebook. Import the following libraries:

   ```python
   import pyspark
   from pyspark.sql import SparkSession
   from pyspark.conf import SparkConf
   from pyspark.context import SparkContext
   ```

   Now we need to configure Spark by creating a configuration object. Run the following code to create it:

   ```python
    credentials_location = '/Users/hoang.hai.pham/.google/credentials/google_credentials.json'

    conf = SparkConf() \
      .setMaster('local[*]') \
      .setAppName('test') \
      .set("spark.jars", "/usr/local/Cellar/apache-spark/3.5.0/jars/gcs-connector-hadoop3-2.2.21.jar") \
      .set("spark.hadoop.google.cloud.auth.service.account.enable","true") \
      .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
   ```

   You may have noticed that we're including a couple of options that we previously used when creating a Spark Session with its builder. That's because we implicitly created a **_context_**, which represents a connection to a spark cluster. This time we need to explicitly create and configure the context like so:

   ```python
   sc = SparkContext(conf=conf)

   hadoop_conf = sc._jsc.hadoopConfiguration()

   hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
   hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
   hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
   hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
   ```

   What is this doing? When you see a `fs (file system)` that starts with `gs (google cloud)` -> then we need to use those implementations, which is comming from `.jar` file, and need to be used the credentials.

   This will likely output a warning when running the code. You may ignore it.

   We can now finally instantiate a Spark session:

   ```python
   spark = SparkSession.builder \
       .config(conf=sc.getConf()) \
       .getOrCreate()
   ```

### Reading the remote data

In order to read the parquet files stored in the Data Lake, you simply use the bucket URI as a parameter, like so:

```python
df_green = spark.read.parquet('gs://dtc_data_lake_hpham-dtc-de/pg/green/2020/01/part-00000-fc72b89e-f8fc-4908-9c19-50a5c59c6a30-c000.snappy.parquet')
```

You may now work with the `df_green` dataframe normally.
