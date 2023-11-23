# [DE Zoomcamp 1.2.1 - Introduction to Docker](https://www.youtube.com/watch?v=EYNwNlOrpr0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=4)

- Why do we need Docker
- Creating a simple "data pipeline" in Docker

-it (interactive mode, i: interactive, t: terminal) => able to type something

> docker run -it ubuntu bash \
> docker run -it python:3.9

Have a bash prompt and can execute commands, e.g. _pip install pandas_

> docker run -it --entrypoint=bash python:3.9

When exit the prompt and execute the command above again -> import pandas -> there's no module named pandas -> since we run the container at the state before it's installed pandas. This particular image doesn't have pandas \

**=> Need something to make sure that the pandas library is there when we run our program => Using Dockerfile**

#### Dockerfile

```docker
# based image python ver 3.9.1
FROM python:3.9.1

# Run command to install pandas inside the container
# and it will create new image based on that
RUN pip install pandas

# Specify the working directory, which is the location in the container
# where the file will be copied to
WORKDIR /app

# Copy this file from current working directory to the docker image
# 1st argument: the name in the source on host machine
# 2nd argument: the name on the destination
COPY pipeline.py pipeline.py

# Overwrite entrypoint to get bash prompt (or execute python file)
# ENTRYPOINT ["bash"]
ENTRYPOINT ["python", "pipeline.py"]
```

Build docker image

> docker build -t <image_name>:<tag> . (. means docker searches for dockerfile in current directory and builds an image) \
> EX: docker build -t test:pandas .

Run docker image with passing arguments

> docker run -it test:pandas 2023-10-31

# [DE Zoomcamp 1.2.2 - Investing NY Taxi Data to Postgres](https://www.youtube.com/watch?v=2JM-ziJt0WI&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=5)

- Running Postgres locally with Docker
- Using `pgcli` for connecting to the database
- Exploring the NY Taxi dataset
  > [Yello trip data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) \
  > **Note:** NYC TLC changed the format of the data we use to parquet. But you can still access the csv files [here](https://github.com/DataTalksClub/nyc-tlc-data).
- Ingesting the data into the database
- Note if you have problems with `pgcli`, check [this video](https://www.youtube.com/watch?v=3IkfkTwqHx4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb) for an alternative way to connect to your database

Use Postgres for simple tests in this section. \
Run a containerized version of Postgres that doesn't require any installation steps. \
Requirements: environment variables, a volume for storing data. \
Create a folder anywhere you'd like for Postgres to store data in. \
Example folder "ny_taxi_postgres_data".

#### Run this command to configurate Postgres for the container.

```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/data/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

- Image name and tag: postgres:13
- Environment variables: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, ...
- Volumes is a way of mapping folder that we have on our host machine to a folder in the container. Postgres is a database, it needs to keep files in a file system, where the records are saved in its specific format.
- **Remember docker doesn't keep the state, therefore, next time when docker is run, the state will be lost, results in the data is also lost => In order to keep data => mapping folder on host machine to a folder in the container is necessary => this is called MOUNTING.**

#### Tag explainations

- `-e` flag for setting environments variable in docker run
- `-v` flag for volumes. Pay attention to the path, docker needs a full path in a correct format (different path on Windows, Linux, Mac).
  - <_path to the folder in the host machine_>:<_path to the folder inside the container_>
  - `pwd`: command to show full path of current location. This command will only work if run it from a directory which contains the ny_taxi_postgres_data subdirectory.
- `-p` flag for port mapping. Map a port on host machine to a port in the container

#### Install library pgcli

Install this library to use this cli to access the Postgres database and run the queries.

> pip3 install psycopg psycopg-binary psycopg2-binary
> pip3 install pgcli

In order to log in to the database, some configurations need to be set up.

> pgcli --help \
> pgcli -h localhost -p 5432 -u root -d ny_taxi \
> Type in the password, which is "root" in this case

- `-h` flag: hostname (localhost if run locally).
- `-p` flag: port.
- `-u` flag: username.
- `-d` flag: database name.

Save 1000 rows from the original file to new file

> head -n 1000 yellow_tripdate_2021-01.csv > yellow_head.csv

Count number of rows in csv file

> wc -l yellow_tripdata_2021-01.csv

#### Python

```python
import pandas as pd
df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=1000)

# Convert dataframe to DDL (Data Definition Language)
# that is used for specifying the schema
print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))

OUTPUT:

CREATE TABLE "yellow_taxi_data" (
"VendorID" INTEGER,
  "tpep_pickup_datetime" TEXT,
  "tpep_dropoff_datetime" TEXT,
  "passenger_count" INTEGER,
  "trip_distance" REAL,
  "RatecodeID" INTEGER,
  "store_and_fwd_flag" TEXT,
  "PULocationID" INTEGER,
  "DOLocationID" INTEGER,
  "payment_type" INTEGER,
  "fare_amount" REAL,
  "extra" REAL,
  "mta_tax" REAL,
  "tip_amount" REAL,
  "tolls_amount" REAL,
  "improvement_surcharge" REAL,
  "total_amount" REAL,
  "congestion_surcharge" REAL
)
```

# [DE Zoomcamp 1.2.3 - Connecting pgAdmin and Postgres](https://www.youtube.com/watch?v=hCAIVe9N0ow&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=7)

#### pgAdmin

- It's not convenient to use pgcli for data exploration and querying
- pgAdmin - the standard graphical tool for postgres for that https://www.pgadmin.org/download/pgadmin-4-container/
- Can run with docker
- Docker container can't access the postgres container => need to link them

```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```

- `-e` flag: environment variables for logging into the interface.
- `-p` flag: port mapping, map port 8080 on localhost to port 80 on the container.
- `dpage/pgadmin4` name of the image.

Load pgAdmin on a web browser by url `localhost:8080` with the same email and password used for container.

![pgAdmin-homepage](./images/pgadmin-homepage.png)

![pgAdmin-register-server-general](./images/register-server-general.png)

![pgAdmin-register-server-connection](./images/register-server-connection.png)

<span style="color: red"> Unable to connect to server.</span> Reason for that:

- We are running pgAdmin inside a container and Postgres inside another container.
- That means the pgAdmin cannot find the Postgres.

**How can we connect those two containers???**

- We need to link the Postgres and the pgAdmin together by using `network`.
- Put those two containers in the same `network` => two containers can find each other.

#### Create docker network.

```
docker network create <network-name>
EX: docker network create pg-network
```

#### Postgres for the container running with network.

```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/data/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```

#### pgAdmin for the container running with network.

```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```

- `--network`: name of docker network.
- `--name`: name for other containers to find this Postgres.

![pgAdmin-register-server-general](./images/register-server-general-2.png)

![pgAdmin-register-server-connection](./images/register-server-connection-2.png)

# [DE Zoomcamp 1.2.4 - Dockerizing the Ingestion Script](https://www.youtube.com/watch?v=B1WwATwf-vY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=9)

#### Converting the Jupyter notebook to a Python script

> jupyter nbconvert --to=script upload-data.ipynb

#### Parametrizing the script with argparse

Using `argparse` to handle the following command line arguments:

- Username
- Password
- Host
- Port
- Database name
- Table name
- URL for the CSV file

Test python the script, need to drop the table yellow_taxi_data previously created.

```bash
python3 ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
```

#### Dockerizing the ingestion script

Let's modify the Dockerfile before to include ingest_data.py script and create a new image

```bash
# based image python ver 3.9
FROM python:3.9

# Install wget to download the csv file
RUN apt-get install wget

# Run command to install pandas inside the container
# and it will create new image based on that
# psycopg2 is a postgres db adapter for python: sqlalchemy needs it
RUN pip install pandas sqlalchemy psycopg2

# Specify the working directory, which is the location in the container
# where the file will be copied to
WORKDIR /app

# Copy this file from current working directory to the docker image
# 1st argument: the name in the source on host machine
# 2nd argument: the name on the destination
COPY ingest_data.py ingest_data.py

# Overwrite entrypoint to get bash prompt (or execute python file)
# ENTRYPOINT ["bash"]
ENTRYPOINT ["python", "ingest_data.py"]
```

Build docker image

> docker build -t taxi_ingest:v001 .

Run with docker command

```bash
docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pg-database \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
```

- `-it` to stop the docker run in terminal. Otherwise it will not be interactive. \
- `host` if use `localhost` when running in docker, localhost itself has no postgresql => will be failed. Use pg-database to connect with Postgres database in the network.
- Need to run within `network`, which should be specified before image name.
- Before image name, parameters are for Docker. After image name, parameters are for application.

#### Trick to make docker run faster

Run http server with python to list all the files in the current directory

> python3 -m http.server

Check for IP address

> ifconfig / ipconfig

=> let docker download the data file `yellow_tripdata_2021-01.csv` from your IP address. By setting the URL to the local path of the file.

Change to `os.system(f"wget {url} -O {csv_name}")` in ingest_data.py

```bash
docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pg-database \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url="http://192.168.1.42:8000/yellow_tripdata_2021-01.csv"
```

# Google Cloud Platform

### [DE Zoomcamp 1.1.1 - Introduction to Google Cloud Platform](https://www.youtube.com/watch?v=18jIzE41fJ4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=3)

- Cloud computing services offered by google
- Includes a range of hosted services for compute, storage and application development that run on Google hardware
- Same hardware on which google runs its service
