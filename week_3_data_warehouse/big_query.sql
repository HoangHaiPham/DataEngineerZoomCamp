-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-0201.trips_data_all.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_dtc-de-0201/raw/yellow_trip/yellow_tripdata_2019-*.parquet', 'gs://dtc_data_lake_dtc-de-0201/raw/yellow_trip/yellow_tripdata_2020-*.parquet']
);

-- Check yellow trip data
SELECT * except(airport_fee) FROM `dtc-de-0201.trips_data_all.external_yellow_tripdata` LIMIT 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `dtc-de-0201.trips_data_all.yellow_tripdata_non_partitioned` AS
SELECT * except(airport_fee) FROM `dtc-de-0201.trips_data_all.external_yellow_tripdata`;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `dtc-de-0201.trips_data_all.yellow_tripdata_partitioned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * except(airport_fee) FROM `dtc-de-0201.trips_data_all.external_yellow_tripdata`;

-- Impact of partition
-- Scanning 1.63 GB of data with non-partitioned table
SELECT DISTINCT(VendorID) FROM `dtc-de-0201.trips_data_all.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~106.37 MB of data with partitioned tabled
SELECT DISTINCT(VendorID) FROM `dtc-de-0201.trips_data_all.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitions
SELECT table_name, partition_id, total_rows
FROM `dtc-de-0201.trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `dtc-de-0201.trips_data_all.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * except(airport_fee) FROM `dtc-de-0201.trips_data_all.external_yellow_tripdata`;

-- Query scans 1.07 GB with partitioned table
SELECT COUNT(*) AS trips
FROM `dtc-de-0201.trips_data_all.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
AND VendorID=1;

-- Query scans 859.21 MB with partitioned & clustered table
SELECT COUNT(*) AS trips
FROM `dtc-de-0201.trips_data_all.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
AND VendorID=1;