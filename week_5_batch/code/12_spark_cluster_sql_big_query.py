import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

# Run normal python script
# Hard-code the cluster URL
# spark = SparkSession.builder \
#     .master("spark://DE-C02Y61A5JGH6:7077") \
#     .appName("test") \
#     .getOrCreate()

# Run with spark-submit
# Don't hard-code the cluster URL
spark = SparkSession.builder \
    .appName("test") \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-europe-west3-262435197234-hgf0znnq')

df_green = spark.read.parquet(input_green)

df_green = df_green \
        .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.parquet(input_yellow)
df_yellow = df_yellow \
        .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# Get common columns between green and yellow dataframes
# set(df_green.columns) & set(df_yellow.columns)

# common_columns = []
# yellow_columns = set(df_yellow.columns)

# for col in df_green.columns:
#   if col in yellow_columns:
#     common_columns.append(col)

common_columns = [
  'VendorID',
  'pickup_datetime',
  'dropoff_datetime',
  'store_and_fwd_flag',
  'RatecodeID',
  'PULocationID',
  'DOLocationID',
  'passenger_count',
  'trip_distance',
  'fare_amount',
  'extra',
  'mta_tax',
  'tip_amount',
  'tolls_amount',
  'improvement_surcharge',
  'total_amount',
  'payment_type',
  'congestion_surcharge'
  ]


df_green_sel = df_green \
  .select(common_columns) \
  .withColumn('service_type', F.lit('green'))


df_yellow_sel = df_yellow \
  .select(common_columns) \
  .withColumn('service_type', F.lit('yellow'))


df_trips_data = df_green_sel.unionAll(df_yellow_sel)

# Need to let spark know that this dataframe is a table
# => register a temporary table for this dataframe
df_trips_data.registerTempTable('trips_data')

spark.sql("""
  SELECT service_type,
    count(1)
  FROM trips_data 
  GROUP BY service_type;        
""").show()


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

df_result.write.format('bigquery') \
  .option('table', output) \
  .mode('overwrite') \
  .save()