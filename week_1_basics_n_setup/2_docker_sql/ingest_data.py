from time import time
import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def main(params):
  user = params.user
  password = params.password
  host = params.host
  port = params.port
  db = params.db
  table_name = params.table_name
  url = params.url
  
  csv_name = 'output.csv'
  
  # download the csv file. Use when dowloading file from website.
  # os.system(f"wget {url}") # Linux
  # os.system(f"curl -L -O {url}") # MAC
  # extract csv.gz file, delete the csv.gz & rename extracted file to csv_name
  # os.system("find . -name '*.csv.gz' -exec gzip -d {} + && find . -name '*.csv' -exec mv {} " + csv_name + " \;")
  
  # download the csv file. Use when downloading file from machine IP adress
  os.system(f"wget {url} -O {csv_name}") # Linux

  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
  
  # iterator allows us to chunk the csv file into smaller dataframes
  df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

  # next() in python is a function that returns the next element in an iterator.
  df = next(df_iter)
  
  df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
  df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

  df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

  df.to_sql(name=table_name, con=engine, if_exists='append')  
  
  while True:
    try:
      t_start = time()
      
      df = next(df_iter)
      
      df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
      df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
      
      df.to_sql(name=table_name, con=engine, if_exists='append')
      
      t_end = time()
      
      print('inserted another chunk, took %.3f seconds' % (t_end - t_start))
    
    except StopIteration:
      print('completed')
      break

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

  parser.add_argument('--user', help='user name for postgres')
  parser.add_argument('--password', help='password for postgres')
  parser.add_argument('--host', help='host for postgres')
  parser.add_argument('--port', help='port for postgres')
  parser.add_argument('--db', help='database name for postgres')
  parser.add_argument('--table_name', help='name of the table where we will write the results to')
  parser.add_argument('--url', help='url of the csv file')

  args = parser.parse_args()
  
  main(args)

