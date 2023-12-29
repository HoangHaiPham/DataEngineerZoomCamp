from time import time
import pandas as pd
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
  print(table_name, csv_file, execution_date)
  
  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
  engine.connect()
  
  print('Connection established successfully, inserting data...')
  
  t_start = time()
  
  # iterator allows us to chunk the csv file into smaller dataframes
  df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

  # next() in python is a function that returns the next element in an iterator.
  df = next(df_iter)
    
  df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
  df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

  df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

  df.to_sql(name=table_name, con=engine, if_exists='append')  
  
  t_end = time()
  
  print('inserted the first chunk, took %.3f seconds' % (t_end - t_start))

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