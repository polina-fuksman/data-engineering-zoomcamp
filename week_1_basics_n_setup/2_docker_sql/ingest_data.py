from sqlalchemy import create_engine
from time import time
import argparse
import os

import pandas as pd

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'output.parquet'

    os.system(f'wget {url} -O {parquet_name}')

    df = pd.read_parquet(parquet_name)
    csv_name = 'output.csv'
    df.to_csv(csv_name, index=False)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    taxi_jan_2024 = next(df_iter)

    taxi_jan_2024.tpep_pickup_datetime = pd.to_datetime(taxi_jan_2024.tpep_pickup_datetime)
    taxi_jan_2024.tpep_dropoff_datetime = pd.to_datetime(taxi_jan_2024.tpep_dropoff_datetime)

    taxi_jan_2024.head(n=0).to_sql(name = table_name,con=engine, if_exists='replace')

    taxi_jan_2024.to_sql(name = table_name, con=engine, if_exists='append')

    while True:
    
        t_start = time()
    
        taxi_jan_2024 = next(df_iter)
    
        taxi_jan_2024.tpep_pickup_datetime = pd.to_datetime(taxi_jan_2024.tpep_pickup_datetime)
        taxi_jan_2024.tpep_dropoff_datetime = pd.to_datetime(taxi_jan_2024.tpep_dropoff_datetime)
    
        taxi_jan_2024.to_sql(name = table_name, con=engine, if_exists='append')
    
        t_end = time()
    
        print('inserted another chunk, took %.3f seconds' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data from CSV to PostgreSQL')

    # port, batabase name, user, password, host, table name, url of the scv file

    parser.add_argument('--user', help='username for postgresql')
    parser.add_argument('--password', help='password for postgresql')
    parser.add_argument('--host', help='host for postgresql')
    parser.add_argument('--port', help='port for postgresql')
    parser.add_argument('--db', help='database name for postgresql')
    parser.add_argument('--table_name', help='name of the table to write the results')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)


