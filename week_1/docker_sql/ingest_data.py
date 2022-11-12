#!/usr/bin/env python
# coding: utf-8


import os
import argparse
import pandas as pd
import numpy as np
import pyarrow.parquet as pr
from sqlalchemy import create_engine
import time


def chunkify(df: pd.DataFrame, chunk_size: int):
    start = 0
    length = df.shape[0]

    # If DF is smaller than the chunk, return the DF
    if length <= chunk_size:
        yield df[:]
        return

    # Yield individual chunks
    while start + chunk_size <= length:
        yield df[start:chunk_size + start]
        start = start + chunk_size

    # Yield the remainder chunk, if needed
    if start < length:
        yield df[start:]

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    trips = pr.read_table(os.path.join(os.getcwd(), 'yellow_tripdata_2021-01.parquet'))
    trips = trips.to_pandas()
    trips.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    trips_chunks = chunkify(trips, 100000)
    counter = 0
    while True:
        try:
            start = time.time()
            df = next(trips_chunks)
            df.to_sql(name = table_name, con = engine, if_exists = 'append')
            end = time.time()
            print(f'chunk {counter} inserted in {end - start} seconds..')
            counter+=1
        except StopIteration:
            print('all records Inserted into database successfully...')
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')

    args = parser.parse_args()

    main(args)



