import os
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

def main(user, password, host, port, db, table_name, file):
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    trips = pr.read_table(file)
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
