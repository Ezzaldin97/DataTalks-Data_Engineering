FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY yellow_tripdata_2021-01.parquet yellow_tripdata_2021-01.parquet

ENTRYPOINT [ "python" , "ingest_data.py"]