#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


spark = SparkSession.builder\
                    .appName('dataproc')\
                    .getOrCreate()

green_df = spark.read.parquet('gs://dtc_data_lake_strong-keyword-364715/taxi_data/green/green_tripdata_2022-01.parquet')
yellow_df = spark.read.parquet('gs://dtc_data_lake_strong-keyword-364715/taxi_data/yellow/yellow_tripdata_2022-01.parquet')

# rename columns of datetime in green and yellow dataframes.
# add a column of service type.
green_df = green_df.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime').withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime').withColumn('service_type', F.lit('green'))
                   
yellow_df = yellow_df.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime').withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime').withColumn('service_type', F.lit('yellow'))

# get the same columns in both dataframes..
yellow_cols = set(yellow_df.columns)
similar_cols = [col for col in green_df.columns if col in yellow_cols]
green_df = green_df.select(similar_cols)
yellow_df = yellow_df.select(similar_cols)

green_yellow_taxi_22_df = green_df.unionAll(yellow_df)

green_yellow_taxi_22_df.repartition(4).write.parquet('gs://dtc_data_lake_strong-keyword-364715/taxi_data/proc/green_yellow_1_22.parquet')
