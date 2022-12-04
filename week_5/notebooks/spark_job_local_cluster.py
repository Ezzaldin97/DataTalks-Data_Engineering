#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

spark = SparkSession.builder.master('spark://de-zoomcamp.europe-north1-a.c.strong-keyword-364715.internal:7077')\
                    .appName('test').getOrCreate()
zones_df = spark.read.parquet('/home/ezzaldin/data/zones')

locations_df = spark.read.parquet('/home/ezzaldin/data/taxi_tripdata/all_data_part.parquet')\
                    .select('PULocationID', 'DOLocationID', 'trip_distance')

zones_df.createOrReplaceTempView('zones')
locations_df.createOrReplaceTempView('locations')

locations_zone_distance_df = spark.sql("""
SELECT l.PULocationID as pickup_location,
       z1.Zone as pickup_zone,
       l.DOLocationID as dropoff_location,
       z2.Zone as dropoff_zone,
       l.trip_distance
FROM locations as l
INNER JOIN zones AS z1 ON z1.LocationID = l.PULocationID
INNER JOIN zones AS z2 ON z2.LocationID = l.DOLocationID
""") 

locations_zone_distance_df.repartition(8)\
                          .write.parquet('/home/ezzaldin/data/taxi_tripdata/zones_trip_distance_loc_based.parquet')