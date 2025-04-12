#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

credentials_location = '/home/ojekky/.gc/my-creds.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "/home/ojekky/data-engineering-zoomcamp/05-batch/code/lib/gcs-connector-hadoop2-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

df_recent = spark.read.csv(
    [
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/202[1-5]/*",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202005-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202006-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202007-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202008-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202009-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202010-capitalbikeshare-tripdata.csv"
    ], 
    header=True, 
    inferSchema=True
)

import pandas as pd

df_env = df_recent.head(100)

spark.createDataFrame(df_env).schema

from pyspark.sql import types

df_env_schema = types.StructType([
    types.StructField('ride_id', types.IntegerType(), True), 
    types.StructField('rideable_type', types.IntegerType(), True), 
    types.StructField('started_at', types.TimestampType(), True), 
    types.StructField('ended_at', types.TimestampType(), True), 
    types.StructField('start_station_name', types.StringType(), True), 
    types.StructField('start_station_id', types.IntegerType(), True), 
    types.StructField('end_station_name', types.StringType(), True), 
    types.StructField('end_station_id', types.IntegerType(), True), 
    types.StructField('start_lat', types.DoubleType(), True), 
    types.StructField('start_lng', types.DoubleType(), True), 
    types.StructField('end_lat', types.DoubleType(), True), 
    types.StructField('end_lng', types.DoubleType(), True), 
    types.StructField('member_casual', types.IntegerType(), True)
])

df_recent = spark.read.csv(
    [
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/202[1-5]/*",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202005-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202006-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202007-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202008-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202009-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202010-capitalbikeshare-tripdata.csv"
    ], 
    header = True, 
    schema = df_env_schema,
    escape='"',
    nullValue="null"
)

df_recent \
    .repartition(1) \
    .write.parquet("gs://global-rookery-448215-m8_bike_data_raw/lat_bike_paq")

df_recent.printSchema()
print(f"Number of partitions: {df_recent.rdd.getNumPartitions()}")
print(f"Total records: {df_recent.count()}")



