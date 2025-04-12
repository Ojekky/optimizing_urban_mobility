#!/usr/bin/env python
# coding: utf-8

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

df_old = spark.read.csv(
    [
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/201[0-9]/*",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202001-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202002-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202003-capitalbikeshare-tripdata.csv"
    ], 
    header=True, 
    inferSchema=True
)

import pandas as pd

df_eno = df_old.head(100)

spark.createDataFrame(df_eno).schema

from pyspark.sql import types

df_eno_schema = types.StructType([
    types.StructField('Duration', types.LongType(), True), 
    types.StructField('Start date', types.TimestampType(), True), 
    types.StructField('End date', types.TimestampType(), True), 
    types.StructField('Start station number', types.IntegerType(), True), 
    types.StructField('Start station', types.StringType(), True), 
    types.StructField('End station number', types.IntegerType(), True), 
    types.StructField('End station', types.StringType(), True), 
    types.StructField('Bike number', types.IntegerType(), True), 
    types.StructField('Member type', types.IntegerType(), True)
])

df_old = spark.read.csv(
    [
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/201[0-9]/*",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202001-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202002-capitalbikeshare-tripdata.csv",
        "gs://global-rookery-448215-m8_bike_data_raw/capitalbikeshare/2020/202003-capitalbikeshare-tripdata.csv"
    ], 
    header = True, 
    schema = df_eno_schema,
    escape='"',
    nullValue="null"
)

df_old \
    .repartition(1) \
    .write.parquet("gs://global-rookery-448215-m8_bike_data_raw/old_bike_paq")

df_old.printSchema()
print(f"Number of partitions: {df_old.rdd.getNumPartitions()}")
print(f"Total records: {df_old.count()}")