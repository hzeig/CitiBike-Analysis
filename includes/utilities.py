# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType

def process_citibike_data(dataframe):
  return (
    dataframe
      .select(
        (col("tripduration")/60).alias("tripduration_min"),
        to_timestamp(col("starttime")).alias("starttime"),
        to_timestamp(col("stoptime")).alias("endtime"),
        to_date(col("starttime")).alias("date"),
        "startID",
        "startlat",
        "startlon",
        "endID",
        "endlat",
        "endlon",
        (3959 * 2 * asin(sqrt(sin((radians(col('startlat').cast("float")) - radians(col('startlat').cast("float")))/2)**2 + cos(radians(col('endlat').cast("float"))) * cos(radians(col('startlat').cast("float"))) * sin((radians(col('startlon').cast("float")) - radians(col('endlon').cast("float")))/2)**2))).alias("distance_start-end"),
        "bikeid",
        "usertype",
        "userbirth",
        (year(col("starttime")) - col('userbirth')).alias("userage"),
        "usergender"
    )
  )
