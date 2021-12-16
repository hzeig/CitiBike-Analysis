# Databricks notebook source
# MAGIC %md
# MAGIC ## Retrieve Raw Data
# MAGIC 
# MAGIC Configuration / Utitilies Notebooks in <a href="$./includes" target="_blank">/includes/</a> folder

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Data

# COMMAND ----------

dbutils.fs.mount(s3Path, mntPath)
# dbutils.fs.unmount(mntPath)

# COMMAND ----------

display(dbutils.fs.ls(mntPath))

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

filepath = mntPath + "202101-citibike-tripdata.parquet"
df = (spark.read.format('parquet').load(filepath))
type(df), type(df.toPandas())

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import FloatType
# import numpy as np
import pandas as pd
# from numpy import cos, sin, arcsin, sqrt, radians

def process_citibike_data(dataframe):
  return (
    dataframe
      .select(
        from_unixtime("tripduration", "HH:mm:ss").alias("duration"),
        dayofweek(col("starttime")).alias("DoW"),
        to_timestamp(col("starttime")).alias("starttime"),
        to_timestamp(col("stoptime")).alias("endtime"),
        "startID",
        "endID",
        (3959 * 2 * asin(sqrt(sin((radians(col('startlat').cast("float")) - radians(col('startlat').cast("float")))/2)**2 + cos(radians(col('endlat').cast("float"))) * cos(radians(col('startlat').cast("float"))) * sin((radians(col('startlon').cast("float")) - radians(col('endlon').cast("float")))/2)**2))).alias("startend_distance"),
        "bikeid",
        "usertype",
        "userbirth",
#         year(col("userbirth")).alias("p_userbirth"),
        (year(col("starttime")) - col('userbirth')).alias("userage"),
        "usergender"
    )
  )

processedDF = process_citibike_data(df)

# COMMAND ----------

processedDF.toPandas().head()

# COMMAND ----------


