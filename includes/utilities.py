# Databricks notebook source
from pyspark.sql.functions import *

def recieve_data(filepath):
    df = (spark.read.format('parquet').load(filepath))
    return df

def process_citibike_data(dataframe):
    return (
    dataframe
      .select(
        (col("tripduration")/60).alias("tripduration_min"),
        to_timestamp(col("starttime")).alias("starttime"),
        to_timestamp(col("stoptime")).alias("endtime"),
        to_date(col("starttime")).alias("date"),
        dayofweek(col("starttime")).alias("day"),
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


def load_delta_table(dataframe,delta_table_path) -> bool:
    "Load a parquet file as a Delta table."
    dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy('userage').save(delta_table_path)
    return True


def process_file(filename, path):
    """
    0. check if table exists
    1. read parquet file
    2. transform columns
    3. load as delta table
    4. register table in metastore
    """

#     spark.sql(f"""
#     )
    
    table_name = filename.split('/')[-1].replace('-','')[0:6] + '_table'
    
    load_delta_table(process_citibike_data(recieve_data(filename)),path)
    
    spark.sql(f"""
    CREATE TABLE {table_name}
    USING DELTA
    LOCATION "{path}"
    """)


# COMMAND ----------


