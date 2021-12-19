# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

def recieve_data(filepath):
    df = (spark.read.format('parquet').load(filepath))
    df = df.withColumn("usergender", when(df.usergender == 0,"unknown") \
      .when(df.usergender == 1,"male") \
      .when(df.usergender == 2,"female"))
    df = df.na.drop()
    return df

def process_citibike_data(dataframe):
    return (
    dataframe
      .select(
        (col("tripduration")/60).alias("tripduration_min"),
        to_timestamp(col("starttime")).alias("starttime"),
        to_timestamp(col("stoptime")).alias("endtime"),
        to_date(col("starttime")).alias("date"),
        dayofweek(col("starttime")).alias("weekday_id"),
        date_format(col("starttime"), "EEEE").alias("weekday"),
        "startID",
#         "startlat",
#         "startlon",
        "endID",
#         "endlat",
#         "endlon",
        (3959 * 2 * asin(sqrt(sin((radians(col('startlat').cast("float")) - radians(col('startlat').cast("float")))/2)**2 + cos(radians(col('endlat').cast("float"))) * cos(radians(col('startlat').cast("float"))) * sin((radians(col('startlon').cast("float")) - radians(col('endlon').cast("float")))/2)**2))).alias("distance_start-end"),
#         "bikeid",
        "usertype",
        "userbirth",
        (year(col("starttime")) - col('userbirth')).alias("userage"),
        "usergender"
    )
  )


def load_delta_table(dataframe,delta_table_path) -> bool:
    "Load a parquet file as a Delta table."
    dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy('userage_bin').save(delta_table_path)
    return True


## CREATE BINS FOR AGE
def categorizer(age):
    if age < 33:
        return "young"
    elif age < 55:
        return "mid"
    elif age < 100:
        return "senior"
    else: 
        return "false"



def process_file(filename, path):
    """
    0. drop table if table exists
    1. read parquet file
    2. transform columns
    3. load as delta table
    4. register table in metastore
    """

    tablename = filename.split('/')[-1].replace('-','')[0:6] + '_table'

    spark.sql(f"""
    DROP TABLE IF EXISTS {tablename}
    """)   

    
    df = process_citibike_data(recieve_data(filename))
    agebin_udf = udf(categorizer, StringType())
    df = df.withColumn("userage_bin", agebin_udf("userage"))

    #### LAT-LON REGION CATEGORY COLUMN ####
    
    
    load_delta_table(df,path)
    
    spark.sql(f"""
    CREATE TABLE {tablename}
    USING DELTA
    LOCATION "{path}"
    """)

    
def uniondeltatable(tablename, path):
    """
    0. drop table if table exists
    1. read parquet file
    2. transform columns
    3. load as delta table
    4. register table in metastore
    """
            
    spark.sql(f"""
    DROP TABLE IF EXISTS {tablename}
    """)
    
    dfs = []
    for item in dbutils.fs.ls(mntPath):
        file = item.path
        if file.endswith('data.parquet'):
            df = recieve_data(file)
            df = process_citibike_data(df)
            dfs.append(df)

            
    schema = StructType([
      StructField('tripduration_min', DoubleType(), True),
      StructField('starttime', TimestampType(), True),
      StructField('endtime', TimestampType(), True),
      StructField('date', DateType(), True),
      StructField('day', IntegerType(), True),
      StructField('startID', LongType(), True),
      StructField('startlat', DoubleType(), True),
      StructField('startlon', DoubleType(), True),
      StructField('endID', LongType(), True),
      StructField('endlat', DoubleType(), True),
      StructField('endlon', DoubleType(), True),
      StructField('distance_start-end', DoubleType(), True),
      StructField('bikeid', LongType(), True),
      StructField('usertype', StringType(), True),
      StructField('userbirth', LongType(), True),
      StructField('userage', LongType(), True),
      StructField('usergender', LongType(), True)
    ])

    unionDF = spark.createDataFrame([],schema)
    for item in dfs:
        unionDF = unionDF.union(item)
    
    unionDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy('userage_bin').save(unionPath)

    spark.sql(f"""
    CREATE TABLE {tablename}
    USING DELTA
    LOCATION {path}
    """)
