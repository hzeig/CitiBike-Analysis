# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

### Newbie --- edited for table-by-table use, update functions per table needs
### NOTES: separate user vs location tables for null-management reasons


########### FUNCTIONS FOR DATA PROCESSING #####################


############# RECIEVE DATA ####################

def recieve_data(filepath):
    df = (spark.read.format('parquet').load(filepath))
    return df

def recieve_user_data(filepath):
    df = (spark.read.format('parquet').load(filepath))
    df = df.withColumn("usergender", when(df.usergender == 0,"unknown") \
      .when(df.usergender == 1,"male") \
      .when(df.usergender == 2,"female"))
    df = df.na.drop()
    return df


############## SELECT AND EDIT DATA FEATURES #######################

def process_user_data(dataframe):
    return (
    dataframe
      .select(
        (col("tripduration")/60).alias("tripduration_min"),
        to_timestamp(col("starttime")).alias("starttime"),
        to_timestamp(col("stoptime")).alias("endtime"),
#         to_date(col("starttime")).alias("date"),
#         dayofweek(col("starttime")).alias("weekday_id"),
#         date_format(col("starttime"), "EEEE").alias("weekday"),
        col("startID").cast("string").alias('startID'),
#         "startlat",
#         "startlon",
        col("endID").cast("string").alias('endID'),
#         "endlat",
#         "endlon",
#         (3959 * 2 * asin(sqrt(sin((radians(col('startlat').cast("float")) - radians(col('startlat').cast("float")))/2)**2 + cos(radians(col('endlat').cast("float"))) * cos(radians(col('startlat').cast("float"))) * sin((radians(col('startlon').cast("float")) - radians(col('endlon').cast("float")))/2)**2))).alias("distance_start-end"),
#         "bikeid",
        "usertype",
        "userbirth",
        (year(col("starttime")) - col('userbirth')).alias("userage"),
        "usergender"
    )
  )

def process_location_data(dataframe):
    return (
    dataframe
      .select(
#         (col("tripduration")/60).alias("tripduration_min"),
        to_timestamp(col("starttime")).alias("starttime"),
        to_timestamp(col("stoptime")).alias("endtime"),
#         to_date(col("starttime")).alias("date"),
#         dayofweek(col("starttime")).alias("weekday_id"),
#         date_format(col("starttime"), "EEEE").alias("weekday"),
        col("startID").cast("string").alias('startID'),
        "startlat",
        "startlon",
        col("endID").cast("string").alias('endID'),
        "endlat",
        "endlon",
        (3959 * 2 * asin(sqrt(sin((radians(col('startlat').cast("float")) - radians(col('startlat').cast("float")))/2)**2 + cos(radians(col('endlat').cast("float"))) * cos(radians(col('startlat').cast("float"))) * sin((radians(col('startlon').cast("float")) - radians(col('endlon').cast("float")))/2)**2))).alias("distance_start-end"),
#         "bikeid",
#         "usertype",
#         "userbirth",
#         (year(col("starttime")) - col('userbirth')).alias("userage"),
#         "usergender"
    )
  )


############## LOAD INTO DELTA TABLE ################# 

def load_delta_table(dataframe,delta_table_path) -> bool:
    "Load a parquet file as a Delta table."
    dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy('startID').save(delta_table_path)
    return True


########### UDF FUNCTIONS ########
##### CREATE BINS FOR AGE #######
def categorizer(age):
    if age < 33:
        return "young"
    elif age < 55:
        return "mid"
    elif age < 100:
        return "senior"
    else: 
        return None


    
############## FILES INTO DATABASE ####################

def process_user_file(filename, path):
    """
    0. drop table if table exists
    1. read parquet file
    2. transform columns
    3. load as delta table
    4. register table in metastore
    """

    tablename = filename.split('/')[-1].replace('-','')[0:6] + 'user_table'
    
    df = process_user_data(recieve_user_data(filename))
    df = df.na.drop()
    agebin_udf = udf(categorizer, StringType())
    df = df.withColumn("userage_bin", agebin_udf("userage"))
    df = df.na.drop()
        
    load_delta_table(df,path)
    

def process_location_file(filename, path):
    """
    0. drop table if table exists
    1. read parquet file
    2. transform columns
    3. load as delta table
    4. register table in metastore
    """

    tablename = filename.split('/')[-1].replace('-','')[0:6] + 'location_table'

    spark.sql(f"""
    DROP TABLE IF EXISTS {tablename}
    """)   

    df = process_location_data(recieve_data(filename))
        
    load_delta_table(df,path)
    
    spark.sql(f"""
    CREATE TABLE {tablename}
    USING DELTA
    LOCATION "{path}"
    """)

############### UNION DELTA TABLE #######################
    
def union_user_table(tablename, path):
    """
    0. drop table if table exists
    1. read parquet files
    2. transform columns
    3. union into dataframe
    4. load as delta table
    5. register table in metastore
    """
            
    spark.sql(f"""
    DROP TABLE IF EXISTS {tablename}
    """)
    
    dfs = []
    for item in dbutils.fs.ls(mntPath):
        file = item.path
        if file.endswith('data.parquet'):
            df = recieve_user_data(file)
            df = process_user_data(df)
            df = df.na.drop()
            agebin_udf = udf(categorizer, StringType())
            df = df.withColumn("userage_bin", agebin_udf("userage"))
            df = df.na.drop()
            dfs.append(df)
            
    schema = StructType([
      StructField('tripduration_min', DoubleType(), True),
      StructField('starttime', TimestampType(), True),
      StructField('endtime', TimestampType(), True),
#       StructField('date', DateType(), True),
#       StructField('weekday_id', IntegerType(), True),
#       StructField('weekday', StringType(), True),
      StructField('startID', StringType(), True),
#       StructField('startlat', DoubleType(), True),
#       StructField('startlon', DoubleType(), True),
      StructField('endID', StringType(), True),
#       StructField('endlat', DoubleType(), True),
#       StructField('endlon', DoubleType(), True),
#       StructField('distance_start-end', DoubleType(), True),
#       StructField('bikeid', LongType(), True),
      StructField('usertype', StringType(), True),
      StructField('userbirth', LongType(), True),
      StructField('userage', LongType(), True),
      StructField('usergender', LongType(), True),
      StructField('userage_bin', StringType(), True)
    ])
    unionDF = spark.createDataFrame([],schema)

    for item in dfs:
        unionDF = unionDF.union(item)
        unionDF = unionDF.na.drop()
    unionDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy('startID').save(path)

    spark.sql(f"""
    CREATE TABLE {tablename}
    USING DELTA
    LOCATION "{path}"
    """)
    
def union_location_table(tablename, path):
    """
    0. drop table if table exists
    1. read parquet files
    2. transform columns
    3. union into dataframe
    4. load as delta table
    5. register table in metastore
    """
            
    spark.sql(f"""
    DROP TABLE IF EXISTS {tablename}
    """)
    
    dfs = []
    for item in dbutils.fs.ls(mntPath):
        file = item.path
        if file.endswith('data.parquet'):
            df = recieve_data(file)
            df = process_location_data(df)
            dfs.append(df)
            
    schema = StructType([
#       StructField('tripduration_min', DoubleType(), True),
      StructField('starttime', TimestampType(), True),
      StructField('endtime', TimestampType(), True),
#       StructField('date', DateType(), True),
#       StructField('weekday_id', IntegerType(), True),
#       StructField('weekday', StringType(), True),
      StructField('startID', StringType(), True),
      StructField('startlat', DoubleType(), True),
      StructField('startlon', DoubleType(), True),
      StructField('endID', StringType(), True),
      StructField('endlat', DoubleType(), True),
      StructField('endlon', DoubleType(), True),
      StructField('distance_start-end', DoubleType(), True),
    #   StructField('bikeid', LongType(), True),
#       StructField('usertype', StringType(), True),
    #   StructField('userbirth', LongType(), True),
#       StructField('userage', LongType(), True),
#       StructField('usergender', LongType(), True),
#       StructField('userage_bin', StringType(), True)
    ])
    unionDF = spark.createDataFrame([],schema)

    for item in dfs:
        unionDF = unionDF.union(item)
    unionDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy('startID').save(path)

    spark.sql(f"""
    CREATE TABLE {tablename}
    USING DELTA
    LOCATION "{path}"
    """)
