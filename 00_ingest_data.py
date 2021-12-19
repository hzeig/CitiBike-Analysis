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

# dbutils.fs.unmount(mntPath)
# dbutils.fs.mount(s3Path, mntPath)

# COMMAND ----------

display(dbutils.fs.ls(mntPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Files

# COMMAND ----------

## CREATE DELTA TABLES FOR ALL FILES
for item in dbutils.fs.ls(mntPath):
    if item.path.endswith('data.parquet'):
        process_file(item.path, dailyPath)

# COMMAND ----------

#### EDITED UTILITIES FOR VARIOUS UNION DELTA TABLES #####
union_table("citibike_full", unionPath)

# COMMAND ----------

union_table("citibike_full_latlon", unionPath)

# COMMAND ----------

## CREATE UNION DELTA TABLE (no union option for this data source in tableau)
from pyspark.sql.types import *

dfs = []
for item in dbutils.fs.ls(mntPath):
    file = item.path
    if file.endswith('data.parquet'):
        df = recieve_data(file)
        df = process_citibike_data(df)
        df = df.na.drop()
        agebin_udf = udf(categorizer, StringType())
        df = df.withColumn("userage_bin", agebin_udf("userage"))
        dfs.append(df)

schema = StructType([
  StructField('tripduration_min', DoubleType(), True),
  StructField('starttime', TimestampType(), True),
  StructField('endtime', TimestampType(), True),
#   StructField('date', DateType(), True),
#   StructField('weekday_id', IntegerType(), True),
#   StructField('weekday', StringType(), True),
  StructField('startID', LongType(), True),
  StructField('startlat', DoubleType(), True),
  StructField('startlon', DoubleType(), True),
  StructField('endID', LongType(), True),
  StructField('endlat', DoubleType(), True),
  StructField('endlon', DoubleType(), True),
  StructField('distance_start-end', DoubleType(), True),
#   StructField('bikeid', LongType(), True),
  StructField('usertype', StringType(), True),
#   StructField('userbirth', LongType(), True),
  StructField('userage', LongType(), True),
  StructField('usergender', LongType(), True),
  StructField('userage_bin', StringType(), True)
])
unionDF = spark.createDataFrame([],schema)

for item in dfs:
    unionDF = unionDF.union(item)
unionDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy('userage_bin').save(unionPath)
spark.sql(f"""
DROP TABLE IF EXISTS citibike_full
""")

# COMMAND ----------

## CREATE UNION DELTA TABLE (no union option for this data source in tableau)
from pyspark.sql.types import *

dfs = []
for item in dbutils.fs.ls(mntPath):
    file = item.path
    if file.endswith('data.parquet'):
        df = recieve_data(file)
        df = process_citibike_data(df)
        df = df.na.drop()
        agebin_udf = udf(categorizer, StringType())
        df = df.withColumn("userage_bin", agebin_udf("userage"))
        dfs.append(df)

schema = StructType([
  StructField('tripduration_min', DoubleType(), True),
  StructField('starttime', TimestampType(), True),
  StructField('endtime', TimestampType(), True),
#   StructField('date', DateType(), True),
#   StructField('weekday_id', IntegerType(), True),
#   StructField('weekday', StringType(), True),
  StructField('startID', LongType(), True),
  StructField('startlat', DoubleType(), True),
  StructField('startlon', DoubleType(), True),
  StructField('endID', LongType(), True),
  StructField('endlat', DoubleType(), True),
  StructField('endlon', DoubleType(), True),
  StructField('distance_start-end', DoubleType(), True),
#   StructField('bikeid', LongType(), True),
  StructField('usertype', StringType(), True),
#   StructField('userbirth', LongType(), True),
  StructField('userage', LongType(), True),
  StructField('usergender', LongType(), True),
  StructField('userage_bin', StringType(), True)
])
unionDF = spark.createDataFrame([],schema)

for item in dfs:
    unionDF = unionDF.union(item)
unionDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy('userage_bin').save(unionPath)
spark.sql(f"""
DROP TABLE IF EXISTS citibike_full
""")

spark.sql(f"""
CREATE TABLE citibike_full
USING DELTA
LOCATION "{unionPath}"
""")
