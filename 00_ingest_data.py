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
# MAGIC ### Load Files

# COMMAND ----------

## CREATE DELTA TABLES FOR ALL FILES
for item in dbutils.fs.ls(mntPath):
    if item.path.endswith('data.parquet'):
        process_file(item.path, dailyPath)

# COMMAND ----------

## CREATE UNION DELTA TABLE (no union option for this data source in tableau)

dfs = []
for item in dbutils.fs.ls(mntPath):
    file = item.path
    if file.endswith('data.parquet'):
        df = recieve_data(file)
        df = process_citibike_data(df)
        dfs.append(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

a = unionDF.union(dfs[0])
b = a.union(dfs[1])
print(a.count())
print(b.count())

# COMMAND ----------

from pyspark.sql.types import *

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

# COMMAND ----------

# spark.sql(f"""
# CREATE TABLE citibike_full
# USING DELTA
# LOCATION "projectPath" + "dailyPath"
# """)

# COMMAND ----------

unionDF.count()

# COMMAND ----------


