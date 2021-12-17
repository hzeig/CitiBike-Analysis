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

filepath = mntPath + "202101-citibike-tripdata.parquet"
df = (spark.read.format('parquet').load(filepath))
filepath = mntPath + "station_info.parquet"
stations = (spark.read.format('parquet').load(filepath))

# COMMAND ----------

processedDF = process_citibike_data(df)

# COMMAND ----------

processedDF.toPandas().head()

# COMMAND ----------

display(processedDF.select("startlat","startlon","endlat","endlon"))

# COMMAND ----------


