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

dbutils.fs.unmount(mntPath)
dbutils.fs.mount(s3Path, mntPath)

# COMMAND ----------

display(dbutils.fs.ls(mntPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform, Store in Delta Tables
# MAGIC - Pipeline programming in utilities

# COMMAND ----------

##### CREATE DELTA TABLES FOR USER AND LOCATION FILES #####

for item in dbutils.fs.ls(mntPath):
    if item.path.endswith('data.parquet'):
        process_user_file(item.path, dailyPath)
        
for item in dbutils.fs.ls(mntPath):
    if item.path.endswith('data.parquet'):
        process_location_file(item.path, dailyPath)       

# COMMAND ----------

#### EDITED UTILITIES FUNCTIONS FOR VARIOUS UNION DELTA TABLES #####

union_user_table("citibike_user", userPath)

union_location_table("citibike_location", locatPath)

# COMMAND ----------

######## PROCESS STATION INFO FILE ############

df = recieve_data('dbfs:/mnt/data/station_info.parquet')
load_delta_table(df, nodropPath)

spark.sql(f"""
    DROP TABLE IF EXISTS station_info
    """)   

spark.sql(f"""
CREATE TABLE station_info
USING DELTA
LOCATION "{nodropPath}"
""")
