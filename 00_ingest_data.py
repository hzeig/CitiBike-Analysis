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
        
pd.concat(dfs)

spark.sql(f"""
CREATE TABLE citibike_full
USING DELTA
LOCATION "dailyPath"
""")
