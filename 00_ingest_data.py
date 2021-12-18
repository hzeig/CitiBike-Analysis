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

for item in dbutils.fs.ls(mntPath):
    if item.path.endswith('data.parquet'):
        process_file(item.path, dailyPath)

# COMMAND ----------


