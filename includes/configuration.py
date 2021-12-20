# Databricks notebook source
# MAGIC %md
# MAGIC ####Define Data Paths
# MAGIC 
# MAGIC Practice pipelining with user-specific directories

# COMMAND ----------

# TODO
username = 'hadar'
experiment_id = 3260145608445930

# COMMAND ----------

projectPath = f"/CitiBike/{username}/data/"
s3Path = "s3a://{0}:{1}@{2}".format("AKIAVPSFD5OHVN5FJMWQ",
                                    "ZMe70t9oyebrmBgopaPNtvcdXDolAnmR3Rtsax3I", 
                                    "citibike-capstone-hadar")
mntPath     = "/mnt/data/"
dailyPath   = projectPath + "daily/"
unionPath   = projectPath + "union/"
userPath    = projectPath + "user/"
locatPath   = projectPath + "locat/"
aggPath     = projectPath + "agg/"

# COMMAND ----------

# MAGIC %md
# MAGIC **Configure Database**

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS citibike_{username}")
spark.sql(f"USE citibike_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Import Utility Functions**

# COMMAND ----------

# MAGIC %run ./utilities
