# Databricks notebook source
locDF = spark.table("citibike_hadar.citibike_location") 
userDF = spark.table("citibike_hadar.citibike_user") 

# COMMAND ----------

citi = locDF.join(userDF, on=['startID','endID','starttime','endtime'], how='outer')
