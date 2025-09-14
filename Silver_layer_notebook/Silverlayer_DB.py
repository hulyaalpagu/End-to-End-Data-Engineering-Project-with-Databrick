# Databricks notebook source
spark.sql("create database if not exists globalretail_silver")

# COMMAND ----------

spark.sql("show databases").show()

# COMMAND ----------

spark.sql("use globalretail_silver")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select current_database()