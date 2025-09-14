# Databricks notebook source
spark.sql("create database if not exists globalretail_bronze")

# COMMAND ----------

spark.sql("Show databases").show()

# COMMAND ----------

spark.sql("use globalretail_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()