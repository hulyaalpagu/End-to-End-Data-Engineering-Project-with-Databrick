# Databricks notebook source
spark.sql("create database if not exists globalretail_gold")
spark.sql("use globalretail_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()